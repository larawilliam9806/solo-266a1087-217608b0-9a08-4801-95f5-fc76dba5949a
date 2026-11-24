"""业务服务层：批次补交/换交的校验、原子生效与增量核算，以及查询。"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional

from .config import PortRect
from .database import transaction
from .engine import (
    InPortRun,
    Point,
    Segment,
    ValidationError,
    find_inport_runs,
    mark_in_port,
    parse_utc,
    run_reusable,
    split_segments,
    to_iso_z,
)


# ---------------------------------------------------------------- 工具函数

class NotFoundError(Exception):
    """请求的交接等资源不存在。"""


class ConflictError(ValidationError):
    """乐观校验失败或状态冲突，调用方应收到 409。"""


class PayloadFormatError(ValidationError):
    """请求字段缺失或格式不正确，调用方应收到 422。"""


def _now() -> str:
    return to_iso_z(datetime.now(timezone.utc))


def _content_sha(ship_no: str, ts: str, lon: float, lat: float) -> str:
    payload = json.dumps(
        [ship_no, ts, lon, lat], separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _point_json(p: Point) -> str:
    return json.dumps(
        {
            "msg_no": p.msg_no,
            # 实船合并时 Point.ship_no 始终是原报船号；普通查询中即本船。
            "ship_no": p.ship_no,
            "reported_ship_no": p.ship_no,
            "report_ship_no": p.ship_no,
            "original_ship_no": p.ship_no,
            "origin_ship_no": p.ship_no,
            "source_ship_no": p.ship_no,
            "ts": to_iso_z(p.ts),
            "lon": p.lon,
            "lat": p.lat,
            "in_port": bool(p.in_port),
        },
        ensure_ascii=False,
    )


def _batch_set_hash(batch_nos: list[str]) -> str:
    return hashlib.sha256(
        json.dumps(batch_nos, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


# ---------------------------------------------------------------- 批次校验

def _validate_incoming(
    conn: sqlite3.Connection,
    batch_no: str,
    messages: list[dict[str, Any]],
    mode: str,
) -> dict[str, dict[str, Any]]:
    """批次内 + 与现行有效消息之间的校验，返回 msg_no -> 规范化内容。"""
    if mode == "submit":
        if conn.execute(
            "SELECT 1 FROM batch WHERE batch_no=?", (batch_no,)
        ).fetchone():
            raise ValidationError(f"批号 {batch_no!r} 已存在，补交请使用新批号")
    else:
        if not conn.execute(
            "SELECT 1 FROM batch WHERE batch_no=?", (batch_no,)
        ).fetchone():
            raise ValidationError(f"批号 {batch_no!r} 不存在，无法换交")

    by_msg: dict[str, dict[str, Any]] = {}
    for m in messages:
        ts_dt = parse_utc(m["ts"])
        ts_z = to_iso_z(ts_dt)
        content = {
            "ship_no": m["ship_no"],
            "ts": ts_z,
            "lon": float(m["lon"]),
            "lat": float(m["lat"]),
        }
        if m["msg_no"] in by_msg and by_msg[m["msg_no"]] != content:
            raise ValidationError(
                f"同号异值：消息号 {m['msg_no']!r} 在本批中出现不同内容，整批退回"
            )
        by_msg.setdefault(m["msg_no"], content)

    # 同船同刻异号
    seen_st: dict[tuple[str, str], str] = {}
    for msg_no, c in by_msg.items():
        key = (c["ship_no"], c["ts"])
        if key in seen_st and seen_st[key] != msg_no:
            raise ValidationError(
                f"同船同刻异号：船 {c['ship_no']} 在 {c['ts']} "
                f"存在消息 {seen_st[key]!r} 与 {msg_no!r}，整批退回"
            )
        seen_st.setdefault(key, msg_no)

    # 与现行有效消息冲突（active=1；软删消息行保留但不参与）
    for msg_no, c in by_msg.items():
        row = conn.execute(
            "SELECT ship_no, ts, lon, lat, content_sha FROM message "
            "WHERE msg_no=? AND active=1",
            (msg_no,),
        ).fetchone()
        if row is not None:
            if row["content_sha"] != _content_sha(
                c["ship_no"], c["ts"], c["lon"], c["lat"]
            ):
                raise ValidationError(
                    f"同号异值：消息号 {msg_no!r} 与现行有效批次内容冲突，整批退回"
                )
        clash = conn.execute(
            "SELECT msg_no FROM message WHERE ship_no=? AND ts=? AND msg_no<>? "
            "AND active=1",
            (c["ship_no"], c["ts"], msg_no),
        ).fetchone()
        if clash is not None:
            raise ValidationError(
                f"同船同刻异号：船 {c['ship_no']} 在 {c['ts']} 已有消息 "
                f"{clash['msg_no']!r}，整批退回"
            )
    return by_msg


# ---------------------------------------------------------------- 主事务

def apply_batch(
    conn: sqlite3.Connection,
    batch_no: str,
    messages: list[dict[str, Any]],
    mode: str,
    rect: PortRect,
) -> dict[str, Any]:
    """在调用方已开启的写事务内执行：校验 -> 落批次/消息 -> 增量核算 -> 切版本。

    任何异常都会由外层 transaction 上下文回滚，原批次与原结果保留。
    """
    by_msg = _validate_incoming(conn, batch_no, messages, mode)

    # 换交：找出本批不再提供的消息及其旧内容（作为虚拟删除标记点）
    removed_rows: list[sqlite3.Row] = []
    if mode == "replace":
        placeholders = ",".join("?" * len(by_msg)) if by_msg else ""
        sql = (
            "SELECT m.* FROM message m JOIN message_source s ON s.msg_no=m.msg_no "
            f"WHERE s.batch_no=? AND m.active=1 AND m.msg_no NOT IN ({placeholders})"
        )
        removed_rows = conn.execute(
            sql, [batch_no, *by_msg.keys()]
        ).fetchall()

    affected_ships = {c["ship_no"] for c in by_msg.values()}
    affected_ships.update(r["ship_no"] for r in removed_rows)

    # 船号级修订号：即使最终内容相同，补交新来源/换交来源集合也属于“来源有变”。
    for ship_no in affected_ships:
        conn.execute(
            "INSERT INTO ship_data_revision(ship_no, revision) VALUES(?,1) "
            "ON CONFLICT(ship_no) DO UPDATE SET revision=revision+1",
            (ship_no,),
        )

    # 1) 批次表
    if mode == "submit":
        conn.execute(
            "INSERT INTO batch(batch_no, created_at) VALUES(?,?)",
            (batch_no, _now()),
        )
    else:
        conn.execute(
            "UPDATE batch SET replaced_at=? WHERE batch_no=?", (_now(), batch_no)
        )

    # 2) 消息与来源。
    #    改动点 = 新纳入的消息（首次出现）或换交中被删除的消息；
    #    已由其他批次提供、本批只是重复提供的同内容消息不算改动点。
    newly_inserted: set[str] = set()
    for msg_no, c in by_msg.items():
        row = conn.execute(
            "SELECT active FROM message WHERE msg_no=?", (msg_no,)
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO message(msg_no, ship_no, ts, lon, lat, content_sha, "
                "created_batch, active) VALUES(?,?,?,?,?,?,?,1)",
                (
                    msg_no,
                    c["ship_no"],
                    c["ts"],
                    c["lon"],
                    c["lat"],
                    _content_sha(c["ship_no"], c["ts"], c["lon"], c["lat"]),
                    batch_no,
                ),
            )
            newly_inserted.add(msg_no)
        elif row["active"] == 0:
            # 历史上被软删、同号同内容重新纳入 -> 重新生效，属于改动点
            conn.execute(
                "UPDATE message SET active=1 WHERE msg_no=?", (msg_no,)
            )
            newly_inserted.add(msg_no)
        conn.execute(
            "INSERT OR IGNORE INTO message_source(msg_no, batch_no) VALUES(?,?)",
            (msg_no, batch_no),
        )

    deleted_msg_nos: set[str] = set()
    for r in removed_rows:
        conn.execute(
            "DELETE FROM message_source WHERE msg_no=? AND batch_no=?",
            (r["msg_no"], batch_no),
        )
        left = conn.execute(
            "SELECT 1 FROM message_source WHERE msg_no=?", (r["msg_no"],)
        ).fetchone()
        if not left:
            # 其他批次也不再提供 -> 消息软删失效，行保留以支撑历史版本查询
            conn.execute(
                "UPDATE message SET active=0 WHERE msg_no=?", (r["msg_no"],)
            )
            deleted_msg_nos.add(r["msg_no"])

    # 3) 结果版本
    old_version = conn.execute(
        "SELECT version_id FROM result_version WHERE active=1 ORDER BY version_id DESC"
    ).fetchone()
    old_vid = old_version["version_id"] if old_version else None

    active_batches = [
        r["batch_no"]
        for r in conn.execute("SELECT batch_no FROM batch ORDER BY batch_no")
    ]
    cur = conn.execute(
        "INSERT INTO result_version(created_at, batch_set, batches, active) "
        "VALUES(?,?,?,0)",
        (_now(), _batch_set_hash(active_batches), json.dumps(active_batches)),
    )
    new_vid = cur.lastrowid

    reused = 0
    recomputed = 0

    if old_vid is not None and affected_ships:
        marks = ",".join("?" * len(affected_ships))
        conn.execute(
            f"INSERT INTO stop_record(version_id, ship_no, seg_no, run_no, "
            f"entry_point, exit_point, before_point, after_point, seg_key) "
            f"SELECT ?, ship_no, seg_no, run_no, entry_point, exit_point, "
            f"before_point, after_point, seg_key FROM stop_record "
            f"WHERE version_id=? AND ship_no NOT IN ({marks})",
            [new_vid, old_vid, *affected_ships],
        )
        conn.execute(
            f"INSERT INTO pending_record(version_id, ship_no, seg_no, run_no, "
            f"entry_point, exit_point, before_point, after_point, reason, seg_key) "
            f"SELECT ?, ship_no, seg_no, run_no, entry_point, exit_point, "
            f"before_point, after_point, reason, seg_key FROM pending_record "
            f"WHERE version_id=? AND ship_no NOT IN ({marks})",
            [new_vid, old_vid, *affected_ships],
        )
    elif old_vid is not None:
        conn.execute(
            "INSERT INTO stop_record(version_id, ship_no, seg_no, run_no, "
            "entry_point, exit_point, before_point, after_point, seg_key) "
            "SELECT ?, ship_no, seg_no, run_no, entry_point, exit_point, "
            "before_point, after_point, seg_key FROM stop_record WHERE version_id=?",
            (new_vid, old_vid),
        )
        conn.execute(
            "INSERT INTO pending_record(version_id, ship_no, seg_no, run_no, "
            "entry_point, exit_point, before_point, after_point, reason, seg_key) "
            "SELECT ?, ship_no, seg_no, run_no, entry_point, exit_point, "
            "before_point, after_point, reason, seg_key FROM pending_record "
            "WHERE version_id=?",
            (new_vid, old_vid),
        )

    # 4) 受影响船逐船重建（含段级/港内段级沿用）
    old_records: dict[tuple[str, str, str], dict[str, str]] = {}
    if old_vid is not None:
        for table in ("stop_record", "pending_record"):
            for row in conn.execute(
                f"SELECT * FROM {table} WHERE version_id=? AND ship_no IN "
                f"({','.join('?' * len(affected_ships))})"
                if affected_ships
                else f"SELECT * FROM {table} WHERE 0",
                [old_vid, *affected_ships] if affected_ships else [],
            ).fetchall():
                entry_no = json.loads(row["entry_point"])["msg_no"]
                exit_no = json.loads(row["exit_point"])["msg_no"]
                old_records[(row["seg_key"], entry_no, exit_no)] = dict(row)

    # 仅真正失效的消息作为删除标记；其他批次仍提供的消息不产生任何变化。
    # 删除标记不参与实际判定（判定只基于现存点），仅用于增量沿用的触及判断。
    removed_by_ship: dict[str, list[sqlite3.Row]] = {}
    for r in removed_rows:
        if r["msg_no"] in deleted_msg_nos:
            removed_by_ship.setdefault(r["ship_no"], []).append(r)

    for ship_no in sorted(affected_ships):
        points: list[Point] = []
        for row in conn.execute(
            "SELECT * FROM message WHERE ship_no=? AND active=1 ORDER BY ts, msg_no",
            (ship_no,),
        ):
            points.append(
                Point(
                    msg_no=row["msg_no"],
                    ship_no=ship_no,
                    ts=parse_utc(row["ts"]),
                    lon=row["lon"],
                    lat=row["lat"],
                    changed=row["msg_no"] in newly_inserted,
                )
            )
        removed_markers = [
            Point(
                msg_no=r["msg_no"],
                ship_no=ship_no,
                ts=parse_utc(r["ts"]),
                lon=r["lon"],
                lat=r["lat"],
            )
            for r in removed_by_ship.get(ship_no, [])
        ]
        mark_in_port(points, rect)
        segments = split_segments(points)

        # 段触及判定：段内含改动点，或被删消息的旧时刻落在段的时间跨度内
        # （说明该段的结构可能因删除而变化，保守重算；其他段与所有改动点之间
        #  都隔着未变的港外点，沿用判定且不读取其港内明细）。
        def _seg_touched(seg: Segment) -> bool:
            if seg.touched:
                return True
            start_ts = seg.points[0].ts
            end_ts = seg.points[-1].ts
            return any(start_ts <= mk.ts <= end_ts for mk in removed_markers)

        for seg in segments:
            runs = find_inport_runs(seg)
            if not runs:
                continue
            if not _seg_touched(seg) and old_vid is not None:
                # 整段未触及：整段沿用，不读港内明细
                seg_old = [
                    rec
                    for rec in old_records.values()
                    if rec["ship_no"] == ship_no and rec["seg_key"] == seg.seg_key()
                ]
                if seg_old:
                    for rec in seg_old:
                        _copy_old_record(conn, new_vid, rec)
                        reused += 1
                    continue
            seg_has_removal = any(
                seg.points[0].ts <= mk.ts <= seg.points[-1].ts
                for mk in removed_markers
            )
            for run in runs:
                old_key = (
                    seg.seg_key(),
                    run.entry.msg_no,
                    run.exit_point.msg_no,
                )
                old_rec = old_records.get(old_key)
                if (
                    old_vid is not None
                    and old_rec is not None
                    and not seg_has_removal
                    and run_reusable(run)
                ):
                    _copy_old_record(conn, new_vid, old_rec)
                    reused += 1
                else:
                    _insert_run_record(conn, new_vid, run)
                    recomputed += 1

    # 4.5) 同一事务内生成第二套结果：按交接新船号连续化的实船记录。
    physical_summary = _build_physical_version(
        conn, old_vid, new_vid, affected_ships, rect
    )

    # 5) 切换现行版本
    conn.execute("UPDATE result_version SET active=0 WHERE active=1")
    conn.execute(
        "UPDATE result_version SET active=1 WHERE version_id=?", (new_vid,)
    )
    return {
        "version_id": new_vid,
        "messages_accepted": len(by_msg),
        "affected_ships": sorted(affected_ships),
        "reused_records": reused,
        "recomputed_records": recomputed,
        "physical_reused_records": physical_summary["reused"],
        "physical_recomputed_records": physical_summary["recomputed"],
        "affected_physical_ships": physical_summary["affected_physical_ships"],
    }


def _handover_maps(conn: sqlite3.Connection) -> tuple[dict[str, sqlite3.Row], dict[str, str]]:
    rows = conn.execute("SELECT * FROM ship_handover ORDER BY new_ship_no").fetchall()
    by_new = {r["new_ship_no"]: r for r in rows}
    old_to_new = {r["old_ship_no"]: r["new_ship_no"] for r in rows}
    return by_new, old_to_new


def _copy_reported_to_physical(
    conn: sqlite3.Connection, version_id: int, ship_no: str
) -> None:
    conn.execute(
        "INSERT INTO physical_stop_record(version_id, ship_no, seg_no, run_no, "
        "entry_point, exit_point, before_point, after_point, seg_key) "
        "SELECT ?, ship_no, seg_no, run_no, entry_point, exit_point, "
        "before_point, after_point, seg_key FROM stop_record "
        "WHERE version_id=? AND ship_no=?",
        (version_id, version_id, ship_no),
    )
    conn.execute(
        "INSERT INTO physical_pending_record(version_id, ship_no, seg_no, run_no, "
        "entry_point, exit_point, before_point, after_point, reason, seg_key) "
        "SELECT ?, ship_no, seg_no, run_no, entry_point, exit_point, "
        "before_point, after_point, reason, seg_key FROM pending_record "
        "WHERE version_id=? AND ship_no=?",
        (version_id, version_id, ship_no),
    )


def _build_handover_runs(
    conn: sqlite3.Connection, handover: sqlite3.Row | dict[str, Any], rect: PortRect
) -> list[InPortRun]:
    """合并新旧两号的有效点，并按观测时刻规则去重。

    Point.ship_no 保留报文原船号；上层存储时另外使用交接新船号作为实船主键。
    """
    new_no = handover["new_ship_no"]
    old_no = handover["old_ship_no"]
    enabled_at = parse_utc(handover["enabled_at"])
    disabled_at = parse_utc(handover["disabled_at"])

    rows = conn.execute(
        "SELECT * FROM message WHERE ship_no IN (?, ?) ORDER BY ts, msg_no",
        (new_no, old_no),
    ).fetchall()
    new_by_ts: dict[datetime, Point] = {}
    old_by_ts: dict[datetime, Point] = {}
    inactive_new_ts: set[datetime] = set()
    for row in rows:
        p = Point(
            msg_no=row["msg_no"],
            ship_no=row["ship_no"],
            ts=parse_utc(row["ts"]),
            lon=row["lon"],
            lat=row["lat"],
        )
        if row["ship_no"] == new_no:
            if row["active"]:
                # 同船同刻异号已被批次校验禁止；保留最后一个只是防御性处理。
                new_by_ts[p.ts] = p
            else:
                # 记录“曾有但最后来源已撤下”的新号点，便于停用时刻边界回退。
                inactive_new_ts.add(p.ts)
        elif row["active"]:
            old_by_ts[p.ts] = p

    selected: list[Point] = []
    for ts in sorted(set(new_by_ts) | set(old_by_ts)):
        new_point = new_by_ts.get(ts)
        old_point = old_by_ts.get(ts)
        if ts < enabled_at:
            # 启用前只取旧号，即使新号提前有点也不纳入。
            if old_point is not None:
                selected.append(old_point)
        elif ts < disabled_at:
            # 并报区间 [enabled_at, disabled_at)：同刻优先新号；
            # 新号最后来源撤下后会变为非 active，于是自然回退到同刻旧号。
            selected.append(new_point if new_point is not None else old_point)  # type: ignore[arg-type]
        else:
            # 旧机停用时起只取新号；但若停用时刻的新号点因最后来源撤下而失效，
            # 仍按“同刻来源撤下后恢复有效旧号点”的规则回退到该刻旧号点。
            if new_point is not None:
                selected.append(new_point)
            elif ts == disabled_at and old_point is not None and ts in inactive_new_ts:
                selected.append(old_point)

    selected = [p for p in selected if p is not None]
    selected.sort(key=lambda p: (p.ts, p.msg_no))
    mark_in_port(selected, rect)
    segments = split_segments(selected)
    runs: list[InPortRun] = []
    for seg in segments:
        runs.extend(find_inport_runs(seg))
    return runs


def _replace_physical_for_handover(
    conn: sqlite3.Connection,
    version_id: int,
    handover: sqlite3.Row | dict[str, Any],
    rect: PortRect,
) -> int:
    new_no = handover["new_ship_no"]
    conn.execute(
        "DELETE FROM physical_stop_record WHERE version_id=? AND ship_no=?",
        (version_id, new_no),
    )
    conn.execute(
        "DELETE FROM physical_pending_record WHERE version_id=? AND ship_no=?",
        (version_id, new_no),
    )
    runs = _build_handover_runs(conn, handover, rect)
    for run in runs:
        _insert_run_record(
            conn,
            version_id,
            run,
            physical=True,
            canonical_ship_no=new_no,
        )
    return len(runs)


def _build_physical_version(
    conn: sqlite3.Connection,
    old_vid: Optional[int],
    new_vid: int,
    affected_ships: set[str],
    rect: PortRect,
) -> dict[str, Any]:
    by_new, old_to_new = _handover_maps(conn)
    participant_new = set(by_new)
    participant_old = set(old_to_new)

    affected_canonical = {
        by_new[s]["new_ship_no"] if s in by_new else old_to_new[s]
        for s in affected_ships
        if s in by_new or s in old_to_new
    }
    unlinked_affected = {
        s
        for s in affected_ships
        if s not in participant_new and s not in participant_old
    }
    ships_to_refresh = affected_canonical | unlinked_affected

    reused = 0
    if old_vid is not None:
        if ships_to_refresh:
            marks = ",".join("?" * len(ships_to_refresh))
            reused += conn.execute(
                f"INSERT INTO physical_stop_record(version_id, ship_no, seg_no, run_no, "
                f"entry_point, exit_point, before_point, after_point, seg_key) "
                f"SELECT ?, ship_no, seg_no, run_no, entry_point, exit_point, "
                f"before_point, after_point, seg_key FROM physical_stop_record "
                f"WHERE version_id=? AND ship_no NOT IN ({marks})",
                [new_vid, old_vid, *ships_to_refresh],
            ).rowcount or 0
            reused += conn.execute(
                f"INSERT INTO physical_pending_record(version_id, ship_no, seg_no, run_no, "
                f"entry_point, exit_point, before_point, after_point, reason, seg_key) "
                f"SELECT ?, ship_no, seg_no, run_no, entry_point, exit_point, "
                f"before_point, after_point, reason, seg_key FROM physical_pending_record "
                f"WHERE version_id=? AND ship_no NOT IN ({marks})",
                [new_vid, old_vid, *ships_to_refresh],
            ).rowcount or 0
        else:
            reused += conn.execute(
                "INSERT INTO physical_stop_record(version_id, ship_no, seg_no, run_no, "
                "entry_point, exit_point, before_point, after_point, seg_key) "
                "SELECT ?, ship_no, seg_no, run_no, entry_point, exit_point, "
                "before_point, after_point, seg_key FROM physical_stop_record WHERE version_id=?",
                (new_vid, old_vid),
            ).rowcount or 0
            reused += conn.execute(
                "INSERT INTO physical_pending_record(version_id, ship_no, seg_no, run_no, "
                "entry_point, exit_point, before_point, after_point, reason, seg_key) "
                "SELECT ?, ship_no, seg_no, run_no, entry_point, exit_point, "
                "before_point, after_point, reason, seg_key FROM physical_pending_record "
                "WHERE version_id=?",
                (new_vid, old_vid),
            ).rowcount or 0

    recomputed = 0
    for canonical in sorted(affected_canonical):
        recomputed += _replace_physical_for_handover(
            conn, new_vid, by_new[canonical], rect
        )

    # 未参加交接的受影响船：实船就是报船本身，直接复制同一事务刚算好的结果。
    for ship_no in sorted(unlinked_affected):
        _copy_reported_to_physical(conn, new_vid, ship_no)

    return {
        "reused": reused,
        "recomputed": recomputed,
        "affected_physical_ships": sorted(ships_to_refresh),
    }


def _copy_old_record(conn: sqlite3.Connection, new_vid: int, rec: dict[str, Any]):
    if "reason" in rec:
        conn.execute(
            "INSERT INTO pending_record(version_id, ship_no, seg_no, run_no, "
            "entry_point, exit_point, before_point, after_point, reason, seg_key) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                new_vid,
                rec["ship_no"],
                rec["seg_no"],
                rec["run_no"],
                rec["entry_point"],
                rec["exit_point"],
                rec["before_point"],
                rec["after_point"],
                rec["reason"],
                rec["seg_key"],
            ),
        )
    else:
        conn.execute(
            "INSERT INTO stop_record(version_id, ship_no, seg_no, run_no, "
            "entry_point, exit_point, before_point, after_point, seg_key) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (
                new_vid,
                rec["ship_no"],
                rec["seg_no"],
                rec["run_no"],
                rec["entry_point"],
                rec["exit_point"],
                rec["before_point"],
                rec["after_point"],
                rec["seg_key"],
            ),
        )


def _insert_run_record(
    conn: sqlite3.Connection,
    new_vid: int,
    run: InPortRun,
    *,
    physical: bool = False,
    canonical_ship_no: Optional[str] = None,
):
    entry_json = _point_json(run.entry)
    exit_json = _point_json(run.exit_point)
    before_json = _point_json(run.before) if run.before is not None else None
    after_json = _point_json(run.after) if run.after is not None else None
    table_prefix = "physical_" if physical else ""
    ship_no = canonical_ship_no or run.entry.ship_no
    if run.is_stop:
        conn.execute(
            f"INSERT INTO {table_prefix}stop_record(version_id, ship_no, seg_no, run_no, "
            "entry_point, exit_point, before_point, after_point, seg_key) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (
                new_vid,
                ship_no,
                run.seg_no,
                run.run_no,
                entry_json,
                exit_json,
                before_json,
                after_json,
                run.seg_key,
            ),
        )
    else:
        conn.execute(
            f"INSERT INTO {table_prefix}pending_record(version_id, ship_no, seg_no, run_no, "
            "entry_point, exit_point, before_point, after_point, reason, seg_key) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                new_vid,
                ship_no,
                run.seg_no,
                run.run_no,
                entry_json,
                exit_json,
                before_json,
                after_json,
                run.pending_reason,
                run.seg_key,
            ),
        )


# ---------------------------------------------------------------- 查询

def resolve_version(
    conn: sqlite3.Connection, version_id: Optional[int]
) -> Optional[sqlite3.Row]:
    if version_id is not None:
        return conn.execute(
            "SELECT * FROM result_version WHERE version_id=?", (version_id,)
        ).fetchone()
    return conn.execute(
        "SELECT * FROM result_version WHERE active=1 ORDER BY version_id DESC"
    ).fetchone()


def _enrich_point(conn: sqlite3.Connection, raw: Optional[str]) -> Optional[dict]:
    if raw is None:
        return None
    point = json.loads(raw)
    batches = [
        r["batch_no"]
        for r in conn.execute(
            "SELECT batch_no FROM message_source WHERE msg_no=? ORDER BY batch_no",
            (point["msg_no"],),
        )
    ]
    point["source_batches"] = batches
    reported = point.get("ship_no")
    if reported is not None:
        for alias in (
            "reported_ship_no",
            "report_ship_no",
            "original_ship_no",
            "origin_ship_no",
            "source_ship_no",
        ):
            point.setdefault(alias, reported)
    return point


def _row_to_record(conn: sqlite3.Connection, row: sqlite3.Row, kind: str) -> dict:
    base = {
        "ship_no": row["ship_no"],
        "physical_ship_no": row["ship_no"],
        "actual_ship_no": row["ship_no"],
        "new_ship_no": row["ship_no"],
        "seg_no": row["seg_no"],
        "run_no": row["run_no"],
        "seg_key": row["seg_key"],
        "entry_point": _enrich_point(conn, row["entry_point"]),
        "exit_point": _enrich_point(conn, row["exit_point"]),
    }
    if kind == "stop":
        base.update(
            kind="stop",
            before_point=_enrich_point(conn, row["before_point"]),
            after_point=_enrich_point(conn, row["after_point"]),
        )
    else:
        base.update(
            kind="pending",
            before_point=_enrich_point(conn, row["before_point"]),
            after_point=_enrich_point(conn, row["after_point"]),
            reason=row["reason"],
        )
    return base


def query_records(
    conn: sqlite3.Connection, ship_no: str, kind: str, version_id: Optional[int]
) -> list[dict]:
    version = resolve_version(conn, version_id)
    if version is None:
        return []
    table = "stop_record" if kind == "stop" else "pending_record"
    rows = conn.execute(
        f"SELECT * FROM {table} WHERE version_id=? AND ship_no=? "
        f"ORDER BY seg_no, run_no",
        (version["version_id"], ship_no),
    ).fetchall()
    return [_row_to_record(conn, r, kind) for r in rows]


def list_versions(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM result_version ORDER BY version_id"
    ).fetchall()
    return [
        {
            "version_id": r["version_id"],
            "created_at": r["created_at"],
            "batches": json.loads(r["batches"]),
            "active": bool(r["active"]),
        }
        for r in rows
    ]


def get_batch_info(conn: sqlite3.Connection, batch_no: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT * FROM batch WHERE batch_no=?", (batch_no,)
    ).fetchone()
    if row is None:
        return None
    count = conn.execute(
        "SELECT COUNT(*) AS c FROM message_source WHERE batch_no=?", (batch_no,)
    ).fetchone()["c"]
    return {
        "batch_no": row["batch_no"],
        "created_at": row["created_at"],
        "replaced_at": row["replaced_at"],
        "active": True,
        "message_count": count,
    }


# ---------------------------------------------------------------- 交接与实船查询

def get_handover(conn: sqlite3.Connection, ship_key: str) -> Optional[sqlite3.Row]:
    """按新船号查交接；为兼容路径别名，也允许用旧船号定位同一组。"""
    return conn.execute(
        "SELECT * FROM ship_handover WHERE new_ship_no=? OR old_ship_no=? LIMIT 1",
        (ship_key, ship_key),
    ).fetchone()


def _handover_payload(row: sqlite3.Row | dict[str, Any], **extra: Any) -> dict[str, Any]:
    data = dict(row)
    new_no = data["new_ship_no"]
    old_no = data["old_ship_no"]
    enabled = data["enabled_at"]
    disabled = data["disabled_at"]
    result = {
        "handover_id": data.get("id"),
        "id": data.get("id"),
        "ship_no": new_no,
        "new_ship_no": new_no,
        "old_ship_no": old_no,
        # 简洁命名
        "enabled_at": enabled,
        "disabled_at": disabled,
        "active_at": enabled,
        "inactive_at": disabled,
        # 贴近需求原文的命名
        "new_device_enabled_at": enabled,
        "old_device_disabled_at": disabled,
        "new_machine_enabled_at": enabled,
        "old_machine_disabled_at": disabled,
        "new_enabled_at": enabled,
        "old_disabled_at": disabled,
        "enable_at": enabled,
        "disable_at": disabled,
        "activate_at": enabled,
        "deactivate_at": disabled,
        "activated_at": enabled,
        "deactivated_at": disabled,
        "valid_from": enabled,
        "valid_to": disabled,
        "revision": data.get("revision", 1),
        "created_at": data.get("created_at"),
        "updated_at": data.get("updated_at"),
        "status": "accepted",
    }
    result.update(extra)
    result["handover"] = {
        "id": result.get("id"),
        "handover_id": result.get("handover_id"),
        "ship_no": new_no,
        "new_ship_no": new_no,
        "old_ship_no": old_no,
        "enabled_at": enabled,
        "disabled_at": disabled,
        "new_device_enabled_at": enabled,
        "old_device_disabled_at": disabled,
        "revision": result.get("revision", 1),
        "status": result.get("status"),
        "version_id": result.get("version_id"),
    }
    return result


def _validate_handover_times(enabled_raw: str, disabled_raw: str) -> tuple[str, str]:
    enabled_dt = parse_utc(enabled_raw)
    disabled_dt = parse_utc(disabled_raw)
    if enabled_dt >= disabled_dt:
        raise ValidationError(
            "交接时刻非法：必须先启用新机，再停用旧机，且启用时刻早于停用时刻"
        )
    return to_iso_z(enabled_dt), to_iso_z(disabled_dt)


def create_handover(
    conn: sqlite3.Connection,
    fields: dict[str, Any],
    rect: PortRect,
) -> dict[str, Any]:
    new_no = fields.get("new_ship_no")
    old_no = fields.get("old_ship_no")
    enabled_raw = fields.get("enabled_at")
    disabled_raw = fields.get("disabled_at")
    if not new_no or not old_no or not enabled_raw or not disabled_raw:
        raise PayloadFormatError(
            "交接登记必须提供 new_ship_no、old_ship_no、新机启用时刻和旧机停用时刻"
        )
    if new_no == old_no:
        raise ValidationError("新船号与旧船号不能相同")
    enabled_at, disabled_at = _validate_handover_times(enabled_raw, disabled_raw)

    clash = conn.execute(
        "SELECT * FROM ship_handover "
        "WHERE new_ship_no IN (?, ?) OR old_ship_no IN (?, ?) LIMIT 1",
        (new_no, old_no, new_no, old_no),
    ).fetchone()
    if clash is not None:
        raise ValidationError(
            f"船号 {new_no!r}/{old_no!r} 中已有船号参加交接 "
            f"（新号 {clash['new_ship_no']!r} -> 旧号 {clash['old_ship_no']!r}）；"
            "每个船号只能参加一组交接"
        )

    now = _now()
    cur = conn.execute(
        "INSERT INTO ship_handover(new_ship_no, old_ship_no, enabled_at, disabled_at, "
        "revision, created_at, updated_at) VALUES(?,?,?,?,1,?,?)",
        (new_no, old_no, enabled_at, disabled_at, now, now),
    )
    handover_id = cur.lastrowid
    conn.execute(
        "INSERT INTO handover_participant(ship_no, new_ship_no, role) VALUES(?,?,?)",
        (new_no, new_no, "new"),
    )
    conn.execute(
        "INSERT INTO handover_participant(ship_no, new_ship_no, role) VALUES(?,?,?)",
        (old_no, new_no, "old"),
    )

    row = conn.execute(
        "SELECT * FROM ship_handover WHERE id=?", (handover_id,)
    ).fetchone()
    version = resolve_version(conn, None)
    version_id = version["version_id"] if version is not None else None
    record_count = 0
    if version_id is not None:
        record_count = _replace_physical_for_handover(
            conn, version_id, row, rect
        )
    return _handover_payload(
        row,
        version_id=version_id,
        physical_recomputed_records=record_count,
        affected_physical_ships=[new_no],
    )


def _ship_revisions(conn: sqlite3.Connection, ship_nos: list[str]) -> dict[str, int]:
    if not ship_nos:
        return {}
    marks = ",".join("?" * len(ship_nos))
    rows = conn.execute(
        f"SELECT ship_no, revision FROM ship_data_revision WHERE ship_no IN ({marks})",
        ship_nos,
    ).fetchall()
    found = {r["ship_no"]: int(r["revision"]) for r in rows}
    return {s: found.get(s, 0) for s in ship_nos}


def _scope_fingerprint(conn: sqlite3.Connection, ship_nos: list[str]) -> str:
    """本组新旧船号的有效消息、全部消息状态与来源集合指纹。"""
    if not ship_nos:
        return ""
    marks = ",".join("?" * len(ship_nos))
    messages = [
        tuple(r)
        for r in conn.execute(
            f"SELECT msg_no, ship_no, ts, lon, lat, content_sha, active "
            f"FROM message WHERE ship_no IN ({marks}) ORDER BY msg_no",
            ship_nos,
        ).fetchall()
    ]
    sources = [
        tuple(r)
        for r in conn.execute(
            f"SELECT s.msg_no, s.batch_no FROM message_source s "
            f"JOIN message m ON m.msg_no=s.msg_no "
            f"WHERE m.ship_no IN ({marks}) ORDER BY s.msg_no, s.batch_no",
            ship_nos,
        ).fetchall()
    ]
    payload = json.dumps(
        {"messages": messages, "sources": sources},
        separators=(",", ":"),
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def plan_handover_correction(
    conn: sqlite3.Connection,
    ship_key: str,
    fields: dict[str, Any],
    rect: PortRect,
) -> dict[str, Any]:
    """在写事务外完成读取和核算，允许无关船号批次在此期间先提交。"""
    row = get_handover(conn, ship_key)
    if row is None:
        raise NotFoundError(f"船号 {ship_key!r} 没有交接记录，无法更正")

    enabled_raw = fields.get("enabled_at") or row["enabled_at"]
    disabled_raw = fields.get("disabled_at") or row["disabled_at"]
    enabled_at, disabled_at = _validate_handover_times(enabled_raw, disabled_raw)

    expected_enabled = fields.get("expected_enabled_at")
    expected_disabled = fields.get("expected_disabled_at")
    if expected_enabled is not None:
        expected_enabled = to_iso_z(parse_utc(expected_enabled))
        if expected_enabled != row["enabled_at"]:
            raise ConflictError("交接启用时刻已被其他更正修改，本次更正退回")
    if expected_disabled is not None:
        expected_disabled = to_iso_z(parse_utc(expected_disabled))
        if expected_disabled != row["disabled_at"]:
            raise ConflictError("交接停用时刻已被其他更正修改，本次更正退回")
    expected_revision = fields.get("base_revision")
    if expected_revision is not None and int(expected_revision) != int(row["revision"]):
        raise ConflictError("交接版本号已变化，本次更正退回")

    ship_nos = [row["new_ship_no"], row["old_ship_no"]]
    # 先建立核算前快照，再执行可能耗时的计算。若计算途中有相关批次提交，
    # 发布时会发现当前修订/指纹与这里不同，从而退回本次更正。
    revisions = _ship_revisions(conn, ship_nos)
    fingerprint = _scope_fingerprint(conn, ship_nos)
    candidate = {
        "id": row["id"],
        "new_ship_no": row["new_ship_no"],
        "old_ship_no": row["old_ship_no"],
        "enabled_at": enabled_at,
        "disabled_at": disabled_at,
    }
    runs = _build_handover_runs(conn, candidate, rect)
    version = resolve_version(conn, None)
    return {
        "key": row["new_ship_no"],
        "handover_id": row["id"],
        "base_revision": int(row["revision"]),
        "base_enabled_at": row["enabled_at"],
        "base_disabled_at": row["disabled_at"],
        "enabled_at": enabled_at,
        "disabled_at": disabled_at,
        "revisions": revisions,
        "fingerprint": fingerprint,
        "active_version_id": version["version_id"] if version is not None else None,
        "runs": runs,
    }


def publish_handover_correction(
    conn: sqlite3.Connection, plan: dict[str, Any]
) -> dict[str, Any]:
    """在 BEGIN IMMEDIATE 事务内复检并发布更正。"""
    new_no = plan["key"]
    row = conn.execute(
        "SELECT * FROM ship_handover WHERE new_ship_no=?", (new_no,)
    ).fetchone()
    if row is None:
        raise ConflictError("本组交接已不存在，本次更正退回")
    if (
        int(row["revision"]) != plan["base_revision"]
        or row["enabled_at"] != plan["base_enabled_at"]
        or row["disabled_at"] != plan["base_disabled_at"]
    ):
        raise ConflictError("本组交接在核算期间已变更，本次更正退回")

    ship_nos = [new_no, row["old_ship_no"]]
    if _ship_revisions(conn, ship_nos) != plan["revisions"]:
        raise ConflictError("本组新旧船号的有效消息或来源在核算期间有变，本次更正退回")
    if _scope_fingerprint(conn, ship_nos) != plan["fingerprint"]:
        raise ConflictError("本组新旧船号的有效消息或来源在核算期间有变，本次更正退回")

    now = _now()
    updated = conn.execute(
        "UPDATE ship_handover SET enabled_at=?, disabled_at=?, revision=revision+1, "
        "updated_at=? WHERE id=? AND revision=?",
        (
            plan["enabled_at"],
            plan["disabled_at"],
            now,
            plan["handover_id"],
            plan["base_revision"],
        ),
    )
    if updated.rowcount != 1:
        raise ConflictError("本组交接在核算期间已变更，本次更正退回")

    row = conn.execute(
        "SELECT * FROM ship_handover WHERE id=?", (plan["handover_id"],)
    ).fetchone()
    version = resolve_version(conn, None)
    version_id = version["version_id"] if version is not None else None
    if version_id is not None:
        # 无关船号批次可能已先行生成更新的 active 版本；直接在最新版本上落结果。
        conn.execute(
            "DELETE FROM physical_stop_record WHERE version_id=? AND ship_no=?",
            (version_id, new_no),
        )
        conn.execute(
            "DELETE FROM physical_pending_record WHERE version_id=? AND ship_no=?",
            (version_id, new_no),
        )
        for run in plan["runs"]:
            _insert_run_record(
                conn,
                version_id,
                run,
                physical=True,
                canonical_ship_no=new_no,
            )

    return _handover_payload(
        row,
        status="corrected",
        corrected=True,
        version_id=version_id,
        physical_recomputed_records=len(plan["runs"]),
        affected_physical_ships=[new_no],
    )


def query_physical_records(
    conn: sqlite3.Connection, ship_no: str, kind: str, version_id: Optional[int]
) -> list[dict]:
    version = resolve_version(conn, version_id)
    if version is None:
        return []

    # 物理查询按新船号；若调用方误传旧号，也定位到同一实船。
    handover = get_handover(conn, ship_no)
    canonical = handover["new_ship_no"] if handover is not None else ship_no
    table = "physical_stop_record" if kind == "stop" else "physical_pending_record"
    rows = conn.execute(
        f"SELECT * FROM {table} WHERE version_id=? AND ship_no=? "
        f"ORDER BY seg_no, run_no",
        (version["version_id"], canonical),
    ).fetchall()
    if not rows and handover is None:
        # 兼容升级前生成、尚未回填 physical_* 的版本：无交接时实船即报船。
        return query_records(conn, ship_no, kind, version_id)
    return [_row_to_record(conn, r, kind) for r in rows]


def backfill_physical_results(conn: sqlite3.Connection, rect: PortRect) -> None:
    """启动时为旧库当前版本补齐第二套结果；新库通常没有 active 版本。"""
    version = resolve_version(conn, None)
    if version is None:
        return
    vid = version["version_id"]
    by_new, old_to_new = _handover_maps(conn)

    reported_ships = {
        r["ship_no"]
        for r in conn.execute(
            "SELECT DISTINCT ship_no FROM stop_record WHERE version_id=?", (vid,)
        ).fetchall()
    } | {
        r["ship_no"]
        for r in conn.execute(
            "SELECT DISTINCT ship_no FROM pending_record WHERE version_id=?", (vid,)
        ).fetchall()
    }
    for ship_no in reported_ships:
        if ship_no in by_new or ship_no in old_to_new:
            continue
        exists = conn.execute(
            "SELECT 1 FROM physical_stop_record WHERE version_id=? AND ship_no=? LIMIT 1",
            (vid, ship_no),
        ).fetchone() or conn.execute(
            "SELECT 1 FROM physical_pending_record WHERE version_id=? AND ship_no=? LIMIT 1",
            (vid, ship_no),
        ).fetchone()
        if exists is None:
            _copy_reported_to_physical(conn, vid, ship_no)

    for row in by_new.values():
        _replace_physical_for_handover(conn, vid, row, rect)



# ---------------------------------------------------------------- 命名兼容别名
# 不同设计可能把“实船”称作 real/actual/merged；保留可导入的服务层别名。
register_handover = create_handover
apply_handover = create_handover
create_ship_handover = create_handover
get_ship_handover = get_handover
query_real_ship_records = query_physical_records
query_real_records = query_physical_records
query_actual_ship_records = query_physical_records
query_merged_ship_records = query_physical_records
rebuild_physical_records = _replace_physical_for_handover
build_handover_runs = _build_handover_runs


def correct_handover(
    conn: sqlite3.Connection,
    ship_no: str,
    fields: dict[str, Any],
    rect: PortRect,
) -> dict[str, Any]:
    """便捷服务入口：事务外核算，再在一个写事务内发布更正。"""
    plan = plan_handover_correction(conn, ship_no, fields, rect)
    with transaction(conn):
        return publish_handover_correction(conn, plan)
