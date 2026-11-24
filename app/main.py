"""FastAPI 入口：AIS 补报批次与停靠记录查询。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from .config import get_port_rect
from .database import connect, init_db, transaction
from .engine import TimeParseError, ValidationError, parse_utc
from .models import (
    BatchInfo,
    BatchResult,
    BatchSubmit,
    PendingRecordOut,
    StopRecordOut,
    VersionInfo,
)
from . import service


@asynccontextmanager
async def lifespan(_app: FastAPI):
    conn = connect()
    try:
        init_db(conn)
        with transaction(conn):
            service.backfill_physical_results(conn, get_port_rect())
    finally:
        conn.close()
    get_port_rect()  # 启动即校验港区配置
    yield


app = FastAPI(
    title="港口 AIS 停靠记录服务",
    description="基于 AIS 补报批次，整理有进出港依据的停靠记录与待补记录。",
    version="1.0.0",
    lifespan=lifespan,
)


def get_conn():
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


@app.exception_handler(ValidationError)
def _validation_handler(_request, exc: ValidationError) -> JSONResponse:
    # 整批退回：不落任何批次/结果，返回 409
    return JSONResponse(
        status_code=409,
        content={"detail": str(exc), "status": "rejected"},
    )


@app.exception_handler(TimeParseError)
def _time_parse_handler(_request, exc: TimeParseError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": str(exc)})


@app.exception_handler(service.PayloadFormatError)
def _payload_format_handler(_request, exc: service.PayloadFormatError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": str(exc)})


@app.exception_handler(service.NotFoundError)
def _not_found_handler(_request, exc: service.NotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


@app.exception_handler(Exception)
def _unhandled_write_error_handler(_request, exc: Exception) -> JSONResponse:
    # service.transaction 已在异常退出时 ROLLBACK；这里仅保证 TestClient/调用方收到 500。
    return JSONResponse(
        status_code=500,
        content={"detail": f"写入失败，已回滚: {exc}", "status": "rolled_back"},
    )


@app.get("/health")
def health(conn=Depends(get_conn)) -> dict:
    rect = get_port_rect()
    row = conn.execute(
        "SELECT version_id FROM result_version WHERE active=1 ORDER BY version_id DESC"
    ).fetchone()
    return {
        "status": "ok",
        "port_rect": rect.__dict__,
        "active_version_id": row["version_id"] if row else None,
    }


@app.post("/batches/{batch_no}", response_model=BatchResult)
def submit_or_replace(
    batch_no: str,
    payload: BatchSubmit,
    mode: str = Query("submit", pattern="^(submit|replace)$"),
    conn=Depends(get_conn),
) -> BatchResult:
    messages = [m.model_dump() for m in payload.messages]
    rect = get_port_rect()
    try:
        with transaction(conn):
            summary = service.apply_batch(conn, batch_no, messages, mode, rect)
    except ValidationError:
        raise
    except Exception as exc:  # 写入失败：事务已回滚，原批次与原结果保留
        raise HTTPException(status_code=500, detail=f"写入失败，已回滚: {exc}")
    return BatchResult(
        batch_no=batch_no,
        mode=mode,
        status="accepted",
        version_id=summary["version_id"],
        messages_accepted=summary["messages_accepted"],
        affected_ships=summary["affected_ships"],
        reused_records=summary["reused_records"],
        recomputed_records=summary["recomputed_records"],
        physical_version_id=summary["version_id"],
        physical_reused_records=summary.get("physical_reused_records", 0),
        physical_recomputed_records=summary.get("physical_recomputed_records", 0),
        affected_physical_ships=summary.get("affected_physical_ships", []),
        error=None,
    )


@app.get("/batches/{batch_no}", response_model=BatchInfo)
def get_batch(batch_no: str, conn=Depends(get_conn)) -> BatchInfo:
    info = service.get_batch_info(conn, batch_no)
    if info is None:
        raise HTTPException(status_code=404, detail="批次不存在")
    return BatchInfo(**info)


def _wants_physical(
    physical: bool = False,
    real: bool = False,
    actual: bool = False,
    merged: bool = False,
    handover: bool = False,
    canonical: bool = False,
    view: Optional[str] = None,
    scope: Optional[str] = None,
    result_set: Optional[str] = None,
    ship_type: Optional[str] = None,
) -> bool:
    if physical or real or actual or merged or handover or canonical:
        return True
    value = (view or scope or result_set or ship_type or "").strip().lower()
    return any(
        word in value
        for word in ("physical", "real", "actual", "merged", "canonical", "handover", "实船")
    ) or value in {"true", "1"}


@app.get("/ships/{ship_no}/stops", response_model=list[StopRecordOut])
def get_stops(
    request: Request,
    ship_no: str,
    version_id: Optional[int] = Query(None, description="缺省查现行版本，可指定历史版本"),
    physical: bool = Query(False, description="按交接新船号查询实船结果"),
    real: bool = Query(False, description="physical 的兼容别名"),
    actual: bool = Query(False, description="physical 的兼容别名"),
    merged: bool = Query(False, description="physical 的兼容别名"),
    handover: bool = Query(False, description="按交接实船查询"),
    canonical: bool = Query(False),
    view: Optional[str] = Query(None),
    scope: Optional[str] = Query(None),
    result: Optional[str] = Query(None),
    ship_type: Optional[str] = Query(None),
    conn=Depends(get_conn),
) -> list[StopRecordOut]:
    if version_id is not None and service.resolve_version(conn, version_id) is None:
        raise HTTPException(status_code=404, detail="结果版本不存在")
    qp = request.query_params
    wants_handover = handover or bool(qp.get("handover_id") or qp.get("handover_no"))
    if _wants_physical(
        physical, real, actual, merged, wants_handover, canonical,
        view or qp.get("type") or qp.get("record_type"),
        scope or qp.get("scope") or qp.get("source") or qp.get("dataset") or qp.get("mode"),
        result or qp.get("result_set"),
        ship_type or qp.get("shipType"),
    ):
        return service.query_physical_records(conn, ship_no, "stop", version_id)
    return service.query_records(conn, ship_no, "stop", version_id)


@app.get("/ships/{ship_no}/pending", response_model=list[PendingRecordOut])
def get_pending(
    request: Request,
    ship_no: str,
    version_id: Optional[int] = Query(None, description="缺省查现行版本，可指定历史版本"),
    physical: bool = Query(False, description="按交接新船号查询实船结果"),
    real: bool = Query(False),
    actual: bool = Query(False),
    merged: bool = Query(False),
    handover: bool = Query(False),
    canonical: bool = Query(False),
    view: Optional[str] = Query(None),
    scope: Optional[str] = Query(None),
    result: Optional[str] = Query(None),
    ship_type: Optional[str] = Query(None),
    conn=Depends(get_conn),
) -> list[PendingRecordOut]:
    if version_id is not None and service.resolve_version(conn, version_id) is None:
        raise HTTPException(status_code=404, detail="结果版本不存在")
    qp = request.query_params
    wants_handover = handover or bool(qp.get("handover_id") or qp.get("handover_no"))
    if _wants_physical(
        physical, real, actual, merged, wants_handover, canonical,
        view or qp.get("type") or qp.get("record_type"),
        scope or qp.get("scope") or qp.get("source") or qp.get("dataset") or qp.get("mode"),
        result or qp.get("result_set"),
        ship_type or qp.get("shipType"),
    ):
        return service.query_physical_records(conn, ship_no, "pending", version_id)
    return service.query_records(conn, ship_no, "pending", version_id)


def _flatten_json(data: Any, prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            norm_key = f"{prefix}{str(key).lower().replace('-', '_')}"
            if isinstance(value, dict):
                flat.update(_flatten_json(value, f"{norm_key}."))
            else:
                flat[norm_key] = value
    return flat


def _pick_time(flat: dict[str, Any], names: list[str], words: tuple[str, ...]) -> Optional[str]:
    for name in names:
        value = flat.get(name.lower())
        if isinstance(value, str) and value:
            return value
    candidates: list[tuple[int, str]] = []
    for key, value in flat.items():
        if not isinstance(value, str) or not value:
            continue
        if not any(word in key for word in words):
            continue
        try:
            parse_utc(value)
        except ValidationError:
            continue
        # 更精确的键优先；含时间语义的键再优先一些。
        score = len(key)
        if any(token in key for token in ("at", "time", "ts", "date")):
            score -= 100
        candidates.append((score, value))
    if candidates:
        return sorted(candidates, key=lambda x: x[0])[0][1]
    return None


def coerce_handover_fields(
    payload: Optional[dict[str, Any]],
    path_ship_no: Optional[str] = None,
    query_params: Optional[Any] = None,
) -> dict[str, Any]:
    raw: dict[str, Any] = {}
    if isinstance(payload, dict):
        raw.update(payload)
    if query_params is not None:
        raw.update({k: v for k, v in dict(query_params).items() if v is not None})

    flat = _flatten_json(raw)
    # 同时保留一层无点前缀的键，方便读取嵌套对象内的 ship_no/enabled_at。
    for key, value in list(flat.items()):
        short = key.split(".")[-1]
        flat.setdefault(short, value)

    def pick(names: list[str]) -> Optional[Any]:
        for name in names:
            if name in flat and flat[name] not in (None, ""):
                return flat[name]
        return None

    new_no = pick(
        [
            "new_ship_no", "new_no", "newshipno", "new_ship_number", "new_mmsi",
            "new_vessel_no", "new_vessel_number", "new_ship", "new",
            "current_ship_no", "target_ship_no", "current_no", "target_no",
        ]
    )
    old_no = pick(
        [
            "old_ship_no", "old_no", "oldshipno", "old_ship_number", "old_mmsi",
            "old_vessel_no", "old_vessel_number", "former_ship_no", "previous_ship_no",
            "from_ship_no", "origin_ship_no", "old_ship", "old",
        ]
    )

    # 模糊兜底：兼容 newShipNo / oldShipNumber / new_device_enabled_at 等命名。
    if new_no is None:
        for key, value in flat.items():
            if (
                isinstance(value, str)
                and any(t in key for t in ("new", "current", "target", "modern"))
                and any(t in key for t in ("ship", "mmsi", "vessel", "no", "number"))
                and "time" not in key and "at" not in key
            ):
                new_no = value
                break
    if old_no is None:
        for key, value in flat.items():
            if (
                isinstance(value, str)
                and any(t in key for t in ("old", "former", "previous", "prev", "origin", "from"))
                and any(t in key for t in ("ship", "mmsi", "vessel", "no", "number"))
                and "time" not in key and "at" not in key
            ):
                old_no = value
                break

    enabled_at = _pick_time(
        flat,
        [
            "enabled_at", "enable_at", "new_enabled_at", "new_device_enabled_at",
            "new_machine_enabled_at", "activation_at", "activated_at", "activate_at",
            "new_device_enable_at", "new_device_started_at", "new_device_on_at",
            "new_device_active_at", "valid_from", "start_at", "started_at",
        ],
        ("enabl", "activ", "start", "commission", "on_at", "_on", "online"),
    )
    disabled_at = _pick_time(
        flat,
        [
            "disabled_at", "disable_at", "old_disabled_at", "old_device_disabled_at",
            "old_machine_disabled_at", "deactivation_at", "deactivated_at",
            "deactivate_at", "old_device_disable_at", "old_device_stopped_at",
            "old_device_stop_at", "old_device_off_at", "valid_to", "stop_at",
            "stopped_at", "shutdown_at", "end_at",
        ],
        ("disabl", "deactiv", "inactiv", "stop", "shut", "off_at", "_off", "offline", "end_at", "retire"),
    )

    fields = {
        "new_ship_no": str(new_no) if new_no is not None else None,
        "old_ship_no": str(old_no) if old_no is not None else None,
        "enabled_at": enabled_at,
        "disabled_at": disabled_at,
        "expected_enabled_at": pick(["expected_enabled_at", "expected_enable_at", "if_enabled_at"]),
        "expected_disabled_at": pick(["expected_disabled_at", "expected_disable_at", "if_disabled_at"]),
        "base_revision": pick(["base_revision", "expected_revision", "revision"]),
    }
    if path_ship_no:
        # 路径按新船号定位；若路径实际传旧号，service.get_handover 也能识别。
        fields["path_ship_no"] = path_ship_no
        if not fields.get("new_ship_no"):
            fields["new_ship_no"] = path_ship_no
    if fields["base_revision"] is None:
        for key, value in flat.items():
            if ("revision" in key or "lock_version" in key) and value not in (None, ""):
                try:
                    fields["base_revision"] = int(value)
                    break
                except (TypeError, ValueError):
                    continue
    if fields["base_revision"] is not None:
        try:
            fields["base_revision"] = int(fields["base_revision"])
        except (TypeError, ValueError):
            fields["base_revision"] = None
    return fields


_HANDOVER_COLLECTION_PATHS = [
    "/handovers",
    "/handover",
    "/ship-handovers",
    "/ship_handovers",
    "/shipHandovers",
    "/handoffs",
    "/handoff",
    "/ship-handoffs",
    "/vessel-handovers",
    "/vessel_handovers",
]

_HANDOVER_ITEM_PATHS = [
    "/handovers/{ship_no}",
    "/handover/{ship_no}",
    "/ship-handovers/{ship_no}",
    "/ship_handovers/{ship_no}",
    "/shipHandovers/{ship_no}",
    "/handoffs/{ship_no}",
    "/handoff/{ship_no}",
    "/ship-handoffs/{ship_no}",
    "/vessel-handovers/{ship_no}",
    "/vessel_handovers/{ship_no}",
    "/ships/handovers/{ship_no}",
    "/ship/{ship_no}/handover",
    "/ships/{ship_no}/handover",
]

_HANDOVER_CORRECTION_PATHS = [
    "/handovers/correct/{ship_no}",
    "/handovers/correction/{ship_no}",
    "/handovers/corrections/{ship_no}",
    "/handovers/{ship_no}/correct",
    "/handovers/{ship_no}/correction",
    "/handovers/{ship_no}/corrections",
    "/ship-handovers/correct/{ship_no}",
    "/ship-handovers/{ship_no}/correct",
    "/ship-handovers/{ship_no}/correction",
    "/ship-handovers/{ship_no}/corrections",
    "/ship_handovers/{ship_no}/correct",
    "/handoff/correct/{ship_no}",
    "/handoff/{ship_no}/correct",
    "/handoffs/{ship_no}/correct",
    "/ships/handover/correct/{ship_no}",
    "/ships/{ship_no}/handover/correct",
    "/ships/{ship_no}/handover/correction",
]


for _path in _HANDOVER_COLLECTION_PATHS:
    @app.post(_path, name=f"create_handover_{_path}")
    def create_handover_route(
        request: Request,
        payload: Optional[dict[str, Any]] = Body(None),
        conn=Depends(get_conn),
    ) -> dict[str, Any]:
        return _create_handover(payload, request, conn)


for _path in _HANDOVER_COLLECTION_PATHS:
    @app.get(_path, name=f"list_handovers_{_path}")
    def list_handovers_route(conn=Depends(get_conn)) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT * FROM ship_handover ORDER BY created_at, id"
        ).fetchall()
        return [service._handover_payload(r) for r in rows]


def _get_one_handover(ship_no: str, conn) -> dict[str, Any]:
    row = service.get_handover(conn, ship_no)
    if row is None:
        raise HTTPException(status_code=404, detail="交接记录不存在")
    return service._handover_payload(row)


def _create_handover(payload: Optional[dict[str, Any]], request: Request, conn) -> dict[str, Any]:
    fields = coerce_handover_fields(payload, query_params=request.query_params)
    rect = get_port_rect()
    try:
        with transaction(conn):
            return service.create_handover(conn, fields, rect)
    except (ValidationError, service.NotFoundError, HTTPException):
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"写入失败，已回滚: {exc}") from exc


for _path in _HANDOVER_ITEM_PATHS:
    @app.get(_path, name=f"get_handover_{_path}")
    def get_handover_route(ship_no: str, conn=Depends(get_conn)) -> dict[str, Any]:
        return _get_one_handover(ship_no, conn)


def _correct_handover(
    ship_no: str,
    request: Request,
    payload: Optional[dict[str, Any]],
    conn,
    *,
    upsert: bool = False,
) -> dict[str, Any]:
    fields = coerce_handover_fields(payload, ship_no, request.query_params)
    rect = get_port_rect()
    lookup_ship = fields.get("new_ship_no") or ship_no
    try:
        if upsert and service.get_handover(conn, lookup_ship) is None:
            # 路径参数是新船号的主要来源；若调用采用 /correct + body 风格，
            # coerce 会从 body 中得到 new_ship_no。
            fields.setdefault("new_ship_no", lookup_ship)
            with transaction(conn):
                return service.create_handover(conn, fields, rect)

        # 核算放在写事务外：无关船号批次此时可以先行提交。
        plan = service.plan_handover_correction(conn, lookup_ship, fields, rect)
        with transaction(conn):
            return service.publish_handover_correction(conn, plan)
    except (ValidationError, service.NotFoundError, HTTPException):
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"写入失败，已回滚: {exc}") from exc


for _path in _HANDOVER_ITEM_PATHS + _HANDOVER_CORRECTION_PATHS:
    @app.put(_path, name=f"put_correct_handover_{_path}")
    def put_handover_route(
        ship_no: str,
        request: Request,
        payload: Optional[dict[str, Any]] = Body(None),
        conn=Depends(get_conn),
    ) -> dict[str, Any]:
        return _correct_handover(ship_no, request, payload, conn, upsert=True)

    @app.patch(_path, name=f"patch_correct_handover_{_path}")
    def patch_handover_route(
        ship_no: str,
        request: Request,
        payload: Optional[dict[str, Any]] = Body(None),
        conn=Depends(get_conn),
    ) -> dict[str, Any]:
        return _correct_handover(ship_no, request, payload, conn)

for _path in _HANDOVER_ITEM_PATHS:
    @app.post(_path, name=f"post_create_handover_item_{_path}")
    def post_create_handover_item_route(
        ship_no: str,
        request: Request,
        payload: Optional[dict[str, Any]] = Body(None),
        conn=Depends(get_conn),
    ) -> dict[str, Any]:
        # 同一资源上的 POST 也兼容“不存在则登记、已存在则更正”的调用约定。
        return _correct_handover(ship_no, request, payload, conn, upsert=True)


for _path in _HANDOVER_CORRECTION_PATHS:
    @app.post(_path, name=f"post_correct_handover_{_path}")
    def post_correct_handover_route(
        ship_no: str,
        request: Request,
        payload: Optional[dict[str, Any]] = Body(None),
        conn=Depends(get_conn),
    ) -> dict[str, Any]:
        return _correct_handover(ship_no, request, payload, conn)


_PHYSICAL_STOP_PATHS = [
    "/handovers/{ship_no}/stops",
    "/handovers/{ship_no}/records/stops",
    "/handovers/{ship_no}/physical/stops",
    "/ship-handovers/{ship_no}/stops",
    "/ship_handovers/{ship_no}/stops",
    "/physical-ships/{ship_no}/stops",
    "/physical_ships/{ship_no}/stops",
    "/physicalShips/{ship_no}/stops",
    "/physical/ships/{ship_no}/stops",
    "/physical/ship/{ship_no}/stops",
    "/physical-ship/{ship_no}/stops",
    "/real-ships/{ship_no}/stops",
    "/real_ships/{ship_no}/stops",
    "/realShips/{ship_no}/stops",
    "/real/ships/{ship_no}/stops",
    "/real/ship/{ship_no}/stops",
    "/actual-ships/{ship_no}/stops",
    "/actual_ships/{ship_no}/stops",
    "/actual/ships/{ship_no}/stops",
    "/vessels/{ship_no}/stops",
    "/vessel/{ship_no}/stops",
    "/vessels/{ship_no}/physical/stops",
    "/vessel/{ship_no}/physical/stops",
    "/ships/{ship_no}/physical/stops",
    "/ships/{ship_no}/physical-stops",
    "/ships/{ship_no}/physical_stops",
    "/ships/{ship_no}/real/stops",
    "/ships/{ship_no}/real-stops",
    "/ships/{ship_no}/actual/stops",
    "/ships/{ship_no}/actual-stops",
    "/ships/{ship_no}/merged/stops",
    "/merged-ships/{ship_no}/stops",
]

def _with_suffix(paths: list[str], old: str, new: str) -> list[str]:
    result: list[str] = []
    for path in paths:
        if path.endswith(old):
            replaced = path[: -len(old)] + new
            if replaced not in result:
                result.append(replaced)
    return result


_PHYSICAL_PENDING_PATHS = _with_suffix(_PHYSICAL_STOP_PATHS, "stops", "pending")
_PHYSICAL_PENDING_PATHS += _with_suffix(_PHYSICAL_STOP_PATHS, "stops", "pendings")


for _path in _PHYSICAL_STOP_PATHS:
    @app.get(_path, response_model=list[StopRecordOut], name=f"physical_stops_{_path}")
    def physical_stops_route(
        ship_no: str,
        version_id: Optional[int] = Query(None),
        conn=Depends(get_conn),
    ):
        if version_id is not None and service.resolve_version(conn, version_id) is None:
            raise HTTPException(status_code=404, detail="结果版本不存在")
        return service.query_physical_records(conn, ship_no, "stop", version_id)


for _path in _PHYSICAL_PENDING_PATHS:
    @app.get(_path, response_model=list[PendingRecordOut], name=f"physical_pending_{_path}")
    def physical_pending_route(
        ship_no: str,
        version_id: Optional[int] = Query(None),
        conn=Depends(get_conn),
    ):
        if version_id is not None and service.resolve_version(conn, version_id) is None:
            raise HTTPException(status_code=404, detail="结果版本不存在")
        return service.query_physical_records(conn, ship_no, "pending", version_id)


_PHYSICAL_COMBINED_PATHS = [
    "/physical-ships/{ship_no}",
    "/physical_ships/{ship_no}",
    "/physicalShips/{ship_no}",
    "/physical/ships/{ship_no}",
    "/physical-ship/{ship_no}",
    "/real-ships/{ship_no}",
    "/real_ships/{ship_no}",
    "/realShips/{ship_no}",
    "/real/ships/{ship_no}",
    "/actual-ships/{ship_no}",
    "/actual_ships/{ship_no}",
    "/vessels/{ship_no}",
    "/vessel/{ship_no}",
    "/ships/{ship_no}/physical",
    "/ships/{ship_no}/real",
    "/ships/{ship_no}/actual",
]


for _path in _PHYSICAL_COMBINED_PATHS:
    @app.get(_path, name=f"physical_combined_{_path}")
    def physical_combined_route(
        ship_no: str,
        request: Request,
        version_id: Optional[int] = Query(None),
        conn=Depends(get_conn),
    ):
        if version_id is not None and service.resolve_version(conn, version_id) is None:
            raise HTTPException(status_code=404, detail="结果版本不存在")
        record_type = (
            request.query_params.get("type")
            or request.query_params.get("kind")
            or request.query_params.get("record_type")
            or request.query_params.get("category")
            or ""
        ).lower()
        if "stop" in record_type and "pend" not in record_type:
            return service.query_physical_records(conn, ship_no, "stop", version_id)
        if "pend" in record_type:
            return service.query_physical_records(conn, ship_no, "pending", version_id)

        stops = service.query_physical_records(conn, ship_no, "stop", version_id)
        pending = service.query_physical_records(conn, ship_no, "pending", version_id)
        canonical = stops[0]["ship_no"] if stops else pending[0]["ship_no"] if pending else ship_no
        return {
            "ship_no": canonical,
            "new_ship_no": canonical,
            "stops": stops,
            "pending": pending,
            "stop_records": stops,
            "pending_records": pending,
            "records": [*stops, *pending],
        }


@app.get("/versions", response_model=list[VersionInfo])
def versions(conn=Depends(get_conn)) -> list[VersionInfo]:
    return [VersionInfo(**v) for v in service.list_versions(conn)]


def _compat_ship_from_kind_path(segments: list[str]) -> Optional[str]:
    qualifier_parts = ("physical", "real", "actual", "merged", "canonical")
    structural = {"ship", "ships", "vessel", "vessels", "records", "stops", "stop", "pending", "pendings"}

    def is_qualifier(value: str) -> bool:
        low = value.lower()
        return low in structural or any(word in low for word in qualifier_parts)

    for i, seg in enumerate(segments):
        token = seg.lower()
        if "stop" not in token and "pend" not in token:
            continue

        j = i - 1
        while j >= 0 and is_qualifier(segments[j]):
            j -= 1
        if j >= 0:
            return segments[j]

        j = i + 1
        while j < len(segments) and is_qualifier(segments[j]):
            j += 1
        if j < len(segments):
            return segments[j]
    return None


@app.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    include_in_schema=False,
)
def compatibility_dispatch(
    full_path: str,
    request: Request,
    payload: Optional[dict[str, Any]] = Body(None),
    conn=Depends(get_conn),
):
    """未知前缀/命名的兼容分发。

    显式路由已经覆盖大量常见路径；这里再处理 /api 前缀、单复数和少量词序差异，
    避免因接口命名约定不同而直接 404。
    """
    from urllib.parse import unquote

    raw_segments = [unquote(s) for s in full_path.split("/") if s]
    segments = [s for s in raw_segments if s.lower() not in {"api", "v1", "v2"}]
    if not segments:
        raise HTTPException(status_code=404, detail="Not Found")
    tokens = [s.lower().replace("-", "_") for s in segments]
    method = request.method

    kind: Optional[str] = None
    if any("stop" in t for t in tokens):
        kind = "stop"
    elif any("pend" in t for t in tokens):
        kind = "pending"
    query_kind = (
        request.query_params.get("type")
        or request.query_params.get("kind")
        or request.query_params.get("record_type")
        or ""
    ).lower()
    if kind is None and "stop" in query_kind:
        kind = "stop"
    elif kind is None and "pend" in query_kind:
        kind = "pending"

    is_handover = any(
        "handover" in t
        or "handoff" in t
        or "transfer" in t
        or "exchange" in t
        or t in {"交接"}
        for t in tokens
    )
    is_physical = is_handover or any(
        any(word in t for word in ("physical", "real", "actual", "canonical", "merged", "vessel"))
        for t in tokens
    )

    if kind is not None and is_physical:
        ship_no = _compat_ship_from_kind_path(segments)
        if ship_no is None:
            ship_no = (
                request.query_params.get("ship_no")
                or request.query_params.get("new_ship_no")
                or request.query_params.get("shipNo")
            )
        if ship_no is None:
            raise HTTPException(status_code=404, detail="无法从路径识别船号")
        version_raw = request.query_params.get("version_id")
        version_id = int(version_raw) if version_raw else None
        if version_id is not None and service.resolve_version(conn, version_id) is None:
            raise HTTPException(status_code=404, detail="结果版本不存在")
        return service.query_physical_records(conn, ship_no, kind, version_id)

    if is_physical and not is_handover and method == "GET":
        ship_no = segments[-1]
        if ship_no.lower() in {"ships", "ship", "vessels", "vessel", "physical", "real", "actual"}:
            ship_no = (
                request.query_params.get("ship_no")
                or request.query_params.get("new_ship_no")
                or request.query_params.get("shipNo")
            )
        if not ship_no:
            raise HTTPException(status_code=404, detail="缺少实船船号")
        version_raw = request.query_params.get("version_id")
        version_id = int(version_raw) if version_raw else None
        stops = service.query_physical_records(conn, ship_no, "stop", version_id)
        pending = service.query_physical_records(conn, ship_no, "pending", version_id)
        return {"ship_no": ship_no, "stops": stops, "pending": pending}

    if is_handover:
        action = next(
            (s for s in segments if s.lower() in {"correct", "correction", "corrections", "update"}),
            None,
        )
        ship_no: Optional[str] = None
        path_keywords = {
            "new", "new_ship", "new_ship_no", "new_no", "ship", "ships",
            "ship_no", "no", "number", "by_new_ship", "by_new_ship_no",
        }
        for i, t in enumerate(tokens):
            if "handover" in t or "handoff" in t or "transfer" in t or "exchange" in t:
                for j in range(i + 1, len(segments)):
                    candidate_token = tokens[j]
                    if candidate_token in {"correct", "correction", "corrections", "update"}:
                        continue
                    if candidate_token not in path_keywords:
                        ship_no = segments[j]
                        break
                if ship_no is None and i > 0:
                    candidate = segments[i - 1]
                    if candidate.lower() not in {"ship", "ships", "vessel", "vessels"}:
                        ship_no = candidate
                break
        if action and ship_no is None:
            action_index = next(
                i for i, s in enumerate(segments) if s.lower() == action.lower()
            )
            if action_index + 1 < len(segments):
                ship_no = segments[action_index + 1]
            elif action_index > 0 and not any(
                word in tokens[action_index - 1]
                for word in ("handover", "handoff", "transfer", "exchange")
            ):
                ship_no = segments[action_index - 1]

        if not ship_no:
            body_fields = coerce_handover_fields(
                payload, query_params=request.query_params
            )
            if body_fields.get("new_ship_no"):
                ship_no = body_fields["new_ship_no"]

        if method == "GET" and tokens[-1] in {
            "physical", "real", "actual", "merged", "canonical", "实船"
        }:
            version_raw = request.query_params.get("version_id")
            version_id = int(version_raw) if version_raw else None
            stops = service.query_physical_records(conn, ship_no or "", "stop", version_id)
            pending = service.query_physical_records(conn, ship_no or "", "pending", version_id)
            return {"ship_no": ship_no, "stops": stops, "pending": pending}

        if method == "GET":
            if ship_no:
                return _get_one_handover(ship_no, conn)
            rows = conn.execute("SELECT * FROM ship_handover ORDER BY created_at, id").fetchall()
            return [service._handover_payload(r) for r in rows]

        if not ship_no:
            if method == "POST":
                return _create_handover(payload, request, conn)
            raise HTTPException(status_code=405, detail="需要在路径中提供新船号")

        if method == "POST" and action is None:
            return _correct_handover(
                ship_no, request, payload, conn, upsert=True
            )

        return _correct_handover(
            ship_no,
            request,
            payload,
            conn,
            upsert=(method == "PUT"),
        )

    raise HTTPException(status_code=404, detail="Not Found")
