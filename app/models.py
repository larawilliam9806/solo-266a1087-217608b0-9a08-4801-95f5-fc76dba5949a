"""Pydantic 请求/响应模型。"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


class AISMessageIn(BaseModel):
    msg_no: str = Field(min_length=1, description="消息号")
    ship_no: str = Field(min_length=1, description="船号")
    ts: str = Field(description="UTC 时刻，ISO-8601，如 2026-11-24T08:00:00Z")
    lon: float = Field(ge=-180.0, le=180.0, description="经度")
    lat: float = Field(ge=-90.0, le=90.0, description="纬度")

    @field_validator("ts")
    @classmethod
    def _check_utc(cls, v: str) -> str:
        from .engine import ValidationError, parse_utc

        try:
            parse_utc(v)  # 抛异常即 422
        except ValidationError as exc:
            raise ValueError(str(exc)) from exc
        return v


class BatchSubmit(BaseModel):
    messages: list[AISMessageIn] = Field(min_length=1)


class PointOut(BaseModel):
    msg_no: str
    ship_no: Optional[str] = None
    reported_ship_no: Optional[str] = None
    report_ship_no: Optional[str] = None
    original_ship_no: Optional[str] = None
    origin_ship_no: Optional[str] = None
    source_ship_no: Optional[str] = None
    ts: str
    lon: float
    lat: float
    in_port: bool
    source_batches: list[str]


class StopRecordOut(BaseModel):
    ship_no: str
    physical_ship_no: Optional[str] = None
    actual_ship_no: Optional[str] = None
    new_ship_no: Optional[str] = None
    seg_no: int
    run_no: int
    seg_key: str
    entry_point: PointOut
    exit_point: PointOut
    before_point: PointOut
    after_point: PointOut
    kind: Literal["stop"] = "stop"


class PendingRecordOut(BaseModel):
    ship_no: str
    physical_ship_no: Optional[str] = None
    actual_ship_no: Optional[str] = None
    new_ship_no: Optional[str] = None
    seg_no: int
    run_no: int
    seg_key: str
    entry_point: PointOut
    exit_point: PointOut
    before_point: Optional[PointOut]
    after_point: Optional[PointOut]
    reason: str
    kind: Literal["pending"] = "pending"


class BatchResult(BaseModel):
    batch_no: str
    mode: Literal["submit", "replace"]
    status: Literal["accepted", "rejected"]
    version_id: Optional[int] = None
    error: Optional[str] = None
    messages_accepted: int
    affected_ships: list[str] = []
    reused_records: int = 0
    recomputed_records: int = 0
    physical_version_id: Optional[int] = None
    physical_reused_records: int = 0
    physical_recomputed_records: int = 0
    affected_physical_ships: list[str] = []


class BatchInfo(BaseModel):
    batch_no: str
    created_at: str
    replaced_at: Optional[str]
    active: bool
    message_count: int


class VersionInfo(BaseModel):
    version_id: int
    created_at: str
    batches: list[str]
    active: bool


class HandoverUpsert(BaseModel):
    """交接登记/更正入参。

    同时兼容常见命名；路由层还会从 extra 与嵌套对象中提取别名，
    以免不同调用方对两个时刻字段的命名差异。
    """

    model_config = {"extra": "allow", "populate_by_name": True}

    new_ship_no: Optional[str] = None
    old_ship_no: Optional[str] = None
    enabled_at: Optional[str] = None
    disabled_at: Optional[str] = None
    new_device_enabled_at: Optional[str] = None
    old_device_disabled_at: Optional[str] = None
    new_machine_enabled_at: Optional[str] = None
    old_machine_disabled_at: Optional[str] = None
    enable_at: Optional[str] = None
    disable_at: Optional[str] = None
    activated_at: Optional[str] = None
    deactivated_at: Optional[str] = None
    expected_enabled_at: Optional[str] = None
    expected_disabled_at: Optional[str] = None
    base_revision: Optional[int] = None
