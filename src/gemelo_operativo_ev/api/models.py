from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LiveResponse(StrictModel):
    status: Literal["ok"]
    service: Literal["gemelo-operativo-ev"]
    version: str


class ReadyResponse(StrictModel):
    status: Literal["ready", "not_ready"]
    release_status: str
    pipeline_status: str
    sla_status: str


class StatusResponse(StrictModel):
    package_version: str
    release_status: str
    release_grade: str
    dashboard_version: str | None
    pipeline_status: str
    pipeline_run_id: str | None
    pipeline_duration_seconds: float | None
    sla_status: str


class KpiSnapshotResponse(StrictModel):
    total_ordenes: int
    throughput_real: int
    throughput_gap: int
    share_ev: float
    ocupacion_pico_patio: float
    utilizacion_media_cargadores: float
    tiempo_medio_espera_carga_min: float
    vehiculos_no_ready: int
    ratio_salida_retrasada: float
    score_readiness_global: float
    causa_principal_cuello: str
    area_mayor_perdida_throughput: str


class PriorityResponse(StrictModel):
    area: str
    operational_priority_index: float
    main_risk_driver: str
    recommended_action: str
    area_priority_tier: str


class PriorityListResponse(StrictModel):
    items: list[PriorityResponse]
    count: int


class LineageResponse(StrictModel):
    available: bool
    run_id: str | None = None
    status: str | None = None
    mode: str | None = None
    finished_at: str | None = None
    table_count: int = 0


class ReleaseCheckResponse(StrictModel):
    approved: bool
    release_grade: str
    reason: str


class ErrorResponse(StrictModel):
    detail: str
    request_id: str | None = Field(default=None)
