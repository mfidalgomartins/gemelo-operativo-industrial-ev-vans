from __future__ import annotations

import json
import logging
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, Query, Request
from fastapi.responses import JSONResponse, Response
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .. import __version__
from ..config import DATA_PROCESSED_DIR, OUTPUT_REPORTS_DIR, RUNTIME_STATE_DIR
from ..ev_release_gate import run_release_gate
from .auth import BearerAuthorizer, Role
from .models import (
    KpiSnapshotResponse,
    LineageResponse,
    LiveResponse,
    PriorityListResponse,
    ReadyResponse,
    ReleaseCheckResponse,
    StatusResponse,
)
from .repository import DataUnavailableError, read_kpi_snapshot, read_lineage, read_priorities, read_status
from .settings import ApiSettings

LOGGER = logging.getLogger("gemelo_operativo_ev.api")


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    settings = settings or ApiSettings.from_env()
    docs_url = "/docs" if settings.docs_enabled else None
    openapi_url = "/openapi.json" if settings.docs_enabled else None
    app = FastAPI(
        title="Gemelo Operativo EV API",
        version=__version__,
        debug=False,
        docs_url=docs_url,
        redoc_url=None,
        openapi_url=openapi_url,
    )
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(settings.trusted_hosts))
    authorizer = BearerAuthorizer(settings)

    @app.middleware("http")
    async def security_middleware(request: Request, call_next):
        request_id = uuid.uuid4().hex
        content_length = request.headers.get("content-length")
        response: Response
        if content_length is not None:
            try:
                body_size = int(content_length)
            except ValueError:
                response = JSONResponse(
                    status_code=400, content={"detail": "Content-Length inválido", "request_id": request_id}
                )
            else:
                response = (
                    JSONResponse(
                        status_code=413,
                        content={"detail": "Cuerpo de petición demasiado grande", "request_id": request_id},
                    )
                    if body_size > settings.max_request_bytes
                    else await call_next(request)
                )
        else:
            response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(DataUnavailableError)
    async def data_unavailable_handler(request: Request, exc: DataUnavailableError) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    @app.exception_handler(Exception)
    async def internal_error_handler(request: Request, exc: Exception) -> JSONResponse:
        LOGGER.error(
            json.dumps(
                {
                    "event": "api_unhandled_error",
                    "path": request.url.path,
                    "error_type": type(exc).__name__,
                },
                separators=(",", ":"),
            )
        )
        return JSONResponse(status_code=500, content={"detail": "Error interno"})

    @app.get("/health/live", response_model=LiveResponse, tags=["health"])
    async def live() -> LiveResponse:
        return LiveResponse(status="ok", service="gemelo-operativo-ev", version=__version__)

    @app.get("/health/ready", response_model=ReadyResponse, tags=["health"])
    async def ready() -> ReadyResponse:
        try:
            status_payload = read_status(reports_dir=OUTPUT_REPORTS_DIR, runtime_state_dir=RUNTIME_STATE_DIR)
        except DataUnavailableError:
            return ReadyResponse(
                status="not_ready",
                release_status="UNKNOWN",
                pipeline_status="UNKNOWN",
                sla_status="UNKNOWN",
            )
        is_ready = (
            status_payload["release_status"] == "PASS"
            and status_payload["pipeline_status"] == "PASS"
            and status_payload["sla_status"] == "PASS"
        )
        return ReadyResponse(
            status="ready" if is_ready else "not_ready",
            release_status=str(status_payload["release_status"]),
            pipeline_status=str(status_payload["pipeline_status"]),
            sla_status=str(status_payload["sla_status"]),
        )

    viewer_router = APIRouter(
        prefix="/v1",
        dependencies=[Depends(authorizer.require(Role.VIEWER))],
    )

    @viewer_router.get("/status", response_model=StatusResponse)
    async def status_endpoint() -> StatusResponse:
        payload = read_status(reports_dir=OUTPUT_REPORTS_DIR, runtime_state_dir=RUNTIME_STATE_DIR)
        return StatusResponse(package_version=__version__, **payload)

    @viewer_router.get("/kpis", response_model=KpiSnapshotResponse)
    async def kpis_endpoint() -> KpiSnapshotResponse:
        return KpiSnapshotResponse(**read_kpi_snapshot(DATA_PROCESSED_DIR))

    @viewer_router.get("/priorities", response_model=PriorityListResponse)
    async def priorities_endpoint(limit: Annotated[int, Query(ge=1, le=50)] = 10) -> PriorityListResponse:
        items = read_priorities(DATA_PROCESSED_DIR, limit=limit)
        return PriorityListResponse(items=items, count=len(items))

    @viewer_router.get("/lineage", response_model=LineageResponse)
    async def lineage_endpoint() -> LineageResponse:
        return LineageResponse(**read_lineage(RUNTIME_STATE_DIR))

    app.include_router(viewer_router)

    operator_router = APIRouter(
        prefix="/v1/operator",
        dependencies=[Depends(authorizer.require(Role.OPERATOR))],
    )

    @operator_router.post("/release-check", response_model=ReleaseCheckResponse)
    async def release_check() -> ReleaseCheckResponse:
        result = run_release_gate()
        return ReleaseCheckResponse(
            approved=result.approved,
            release_grade=result.release_grade,
            reason=result.reason,
        )

    app.include_router(operator_router)
    return app
