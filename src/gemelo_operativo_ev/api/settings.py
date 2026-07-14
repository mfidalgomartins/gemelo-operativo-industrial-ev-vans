from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ApiSettings:
    environment: str
    trusted_hosts: tuple[str, ...]
    viewer_token: str = field(repr=False)
    operator_token: str = field(repr=False)
    docs_enabled: bool = False
    max_request_bytes: int = 1_048_576

    def __post_init__(self) -> None:
        if self.environment not in {"development", "test", "production"}:
            raise ValueError("environment debe ser development, test o production")
        if not self.trusted_hosts or any(not host or "/" in host for host in self.trusted_hosts):
            raise ValueError("trusted_hosts debe contener hosts válidos")
        if len(self.viewer_token) < 32 or len(self.operator_token) < 32:
            raise ValueError("Los tokens API deben tener al menos 32 caracteres")
        if self.viewer_token == self.operator_token:
            raise ValueError("Los tokens viewer y operator deben ser distintos")
        if self.environment == "production" and self.docs_enabled:
            raise ValueError("La documentación interactiva no se expone en producción")
        if self.max_request_bytes <= 0:
            raise ValueError("max_request_bytes debe ser positivo")

    @classmethod
    def from_env(cls) -> ApiSettings:
        environment = os.getenv("EV_TWIN_ENV", "production").strip().lower()
        trusted_hosts = tuple(
            host.strip() for host in os.getenv("EV_TWIN_TRUSTED_HOSTS", "").split(",") if host.strip()
        )
        viewer_token = os.getenv("EV_TWIN_VIEWER_TOKEN", "")
        operator_token = os.getenv("EV_TWIN_OPERATOR_TOKEN", "")
        docs_enabled = os.getenv("EV_TWIN_DOCS_ENABLED", "false").strip().lower() == "true"
        try:
            max_request_bytes = int(os.getenv("EV_TWIN_MAX_REQUEST_BYTES", "1048576"))
        except ValueError as exc:
            raise ValueError("EV_TWIN_MAX_REQUEST_BYTES debe ser entero") from exc
        return cls(
            environment=environment,
            trusted_hosts=trusted_hosts,
            viewer_token=viewer_token,
            operator_token=operator_token,
            docs_enabled=docs_enabled,
            max_request_bytes=max_request_bytes,
        )
