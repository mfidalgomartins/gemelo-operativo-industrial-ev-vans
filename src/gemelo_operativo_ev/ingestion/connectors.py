from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Protocol
from urllib.parse import urlparse

import pandas as pd

from .contracts import SourceSystem, TableContract


@dataclass(frozen=True)
class ExtractBatch:
    table: str
    frame: pd.DataFrame
    source_ref: str
    extracted_at: datetime


class SourceConnector(Protocol):
    @property
    def connector_id(self) -> str: ...

    def extract(self, contract: TableContract, *, since: str | None = None) -> ExtractBatch: ...


@dataclass(frozen=True)
class CsvDirectoryConnector:
    source_dir: Path
    name: str = "csv_directory"

    @property
    def connector_id(self) -> str:
        return self.name

    def extract(self, contract: TableContract, *, since: str | None = None) -> ExtractBatch:
        path = self.source_dir / contract.filename
        if not path.is_file():
            raise FileNotFoundError(f"Falta fuente para {contract.name}: {path}")

        frame = pd.read_csv(path)
        contract.validate(frame)
        frame = contract.project(frame)
        if since is not None and contract.watermark_column is not None:
            watermark = pd.to_datetime(frame[contract.watermark_column], errors="raise", utc=True)
            since_ts = pd.Timestamp(since)
            since_ts = since_ts.tz_localize("UTC") if since_ts.tzinfo is None else since_ts.tz_convert("UTC")
            frame = frame.loc[watermark > since_ts].copy()

        contract.validate(frame, allow_empty=True)
        return ExtractBatch(
            table=contract.name,
            frame=frame,
            source_ref=f"csv://{contract.filename}",
            extracted_at=datetime.now(timezone.utc),
        )


@dataclass(frozen=True)
class HttpJsonConnector:
    """Conector paginado para APIs industriales con destinos preautorizados."""

    name: str
    base_url: str
    endpoints: Mapping[str, str]
    allowed_hosts: frozenset[str]
    bearer_token: str
    production: bool = True
    timeout_seconds: float = 20.0
    page_size: int = 5_000
    max_pages: int = 1_000
    max_response_bytes: int = 20_000_000
    _host: str = field(init=False, repr=False)
    _scheme: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        parsed = urlparse(self.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("base_url debe ser una URL HTTP(S) absoluta")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("base_url no puede contener credenciales, query ni fragmento")
        if self.production and parsed.scheme != "https":
            raise ValueError("Los conectores HTTP de producción requieren HTTPS")
        if parsed.hostname not in self.allowed_hosts:
            raise ValueError("El host del conector no figura en la allowlist")
        if len(self.bearer_token) < 32:
            raise ValueError("El token del conector debe tener al menos 32 caracteres")
        if self.timeout_seconds <= 0 or self.page_size <= 0 or self.max_pages <= 0:
            raise ValueError("Timeout, tamaño de página y máximo de páginas deben ser positivos")
        if self.max_response_bytes <= 0:
            raise ValueError("max_response_bytes debe ser positivo")

        for table, endpoint in self.endpoints.items():
            endpoint_url = urlparse(endpoint)
            endpoint_path = PurePosixPath(endpoint_url.path)
            if endpoint_url.scheme or endpoint_url.netloc or ".." in endpoint_path.parts:
                raise ValueError(f"Endpoint no permitido para {table}: debe ser una ruta relativa segura")
        object.__setattr__(self, "_host", parsed.hostname)
        object.__setattr__(self, "_scheme", parsed.scheme)

    @property
    def connector_id(self) -> str:
        return self.name

    def extract(self, contract: TableContract, *, since: str | None = None) -> ExtractBatch:
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - depende del extra service
            raise RuntimeError('Instalar el extra "service" para usar conectores HTTP') from exc

        endpoint = self.endpoints.get(contract.name)
        if endpoint is None:
            raise KeyError(f"{self.name}: no existe endpoint configurado para {contract.name}")

        items: list[dict[str, object]] = []
        cursor: str | None = None
        headers = {"Authorization": f"Bearer {self.bearer_token}", "Accept": "application/json"}
        with httpx.Client(
            base_url=self.base_url,
            headers=headers,
            timeout=self.timeout_seconds,
            follow_redirects=False,
            verify=True,
        ) as client:
            for _ in range(self.max_pages):
                params: dict[str, str | int] = {"limit": self.page_size}
                if since is not None:
                    params["since"] = since
                if cursor is not None:
                    params["cursor"] = cursor

                with client.stream("GET", endpoint, params=params) as response:
                    response.raise_for_status()
                    content_length = response.headers.get("content-length")
                    if content_length is not None and int(content_length) > self.max_response_bytes:
                        raise ValueError(f"{self.name}/{contract.name}: respuesta superior al límite permitido")
                    body = bytearray()
                    for chunk in response.iter_bytes():
                        body.extend(chunk)
                        if len(body) > self.max_response_bytes:
                            raise ValueError(f"{self.name}/{contract.name}: respuesta superior al límite permitido")
                try:
                    payload = json.loads(body)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{self.name}/{contract.name}: respuesta JSON inválida") from exc
                if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
                    raise ValueError(f"{self.name}/{contract.name}: respuesta JSON sin lista items")
                page_items = payload["items"]
                if not all(isinstance(item, dict) for item in page_items):
                    raise ValueError(f"{self.name}/{contract.name}: items contiene registros no válidos")
                items.extend(page_items)

                next_cursor = payload.get("next_cursor")
                if next_cursor is None:
                    break
                if not isinstance(next_cursor, str) or not next_cursor or next_cursor == cursor:
                    raise ValueError(f"{self.name}/{contract.name}: cursor de paginación inválido")
                cursor = next_cursor
            else:
                raise RuntimeError(f"{self.name}/{contract.name}: se alcanzó max_pages sin finalizar")

        frame = pd.DataFrame.from_records(items, columns=list(contract.columns))
        contract.validate(frame, allow_empty=True)
        return ExtractBatch(
            table=contract.name,
            frame=contract.project(frame),
            source_ref=f"{self._scheme}://{self._host}/{contract.name}",
            extracted_at=datetime.now(timezone.utc),
        )


@dataclass(frozen=True)
class ConnectorRegistry:
    connectors: Mapping[SourceSystem, SourceConnector]

    def for_contract(self, contract: TableContract) -> SourceConnector:
        try:
            return self.connectors[contract.source_system]
        except KeyError as exc:
            raise KeyError(f"No hay conector configurado para {contract.source_system.value}") from exc
