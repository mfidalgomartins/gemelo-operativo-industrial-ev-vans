from __future__ import annotations

import json
import os
import re
from pathlib import Path

from .connectors import ConnectorRegistry, CsvDirectoryConnector, HttpJsonConnector, SourceConnector
from .contracts import SOURCE_CONTRACTS, SourceSystem

_ENV_NAME = re.compile(r"^[A-Z][A-Z0-9_]*$")


def _require_keys(payload: dict[str, object], *, required: set[str], allowed: set[str], context: str) -> None:
    missing = sorted(required - payload.keys())
    unexpected = sorted(payload.keys() - allowed)
    if missing or unexpected:
        raise ValueError(f"{context}: configuración inválida; faltan={missing}, sobran={unexpected}")


def _csv_connector(payload: dict[str, object], *, config_dir: Path, system: SourceSystem) -> SourceConnector:
    _require_keys(
        payload,
        required={"kind", "source_dir"},
        allowed={"kind", "source_dir", "name"},
        context=system.value,
    )
    source_dir = Path(str(payload["source_dir"])).expanduser()
    if not source_dir.is_absolute():
        source_dir = config_dir / source_dir
    return CsvDirectoryConnector(source_dir.resolve(), name=str(payload.get("name", f"{system.value}_csv")))


def _http_connector(
    payload: dict[str, object],
    *,
    system: SourceSystem,
    production: bool,
) -> SourceConnector:
    required = {"kind", "base_url", "allowed_hosts", "token_env", "endpoints"}
    allowed = required | {"name", "timeout_seconds", "page_size", "max_pages", "max_response_bytes"}
    _require_keys(payload, required=required, allowed=allowed, context=system.value)

    token_env = str(payload["token_env"])
    if not _ENV_NAME.fullmatch(token_env):
        raise ValueError(f"{system.value}: token_env no es un nombre de variable válido")
    token = os.getenv(token_env)
    if token is None:
        raise ValueError(f"{system.value}: falta la variable de entorno {token_env}")

    hosts = payload["allowed_hosts"]
    endpoints = payload["endpoints"]
    if not isinstance(hosts, list) or not hosts or not all(isinstance(host, str) and host for host in hosts):
        raise ValueError(f"{system.value}: allowed_hosts debe ser una lista no vacía")
    if not isinstance(endpoints, dict) or not all(
        isinstance(table, str) and isinstance(endpoint, str) for table, endpoint in endpoints.items()
    ):
        raise ValueError(f"{system.value}: endpoints debe ser un objeto string -> string")
    expected_tables = {contract.name for contract in SOURCE_CONTRACTS.values() if contract.source_system is system}
    missing_endpoints = sorted(expected_tables - endpoints.keys())
    unexpected_endpoints = sorted(endpoints.keys() - expected_tables)
    if missing_endpoints or unexpected_endpoints:
        raise ValueError(
            f"{system.value}: cobertura de endpoints inválida; "
            f"faltan={missing_endpoints}, sobran={unexpected_endpoints}"
        )

    return HttpJsonConnector(
        name=str(payload.get("name", f"{system.value}_http")),
        base_url=str(payload["base_url"]),
        endpoints=endpoints,
        allowed_hosts=frozenset(hosts),
        bearer_token=token,
        production=production,
        timeout_seconds=float(payload.get("timeout_seconds", 20.0)),
        page_size=int(payload.get("page_size", 5_000)),
        max_pages=int(payload.get("max_pages", 1_000)),
        max_response_bytes=int(payload.get("max_response_bytes", 20_000_000)),
    )


def build_connector_registry(config_path: Path, *, production: bool = True) -> ConnectorRegistry:
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"No se pudo leer la configuración de conectores: {config_path}") from exc
    if not isinstance(payload, dict):
        raise ValueError("La configuración de conectores debe ser un objeto JSON")
    _require_keys(
        payload,
        required={"schema_version", "systems"},
        allowed={"schema_version", "systems"},
        context="raíz",
    )
    if payload["schema_version"] != 1 or not isinstance(payload["systems"], dict):
        raise ValueError("Versión o sección systems no válida")

    systems = payload["systems"]
    connectors: dict[SourceSystem, SourceConnector] = {}
    for system in SourceSystem:
        raw_connector = systems.get(system.value)
        if not isinstance(raw_connector, dict):
            raise ValueError(f"Falta configuración del sistema {system.value}")
        kind = raw_connector.get("kind")
        if kind == "csv_directory":
            connector = _csv_connector(raw_connector, config_dir=config_path.parent, system=system)
        elif kind == "http_json":
            connector = _http_connector(raw_connector, system=system, production=production)
        else:
            raise ValueError(f"{system.value}: kind no soportado: {kind!r}")
        connectors[system] = connector
    return ConnectorRegistry(connectors)
