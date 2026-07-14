from __future__ import annotations

from functools import lru_cache
from importlib.resources import files

PAYLOAD_MARKER = "__DASHBOARD_PAYLOAD__"
VERSION_MARKER = "__DASHBOARD_VERSION__"


@lru_cache(maxsize=1)
def _template() -> str:
    template = files("gemelo_operativo_ev.dashboard").joinpath("dashboard.html").read_text(encoding="utf-8")
    if template.count(PAYLOAD_MARKER) != 1:
        raise ValueError("La plantilla del panel debe contener un único marcador de payload")
    if VERSION_MARKER not in template:
        raise ValueError("La plantilla del panel no contiene marcador de versión")
    return template


def render_dashboard_html(*, serialized_payload: str, version: str) -> str:
    if not serialized_payload or not version:
        raise ValueError("Payload y versión son obligatorios")
    return _template().replace(PAYLOAD_MARKER, serialized_payload).replace(VERSION_MARKER, version)
