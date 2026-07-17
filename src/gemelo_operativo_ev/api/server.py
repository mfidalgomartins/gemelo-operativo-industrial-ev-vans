from __future__ import annotations

import os


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} debe ser entero") from exc
    if value <= 0:
        raise ValueError(f"{name} debe ser positivo")
    return value


def main() -> None:
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover - depende del extra service
        raise RuntimeError('Instalar el extra "service" para ejecutar la API') from exc

    host = os.getenv("EV_TWIN_API_HOST", "127.0.0.1").strip()
    if not host or "/" in host:
        raise ValueError("EV_TWIN_API_HOST no es válido")

    uvicorn.run(
        "gemelo_operativo_ev.api.app:create_app",
        factory=True,
        host=host,
        port=_positive_int("EV_TWIN_API_PORT", 8000),
        workers=_positive_int("EV_TWIN_API_WORKERS", 1),
        proxy_headers=False,
        server_header=False,
    )


if __name__ == "__main__":
    main()
