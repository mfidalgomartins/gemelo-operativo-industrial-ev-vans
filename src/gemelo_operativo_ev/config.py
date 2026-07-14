from __future__ import annotations

import os
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent


def _resolve_project_root() -> Path:
    configured_root = os.getenv("EV_TWIN_HOME")
    if configured_root:
        return Path(configured_root).expanduser().resolve()

    source_checkout = PACKAGE_ROOT.parents[1]
    if (source_checkout / "pyproject.toml").is_file():
        return source_checkout
    return Path.cwd().resolve()


PROJECT_ROOT = _resolve_project_root()
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
EV_DATA_RAW_DIR = DATA_RAW_DIR / "ev_factory"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
SQL_DIR = PACKAGE_ROOT / "sql"
OUTPUT_DASHBOARD_DIR = PROJECT_ROOT / "outputs" / "dashboard"
OUTPUT_GRAPHS_DIR = PROJECT_ROOT / "outputs" / "graphs"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
RUNTIME_STATE_DIR = PROJECT_ROOT / ".ev_twin"


def ensure_directories() -> None:
    for path in [
        DATA_RAW_DIR,
        EV_DATA_RAW_DIR,
        DATA_PROCESSED_DIR,
        OUTPUT_DASHBOARD_DIR,
        OUTPUT_GRAPHS_DIR,
        OUTPUT_REPORTS_DIR,
        NOTEBOOKS_DIR,
        RUNTIME_STATE_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)
