from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
EV_DATA_RAW_DIR = DATA_RAW_DIR / "ev_factory"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
SQL_DIR = PROJECT_ROOT / "sql"
OUTPUT_DASHBOARD_DIR = PROJECT_ROOT / "outputs" / "dashboard"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"


def ensure_directories() -> None:
    for path in [
        DATA_RAW_DIR,
        EV_DATA_RAW_DIR,
        DATA_PROCESSED_DIR,
        OUTPUT_DASHBOARD_DIR,
        OUTPUT_REPORTS_DIR,
        NOTEBOOKS_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)
