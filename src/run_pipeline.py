from __future__ import annotations

from .analysis import run_analysis
from .create_notebooks import create_notebooks
from .dashboard_builder import build_dashboard
from .data_generation import generate_raw_data
from .data_quality import run_data_quality_audit
from .reports import build_reports
from .scenario_engine import run_scenario_engine
from .sql_modeling import run_sql_modeling


def run_pipeline() -> None:
    generate_raw_data()
    run_data_quality_audit()
    run_sql_modeling()
    run_analysis()
    run_scenario_engine()
    build_dashboard()
    build_reports()
    create_notebooks()


if __name__ == "__main__":
    run_pipeline()
