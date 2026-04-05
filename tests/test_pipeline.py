from pathlib import Path

import pandas as pd

from src.analysis import run_analysis
from src.create_notebooks import create_notebooks
from src.dashboard_builder import build_dashboard
from src.data_generation import GenerationConfig, generate_raw_data
from src.data_quality import run_data_quality_audit
from src.reports import build_reports
from src.scenario_engine import run_scenario_engine
from src.sql_modeling import run_sql_modeling


ROOT = Path(__file__).resolve().parents[1]


def test_end_to_end_small_scale() -> None:
    cfg = GenerationConfig(seed=7, days=14, avg_orders_per_day=18, start_date="2025-01-01")
    tables = generate_raw_data(cfg)

    assert len(tables["ordenes_produccion"]) > 150
    ev_share = (tables["ordenes_produccion"]["tipo_propulsion"] == "EV").mean()
    assert 0.20 <= ev_share <= 0.70

    audit = run_data_quality_audit()
    assert audit.status in {"PASS", "WARN"}

    counts = run_sql_modeling()
    assert counts["scores_operativos"] > 0

    kpis = run_analysis()
    assert 0 <= kpis["cumplimiento_sla_expedicion_pct"] <= 100

    escenarios = run_scenario_engine()
    assert len(escenarios) == 5

    dashboard_path = build_dashboard()
    assert Path(dashboard_path).exists()

    build_reports()
    create_notebooks()

    scores = pd.read_csv(ROOT / "data" / "processed" / "scores_operativos.csv")
    assert scores["score_readiness_operativa"].between(0, 100).all()
    assert scores["score_riesgo_cuello_botella"].between(0, 100).all()
    assert scores["score_prioridad_despacho"].between(0, 100).all()

    assert (ROOT / "outputs" / "reports" / "validation_report.md").exists()
    assert (ROOT / "notebooks" / "01_notebook_principal.ipynb").exists()
