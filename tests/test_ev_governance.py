from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.config import EV_DATA_RAW_DIR, OUTPUT_REPORTS_DIR
from src.ev_build_dashboard import run_ev_build_dashboard
from src.ev_diagnostic_analysis import run_ev_diagnostic_analysis
from src.ev_feature_engineering import run_ev_feature_engineering
from src.ev_scenario_twin import run_ev_scenario_twin
from src.ev_scoring_framework import run_ev_scoring_framework
from src.ev_sql_layer import run_ev_sql_layer
from src.ev_validate_project import run_ev_validation
from src.synthetic_data_gen import SyntheticGenerationConfig, generate_synthetic_factory_data


def test_ev_release_governance_contract() -> None:
    cfg = SyntheticGenerationConfig(
        seed=20260402,
        start_date="2025-01-01",
        months=9,
        output_raw_dir=EV_DATA_RAW_DIR,
        output_report_dir=OUTPUT_REPORTS_DIR,
    )
    summary = generate_synthetic_factory_data(cfg)
    assert summary["validation"]["status_global"] == "PASS"

    run_ev_sql_layer()
    run_ev_feature_engineering()
    run_ev_diagnostic_analysis()
    run_ev_scenario_twin()
    run_ev_scoring_framework()
    run_ev_build_dashboard()
    val_res = run_ev_validation()

    mart_area = pd.read_csv(Path("data/processed/ev_factory/mart_area_shift.csv"))
    assert set(mart_area["turno"].dropna().unique()).issubset({"A", "B", "C"})
    assert {"A", "B", "C"}.issubset(set(mart_area["turno"].dropna().unique()))

    area_means = (
        mart_area.groupby("area", as_index=False)
        .agg(
            congestion_index=("congestion_index", "mean"),
            avg_wait_time=("avg_wait_time", "mean"),
            bottleneck_density=("bottleneck_density", "mean"),
            operational_stress_score=("operational_stress_score", "mean"),
        )
    )
    dispersion = area_means[["congestion_index", "avg_wait_time", "bottleneck_density", "operational_stress_score"]].std()
    assert (dispersion > 0).any()

    governance_checks = pd.read_csv(Path("data/processed/ev_factory/scoring_governance_checks.csv"))
    assert not governance_checks.empty
    assert set(governance_checks["status"].unique()).issubset({"PASS", "WARN"})

    release_path = OUTPUT_REPORTS_DIR / "release_readiness.json"
    assert release_path.exists()
    release = json.loads(release_path.read_text(encoding="utf-8"))
    required = {
        "status",
        "confidence",
        "release_grade",
        "technically_valid",
        "analytically_acceptable",
        "decision_support_only",
        "screening_grade_only",
        "committee_grade_candidate",
        "publish_blocked",
        "issues_total",
        "critical_issues",
        "high_issues",
        "medium_issues",
        "sql_warn_ratio",
        "kpi_single_source_of_truth",
    }
    assert required.issubset(release.keys())
    assert release["release_grade"] in {
        "publish-blocked",
        "screening-grade only",
        "decision-support only",
        "not committee-grade",
        "committee-grade candidate",
    }
    assert val_res.release_grade == release["release_grade"]
