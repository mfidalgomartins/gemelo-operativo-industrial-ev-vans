from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import json

from .config import EV_DATA_RAW_DIR, OUTPUT_CHARTS_DIR, OUTPUT_DASHBOARD_DIR, OUTPUT_REPORTS_DIR
from .create_notebooks import create_notebooks
from .ev_build_dashboard import run_ev_build_dashboard
from .ev_create_visuals import run_ev_create_visuals
from .ev_diagnostic_analysis import run_ev_diagnostic_analysis
from .ev_feature_engineering import run_ev_feature_engineering
from .ev_release_gate import run_release_gate
from .ev_scenario_twin import run_ev_scenario_twin
from .ev_scoring_framework import run_ev_scoring_framework
from .ev_sql_layer import run_ev_sql_layer
from .ev_validate_project import run_ev_validation
from .explore_data_audit import run_explore_data_audit
from .synthetic_data_gen import SyntheticGenerationConfig, generate_synthetic_factory_data


@dataclass
class PipelineRunResult:
    generation_enabled: bool
    dashboard_path: str
    release_grade: str
    release_approved: bool
    release_reason: str
    explore_report: str
    validation_status: str
    curated_removed_files: int


def _curate_outputs() -> int:
    """Mantém apenas artefactos de alto sinal para apresentação de portfolio."""
    remove_candidates = [
        OUTPUT_REPORTS_DIR / "dashboard_legacy_deprecated.md",
        OUTPUT_REPORTS_DIR / "data_quality_audit.json",
        OUTPUT_REPORTS_DIR / "data_quality_audit.md",
        OUTPUT_REPORTS_DIR / "explore_data_audit.html",
        OUTPUT_REPORTS_DIR / "explore_data_column_classification.csv",
        OUTPUT_REPORTS_DIR / "explore_data_table_summary.csv",
        OUTPUT_REPORTS_DIR / "final_assembly_report.md",
        OUTPUT_REPORTS_DIR / "kpi_summary.csv",
        OUTPUT_REPORTS_DIR / "synthetic_data_plausibility.md",
        OUTPUT_REPORTS_DIR / "synthetic_data_validation.json",
        OUTPUT_REPORTS_DIR / "synthetic_generation_run.json",
        OUTPUT_REPORTS_DIR / "visualizations_index.md",
        OUTPUT_REPORTS_DIR / "bottleneck_matrix.csv",
    ]
    removed = 0
    for path in remove_candidates:
        if path.exists():
            path.unlink()
            removed += 1

    # Conserva apenas gráficos EV oficiais.
    for chart in OUTPUT_CHARTS_DIR.glob("*.png"):
        if not chart.name.startswith("ev_"):
            chart.unlink()
            removed += 1

    # Limpa dashboards legacy arquivados.
    legacy_dir = OUTPUT_DASHBOARD_DIR / "legacy"
    if legacy_dir.exists():
        for p in legacy_dir.glob("*.html"):
            p.unlink()
            removed += 1

    return removed


def run_pipeline(generate_data: bool = False, seed: int = 20260328, months: int = 12) -> PipelineRunResult:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    if generate_data:
        cfg = SyntheticGenerationConfig(
            seed=seed,
            months=months,
            output_raw_dir=EV_DATA_RAW_DIR,
            output_report_dir=OUTPUT_REPORTS_DIR,
        )
        generate_synthetic_factory_data(cfg)

    run_explore_data_audit()
    run_ev_sql_layer()
    run_ev_feature_engineering()
    run_ev_diagnostic_analysis()
    run_ev_scenario_twin()
    run_ev_scoring_framework()
    run_ev_create_visuals()
    dashboard_result = run_ev_build_dashboard()
    validation_result = run_ev_validation()
    release_result = run_release_gate()
    curated_removed_files = _curate_outputs()
    create_notebooks()

    result = PipelineRunResult(
        generation_enabled=generate_data,
        dashboard_path=dashboard_result.path,
        release_grade=validation_result.release_grade,
        release_approved=release_result.approved,
        release_reason=release_result.reason,
        explore_report=str(Path("outputs/reports/explore_data_audit.md")),
        validation_status=validation_result.status,
        curated_removed_files=curated_removed_files,
    )

    (OUTPUT_REPORTS_DIR / "pipeline_run_summary.json").write_text(
        json.dumps(asdict(result), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return result


if __name__ == "__main__":
    run_pipeline(generate_data=False)
