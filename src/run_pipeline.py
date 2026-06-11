from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from .config import EV_DATA_RAW_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT
from .ev_build_dashboard import run_ev_build_dashboard
from .ev_diagnostic_analysis import run_ev_diagnostic_analysis
from .ev_feature_engineering import run_ev_feature_engineering
from .ev_release_gate import run_release_gate
from .ev_scenario_twin import run_ev_scenario_twin
from .ev_scoring_framework import run_ev_scoring_framework
from .ev_sql_layer import run_ev_sql_layer
from .ev_validate_project import run_ev_validation
from .explore_data_audit import run_explore_data_audit
from .synthetic_data_gen import SyntheticGenerationConfig, generate_synthetic_factory_data
from .utils import write_json_utf8


@dataclass
class PipelineRunResult:
    generation_enabled: bool
    dashboard_path: str
    release_grade: str
    release_approved: bool
    release_reason: str
    explore_report: str
    validation_status: str


def _relative(path: str) -> str:
    value = Path(path)
    return str(value.resolve().relative_to(PROJECT_ROOT)) if value.is_absolute() else value.as_posix()


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
    dashboard_result = run_ev_build_dashboard()
    validation_result = run_ev_validation()
    release_result = run_release_gate()

    result = PipelineRunResult(
        generation_enabled=generate_data,
        dashboard_path=_relative(dashboard_result.path),
        release_grade=validation_result.release_grade,
        release_approved=release_result.approved,
        release_reason=release_result.reason,
        explore_report=str((OUTPUT_REPORTS_DIR / "explore_data_audit.md").relative_to(PROJECT_ROOT)),
        validation_status=validation_result.status,
    )

    write_json_utf8(OUTPUT_REPORTS_DIR / "pipeline_run_summary.json", asdict(result))
    return result


if __name__ == "__main__":
    run_pipeline(generate_data=False)
