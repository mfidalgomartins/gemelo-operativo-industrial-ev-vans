from __future__ import annotations

import json
from pathlib import Path

from src.run_pipeline import run_pipeline


def test_ev_pipeline_official_path_end_to_end_without_regeneration() -> None:
    result = run_pipeline(generate_data=False)

    assert result.dashboard_path.endswith("outputs/dashboard/dashboard_gemelo_operativo_ev.html")
    assert result.validation_status in {"PASS", "WARN"}
    assert result.release_grade in {
        "publish-blocked",
        "screening-grade only",
        "decision-support only",
        "not committee-grade",
        "committee-grade candidate",
    }

    manifest = Path("outputs/reports/dashboard_build_manifest.json")
    release = Path("outputs/reports/release_readiness.json")
    validation = Path("outputs/reports/validation_report.md")
    pipeline_summary = Path("outputs/reports/pipeline_run_summary.json")

    assert manifest.exists()
    assert release.exists()
    assert validation.exists()
    assert pipeline_summary.exists()

    payload = json.loads(pipeline_summary.read_text(encoding="utf-8"))
    assert payload["dashboard_path"] == result.dashboard_path
