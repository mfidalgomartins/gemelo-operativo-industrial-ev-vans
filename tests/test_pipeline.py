from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from gemelo_operativo_ev.config import PROJECT_ROOT
from gemelo_operativo_ev.run_pipeline import _relative, run_pipeline


@pytest.mark.integration
def test_ev_pipeline_official_path_end_to_end_without_regeneration() -> None:
    result = run_pipeline(generate_data=False)

    assert result.dashboard_path.endswith("outputs/dashboard/industrial-ev-operating-command-center.html")
    assert result.validation_status in {"PASS", "WARN"}
    assert result.release_grade in {
        "publish-blocked",
        "screening-grade only",
        "decision-support only",
        "not committee-grade",
    }

    manifest = Path("outputs/reports/dashboard_build_manifest.json")
    release = Path("outputs/reports/release_readiness.json")
    validation = Path("outputs/reports/pipeline_audit/validation_report.md")
    pipeline_summary = Path("outputs/reports/pipeline_audit/pipeline_run_summary.json")

    assert manifest.exists()
    assert release.exists()
    assert validation.exists()
    assert pipeline_summary.exists()

    payload = json.loads(pipeline_summary.read_text(encoding="utf-8"))
    assert payload["dashboard_path"] == result.dashboard_path
    assert not Path(payload["dashboard_path"]).is_absolute()
    assert not Path(payload["explore_report"]).is_absolute()


def test_pipeline_relative_keeps_project_paths_portable() -> None:
    absolute_dashboard = PROJECT_ROOT / "outputs" / "dashboard" / "dashboard.html"

    assert _relative(str(absolute_dashboard)) == "outputs/dashboard/dashboard.html"
    assert _relative("outputs/reports/report.json") == "outputs/reports/report.json"


def test_pipeline_relative_rejects_absolute_paths_outside_project(tmp_path: Path) -> None:
    outside_path = tmp_path / "external.html"

    with pytest.raises(ValueError):
        _relative(str(outside_path))


def test_run_pipeline_executes_stages_in_order_and_writes_portable_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    fake_project_root = tmp_path / "project"
    reports_dir = fake_project_root / "outputs" / "reports"
    audit_dir = reports_dir / "pipeline_audit"
    runtime_dir = fake_project_root / ".ev_twin"

    def record(name: str):
        def _inner():
            calls.append(name)

        return _inner

    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.PROJECT_ROOT", fake_project_root)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.OUTPUT_REPORTS_DIR", reports_dir)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.OUTPUT_REPORTS_AUDIT_DIR", audit_dir)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.RUNTIME_STATE_DIR", runtime_dir)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.run_explore_data_audit", record("audit"))
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.run_ev_sql_layer", record("sql"))
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.run_ev_feature_engineering", record("features"))
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.run_ev_diagnostic_analysis", record("diagnostic"))
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.run_ev_scenario_twin", record("scenario"))
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.run_ev_scoring_framework", record("scoring"))
    monkeypatch.setattr(
        "gemelo_operativo_ev.run_pipeline.run_ev_build_dashboard",
        lambda: SimpleNamespace(path="outputs/dashboard/test-dashboard.html"),
    )
    monkeypatch.setattr(
        "gemelo_operativo_ev.run_pipeline.run_ev_validation",
        lambda: SimpleNamespace(status="WARN", release_grade="screening-grade only"),
    )
    monkeypatch.setattr(
        "gemelo_operativo_ev.run_pipeline.run_release_gate",
        lambda: SimpleNamespace(approved=False, reason="Publicación bloqueada por validación"),
    )

    result = run_pipeline(generate_data=False)

    assert calls == ["audit", "sql", "features", "diagnostic", "scenario", "scoring"]
    assert result.generation_enabled is False
    assert result.dashboard_path == "outputs/dashboard/test-dashboard.html"
    assert result.release_grade == "screening-grade only"
    assert result.release_approved is False
    assert result.validation_status == "WARN"

    summary = json.loads((audit_dir / "pipeline_run_summary.json").read_text(encoding="utf-8"))
    assert summary == {
        "generation_enabled": False,
        "dashboard_path": "outputs/dashboard/test-dashboard.html",
        "release_grade": "screening-grade only",
        "release_approved": False,
        "release_reason": "Publicación bloqueada por validación",
        "explore_report": "outputs/reports/pipeline_audit/explore_data_audit.md",
        "validation_status": "WARN",
    }


def test_run_pipeline_generation_uses_requested_seed_months_and_output_dirs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    fake_project_root = tmp_path / "project"
    raw_dir = fake_project_root / "data" / "raw" / "ev_factory"
    reports_dir = fake_project_root / "outputs" / "reports"
    audit_dir = reports_dir / "pipeline_audit"
    runtime_dir = fake_project_root / ".ev_twin"

    def fake_generate(cfg) -> None:
        captured["seed"] = cfg.seed
        captured["months"] = cfg.months
        captured["output_raw_dir"] = cfg.output_raw_dir
        captured["output_report_dir"] = cfg.output_report_dir

    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.PROJECT_ROOT", fake_project_root)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.OUTPUT_REPORTS_DIR", reports_dir)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.OUTPUT_REPORTS_AUDIT_DIR", audit_dir)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.RUNTIME_STATE_DIR", runtime_dir)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.EV_DATA_RAW_DIR", raw_dir)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.generate_synthetic_factory_data", fake_generate)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.run_explore_data_audit", lambda: None)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.run_ev_sql_layer", lambda: None)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.run_ev_feature_engineering", lambda: None)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.run_ev_diagnostic_analysis", lambda: None)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.run_ev_scenario_twin", lambda: None)
    monkeypatch.setattr("gemelo_operativo_ev.run_pipeline.run_ev_scoring_framework", lambda: None)
    monkeypatch.setattr(
        "gemelo_operativo_ev.run_pipeline.run_ev_build_dashboard",
        lambda: SimpleNamespace(path="outputs/dashboard/test-dashboard.html"),
    )
    monkeypatch.setattr(
        "gemelo_operativo_ev.run_pipeline.run_ev_validation",
        lambda: SimpleNamespace(status="PASS", release_grade="decision-support only"),
    )
    monkeypatch.setattr(
        "gemelo_operativo_ev.run_pipeline.run_release_gate",
        lambda: SimpleNamespace(approved=True, reason="Publicación apta"),
    )

    result = run_pipeline(generate_data=True, seed=99, months=3)

    assert result.generation_enabled is True
    assert captured == {
        "seed": 99,
        "months": 3,
        "output_raw_dir": raw_dir,
        "output_report_dir": audit_dir,
    }
