from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

from gemelo_operativo_ev import cli
from gemelo_operativo_ev.calibration import CalibrationResult
from gemelo_operativo_ev.ev_release_gate import ReleaseGateResult
from gemelo_operativo_ev.ingestion.service import IngestionMode, IngestionResult
from gemelo_operativo_ev.run_pipeline import PipelineRunResult


def test_cli_generate_data_writes_run_manifest(tmp_path: Path, monkeypatch, capsys) -> None:
    raw_dir = tmp_path / "raw"
    reports_dir = tmp_path / "reports"

    def fake_generate(config):
        return {
            "validation": {"status_global": "PASS"},
            "cardinalidades": {"turnos": 2},
        }

    monkeypatch.setattr(cli, "generate_synthetic_factory_data", fake_generate)
    args = argparse.Namespace(
        seed=7,
        start_date="2025-01-01",
        months=9,
        output_raw=raw_dir,
        output_reports=reports_dir,
    )

    assert cli._run_generate_data(args) == 0
    assert (
        json.loads((reports_dir / "synthetic_generation_run.json").read_text(encoding="utf-8"))["validation"][
            "status_global"
        ]
        == "PASS"
    )
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"


def test_cli_run_release_status_and_parser(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "run_pipeline",
        lambda **kwargs: PipelineRunResult(
            generation_enabled=kwargs["generate_data"],
            dashboard_path="outputs/dashboard/dashboard.html",
            release_grade="decision-support only",
            release_approved=True,
            release_reason="Apta",
            explore_report="outputs/reports/explore.md",
            validation_status="PASS",
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_release_gate",
        lambda: ReleaseGateResult(
            approved=False,
            release_grade="publish-blocked",
            reason="Bloqueada",
        ),
    )
    monkeypatch.setattr(cli, "read_status", lambda **kwargs: {"release_status": "PASS"})

    assert cli._execute(argparse.Namespace(command="run", generate_data=True, seed=7, months=9)) == 0
    assert cli._execute(argparse.Namespace(command="release-check")) == 1
    assert cli._execute(argparse.Namespace(command="status")) == 0
    assert "release_status" in capsys.readouterr().out

    with pytest.raises(SystemExit) as exit_info:
        cli.main(["status"])
    assert exit_info.value.code == 0


def test_cli_ingest_and_calibrate(tmp_path: Path, monkeypatch, capsys) -> None:
    registry = object()
    captured: dict[str, object] = {}
    monkeypatch.setattr(cli, "build_connector_registry", lambda path, production: registry)

    def fake_ingestion(registry_arg, **kwargs):
        captured.update(kwargs)
        assert registry_arg is registry
        return IngestionResult(
            run_id="run-1",
            mode="incremental",
            started_at="2026-01-01T00:00:00+00:00",
            finished_at="2026-01-01T00:00:01+00:00",
            status="PASS",
            tables=(),
        )

    monkeypatch.setattr(cli, "run_ingestion", fake_ingestion)
    ingest_args = argparse.Namespace(
        command="ingest",
        config=tmp_path / "connectors.json",
        mode="incremental",
        tables=["turnos"],
        allow_http_development=False,
    )
    assert cli._execute(ingest_args) == 0
    assert captured["mode"] is IngestionMode.INCREMENTAL
    assert captured["tables"] == ("turnos",)

    source = tmp_path / "calibration.csv"
    pd.DataFrame({"input": [1]}).to_csv(source, index=False)
    output = tmp_path / "coefficients.csv"
    monkeypatch.setattr(
        cli,
        "calibrate_scenario_coefficients",
        lambda frame, config: CalibrationResult(
            coefficients=pd.DataFrame({"metric": ["throughput"], "estimate": [0.1]}),
            metrics_estimated=1,
            observations=len(frame),
        ),
    )
    calibrate_args = argparse.Namespace(
        command="calibrate",
        input=source,
        output=output,
        min_observations=1,
        min_clusters=2,
    )
    assert cli._execute(calibrate_args) == 0
    assert output.exists()
    assert '"status": "PASS"' in capsys.readouterr().out


def test_report_import_does_not_read_marts(monkeypatch) -> None:
    module_name = "gemelo_operativo_ev.reporting.report"
    sys.modules.pop(module_name, None)

    def fail_on_read(*args, **kwargs):
        raise AssertionError("El módulo no debe leer marts durante el import")

    monkeypatch.setattr(pd, "read_csv", fail_on_read)
    importlib.import_module(module_name)


def test_cli_artifact_commands_delegate(monkeypatch) -> None:
    from gemelo_operativo_ev.reporting import chart_pack, report

    calls: list[str] = []
    monkeypatch.setattr(chart_pack, "main", lambda: calls.append("charts"))
    monkeypatch.setattr(report, "main", lambda: calls.append("report"))

    assert cli._execute(argparse.Namespace(command="charts")) == 0
    assert cli._execute(argparse.Namespace(command="report")) == 0
    assert cli._execute(argparse.Namespace(command="artifacts")) == 0
    assert calls == ["charts", "report", "charts", "report"]


def test_cli_rejects_unknown_command() -> None:
    with pytest.raises(RuntimeError, match="no implementado"):
        cli._execute(argparse.Namespace(command="unknown"))
