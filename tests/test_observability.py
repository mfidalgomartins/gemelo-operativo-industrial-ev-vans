from __future__ import annotations

import json
from pathlib import Path

import pytest

from gemelo_operativo_ev.observability import PipelineRunRecorder, evaluate_sla


def test_pipeline_recorder_writes_stage_and_sla_manifest(tmp_path: Path) -> None:
    manifest = tmp_path / "observability" / "latest.json"
    recorder = PipelineRunRecorder(manifest)

    with recorder.stage("extract"):
        pass
    payload = recorder.finish(status="PASS", release_approved=True)

    persisted = json.loads(manifest.read_text(encoding="utf-8"))
    assert persisted["run_id"] == payload["run_id"]
    assert persisted["stages"][0]["name"] == "extract"
    assert persisted["stages"][0]["status"] == "PASS"
    assert persisted["sla"]["status"] == "PASS"


def test_pipeline_recorder_records_sanitized_failure(tmp_path: Path) -> None:
    recorder = PipelineRunRecorder(tmp_path / "failed.json")

    with pytest.raises(RuntimeError, match="secret detail"):
        with recorder.stage("load"):
            raise RuntimeError("secret detail")
    payload = recorder.finish(status="FAIL", release_approved=False, error_type="RuntimeError")

    assert payload["stages"][0]["status"] == "FAIL"
    assert payload["stages"][0]["error_type"] == "RuntimeError"
    assert "secret detail" not in json.dumps(payload)
    assert payload["sla"]["violations"] == ("release_gate",)


def test_sla_configuration_is_validated(monkeypatch) -> None:
    monkeypatch.setenv("EV_TWIN_SLA_MAX_RUN_SECONDS", "2")
    result = evaluate_sla(duration_seconds=3, release_approved=True)
    assert result.status == "FAIL"
    assert result.violations == ("pipeline_duration",)

    monkeypatch.setenv("EV_TWIN_SLA_MAX_RUN_SECONDS", "invalid")
    with pytest.raises(ValueError, match="debe ser numérico"):
        evaluate_sla(duration_seconds=1, release_approved=True)
