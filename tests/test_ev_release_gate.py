from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.config import OUTPUT_REPORTS_DIR
from src.ev_release_gate import run_release_gate

_REQUIRED_FILES = [
    OUTPUT_REPORTS_DIR / "release_readiness.json",
    OUTPUT_REPORTS_DIR / "dashboard_build_manifest.json",
]
_PIPELINE_OUTPUTS_EXIST = all(p.exists() for p in _REQUIRED_FILES)


@pytest.mark.skipif(
    not _PIPELINE_OUTPUTS_EXIST,
    reason="Pipeline outputs not present — run `python -m src.run_pipeline` first",
)
def test_ev_release_gate_uses_generated_governance_outputs() -> None:
    result = run_release_gate()
    assert result.release_grade in {
        "publish-blocked",
        "screening-grade only",
        "decision-support only",
        "not committee-grade",
    }
    if result.approved:
        assert result.reason.startswith("Release apto")


def test_ev_release_gate_returns_unapproved_when_readiness_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """run_release_gate returns approved=False, grade='unknown' when files are absent."""
    monkeypatch.setattr("src.ev_release_gate.OUTPUT_REPORTS_DIR", tmp_path)
    result = run_release_gate()
    assert not result.approved
    assert result.release_grade == "unknown"
    assert "release_readiness.json" in result.reason


def test_ev_release_gate_returns_unapproved_when_manifest_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Gate returns unapproved when manifest is missing but readiness file exists."""
    readiness = {
        "release_grade": "decision-support only",
        "publish_blocked": False,
        "kpi_single_source_of_truth": True,
    }
    (tmp_path / "release_readiness.json").write_text(json.dumps(readiness), encoding="utf-8")

    monkeypatch.setattr("src.ev_release_gate.OUTPUT_REPORTS_DIR", tmp_path)
    result = run_release_gate()
    assert not result.approved
    assert "dashboard_build_manifest.json" in result.reason


@pytest.mark.parametrize(
    "filename, expected_reason",
    [
        ("release_readiness.json", "release_readiness.json no es JSON válido"),
        ("dashboard_build_manifest.json", "dashboard_build_manifest.json no es JSON válido"),
    ],
)
def test_ev_release_gate_rejects_invalid_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    expected_reason: str,
) -> None:
    readiness = {
        "release_grade": "decision-support only",
        "publish_blocked": False,
        "kpi_single_source_of_truth": True,
    }
    manifest = {"checks": {"html_exists": True, "payload_size_ok": True}}

    (tmp_path / "release_readiness.json").write_text(json.dumps(readiness), encoding="utf-8")
    (tmp_path / "dashboard_build_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (tmp_path / filename).write_text("{json-invalido", encoding="utf-8")

    monkeypatch.setattr("src.ev_release_gate.OUTPUT_REPORTS_DIR", tmp_path)
    result = run_release_gate()

    assert not result.approved
    assert result.release_grade == "unknown"
    assert result.reason == expected_reason


@pytest.mark.parametrize(
    "readiness, manifest, expected_reason",
    [
        (
            {
                "release_grade": "publish-blocked",
                "publish_blocked": True,
                "kpi_single_source_of_truth": True,
            },
            {"checks": {"html_exists": True}},
            "Release bloqueado por validación",
        ),
        (
            {
                "release_grade": "decision-support only",
                "publish_blocked": False,
                "kpi_single_source_of_truth": False,
            },
            {"checks": {"html_exists": True}},
            "KPI source of truth inconsistente",
        ),
        (
            {
                "release_grade": "decision-support only",
                "publish_blocked": False,
                "kpi_single_source_of_truth": True,
            },
            {"checks": {"html_exists": True, "payload_size_ok": False}},
            "Dashboard manifest con checks en WARN",
        ),
    ],
)
def test_ev_release_gate_quality_gate_failure_reasons(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    readiness: dict[str, object],
    manifest: dict[str, object],
    expected_reason: str,
) -> None:
    (tmp_path / "release_readiness.json").write_text(json.dumps(readiness), encoding="utf-8")
    (tmp_path / "dashboard_build_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr("src.ev_release_gate.OUTPUT_REPORTS_DIR", tmp_path)
    result = run_release_gate()

    assert not result.approved
    assert expected_reason in result.reason
    assert result.release_grade == readiness["release_grade"]


def test_ev_release_gate_approves_when_all_quality_gates_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    readiness = {
        "release_grade": "decision-support only",
        "publish_blocked": False,
        "kpi_single_source_of_truth": True,
    }
    manifest = {"checks": {"html_exists": True, "payload_size_ok": True, "filters_ok": True}}
    (tmp_path / "release_readiness.json").write_text(json.dumps(readiness), encoding="utf-8")
    (tmp_path / "dashboard_build_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr("src.ev_release_gate.OUTPUT_REPORTS_DIR", tmp_path)
    result = run_release_gate()

    assert result.approved
    assert result.release_grade == "decision-support only"
    assert result.reason == "Release apto para publicación"
