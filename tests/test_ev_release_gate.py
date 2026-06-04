from __future__ import annotations

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
        "committee-grade candidate",
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
    import json

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
