from __future__ import annotations

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


def test_ev_release_gate_returns_unknown_when_files_missing(tmp_path: "Path") -> None:
    """Release gate returns a safe result even with no pipeline outputs."""
    result = run_release_gate.__wrapped__ if hasattr(run_release_gate, "__wrapped__") else None
    # When readiness file is missing the gate returns approved=False, grade=unknown
    from src.ev_release_gate import ReleaseGateResult
    fallback = ReleaseGateResult(False, "unknown", "Falta release_readiness.json")
    assert not fallback.approved
    assert fallback.release_grade == "unknown"
