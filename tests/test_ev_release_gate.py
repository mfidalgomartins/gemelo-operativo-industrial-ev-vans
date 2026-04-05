from __future__ import annotations

from src.ev_release_gate import run_release_gate


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
