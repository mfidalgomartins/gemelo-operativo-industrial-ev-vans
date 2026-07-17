from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "outputs" / "reports" / "ev_transition_operating_twin_report.pdf"


@pytest.mark.integration
def test_report_generation_is_byte_stable() -> None:
    command = [sys.executable, "-m", "gemelo_operativo_ev.reporting.report"]

    subprocess.run(command, cwd=ROOT, check=True)
    first_hash = hashlib.sha256(REPORT.read_bytes()).hexdigest()

    subprocess.run(command, cwd=ROOT, check=True)
    second_hash = hashlib.sha256(REPORT.read_bytes()).hexdigest()

    assert second_hash == first_hash
