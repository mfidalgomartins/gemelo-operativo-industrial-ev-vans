from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from .config import OUTPUT_REPORTS_DIR


@dataclass
class ReleaseGateResult:
    approved: bool
    release_grade: str
    reason: str


def run_release_gate() -> ReleaseGateResult:
    readiness_path = OUTPUT_REPORTS_DIR / "release_readiness.json"
    manifest_path = OUTPUT_REPORTS_DIR / "dashboard_build_manifest.json"

    if not readiness_path.exists():
        return ReleaseGateResult(False, "unknown", "Falta release_readiness.json")
    if not manifest_path.exists():
        return ReleaseGateResult(False, "unknown", "Falta dashboard_build_manifest.json")

    readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    release_grade = str(readiness.get("release_grade", "unknown"))
    publish_blocked = bool(readiness.get("publish_blocked", True))
    dashboard_checks_ok = all(bool(v) for v in manifest.get("checks", {}).values())
    kpi_ssot_ok = bool(readiness.get("kpi_single_source_of_truth", False))

    if publish_blocked:
        return ReleaseGateResult(False, release_grade, "Release bloqueado por validación")
    if not kpi_ssot_ok:
        return ReleaseGateResult(False, release_grade, "KPI source of truth inconsistente (artefacto legacy detectado)")
    if not dashboard_checks_ok:
        return ReleaseGateResult(False, release_grade, "Dashboard manifest con checks en WARN")

    return ReleaseGateResult(True, release_grade, "Release apto para publicación de portfolio")


if __name__ == "__main__":
    result = run_release_gate()
    print("Release gate EV")
    print(f"- approved: {result.approved}")
    print(f"- release_grade: {result.release_grade}")
    print(f"- reason: {result.reason}")
    raise SystemExit(0 if result.approved else 1)
