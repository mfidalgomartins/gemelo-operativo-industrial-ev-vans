from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .config import OUTPUT_REPORTS_DIR


@dataclass
class ReleaseGateResult:
    approved: bool
    release_grade: str
    reason: str


RELEASE_GRADE_LABELS = {
    "decision-support only": "solo apoyo a decisión",
    "screening-grade only": "solo screening",
    "publish-blocked": "publicación bloqueada",
    "not committee-grade": "no apto para comité",
    "unknown": "desconocido",
}


def _release_grade_label(value: str) -> str:
    return RELEASE_GRADE_LABELS.get(value, value)


def _read_json_object(path: Path, label: str) -> tuple[dict[str, object] | None, str | None]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None, f"{label} no es JSON válido"
    except OSError as exc:
        return None, f"No se pudo leer {label}: {exc}"

    if not isinstance(payload, dict):
        return None, f"{label} debe contener un objeto JSON"
    return payload, None


def run_release_gate() -> ReleaseGateResult:
    readiness_path = OUTPUT_REPORTS_DIR / "release_readiness.json"
    manifest_path = OUTPUT_REPORTS_DIR / "dashboard_build_manifest.json"

    if not readiness_path.exists():
        return ReleaseGateResult(False, "unknown", "Falta release_readiness.json")
    if not manifest_path.exists():
        return ReleaseGateResult(False, "unknown", "Falta dashboard_build_manifest.json")

    readiness, readiness_error = _read_json_object(readiness_path, "release_readiness.json")
    if readiness_error:
        return ReleaseGateResult(False, "unknown", readiness_error)

    manifest, manifest_error = _read_json_object(manifest_path, "dashboard_build_manifest.json")
    if manifest_error:
        return ReleaseGateResult(False, "unknown", manifest_error)

    release_grade = str(readiness.get("release_grade", "unknown"))
    publish_blocked = bool(readiness.get("publish_blocked", True))
    checks = manifest.get("checks", {})
    if not isinstance(checks, dict):
        return ReleaseGateResult(False, release_grade, "Manifiesto del panel sin objeto de comprobaciones válido")
    dashboard_checks_ok = all(bool(v) for v in checks.values())
    kpi_ssot_ok = bool(readiness.get("kpi_single_source_of_truth", False))

    if publish_blocked:
        return ReleaseGateResult(False, release_grade, "Publicación bloqueada por validación")
    if not kpi_ssot_ok:
        return ReleaseGateResult(
            False, release_grade, "Fuente única de verdad KPI inconsistente (artefacto heredado detectado)"
        )
    if not dashboard_checks_ok:
        return ReleaseGateResult(False, release_grade, "Manifiesto del panel con comprobaciones en alerta")

    return ReleaseGateResult(True, release_grade, "Publicación apta")


if __name__ == "__main__":
    result = run_release_gate()
    print("Puerta de publicación EV")
    print(f"- aprobado: {result.approved}")
    print(f"- grado_publicacion: {_release_grade_label(result.release_grade)}")
    print(f"- motivo: {result.reason}")
    raise SystemExit(0 if result.approved else 1)
