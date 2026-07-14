from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from .config import OUTPUT_REPORTS_DIR, PROJECT_ROOT


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

ALLOWED_RELEASE_GRADES = frozenset(
    {
        "decision-support only",
        "screening-grade only",
        "publish-blocked",
        "not committee-grade",
    }
)

REQUIRED_DASHBOARD_CHECKS = frozenset(
    {
        "placeholder_free",
        "single_official_dashboard",
        "chart_js_cdn_declared",
        "kpi_payload_bound",
        "html_size_under_6mb",
        "canvas_count_expected",
        "severity_filter_wired",
        "executive_snapshot_consistent",
        "density_guard",
        "kpi_logic_valid",
    }
)

OFFICIAL_DASHBOARD = "outputs/dashboard/industrial-ev-operating-command-center.html"
_DASHBOARD_VERSION_RE = re.compile(r"^ev-[0-9a-f]{10}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


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


def _require_field(payload: dict[str, object], field: str, expected_type: type, label: str) -> str | None:
    if field not in payload:
        return f"{label} no contiene el campo obligatorio {field!r}"
    value = payload[field]
    if expected_type is int:
        valid_type = isinstance(value, int) and not isinstance(value, bool)
    else:
        valid_type = isinstance(value, expected_type)
    if not valid_type:
        return f"{label}.{field} debe ser {expected_type.__name__}"
    return None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_readiness(readiness: dict[str, object]) -> tuple[str, str | None]:
    label = "release_readiness.json"
    for field, expected_type in (
        ("release_grade", str),
        ("publish_blocked", bool),
        ("kpi_single_source_of_truth", bool),
        ("dashboard_version", str),
        ("dashboard_html_sha256", str),
    ):
        error = _require_field(readiness, field, expected_type, label)
        if error:
            return "unknown", error

    release_grade = str(readiness["release_grade"])
    if release_grade not in ALLOWED_RELEASE_GRADES:
        return release_grade, f"{label}.release_grade no permitido: {release_grade!r}"
    if not _DASHBOARD_VERSION_RE.fullmatch(str(readiness["dashboard_version"])):
        return release_grade, f"{label}.dashboard_version tiene formato inválido"
    if not _SHA256_RE.fullmatch(str(readiness["dashboard_html_sha256"])):
        return release_grade, f"{label}.dashboard_html_sha256 tiene formato inválido"

    publish_blocked = readiness["publish_blocked"]
    if (release_grade == "publish-blocked") is not publish_blocked:
        return release_grade, "release_readiness.json contiene un estado de bloqueo incoherente"
    return release_grade, None


def _validate_manifest_schema(manifest: dict[str, object]) -> str | None:
    label = "dashboard_build_manifest.json"
    for field, expected_type in (
        ("dashboard_version", str),
        ("official_dashboard", str),
        ("html_size_bytes", int),
        ("html_sha256", str),
        ("checks", dict),
    ):
        error = _require_field(manifest, field, expected_type, label)
        if error:
            return error

    version = str(manifest["dashboard_version"])
    if not _DASHBOARD_VERSION_RE.fullmatch(version):
        return f"{label}.dashboard_version tiene formato inválido"
    if manifest["official_dashboard"] != OFFICIAL_DASHBOARD:
        return f"{label}.official_dashboard no apunta al panel oficial"
    if int(manifest["html_size_bytes"]) <= 0:
        return f"{label}.html_size_bytes debe ser positivo"
    if not _SHA256_RE.fullmatch(str(manifest["html_sha256"])):
        return f"{label}.html_sha256 tiene formato inválido"

    checks = manifest["checks"]
    if not isinstance(checks, dict):
        return f"{label}.checks debe ser dict"
    missing_checks = sorted(REQUIRED_DASHBOARD_CHECKS - checks.keys())
    if missing_checks:
        return f"{label}.checks omite comprobaciones obligatorias: {', '.join(missing_checks)}"
    invalid_types = sorted(name for name, value in checks.items() if not isinstance(value, bool))
    if invalid_types:
        return f"{label}.checks contiene valores no booleanos: {', '.join(invalid_types)}"
    return None


def _validate_dashboard_artifact(manifest: dict[str, object]) -> str | None:
    dashboard_path = (PROJECT_ROOT / OFFICIAL_DASHBOARD).resolve()
    try:
        dashboard_path.relative_to(PROJECT_ROOT.resolve())
    except ValueError:
        return "La ruta del panel oficial queda fuera del proyecto"

    if not dashboard_path.is_file():
        return f"Falta panel oficial declarado en el manifiesto: {OFFICIAL_DASHBOARD}"

    expected_size = int(manifest["html_size_bytes"])
    if dashboard_path.stat().st_size != expected_size:
        return "Panel oficial no coincide con el tamaño registrado en el manifiesto"

    expected_hash = str(manifest["html_sha256"])
    if _sha256_file(dashboard_path) != expected_hash:
        return "Panel oficial no coincide con el hash SHA-256 del manifiesto"

    version = str(manifest["dashboard_version"])
    version_marker = f'<meta name="dashboard-version" content="{version}" />'
    try:
        html_head = dashboard_path.read_text(encoding="utf-8")[:2_000]
    except OSError as exc:
        return f"No se pudo leer el panel oficial: {exc}"
    if version_marker not in html_head:
        return "Versión del panel oficial no coincide con el manifiesto"

    return None


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

    if readiness is None or manifest is None:
        return ReleaseGateResult(False, "unknown", "No se pudieron cargar los artefactos de publicación")

    release_grade, readiness_schema_error = _validate_readiness(readiness)
    if readiness_schema_error:
        return ReleaseGateResult(False, release_grade, readiness_schema_error)

    manifest_schema_error = _validate_manifest_schema(manifest)
    if manifest_schema_error:
        return ReleaseGateResult(False, release_grade, manifest_schema_error)

    if readiness["dashboard_version"] != manifest["dashboard_version"]:
        return ReleaseGateResult(False, release_grade, "Validación vinculada a otra versión del panel")
    if readiness["dashboard_html_sha256"] != manifest["html_sha256"]:
        return ReleaseGateResult(False, release_grade, "Validación vinculada a otro hash del panel")

    if readiness["publish_blocked"]:
        return ReleaseGateResult(False, release_grade, "Publicación bloqueada por validación")
    if not readiness["kpi_single_source_of_truth"]:
        return ReleaseGateResult(
            False, release_grade, "Fuente única de verdad KPI inconsistente (artefacto heredado detectado)"
        )

    checks = manifest["checks"]
    if not isinstance(checks, dict):
        return ReleaseGateResult(False, release_grade, "dashboard_build_manifest.json.checks debe ser dict")
    if not all(checks.values()):
        return ReleaseGateResult(False, release_grade, "Manifiesto del panel con comprobaciones en alerta")

    artifact_error = _validate_dashboard_artifact(manifest)
    if artifact_error:
        return ReleaseGateResult(False, release_grade, artifact_error)

    return ReleaseGateResult(True, release_grade, "Publicación apta")


if __name__ == "__main__":
    result = run_release_gate()
    print("Puerta de publicación EV")
    print(f"- aprobado: {result.approved}")
    print(f"- grado_publicacion: {_release_grade_label(result.release_grade)}")
    print(f"- motivo: {result.reason}")
    raise SystemExit(0 if result.approved else 1)
