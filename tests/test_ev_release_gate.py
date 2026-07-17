from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from gemelo_operativo_ev.config import OUTPUT_REPORTS_DIR
from gemelo_operativo_ev.ev_release_gate import OFFICIAL_DASHBOARD, REQUIRED_DASHBOARD_CHECKS, run_release_gate

_REQUIRED_FILES = [
    OUTPUT_REPORTS_DIR / "release_readiness.json",
    OUTPUT_REPORTS_DIR / "dashboard_build_manifest.json",
]
_PIPELINE_OUTPUTS_EXIST = all(p.exists() for p in _REQUIRED_FILES)


def _write_valid_gate_inputs(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    readiness_overrides: dict[str, object] | None = None,
    checks_overrides: dict[str, object] | None = None,
) -> tuple[Path, Path, Path]:
    reports_dir = root / "outputs" / "reports"
    dashboard_path = root / OFFICIAL_DASHBOARD
    reports_dir.mkdir(parents=True, exist_ok=True)
    dashboard_path.parent.mkdir(parents=True, exist_ok=True)

    version = "ev-0123456789"
    html = f'<!doctype html><meta name="dashboard-version" content="{version}" />'
    dashboard_path.write_text(html, encoding="utf-8")

    checks: dict[str, object] = {name: True for name in REQUIRED_DASHBOARD_CHECKS}
    checks.update(checks_overrides or {})
    manifest = {
        "dashboard_version": version,
        "official_dashboard": OFFICIAL_DASHBOARD,
        "html_size_bytes": dashboard_path.stat().st_size,
        "html_sha256": hashlib.sha256(dashboard_path.read_bytes()).hexdigest(),
        "checks": checks,
    }
    readiness = {
        "release_grade": "decision-support only",
        "publish_blocked": False,
        "kpi_single_source_of_truth": True,
        "dashboard_version": version,
        "dashboard_html_sha256": manifest["html_sha256"],
    }
    readiness.update(readiness_overrides or {})

    manifest_path = reports_dir / "dashboard_build_manifest.json"
    readiness_path = reports_dir / "release_readiness.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    readiness_path.write_text(json.dumps(readiness), encoding="utf-8")

    monkeypatch.setattr("gemelo_operativo_ev.ev_release_gate.PROJECT_ROOT", root)
    monkeypatch.setattr("gemelo_operativo_ev.ev_release_gate.OUTPUT_REPORTS_DIR", reports_dir)
    return dashboard_path, manifest_path, readiness_path


@pytest.mark.skipif(
    not _PIPELINE_OUTPUTS_EXIST,
    reason="Outputs de canalización no presentes; ejecutar `python -m gemelo_operativo_ev.run_pipeline` primero",
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
        assert result.reason.startswith("Publicación apta")


def test_ev_release_gate_returns_unapproved_when_readiness_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """run_release_gate devuelve approved=False y grado unknown cuando faltan archivos."""
    monkeypatch.setattr("gemelo_operativo_ev.ev_release_gate.OUTPUT_REPORTS_DIR", tmp_path)
    result = run_release_gate()
    assert not result.approved
    assert result.release_grade == "unknown"
    assert "release_readiness.json" in result.reason


def test_ev_release_gate_returns_unapproved_when_manifest_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """La puerta rechaza cuando falta el manifiesto aunque exista release_readiness."""
    readiness = {
        "release_grade": "decision-support only",
        "publish_blocked": False,
        "kpi_single_source_of_truth": True,
    }
    (tmp_path / "release_readiness.json").write_text(json.dumps(readiness), encoding="utf-8")

    monkeypatch.setattr("gemelo_operativo_ev.ev_release_gate.OUTPUT_REPORTS_DIR", tmp_path)
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

    monkeypatch.setattr("gemelo_operativo_ev.ev_release_gate.OUTPUT_REPORTS_DIR", tmp_path)
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
            "Publicación bloqueada por validación",
        ),
        (
            {
                "release_grade": "decision-support only",
                "publish_blocked": False,
                "kpi_single_source_of_truth": False,
            },
            {"checks": {"html_exists": True}},
            "Fuente única de verdad KPI inconsistente",
        ),
        (
            {
                "release_grade": "decision-support only",
                "publish_blocked": False,
                "kpi_single_source_of_truth": True,
            },
            {"checks": {"html_exists": True, "payload_size_ok": False}},
            "Manifiesto del panel con comprobaciones en alerta",
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
    checks = manifest.get("checks", {})
    assert isinstance(checks, dict)
    _write_valid_gate_inputs(
        tmp_path,
        monkeypatch,
        readiness_overrides=readiness,
        checks_overrides=checks,
    )
    result = run_release_gate()

    assert not result.approved
    assert expected_reason in result.reason
    assert result.release_grade == readiness["release_grade"]


def test_ev_release_gate_approves_when_all_quality_gates_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_valid_gate_inputs(tmp_path, monkeypatch)
    result = run_release_gate()

    assert result.approved
    assert result.release_grade == "decision-support only"
    assert result.reason == "Publicación apta"


@pytest.mark.parametrize(
    "readiness_overrides, expected_reason",
    [
        ({"publish_blocked": "false"}, "publish_blocked debe ser bool"),
        ({"kpi_single_source_of_truth": 1}, "kpi_single_source_of_truth debe ser bool"),
        ({"release_grade": "unknown"}, "release_grade no permitido"),
        ({"release_grade": "publish-blocked", "publish_blocked": False}, "estado de bloqueo incoherente"),
        ({"dashboard_version": "ev-other"}, "dashboard_version tiene formato inválido"),
    ],
)
def test_ev_release_gate_rejects_invalid_readiness_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    readiness_overrides: dict[str, object],
    expected_reason: str,
) -> None:
    _write_valid_gate_inputs(tmp_path, monkeypatch, readiness_overrides=readiness_overrides)

    result = run_release_gate()

    assert not result.approved
    assert expected_reason in result.reason


def test_ev_release_gate_rejects_empty_checks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, manifest_path, _ = _write_valid_gate_inputs(tmp_path, monkeypatch)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["checks"] = {}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    result = run_release_gate()

    assert not result.approved
    assert "omite comprobaciones obligatorias" in result.reason


def test_ev_release_gate_rejects_non_boolean_check(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_valid_gate_inputs(tmp_path, monkeypatch, checks_overrides={"placeholder_free": "true"})

    result = run_release_gate()

    assert not result.approved
    assert "valores no booleanos" in result.reason


def test_ev_release_gate_rejects_readiness_bound_to_another_dashboard_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_valid_gate_inputs(
        tmp_path,
        monkeypatch,
        readiness_overrides={"dashboard_html_sha256": "0" * 64},
    )

    result = run_release_gate()

    assert not result.approved
    assert "Validación vinculada a otro hash" in result.reason


def test_ev_release_gate_rejects_tampered_dashboard_hash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dashboard_path, _, _ = _write_valid_gate_inputs(tmp_path, monkeypatch)
    html = dashboard_path.read_text(encoding="utf-8")
    dashboard_path.write_text(html.replace("doctype", "doctypf", 1), encoding="utf-8")

    result = run_release_gate()

    assert not result.approved
    assert "hash SHA-256" in result.reason


def test_ev_release_gate_rejects_missing_dashboard_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dashboard_path, _, _ = _write_valid_gate_inputs(tmp_path, monkeypatch)
    dashboard_path.unlink()

    result = run_release_gate()

    assert not result.approved
    assert "Falta panel oficial" in result.reason
