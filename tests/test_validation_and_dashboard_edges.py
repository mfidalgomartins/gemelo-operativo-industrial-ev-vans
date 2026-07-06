from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src import ev_build_dashboard as dashboard
from src import ev_validate_project as validation
from src.ev_release_gate import _read_json_object
from src.utils import require_columns, to_markdown_safe, write_json_utf8


def test_require_columns_lists_missing_columns_in_context() -> None:
    df = pd.DataFrame({"orden_id": ["O1"]})

    with pytest.raises(
        ValueError, match="vw_vehicle_flow_timeline: faltan columnas requeridas: vehiculo_id, fecha_real"
    ):
        require_columns(df, ["orden_id", "vehiculo_id", "fecha_real"], "vw_vehicle_flow_timeline")


def test_to_markdown_safe_fallback_handles_empty_dataframe(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_tabulate_error(self: pd.DataFrame, index: bool = False) -> str:
        raise ImportError("tabulate unavailable")

    monkeypatch.setattr(pd.DataFrame, "to_markdown", _raise_tabulate_error)

    assert to_markdown_safe(pd.DataFrame()) == "_(sin filas)_"


def test_write_json_utf8_supports_custom_default_and_utf8(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "payload.json"

    write_json_utf8(output, {"estado": "aprobado", "ruta": tmp_path}, default=str)

    payload = output.read_text(encoding="utf-8")
    assert '"estado": "aprobado"' in payload
    assert str(tmp_path) in payload
    assert payload.endswith("\n")


@pytest.mark.parametrize("payload", [[1, 2, 3], "texto", None])
def test_release_gate_read_json_object_rejects_non_object_json(tmp_path: Path, payload: object) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    data, error = _read_json_object(path, "manifest.json")

    assert data is None
    assert error == "manifest.json debe contener un objeto JSON"


def test_validation_dashboard_manifest_missing_returns_empty_dict(tmp_path: Path) -> None:
    assert validation._read_dashboard_manifest(tmp_path / "missing.json") == {}


def test_validation_dashboard_manifest_rejects_invalid_json(tmp_path: Path) -> None:
    manifest = tmp_path / "dashboard_build_manifest.json"
    manifest.write_text("{bad-json", encoding="utf-8")

    with pytest.raises(ValueError, match="dashboard_build_manifest.json no es JSON válido"):
        validation._read_dashboard_manifest(manifest)


def test_validation_dashboard_manifest_rejects_non_object_json(tmp_path: Path) -> None:
    manifest = tmp_path / "dashboard_build_manifest.json"
    manifest.write_text("[1, 2, 3]", encoding="utf-8")

    with pytest.raises(ValueError, match="debe contener un objeto JSON"):
        validation._read_dashboard_manifest(manifest)


def test_resolve_ev_raw_prefers_official_directory_and_falls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    official = tmp_path / "ev_factory"
    fallback = tmp_path / "raw"
    official.mkdir()
    fallback.mkdir()
    official_path = official / "ordenes.csv"
    fallback_path = fallback / "ordenes.csv"
    official_path.write_text("id\n1\n", encoding="utf-8")
    fallback_path.write_text("id\n2\n", encoding="utf-8")

    monkeypatch.setattr(validation, "EV_DATA_RAW_DIR", official)
    monkeypatch.setattr(validation, "DATA_RAW_DIR", fallback)
    assert validation._resolve_ev_raw("ordenes") == official_path

    official_path.unlink()
    assert validation._resolve_ev_raw("ordenes") == fallback_path


def test_resolve_ev_raw_raises_when_missing_from_all_locations(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(validation, "EV_DATA_RAW_DIR", tmp_path / "ev_factory")
    monkeypatch.setattr(validation, "DATA_RAW_DIR", tmp_path / "raw")

    with pytest.raises(FileNotFoundError, match="No existe tabla EV de origen requerida"):
        validation._resolve_ev_raw("ordenes")


def test_remove_non_official_dashboards_removes_only_inherited_html(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_dir = tmp_path / "outputs" / "dashboard"
    output_dir.mkdir(parents=True)
    official = output_dir / dashboard.OFFICIAL_DASHBOARD_NAME
    inherited = output_dir / "heredado.html"
    official.write_text("<html>oficial</html>", encoding="utf-8")
    inherited.write_text("<html>heredado</html>", encoding="utf-8")

    monkeypatch.setattr(dashboard, "PROJECT_ROOT", tmp_path)

    removed = dashboard._remove_non_official_dashboards(output_dir, dashboard.OFFICIAL_DASHBOARD_NAME)

    assert removed == ["outputs/dashboard/heredado.html"]
    assert official.exists()
    assert not inherited.exists()


def test_build_payload_creates_sorted_filters_and_serialized_data() -> None:
    datasets = {
        "throughput": pd.DataFrame({"turno": ["B", "A"], "fecha": pd.to_datetime(["2025-01-02", "2025-01-01"])}),
        "seq_gap": pd.DataFrame({"tipo_propulsion": ["ICE", "EV"], "metric": [1.0, 2.0]}),
        "lead_version": pd.DataFrame({"version_id": ["V2", "V1"]}),
        "priorities": pd.DataFrame({"area": ["CARGA", "PATIO"]}),
        "yard_daily": pd.DataFrame({"zona_patio": ["Z2", "Z1"]}),
        "charge_daily": pd.DataFrame({"zona_carga": ["C2", "C1"]}),
        "b_detail": pd.DataFrame({"severidad": ["media", "alta"]}),
    }

    payload = dashboard._build_payload({"coverage": "2025"}, datasets)

    assert payload["filters"] == {
        "turno": ["A", "B"],
        "propulsion": ["EV", "ICE"],
        "version": ["V1", "V2"],
        "area": ["CARGA", "PATIO"],
        "zona_patio": ["Z1", "Z2"],
        "zona_carga": ["C1", "C2"],
        "severidad": ["alta", "media"],
    }
    assert payload["data"]["throughput"][0]["fecha"] == "2025-01-02"
