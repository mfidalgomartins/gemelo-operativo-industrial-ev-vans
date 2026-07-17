from pathlib import Path

import pandas as pd
import pytest

from gemelo_operativo_ev.synthetic_data_gen import SyntheticGenerationConfig, generate_synthetic_factory_data
from gemelo_operativo_ev.synthetic_data_gen.operations import _active_charging_slots, _deduplicate_patio_points

REQUIRED_TABLES = {
    "ordenes",
    "versiones_vehiculo",
    "vehiculos",
    "estado_bateria",
    "slots_carga",
    "sesiones_carga",
    "patio",
    "movimientos_patio",
    "turnos",
    "logistica_salida",
    "cuellos_botella",
    "recursos_operativos",
    "restricciones_operativas",
    "escenarios_transicion",
}


def test_patio_point_deduplication_has_stable_tie_breaking() -> None:
    timestamp = pd.Timestamp("2025-01-01 08:00:00")
    points = [
        (timestamp, "SUR", "INGRESO_PATIO"),
        (timestamp, "NORTE", "INGRESO_PATIO"),
        (timestamp, "SUR", "INGRESO_PATIO"),
        (timestamp, "NORTE", "POST_CARGA"),
    ]

    assert _deduplicate_patio_points(points) == [
        (timestamp, "NORTE", "POST_CARGA"),
    ]


def test_active_charging_slots_rejects_unavailable_capacity() -> None:
    slots = pd.DataFrame(
        {
            "slot_id": ["S1", "S2"],
            "disponibilidad_flag": [0, 1],
            "mantenimiento_flag": [0, 1],
        }
    )

    with pytest.raises(ValueError, match="al menos un punto disponible"):
        _active_charging_slots(slots)


def test_synthetic_generator_end_to_end(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    report_dir = tmp_path / "reports"

    cfg = SyntheticGenerationConfig(
        seed=123,
        start_date="2025-01-01",
        months=9,
        output_raw_dir=raw_dir,
        output_report_dir=report_dir,
    )
    result = generate_synthetic_factory_data(cfg)

    assert set(result["cardinalidades"].keys()) == REQUIRED_TABLES
    assert result["validation"]["status_global"] == "PASS"

    for table in REQUIRED_TABLES:
        assert (raw_dir / f"{table}.csv").exists()

    escenarios = pd.read_csv(raw_dir / "escenarios_transicion.csv")
    first_ev = escenarios.head(20)["share_ev"].mean()
    last_ev = escenarios.tail(20)["share_ev"].mean()
    assert last_ev > first_ev

    ordenes = pd.read_csv(raw_dir / "ordenes.csv")
    assert ordenes["orden_id"].is_unique
    assert ordenes["fecha_turno_operativo"].notna().all()
    assert not ordenes.duplicated(subset=["fecha_turno_operativo", "turno", "secuencia_planeada"]).any()

    bateria = pd.read_csv(raw_dir / "estado_bateria.csv")
    assert bateria["soc_pct"].between(0, 100).all()

    logistica = pd.read_csv(raw_dir / "logistica_salida.csv")
    departure_ts = pd.to_datetime(logistica["fecha_salida_real"])
    readiness_ts = pd.to_datetime(logistica["timestamp_readiness"])
    assert not (departure_ts.notna() & (readiness_ts.isna() | departure_ts.lt(readiness_ts))).any()
    assert not (
        logistica["retraso_min"].gt(120)
        & logistica["causa_retraso"].fillna("SIN_DATO").isin(["SIN_RETRASO", "N/A", "SIN_DATO"])
    ).any()

    patio = pd.read_csv(raw_dir / "patio.csv")
    assert not patio.duplicated(subset=["timestamp", "vehiculo_id"]).any()

    assert (report_dir / "synthetic_data_plausibility.md").exists()
    assert (report_dir / "synthetic_data_summary.md").exists()
