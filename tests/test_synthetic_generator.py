from pathlib import Path

import pandas as pd

from src.synthetic_data_gen import SyntheticGenerationConfig, generate_synthetic_factory_data
from src.synthetic_data_gen.operations import _deduplicate_patio_points

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
        (timestamp, "NORTE", "INGRESO_PATIO"),
        (timestamp, "NORTE", "POST_CARGA"),
        (timestamp, "SUR", "INGRESO_PATIO"),
    ]


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
    assert not (logistica["fecha_salida_real"].notna() & logistica["readiness_salida_flag"].eq(0)).any()

    assert (report_dir / "synthetic_data_plausibility.md").exists()
    assert (report_dir / "synthetic_data_summary.md").exists()
