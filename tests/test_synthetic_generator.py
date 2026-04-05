from pathlib import Path

import pandas as pd

from src.synthetic_data_gen import SyntheticGenerationConfig, generate_synthetic_factory_data


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

    bateria = pd.read_csv(raw_dir / "estado_bateria.csv")
    assert bateria["soc_pct"].between(0, 100).all()

    assert (report_dir / "synthetic_data_plausibility.md").exists()
    assert (report_dir / "synthetic_data_summary.md").exists()
