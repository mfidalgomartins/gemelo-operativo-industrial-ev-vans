from __future__ import annotations

from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

from .bottlenecks import generate_cuellos_botella
from .config import SyntheticGenerationConfig
from .master_data import generate_recursos_operativos, generate_slots_carga, generate_versiones_vehiculo
from .operations import generate_operational_tables
from .planning import (
    build_daily_restriction_map,
    generate_escenarios_transicion,
    generate_restricciones_operativas,
    generate_turnos,
)
from .validation import validate_synthetic_data


def _save_tables(tables: Dict[str, pd.DataFrame], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.to_csv(output_dir / f"{name}.csv", index=False)


def generate_synthetic_factory_data(cfg: SyntheticGenerationConfig) -> Dict[str, object]:
    cfg.ensure_valid()
    rng = np.random.default_rng(cfg.seed)

    escenarios = generate_escenarios_transicion(cfg.start_date, cfg.months, rng)
    turnos = generate_turnos(escenarios, rng)
    restricciones = generate_restricciones_operativas(escenarios, turnos, rng)
    daily_map = build_daily_restriction_map(restricciones)

    versiones = generate_versiones_vehiculo()
    slots = generate_slots_carga(rng, n_slots=32)

    operational = generate_operational_tables(
        rng=rng,
        escenarios=escenarios,
        turnos=turnos,
        versiones=versiones,
        slots_carga=slots,
        daily_restriction_map=daily_map,
    )

    cuellos = generate_cuellos_botella(
        rng=rng,
        sesiones_carga=operational.sesiones_carga,
        patio=operational.patio,
        logistica_salida=operational.logistica_salida,
        turnos=turnos,
        restricciones_operativas=restricciones,
    )

    recursos = generate_recursos_operativos(rng, turnos, restricciones)

    tables = {
        "ordenes": operational.ordenes,
        "versiones_vehiculo": versiones,
        "vehiculos": operational.vehiculos,
        "estado_bateria": operational.estado_bateria,
        "slots_carga": operational.slots_carga_actualizados,
        "sesiones_carga": operational.sesiones_carga,
        "patio": operational.patio,
        "movimientos_patio": operational.movimientos_patio,
        "turnos": turnos,
        "logistica_salida": operational.logistica_salida,
        "cuellos_botella": cuellos,
        "recursos_operativos": recursos,
        "restricciones_operativas": restricciones,
        "escenarios_transicion": escenarios,
    }

    _save_tables(tables, cfg.output_raw_dir)

    validation_summary = validate_synthetic_data(tables, cfg.output_report_dir)

    return {
        "seed": cfg.seed,
        "months": cfg.months,
        "start_date": cfg.start_date,
        "output_raw_dir": str(cfg.output_raw_dir),
        "output_report_dir": str(cfg.output_report_dir),
        "cardinalidades": {k: int(v.shape[0]) for k, v in tables.items()},
        "validation": validation_summary,
    }
