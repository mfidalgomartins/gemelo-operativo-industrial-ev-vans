from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import duckdb
import pandas as pd

from .config import DATA_PROCESSED_DIR, DATA_RAW_DIR, EV_DATA_RAW_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT


SQL_LAYER_DIR = PROJECT_ROOT / "sql" / "ev_factory"
DB_PATH = DATA_PROCESSED_DIR / "gemelo_operativo_ev.duckdb"

RAW_TABLES = [
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
]

SQL_FILES_IN_ORDER = [
    "01_staging_orders.sql",
    "02_staging_charging.sql",
    "03_staging_yard.sql",
    "04_staging_dispatch.sql",
    "05_integrated_vehicle_flow.sql",
    "06_integrated_shift_operations.sql",
    "07_analytical_mart_vehicle_day.sql",
    "08_analytical_mart_area_shift.sql",
    "09_analytical_mart_dispatch_readiness.sql",
    "10_kpi_queries.sql",
    "11_validation_queries.sql",
]

EXPORT_OBJECTS = [
    "vw_vehicle_flow_timeline",
    "vw_charging_utilization",
    "vw_yard_congestion",
    "vw_dispatch_readiness",
    "vw_shift_bottleneck_summary",
    "mart_vehicle_day",
    "mart_area_shift",
    "mart_dispatch_readiness",
    "kpi_operativos",
    "kpi_readiness_shift_version",
    "validation_checks",
]


@dataclass
class SQLRunResult:
    db_path: str
    executed_files: List[str]
    exported_rows: Dict[str, int]


def _resolve_raw_csv(table: str) -> Path:
    primary = EV_DATA_RAW_DIR / f"{table}.csv"
    if primary.exists():
        return primary

    fallback = DATA_RAW_DIR / f"{table}.csv"
    if fallback.exists():
        # Permite compatibilidad, pero evita mezclar esquema legacy para tablas críticas.
        if table == "versiones_vehiculo":
            cols = pd.read_csv(fallback, nrows=1).columns.tolist()
            if "version_id" not in cols:
                raise FileNotFoundError(
                    "No existe dataset EV válido de versiones en `data/raw/ev_factory/versiones_vehiculo.csv`. "
                    "Detectado archivo legacy en `data/raw/versiones_vehiculo.csv`."
                )
        return fallback

    raise FileNotFoundError(
        f"Falta tabla raw requerida: {primary} (fallback también ausente: {fallback})"
    )


def _load_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    for table in RAW_TABLES:
        csv_path = _resolve_raw_csv(table)
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT
                *
            FROM read_csv_auto('{csv_path.as_posix()}', HEADER=TRUE);
            """
        )


def _run_sql_files(con: duckdb.DuckDBPyConnection) -> List[str]:
    executed: List[str] = []
    for file_name in SQL_FILES_IN_ORDER:
        sql_path = SQL_LAYER_DIR / file_name
        if not sql_path.exists():
            raise FileNotFoundError(f"Falta script SQL requerido: {sql_path}")
        con.execute(sql_path.read_text(encoding="utf-8"))
        executed.append(file_name)
    return executed


def _export_objects(con: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    export_dir = DATA_PROCESSED_DIR / "ev_factory"
    export_dir.mkdir(parents=True, exist_ok=True)

    rows: Dict[str, int] = {}
    for obj in EXPORT_OBJECTS:
        out_csv = export_dir / f"{obj}.csv"
        con.execute(
            f"""
            COPY (
                SELECT
                    *
                FROM {obj}
            )
            TO '{out_csv.as_posix()}'
            (HEADER, DELIMITER ',');
            """
        )
        rows[obj] = int(con.execute(f"SELECT COUNT(*) FROM {obj}").fetchone()[0])
    return rows


def run_ev_sql_layer() -> SQLRunResult:
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH.as_posix())
    try:
        _load_raw_tables(con)
        executed = _run_sql_files(con)
        exported_rows = _export_objects(con)
    finally:
        con.close()

    summary_path = OUTPUT_REPORTS_DIR / "sql_layer_execution_summary.md"
    lines = [
        "# SQL Layer Execution Summary (DuckDB)",
        "",
        f"- Base de datos: `{DB_PATH.as_posix()}`",
        f"- Raw source EV (preferente): `{EV_DATA_RAW_DIR.as_posix()}`",
        f"- Fallback raw legacy (solo compatibilidad): `{DATA_RAW_DIR.as_posix()}`",
        f"- Scripts ejecutados: {len(executed)}",
        "",
        "## Orden de ejecución",
    ]
    lines.extend([f"- {f}" for f in executed])
    lines.append("")
    lines.append("## Filas exportadas")
    for k, v in exported_rows.items():
        lines.append(f"- {k}: {v}")
    summary_path.write_text("\n".join(lines), encoding="utf-8")

    return SQLRunResult(
        db_path=DB_PATH.as_posix(),
        executed_files=executed,
        exported_rows=exported_rows,
    )


def load_ev_table(name: str) -> pd.DataFrame:
    path = DATA_PROCESSED_DIR / "ev_factory" / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"No existe tabla exportada: {path}")
    return pd.read_csv(path)


if __name__ == "__main__":
    result = run_ev_sql_layer()
    print("SQL layer EV ejecutada")
    print(f"DB: {result.db_path}")
    for item in result.executed_files:
        print(f"- {item}")
    for key, value in result.exported_rows.items():
        print(f"{key}: {value}")
