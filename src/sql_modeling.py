from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import duckdb

from .config import DATA_PROCESSED_DIR, DATA_RAW_DIR, SQL_DIR, ensure_directories


LEGACY_SQL_DIR = SQL_DIR / "legacy"


RAW_TABLES = [
    "ordenes_produccion",
    "versiones_vehiculo",
    "eventos_produccion",
    "eventos_patio",
    "sesiones_carga",
    "disponibilidad_energia",
    "eventos_expedicion",
    "calendario_turnos",
]

EXPORT_TABLES = [
    "fct_flujo_unidad",
    "fct_carga_operativa",
    "fct_ocupacion_patio_hora",
    "fct_expedicion",
    "dim_vehiculo",
    "dim_turno",
    "dim_destino",
    "features_operativas",
    "scores_operativos",
    "recomendaciones_operativas",
]


def _load_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    for table in RAW_TABLES:
        csv_path = DATA_RAW_DIR / f"{table}.csv"
        con.execute(
            f"""
            CREATE OR REPLACE TABLE raw_{table} AS
            SELECT *
            FROM read_csv_auto('{csv_path.as_posix()}', HEADER=TRUE);
            """
        )


def _execute_sql_files(con: duckdb.DuckDBPyConnection) -> None:
    if not LEGACY_SQL_DIR.exists():
        raise FileNotFoundError(f"No existe directorio SQL legacy: {LEGACY_SQL_DIR}")
    sql_files = sorted(LEGACY_SQL_DIR.glob("*.sql"))
    if not sql_files:
        raise FileNotFoundError(f"No se encontraron scripts SQL legacy en: {LEGACY_SQL_DIR}")
    for sql_file in sql_files:
        query = sql_file.read_text(encoding="utf-8")
        con.execute(query)


def run_sql_modeling() -> Dict[str, int]:
    ensure_directories()
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    db_path = DATA_PROCESSED_DIR / "gemelo_operativo.duckdb"
    con = duckdb.connect(db_path.as_posix())

    try:
        _load_raw_tables(con)
        _execute_sql_files(con)

        counts: Dict[str, int] = {}
        for table in EXPORT_TABLES:
            out_csv = DATA_PROCESSED_DIR / f"{table}.csv"
            con.execute(
                f"""
                COPY (SELECT * FROM {table})
                TO '{out_csv.as_posix()}'
                (HEADER, DELIMITER ',');
                """
            )
            counts[table] = int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    finally:
        con.close()

    return counts


if __name__ == "__main__":
    run_sql_modeling()
