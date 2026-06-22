from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from .config import DATA_PROCESSED_DIR, EV_DATA_RAW_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT
from .utils import write_text_utf8

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

EXPORT_SORT_KEYS = {
    "vw_vehicle_flow_timeline": ["fecha_real", "turno", "orden_id", "vehiculo_id"],
    "vw_charging_utilization": ["fecha", "turno", "zona_carga", "slot_id"],
    "vw_yard_congestion": ["ts_hour", "zona_patio"],
    "vw_dispatch_readiness": ["fecha", "turno", "orden_id", "vehiculo_id"],
    "vw_shift_bottleneck_summary": ["fecha", "turno", "area"],
    "mart_vehicle_day": ["fecha", "turno", "orden_id", "vehiculo_id"],
    "mart_area_shift": ["fecha", "turno", "area"],
    "mart_dispatch_readiness": ["fecha", "turno", "tipo_propulsion", "version_id"],
    "kpi_readiness_shift_version": ["turno", "version_id", "tipo_propulsion"],
    "validation_checks": ["check_name"],
}

_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class SQLRunResult:
    db_path: str
    executed_files: list[str]
    exported_rows: dict[str, int]


def _resolve_raw_csv(table: str) -> Path:
    primary = EV_DATA_RAW_DIR / f"{table}.csv"
    if primary.exists():
        return primary

    raise FileNotFoundError(f"Falta tabla raw requerida en la ruta oficial EV: {primary}")


def _quote_identifier(identifier: str) -> str:
    if not _SQL_IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"Identificador SQL inválido: {identifier!r}")
    return f'"{identifier}"'


def _load_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    raw_paths = {table: _resolve_raw_csv(table) for table in RAW_TABLES}
    for table in RAW_TABLES:
        csv_path = raw_paths[table]
        table_identifier = _quote_identifier(table)
        # Identificador validado por _quote_identifier; path parametrizado.
        load_sql = f"CREATE OR REPLACE TABLE {table_identifier} AS\nSELECT\n    *\nFROM read_csv_auto(?, HEADER=TRUE);"
        con.execute(load_sql, [csv_path.as_posix()])


def _run_sql_files(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not SQL_LAYER_DIR.exists():
        raise FileNotFoundError(f"No existe directorio SQL requerido: {SQL_LAYER_DIR}")

    executed: list[str] = []
    for file_name in SQL_FILES_IN_ORDER:
        sql_path = SQL_LAYER_DIR / file_name
        if not sql_path.exists():
            raise FileNotFoundError(f"Falta script SQL requerido: {sql_path}")
        con.execute(sql_path.read_text(encoding="utf-8"))
        executed.append(file_name)
    return executed


def _export_objects(con: duckdb.DuckDBPyConnection, *, deterministic: bool = True) -> dict[str, int]:
    export_dir = DATA_PROCESSED_DIR / "ev_factory"
    export_dir.mkdir(parents=True, exist_ok=True)

    exported_row_counts: dict[str, int] = {}
    for obj in EXPORT_OBJECTS:
        out_csv = export_dir / f"{obj}.csv"
        obj_identifier = _quote_identifier(obj)
        # Orden determinista: SELECT sin ORDER BY puede devolver filas en orden
        # arbitrario (paralelismo de DuckDB). Ordenamos por todas las columnas
        # para que el CSV exportado sea byte-estable entre ejecuciones.
        columns = [
            row[0]
            for row in con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = ?
                ORDER BY ordinal_position
                """,
                [obj],
            ).fetchall()
        ]
        if not columns:
            raise ValueError(f"No se puede exportar objeto sin columnas o inexistente: {obj}")
        order_columns = [col for col in EXPORT_SORT_KEYS.get(obj, columns) if col in columns]
        order_clause = ", ".join(_quote_identifier(c) for c in order_columns)
        order_sql = f"ORDER BY {order_clause}" if deterministic and order_clause else ""
        # Objeto/columnas validados por _quote_identifier; destino parametrizado.
        export_sql = f"COPY (\nSELECT *\nFROM {obj_identifier}\n{order_sql}\n) TO ?\n(HEADER, DELIMITER ',');"
        con.execute(export_sql, [out_csv.as_posix()])
        exported_row_counts[obj] = int(con.execute(f"SELECT COUNT(*) FROM {obj_identifier}").fetchone()[0])
    return exported_row_counts


def run_ev_sql_layer(*, deterministic: bool = True, threads: int | None = None) -> SQLRunResult:
    if not isinstance(deterministic, bool):
        raise TypeError("deterministic debe ser booleano")
    if threads is not None and (not isinstance(threads, int) or threads <= 0):
        raise ValueError("threads debe ser un entero positivo o None")

    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH.as_posix())
    try:
        # Determinismo: la reducción paralela de DuckDB introduce diferencias
        # de último dígito en las agregaciones float entre ejecuciones. Forzar
        # un único thread garantiza salidas byte-idénticas reproducibles.
        if deterministic and threads is None:
            threads = 1
        if threads is not None:
            con.execute("PRAGMA threads=?", [threads])
        _load_raw_tables(con)
        executed = _run_sql_files(con)
        exported_rows = _export_objects(con, deterministic=deterministic)
    finally:
        con.close()

    summary_path = OUTPUT_REPORTS_DIR / "sql_layer_execution_summary.md"
    lines = [
        "# SQL Layer Execution Summary (DuckDB)",
        "",
        f"- Base de datos: `{DB_PATH.relative_to(PROJECT_ROOT).as_posix()}`",
        f"- Fuente raw EV: `{EV_DATA_RAW_DIR.relative_to(PROJECT_ROOT).as_posix()}`",
        f"- Scripts ejecutados: {len(executed)}",
        "",
        "## Orden de ejecución",
    ]
    lines.extend([f"- {f}" for f in executed])
    lines.append("")
    lines.append("## Filas exportadas")
    for k, v in exported_rows.items():
        lines.append(f"- {k}: {v}")
    write_text_utf8(summary_path, "\n".join(lines))

    return SQLRunResult(
        db_path=DB_PATH.relative_to(PROJECT_ROOT).as_posix(),
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
