from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


RAW_DIR = Path("data/raw/ev_factory")
REPORT_DIR = Path("outputs/reports")


@dataclass(frozen=True)
class TableSpec:
    name: str
    grain: str
    key_candidates: List[str]
    expected_fks: Dict[str, str]


TABLE_SPECS = [
    TableSpec("ordenes", "1 fila por orden de producción", ["orden_id"], {"vehiculo_id": "vehiculos.vehiculo_id", "version_id": "versiones_vehiculo.version_id"}),
    TableSpec("versiones_vehiculo", "1 fila por versión de vehículo", ["version_id"], {}),
    TableSpec("vehiculos", "1 fila por vehículo", ["vehiculo_id"], {"version_id": "versiones_vehiculo.version_id"}),
    TableSpec("estado_bateria", "1 fila por lectura temporal de batería por vehículo", ["timestamp", "vehiculo_id"], {"vehiculo_id": "vehiculos.vehiculo_id"}),
    TableSpec("slots_carga", "1 fila por slot de carga", ["slot_id"], {}),
    TableSpec("sesiones_carga", "1 fila por sesión de carga", ["sesion_id"], {"vehiculo_id": "vehiculos.vehiculo_id", "slot_id": "slots_carga.slot_id"}),
    TableSpec("patio", "1 fila por estado temporal en patio por vehículo", ["timestamp", "vehiculo_id", "zona_patio"], {"vehiculo_id": "vehiculos.vehiculo_id"}),
    TableSpec("movimientos_patio", "1 fila por movimiento de patio", ["movimiento_id"], {"vehiculo_id": "vehiculos.vehiculo_id"}),
    TableSpec("turnos", "1 fila por fecha-turno", ["fecha", "turno"], {}),
    TableSpec("logistica_salida", "1 fila por evento de salida por vehículo", ["salida_id"], {"vehiculo_id": "vehiculos.vehiculo_id"}),
    TableSpec("cuellos_botella", "1 fila por evento de cuello de botella", ["evento_id"], {}),
    TableSpec("recursos_operativos", "1 fila por recurso operativo", ["recurso_id"], {}),
    TableSpec("restricciones_operativas", "1 fila por restricción operativa", ["restriccion_id"], {}),
    TableSpec("escenarios_transicion", "1 fila por día de transición", ["fecha"], {}),
]


DATETIME_CANDIDATES = [
    "fecha_programada",
    "fecha_real",
    "timestamp_fin_linea",
    "timestamp_entrada_patio",
    "timestamp_inicio_carga",
    "timestamp_fin_carga",
    "timestamp_salida",
    "timestamp",
    "inicio_sesion",
    "fin_sesion",
    "timestamp_inicio",
    "timestamp_fin",
    "fecha",
    "fecha_salida_planificada",
    "fecha_salida_real",
]


def _to_markdown_safe(df: pd.DataFrame) -> str:
    """Render markdown sin depender obligatoriamente de tabulate."""
    try:
        return df.to_markdown(index=False)
    except Exception:
        if df.empty:
            return "_(sin filas)_"
        cols = [str(c) for c in df.columns]
        header = "| " + " | ".join(cols) + " |"
        sep = "| " + " | ".join(["---"] * len(cols)) + " |"
        rows = []
        for _, row in df.iterrows():
            vals = [str(row[c]).replace("\n", " ") for c in df.columns]
            rows.append("| " + " | ".join(vals) + " |")
        return "\n".join([header, sep] + rows)


def _load_tables() -> Dict[str, pd.DataFrame]:
    tables: Dict[str, pd.DataFrame] = {}
    for spec in TABLE_SPECS:
        path = RAW_DIR / f"{spec.name}.csv"
        if not path.exists():
            raise FileNotFoundError(f"No existe tabla requerida: {path}")
        df = pd.read_csv(path)
        for col in DATETIME_CANDIDATES:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        tables[spec.name] = df
    return tables


def _classify_column(series: pd.Series, col_name: str) -> str:
    lname = col_name.lower()
    if lname.endswith("_id") or lname in {"vin_proxy", "orden_id", "vehiculo_id", "version_id", "slot_id"}:
        return "identificadores"
    if "flag" in lname:
        return "booleanas"
    if "fecha" in lname or "timestamp" in lname or lname.startswith("inicio_") or lname.startswith("fin_"):
        return "temporales"
    if pd.api.types.is_numeric_dtype(series):
        if any(tok in lname for tok in ["rate", "score", "indice", "pct", "kwh", "min", "severidad", "capacidad"]):
            return "metricas"
        return "metricas"
    if any(tok in lname for tok in ["turno", "zona", "tipo", "area", "estado", "mercado", "escenario", "causa"]):
        return "dimensiones"
    return "estructurales"


def _table_temporal_coverage(df: pd.DataFrame) -> str:
    time_cols = [c for c in df.columns if ("fecha" in c.lower() or "timestamp" in c.lower()) and pd.api.types.is_datetime64_any_dtype(df[c])]
    if not time_cols:
        return "N/A"
    mins = []
    maxs = []
    for c in time_cols:
        if df[c].notna().any():
            mins.append(df[c].min())
            maxs.append(df[c].max())
    if not mins:
        return "N/A"
    return f"{min(mins)} -> {max(maxs)}"


def _profile_tables(tables: Dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_rows = []
    column_rows = []

    for spec in TABLE_SPECS:
        df = tables[spec.name]

        null_rate = float(df.isna().mean().mean() * 100)
        dup_rate = float(df.duplicated().mean() * 100)

        key_uniqueness = []
        for key_col in spec.key_candidates:
            if key_col in df.columns:
                key_uniqueness.append(f"{key_col}:{df[key_col].is_unique}")

        summary_rows.append(
            {
                "tabla": spec.name,
                "grain": spec.grain,
                "key_candidates": "; ".join(spec.key_candidates),
                "foreign_keys_esperadas": "; ".join([f"{k}->{v}" for k, v in spec.expected_fks.items()]) if spec.expected_fks else "N/A",
                "n_filas": int(df.shape[0]),
                "n_columnas": int(df.shape[1]),
                "cobertura_temporal": _table_temporal_coverage(df),
                "null_rate_pct_promedio": round(null_rate, 3),
                "duplicados_pct": round(dup_rate, 4),
                "candidate_key_unique": "; ".join(key_uniqueness) if key_uniqueness else "N/A",
            }
        )

        for col in df.columns:
            series = df[col]
            classification = _classify_column(series, col)
            distinct = int(series.nunique(dropna=True))
            top_values = series.value_counts(dropna=True).head(3)
            top_repr = " | ".join([f"{idx}:{int(val)}" for idx, val in top_values.items()]) if not top_values.empty else "N/A"

            col_row = {
                "tabla": spec.name,
                "columna": col,
                "clasificacion": classification,
                "dtype": str(series.dtype),
                "null_rate_pct": round(float(series.isna().mean() * 100), 3),
                "cardinalidad": distinct,
                "top_values": top_repr,
                "utilidad_analitica": "alta" if classification in {"metricas", "temporales", "identificadores"} else "media",
            }

            if pd.api.types.is_numeric_dtype(series):
                col_row["min"] = round(float(series.min()), 4) if series.notna().any() else np.nan
                col_row["p50"] = round(float(series.median()), 4) if series.notna().any() else np.nan
                col_row["p95"] = round(float(series.quantile(0.95)), 4) if series.notna().any() else np.nan
                col_row["max"] = round(float(series.max()), 4) if series.notna().any() else np.nan
            else:
                col_row["min"] = np.nan
                col_row["p50"] = np.nan
                col_row["p95"] = np.nan
                col_row["max"] = np.nan

            column_rows.append(col_row)

    return pd.DataFrame(summary_rows), pd.DataFrame(column_rows)


def _detect_issues(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    issues: List[Dict[str, object]] = []

    ordenes = tables["ordenes"]
    vehiculos = tables["vehiculos"]
    versiones = tables["versiones_vehiculo"]
    bateria = tables["estado_bateria"]
    sesiones = tables["sesiones_carga"]
    patio = tables["patio"]
    movimientos = tables["movimientos_patio"]
    logistica = tables["logistica_salida"]
    cuellos = tables["cuellos_botella"]
    recursos = tables["recursos_operativos"]
    restricciones = tables["restricciones_operativas"]

    def add_issue(issue: str, severity: str, affected: int, rule: str, recommendation: str) -> None:
        if affected <= 0:
            return
        issues.append(
            {
                "issue": issue,
                "severity": severity,
                "affected_rows": int(affected),
                "rule": rule,
                "recommended_fix": recommendation,
            }
        )

    duplicate_orders = int(ordenes["orden_id"].duplicated().sum())
    add_issue(
        "ordenes_duplicadas",
        "critical",
        duplicate_orders,
        "orden_id debe ser único",
        "Aplicar de-duplicación por orden_id y timestamp de ingesta más reciente.",
    )

    seq_dup_planned = int(ordenes.duplicated(subset=["fecha_programada", "turno", "secuencia_planeada"]).sum())
    seq_dup_real = int(ordenes.duplicated(subset=["fecha_real", "turno", "secuencia_real"]).sum())
    add_issue(
        "secuencias_incoherentes",
        "high",
        seq_dup_planned + seq_dup_real,
        "No debe haber colisiones de secuencia por turno",
        "Normalizar lógica de secuencia y resolver ties por timestamp de fin de línea.",
    )

    ts_order_issues = int(
        (
            (vehiculos["timestamp_entrada_patio"] < vehiculos["timestamp_fin_linea"]) |
            (vehiculos["timestamp_inicio_carga"].notna() & (vehiculos["timestamp_inicio_carga"] < vehiculos["timestamp_entrada_patio"])) |
            (vehiculos["timestamp_fin_carga"].notna() & (vehiculos["timestamp_inicio_carga"].notna()) & (vehiculos["timestamp_fin_carga"] < vehiculos["timestamp_inicio_carga"])) |
            (vehiculos["timestamp_salida"].notna() & (vehiculos["timestamp_salida"] < vehiculos["timestamp_entrada_patio"]))
        ).sum()
    )
    add_issue(
        "timestamps_fuera_de_orden",
        "critical",
        ts_order_issues,
        "Secuencia temporal fin_linea <= entrada_patio <= inicio_carga <= fin_carga <= salida",
        "Aplicar reglas de saneamiento temporal y descartar eventos desordenados por vehículo.",
    )

    salida_no_ready = int(((logistica["fecha_salida_real"].notna()) & (logistica["readiness_salida_flag"] == 0)).sum())
    add_issue(
        "vehiculos_salen_sin_ready",
        "critical",
        salida_no_ready,
        "No debe haber salida real con readiness_salida_flag=0",
        "Introducir bloqueo hard en lógica de expedición o marca explícita de override operativo.",
    )

    impossible_sessions = int(
        (
            (sesiones["fin_sesion"] < sesiones["inicio_sesion"]) |
            (sesiones["energia_entregada_kwh"] <= 0) |
            (sesiones["tiempo_espera_previo_min"] < 0)
        ).sum()
    )
    add_issue(
        "sesiones_carga_imposibles",
        "critical",
        impossible_sessions,
        "fin_sesion >= inicio_sesion, energia>0, espera>=0",
        "Corregir función de generación de sesiones y aplicar constraints en staging SQL.",
    )

    patio_dup_positions = int(patio.duplicated(subset=["timestamp", "vehiculo_id"]).sum())
    add_issue(
        "ocupaciones_patio_incompatibles",
        "high",
        patio_dup_positions,
        "vehículo no puede estar en dos posiciones al mismo timestamp",
        "Deduplicar snapshots por timestamp+vehiculo y conservar estado de mayor prioridad.",
    )

    soc_out = int((~bateria["soc_pct"].between(0, 100) | ~bateria["target_soc_pct"].between(0, 100)).sum())
    add_issue(
        "soc_fuera_de_rango",
        "critical",
        soc_out,
        "soc_pct y target_soc_pct en [0,100]",
        "Aplicar clipping y auditoría adicional por sensor o fuente sintética.",
    )

    ev_versions = set(versiones[versiones["requiere_carga_salida_flag"] == 1]["version_id"])
    ev_vehicles = vehiculos[vehiculos["version_id"].isin(ev_versions)]["vehiculo_id"]
    ev_without_charge = int((~ev_vehicles.isin(sesiones["vehiculo_id"])).sum())
    add_issue(
        "ev_sin_carga_requerida",
        "high",
        ev_without_charge,
        "Vehículos EV con requiere_carga_salida_flag=1 deben tener sesión de carga",
        "Forzar sesión mínima o marcar excepción operativa documentada.",
    )

    delay_no_cause = int(((logistica["retraso_min"] > 0) & (logistica["causa_retraso"].fillna("SIN_DATO") == "SIN_RETRASO")).sum())
    add_issue(
        "retrasos_sin_causa",
        "medium",
        delay_no_cause,
        "Retraso positivo requiere causa de retraso válida",
        "Imponer catálogo de causas y fallback AUTOMATIC_CLASSIFICATION.",
    )

    bottleneck_no_impact = int(((cuellos["impacto_throughput_proxy"] <= 0) | (cuellos["impacto_salida_proxy"] <= 0)).sum())
    add_issue(
        "cuellos_sin_impacto",
        "high",
        bottleneck_no_impact,
        "Todo cuello de botella debe modelar impacto > 0",
        "Regla de negocio: evento sin impacto se clasifica como observación, no cuello.",
    )

    latest_restriction_area = set(restricciones["area"].unique())
    recursos_sin_flag = recursos[(recursos["area"].isin(latest_restriction_area)) & (recursos["restriccion_actual_flag"] == 0)]
    inconsistent_capacity = int((recursos_sin_flag["capacidad_disponible"] < recursos_sin_flag["capacidad_nominal"] * 0.85).sum())
    add_issue(
        "restriccion_inconsistente_capacidad",
        "medium",
        inconsistent_capacity,
        "Si hay restricciones severas por área, recurso debería reflejar restriccion_actual_flag",
        "Sincronizar estado de recursos con restricciones activas por corte temporal.",
    )

    return pd.DataFrame(issues).sort_values(["severity", "affected_rows"], ascending=[True, False]) if issues else pd.DataFrame(
        columns=["issue", "severity", "affected_rows", "rule", "recommended_fix"]
    )


def _build_recommendations_md(issues: pd.DataFrame) -> str:
    recommendations = [
        "## Recomendaciones para transformación analítica",
        "- Normalizar timestamps a UTC + timezone operacional de planta.",
        "- Construir `vehicle_timeline_canonical` como fuente única para lead times.",
        "- Aplicar constraints de integridad referencial en capa staging SQL.",
        "- Mantener catálogo controlado de estados y causas para evitar ruido semántico.",
        "- Definir reglas de override operativo para salidas sin readiness.",
        "- Versionar reglas de scoring y validación para trazabilidad auditada.",
    ]

    if not issues.empty:
        critical = issues[issues["severity"] == "critical"]
        if not critical.empty:
            recommendations.append("- Prioridad inmediata: resolver issues `critical` antes de consumo ejecutivo.")

    recommendations.extend(
        [
            "",
            "## Propuesta de joins oficiales",
            "- `ordenes.vehiculo_id` -> `vehiculos.vehiculo_id`",
            "- `ordenes.version_id` -> `versiones_vehiculo.version_id`",
            "- `sesiones_carga.vehiculo_id` -> `vehiculos.vehiculo_id`",
            "- `sesiones_carga.slot_id` -> `slots_carga.slot_id`",
            "- `estado_bateria.vehiculo_id` -> `vehiculos.vehiculo_id`",
            "- `patio.vehiculo_id` -> `vehiculos.vehiculo_id`",
            "- `movimientos_patio.vehiculo_id` -> `vehiculos.vehiculo_id`",
            "- `logistica_salida.vehiculo_id` -> `vehiculos.vehiculo_id`",
            "- `turnos(fecha, turno)` -> `ordenes(fecha_programada::date, turno)`",
            "",
            "## Tablas candidatas para marts analíticos",
            "- `mart_vehicle_flow_day`: flujo integral diario por vehículo (lead times, readiness, salida).",
            "- `mart_area_shift_ops`: presión operativa y cuellos por área-turno.",
            "- `mart_charging_readiness`: utilización, colas, SOC gap e interrupciones.",
            "- `mart_yard_congestion`: dwell, blocking y movimientos no productivos por zona.",
            "- `mart_dispatch_risk`: riesgo de salida por causa, turno, versión y mercado.",
        ]
    )
    return "\n".join(recommendations)


def run_explore_data_audit() -> Dict[str, object]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    tables = _load_tables()
    summary_df, columns_df = _profile_tables(tables)
    issues_df = _detect_issues(tables)

    issues_path = REPORT_DIR / "explore_data_issues.csv"
    md_path = REPORT_DIR / "explore_data_audit.md"

    issues_df.to_csv(issues_path, index=False)

    lines = [
        "# /explore-data Audit - Operational Data Readiness",
        "",
        "## Alcance",
        "Auditoría formal de calidad y readiness operacional sobre las 14 tablas base del gemelo operativo EV.",
        "",
        "## Resumen por dataset",
        _to_markdown_safe(summary_df),
        "",
        "## Issues priorizados",
    ]

    if issues_df.empty:
        lines.append("No se detectaron issues materiales en esta ejecución.")
    else:
        lines.append(_to_markdown_safe(issues_df))

    lines.append("")
    lines.append(_build_recommendations_md(issues_df))

    md_text = "\n".join(lines)
    md_path.write_text(md_text, encoding="utf-8")

    return {
        "issues_path": str(issues_path),
        "md_path": str(md_path),
        "tables_profiled": int(summary_df.shape[0]),
        "columns_profiled": int(columns_df.shape[0]),
        "issues_count": int(issues_df.shape[0]),
    }


if __name__ == "__main__":
    out = run_explore_data_audit()
    print("Explore-data audit generado")
    for k, v in out.items():
        print(f"- {k}: {v}")
