from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import pandas as pd

from .config import DATA_RAW_DIR, OUTPUT_REPORTS_DIR, ensure_directories


@dataclass
class AuditResult:
    status: str
    resumen: Dict[str, object]


def _read_csv(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    path = DATA_RAW_DIR / f"{name}.csv"
    return pd.read_csv(path, parse_dates=parse_dates)


def run_data_quality_audit() -> AuditResult:
    ensure_directories()

    ordenes = _read_csv("ordenes_produccion", ["fecha_plan", "fecha_objetivo_salida"])
    versiones = _read_csv("versiones_vehiculo")
    prod = _read_csv("eventos_produccion", ["ts_inicio", "ts_fin"])
    patio = _read_csv("eventos_patio", ["ts_evento"])
    carga = _read_csv("sesiones_carga", ["ts_inicio_carga", "ts_fin_carga"])
    energia = _read_csv("disponibilidad_energia", ["ts_hora"])
    exped = _read_csv("eventos_expedicion", ["ts_ready_expedicion", "ts_salida_real"])

    tables = {
        "ordenes_produccion": ordenes,
        "versiones_vehiculo": versiones,
        "eventos_produccion": prod,
        "eventos_patio": patio,
        "sesiones_carga": carga,
        "disponibilidad_energia": energia,
        "eventos_expedicion": exped,
    }

    id_columns = {
        "ordenes_produccion": "order_id",
        "versiones_vehiculo": "vehicle_version_id",
        "eventos_produccion": "event_id",
        "eventos_patio": "yard_event_id",
        "sesiones_carga": "charge_session_id",
        "eventos_expedicion": "dispatch_event_id",
    }

    row_counts = {name: int(df.shape[0]) for name, df in tables.items()}
    null_rate_max = {
        name: round(float((df.isna().sum().max() / max(len(df), 1)) * 100), 2) for name, df in tables.items()
    }
    duplicates = {
        name: int(tables[name].duplicated(subset=[col]).sum())
        for name, col in id_columns.items()
    }

    prod_negative = int((prod["duracion_real_min"] < 0).sum())
    prod_temporal_errors = int((prod["ts_fin"] < prod["ts_inicio"]).sum())

    charge_negative = int((carga["kwh_entregados"] < 0).sum()) if not carga.empty else 0
    charge_temporal_errors = int((carga["ts_fin_carga"] < carga["ts_inicio_carga"]).sum()) if not carga.empty else 0
    soc_out_of_range = int(
        ((carga["soc_inicio"] < 0) | (carga["soc_inicio"] > 100) | (carga["soc_fin"] < 0) | (carga["soc_fin"] > 100)).sum()
    ) if not carga.empty else 0

    order_set = set(ordenes["order_id"])
    orphan_prod = int((~prod["order_id"].isin(order_set)).sum())
    orphan_patio = int((~patio["order_id"].isin(order_set)).sum())
    orphan_exped = int((~exped["order_id"].isin(order_set)).sum())

    yard_window = (
        patio.pivot_table(index="order_id", columns="tipo_evento", values="ts_evento", aggfunc="min")
        .reset_index()
        .rename_axis(None, axis=1)
    )
    merged_flow = (
        prod.groupby("order_id", as_index=False)
        .agg(ts_prod_fin=("ts_fin", "max"))
        .merge(yard_window[["order_id", "ENTRY", "EXIT"]], on="order_id", how="left")
        .merge(exped[["order_id", "ts_ready_expedicion", "ts_salida_real"]], on="order_id", how="left")
    )

    flow_temporal_issues = int(
        (
            (merged_flow["ENTRY"].notna() & (merged_flow["ENTRY"] < merged_flow["ts_prod_fin"]))
            | (merged_flow["EXIT"].notna() & merged_flow["ENTRY"].notna() & (merged_flow["EXIT"] < merged_flow["ENTRY"]))
            | (merged_flow["ts_salida_real"] < merged_flow["ts_ready_expedicion"])
        ).sum()
    )

    critical_issues = (
        sum(duplicates.values())
        + prod_negative
        + prod_temporal_errors
        + charge_negative
        + charge_temporal_errors
        + soc_out_of_range
        + orphan_prod
        + orphan_patio
        + orphan_exped
        + flow_temporal_issues
    )

    status = "PASS" if critical_issues == 0 else "WARN"

    summary = {
        "status": status,
        "row_counts": row_counts,
        "max_null_rate_pct": null_rate_max,
        "duplicates_by_table": duplicates,
        "critical_checks": {
            "prod_negative_duration": prod_negative,
            "prod_temporal_errors": prod_temporal_errors,
            "charge_negative_kwh": charge_negative,
            "charge_temporal_errors": charge_temporal_errors,
            "soc_out_of_range": soc_out_of_range,
            "orphan_prod": orphan_prod,
            "orphan_patio": orphan_patio,
            "orphan_exped": orphan_exped,
            "flow_temporal_issues": flow_temporal_issues,
        },
        "critical_issues_total": int(critical_issues),
    }

    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_REPORTS_DIR / "data_quality_audit.json"
    json_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    markdown_lines = [
        "# Data Quality Audit",
        "",
        f"Estado general: **{status}**",
        "",
        "## Recuento de filas",
    ]
    markdown_lines.extend([f"- `{k}`: {v}" for k, v in row_counts.items()])
    markdown_lines.append("")
    markdown_lines.append("## Incidencias críticas")
    markdown_lines.extend([f"- `{k}`: {v}" for k, v in summary["critical_checks"].items()])
    markdown_lines.append("")
    markdown_lines.append(f"Total incidencias críticas: **{critical_issues}**")

    md_path = OUTPUT_REPORTS_DIR / "data_quality_audit.md"
    md_path.write_text("\n".join(markdown_lines), encoding="utf-8")

    return AuditResult(status=status, resumen=summary)


if __name__ == "__main__":
    run_data_quality_audit()
