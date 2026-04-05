from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import pandas as pd


REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "ordenes": [
        "orden_id",
        "fecha_programada",
        "fecha_real",
        "turno",
        "secuencia_planeada",
        "secuencia_real",
        "vehiculo_id",
        "version_id",
        "prioridad_cliente",
        "mercado_destino",
        "estado_orden",
        "ready_for_dispatch_flag",
    ],
    "versiones_vehiculo": [
        "version_id",
        "familia_modelo",
        "tipo_propulsion",
        "capacidad_bateria_kwh",
        "tiempo_medio_produccion",
        "complejidad_montaje",
        "requiere_carga_salida_flag",
        "nivel_criticidad_logistica",
    ],
    "vehiculos": [
        "vehiculo_id",
        "vin_proxy",
        "version_id",
        "estado_fabricacion",
        "timestamp_fin_linea",
        "timestamp_entrada_patio",
        "timestamp_inicio_carga",
        "timestamp_fin_carga",
        "timestamp_salida",
        "nivel_bateria_salida",
        "readiness_score_inicial",
    ],
    "estado_bateria": [
        "timestamp",
        "vehiculo_id",
        "soc_pct",
        "target_soc_pct",
        "battery_temp_proxy",
        "charging_status",
        "energia_cargada_kwh",
        "tiempo_en_carga_min",
    ],
    "slots_carga": [
        "slot_id",
        "zona_carga",
        "potencia_max_kw",
        "tipo_cargador",
        "disponibilidad_flag",
        "mantenimiento_flag",
        "ocupacion_actual_flag",
    ],
    "sesiones_carga": [
        "sesion_id",
        "vehiculo_id",
        "slot_id",
        "inicio_sesion",
        "fin_sesion",
        "energia_entregada_kwh",
        "tiempo_espera_previo_min",
        "carga_interrumpida_flag",
        "causa_interrupcion",
    ],
    "patio": [
        "timestamp",
        "vehiculo_id",
        "zona_patio",
        "fila",
        "posicion",
        "estado_en_patio",
        "dwell_time_min",
        "blocking_flag",
        "requiere_movimiento_flag",
    ],
    "movimientos_patio": [
        "movimiento_id",
        "vehiculo_id",
        "timestamp_inicio",
        "timestamp_fin",
        "origen",
        "destino",
        "motivo_movimiento",
        "operador_turno",
        "movimiento_no_productivo_flag",
    ],
    "turnos": [
        "fecha",
        "turno",
        "headcount_proxy",
        "absentismo_proxy",
        "productividad_turno_indice",
        "presion_operativa_indice",
        "overtime_flag",
    ],
    "logistica_salida": [
        "salida_id",
        "vehiculo_id",
        "fecha_salida_planificada",
        "fecha_salida_real",
        "modo_salida",
        "transportista_proxy",
        "readiness_salida_flag",
        "retraso_min",
        "causa_retraso",
    ],
    "cuellos_botella": [
        "evento_id",
        "timestamp",
        "area",
        "tipo_cuello_botella",
        "severidad",
        "duracion_min",
        "impacto_throughput_proxy",
        "impacto_salida_proxy",
        "causa_probable",
    ],
    "recursos_operativos": [
        "recurso_id",
        "tipo_recurso",
        "area",
        "capacidad_nominal",
        "capacidad_disponible",
        "restriccion_actual_flag",
    ],
    "restricciones_operativas": [
        "restriccion_id",
        "timestamp_inicio",
        "timestamp_fin",
        "area",
        "tipo_restriccion",
        "severidad",
        "impacto_capacidad_pct",
    ],
    "escenarios_transicion": [
        "fecha",
        "escenario",
        "share_ev",
        "intensidad_ramp_up",
        "disponibilidad_slots_carga",
        "presion_patio_indice",
        "restriccion_logistica_indice",
    ],
}


def validate_synthetic_data(
    tables: Dict[str, pd.DataFrame],
    report_dir: Path,
) -> Dict[str, object]:
    report_dir.mkdir(parents=True, exist_ok=True)

    checks: List[Dict[str, object]] = []

    for table_name, required_cols in REQUIRED_COLUMNS.items():
        df = tables[table_name]
        missing = [c for c in required_cols if c not in df.columns]
        checks.append(
            {
                "check": f"columnas_{table_name}",
                "status": "PASS" if not missing else "FAIL",
                "detail": "ok" if not missing else f"faltan: {missing}",
            }
        )

    period_start = pd.to_datetime(tables["ordenes"]["fecha_programada"]).min()
    period_end = pd.to_datetime(tables["ordenes"]["fecha_programada"]).max()
    period_months = (period_end.year - period_start.year) * 12 + (period_end.month - period_start.month) + 1
    checks.append(
        {
            "check": "horizonte_meses_9_15",
            "status": "PASS" if 9 <= period_months <= 15 else "FAIL",
            "detail": f"meses={period_months}",
        }
    )

    ordenes = tables["ordenes"]
    vehiculos = tables["vehiculos"]
    sesiones = tables["sesiones_carga"]
    bateria = tables["estado_bateria"]
    escenarios = tables["escenarios_transicion"]
    logistica = tables["logistica_salida"]

    checks.append(
        {
            "check": "unicidad_orden_id",
            "status": "PASS" if ordenes["orden_id"].is_unique else "FAIL",
            "detail": f"duplicados={int(ordenes['orden_id'].duplicated().sum())}",
        }
    )
    checks.append(
        {
            "check": "unicidad_vehiculo_id",
            "status": "PASS" if vehiculos["vehiculo_id"].is_unique else "FAIL",
            "detail": f"duplicados={int(vehiculos['vehiculo_id'].duplicated().sum())}",
        }
    )

    missing_vehicle_orders = int((~ordenes["vehiculo_id"].isin(vehiculos["vehiculo_id"])).sum())
    checks.append(
        {
            "check": "integridad_ordenes_vehiculos",
            "status": "PASS" if missing_vehicle_orders == 0 else "FAIL",
            "detail": f"orphan={missing_vehicle_orders}",
        }
    )

    if not sesiones.empty:
        orphan_sessions = int((~sesiones["vehiculo_id"].isin(vehiculos["vehiculo_id"])).sum())
    else:
        orphan_sessions = 0
    checks.append(
        {
            "check": "integridad_sesiones_vehiculos",
            "status": "PASS" if orphan_sessions == 0 else "FAIL",
            "detail": f"orphan={orphan_sessions}",
        }
    )

    soc_ok = bateria["soc_pct"].between(0, 100).all() and bateria["target_soc_pct"].between(0, 100).all()
    checks.append(
        {
            "check": "rango_soc",
            "status": "PASS" if soc_ok else "FAIL",
            "detail": "soc dentro de [0,100]",
        }
    )

    if not sesiones.empty:
        wait_mean = float(sesiones["tiempo_espera_previo_min"].mean())
        checks.append(
            {
                "check": "espera_carga_no_trivial",
                "status": "PASS" if wait_mean >= 8 else "WARN",
                "detail": f"media_espera={wait_mean:.2f}",
            }
        )

    ev_first = float(escenarios.head(max(10, len(escenarios) // 8))["share_ev"].mean())
    ev_last = float(escenarios.tail(max(10, len(escenarios) // 8))["share_ev"].mean())
    checks.append(
        {
            "check": "progresion_share_ev",
            "status": "PASS" if ev_last - ev_first > 0.18 else "WARN",
            "detail": f"inicio={ev_first:.3f}, fin={ev_last:.3f}",
        }
    )

    ready_ratio = float(ordenes["ready_for_dispatch_flag"].mean())
    checks.append(
        {
            "check": "ready_ratio_plausible",
            "status": "PASS" if 0.45 <= ready_ratio <= 0.98 else "WARN",
            "detail": f"ready_ratio={ready_ratio:.3f}",
        }
    )

    delay_mean = float(logistica["retraso_min"].mean())
    checks.append(
        {
            "check": "retraso_salida_plausible",
            "status": "PASS" if 25 <= delay_mean <= 600 else "WARN",
            "detail": f"delay_mean={delay_mean:.1f}",
        }
    )

    status_global = "PASS" if all(c["status"] != "FAIL" for c in checks) else "FAIL"

    cardinalidades = {
        name: int(df.shape[0]) for name, df in tables.items()
    }

    summary = {
        "status_global": status_global,
        "periodo": {
            "inicio": str(period_start),
            "fin": str(period_end),
            "meses": period_months,
        },
        "checks": checks,
        "cardinalidades": cardinalidades,
    }

    (report_dir / "synthetic_data_validation.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8"
    )

    lines = [
        "# Validaciones de Plausibilidad - Datos Sintéticos Industriales",
        "",
        f"Estado global: **{status_global}**",
        "",
        "## Periodo generado",
        f"- Inicio: `{period_start}`",
        f"- Fin: `{period_end}`",
        f"- Meses: `{period_months}`",
        "",
        "## Validaciones",
    ]
    lines.extend([f"- [{c['status']}] `{c['check']}` -> {c['detail']}" for c in checks])

    lines.extend(["", "## Cardinalidades"]) 
    lines.extend([f"- `{name}`: {count}" for name, count in cardinalidades.items()])

    (report_dir / "synthetic_data_plausibility.md").write_text("\n".join(lines), encoding="utf-8")

    summary_lines = [
        "# Resumen de Dimensiones, Periodos y Cardinalidades",
        "",
        f"- Horizonte: **{period_months} meses**",
        f"- Fecha inicio: **{period_start.date()}**",
        f"- Fecha fin: **{period_end.date()}**",
        "",
        "## Filas por tabla",
    ]
    summary_lines.extend([f"- `{name}`: {count}" for name, count in cardinalidades.items()])

    (report_dir / "synthetic_data_summary.md").write_text("\n".join(summary_lines), encoding="utf-8")

    return summary
