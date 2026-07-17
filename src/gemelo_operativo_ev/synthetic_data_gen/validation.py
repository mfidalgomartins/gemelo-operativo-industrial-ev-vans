from __future__ import annotations

from pathlib import Path

import pandas as pd

from ..utils import write_json_utf8, write_text_utf8

REQUIRED_COLUMNS: dict[str, list[str]] = {
    "ordenes": [
        "orden_id",
        "fecha_programada",
        "fecha_real",
        "fecha_turno_operativo",
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
        "timestamp_readiness",
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
    tables: dict[str, pd.DataFrame],
    report_dir: Path,
) -> dict[str, object]:
    missing_tables = sorted(set(REQUIRED_COLUMNS) - set(tables))
    if missing_tables:
        raise ValueError(f"faltan tablas requeridas: {missing_tables}")
    invalid_tables = [name for name, df in tables.items() if not isinstance(df, pd.DataFrame)]
    if invalid_tables:
        raise TypeError(f"estas tablas no son pandas DataFrame: {invalid_tables}")

    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    checks: list[dict[str, object]] = []
    failed_schema = False

    for table_name, required_cols in REQUIRED_COLUMNS.items():
        df = tables[table_name]
        missing = [c for c in required_cols if c not in df.columns]
        failed_schema = failed_schema or bool(missing)
        checks.append(
            {
                "check": f"columnas_{table_name}",
                "status": "PASS" if not missing else "FAIL",
                "detail": "ok" if not missing else f"faltan: {missing}",
            }
        )

    if failed_schema:
        return _write_validation_summary(
            report_dir=report_dir,
            status_global="FAIL",
            period_start=pd.NaT,
            period_end=pd.NaT,
            period_months=0,
            checks=checks,
            cardinalidades={name: int(df.shape[0]) for name, df in tables.items()},
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
    sequence_collisions = int(ordenes.duplicated(subset=["fecha_turno_operativo", "turno", "secuencia_planeada"]).sum())
    checks.append(
        {
            "check": "unicidad_secuencia_turno",
            "status": "PASS" if sequence_collisions == 0 else "FAIL",
            "detail": f"colisiones={sequence_collisions}",
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
    readiness_ts = pd.to_datetime(logistica["timestamp_readiness"], errors="coerce")
    departure_ts = pd.to_datetime(logistica["fecha_salida_real"], errors="coerce")
    departed_before_readiness = int(
        (departure_ts.notna() & (readiness_ts.isna() | (departure_ts < readiness_ts))).sum()
    )
    checks.append(
        {
            "check": "salida_posterior_a_readiness",
            "status": "PASS" if departed_before_readiness == 0 else "FAIL",
            "detail": f"salidas_antes_readiness={departed_before_readiness}",
        }
    )
    departed = logistica.loc[logistica["fecha_salida_real"].notna()]
    material_delay_rate = float((departed["retraso_min"] > 120).mean()) if not departed.empty else 0.0
    checks.append(
        {
            "check": "ratio_retraso_material_plausible",
            "status": "PASS" if 0.05 <= material_delay_rate <= 0.85 else "WARN",
            "detail": f"ratio_retraso_material={material_delay_rate:.3f}",
        }
    )

    status_global = "PASS" if all(c["status"] != "FAIL" for c in checks) else "FAIL"
    cardinalidades = {name: int(df.shape[0]) for name, df in tables.items()}

    return _write_validation_summary(
        report_dir=report_dir,
        status_global=status_global,
        period_start=period_start,
        period_end=period_end,
        period_months=period_months,
        checks=checks,
        cardinalidades=cardinalidades,
    )


def _write_validation_summary(
    report_dir: Path,
    status_global: str,
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
    period_months: int,
    checks: list[dict[str, object]],
    cardinalidades: dict[str, int],
) -> dict[str, object]:
    period_start_label = "Sin dato" if pd.isna(period_start) else str(period_start.date())
    period_end_label = "Sin dato" if pd.isna(period_end) else str(period_end.date())

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

    write_json_utf8(report_dir / "synthetic_data_validation.json", summary, default=str)

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

    write_text_utf8(report_dir / "synthetic_data_plausibility.md", "\n".join(lines))

    summary_lines = [
        "# Resumen de Dimensiones, Periodos y Cardinalidades",
        "",
        f"- Horizonte: **{period_months} meses**",
        f"- Fecha inicio: **{period_start_label}**",
        f"- Fecha fin: **{period_end_label}**",
        "",
        "## Filas por tabla",
    ]
    summary_lines.extend([f"- `{name}`: {count}" for name, count in cardinalidades.items()])

    write_text_utf8(report_dir / "synthetic_data_summary.md", "\n".join(summary_lines))

    return summary
