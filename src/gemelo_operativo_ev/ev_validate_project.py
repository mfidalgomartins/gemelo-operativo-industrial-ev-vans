from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .config import (
    DATA_PROCESSED_DIR,
    DATA_RAW_DIR,
    EV_DATA_RAW_DIR,
    OUTPUT_DASHBOARD_DIR,
    OUTPUT_REPORTS_AUDIT_DIR,
    OUTPUT_REPORTS_DIR,
)
from .utils import require_columns, to_markdown_safe, write_json_utf8, write_text_utf8

EV_DIR = DATA_PROCESSED_DIR / "ev_factory"

RAW_REQUIRED_COLUMNS = {
    "ordenes": ["orden_id", "vehiculo_id", "fecha_turno_operativo", "turno", "secuencia_planeada"],
    "vehiculos": [
        "vehiculo_id",
        "version_id",
        "timestamp_fin_linea",
        "timestamp_entrada_patio",
        "timestamp_inicio_carga",
        "timestamp_fin_carga",
    ],
    "estado_bateria": ["vehiculo_id", "soc_pct", "target_soc_pct"],
    "sesiones_carga": ["vehiculo_id", "inicio_sesion", "fin_sesion", "energia_entregada_kwh"],
    "logistica_salida": ["fecha_salida_real", "timestamp_readiness", "readiness_salida_flag"],
    "versiones_vehiculo": ["version_id", "requiere_carga_salida_flag"],
}

PROCESSED_REQUIRED_COLUMNS = {
    "vw_vehicle_flow_timeline": ["orden_id", "vehiculo_id", "fecha_real", "tipo_propulsion", "readiness_final_flag"],
    "vw_yard_congestion": ["yard_occupancy_rate"],
    "vw_dispatch_readiness": ["departed_flag", "delayed_flag"],
    "validation_checks": ["status"],
    "area_shift_features": [
        "area",
        "congestion_index",
        "avg_wait_time",
        "slot_utilization",
        "dispatch_risk_density",
        "bottleneck_density",
    ],
    "scenario_table": ["escenario", "share_ev_estimado", "decision_score"],
    "operational_prioritization_table": ["operational_priority_index", "main_risk_driver", "area_priority_tier"],
    "scoring_sensitivity_analysis": ["top3_areas"],
    "scoring_rank_stability": ["freq_share"],
    "kpi_operativos": [
        "share_ev",
        "score_readiness_global",
        "ratio_salida_retrasada",
        "throughput_planificado",
        "throughput_real",
        "throughput_gap",
    ],
}


@dataclass
class ValidationResult:
    status: str
    confidence: str
    issues: int
    release_grade: str


RELEASE_GRADE_LABELS = {
    "decision-support only": "solo apoyo a decisión",
    "screening-grade only": "solo screening",
    "publish-blocked": "publicación bloqueada",
    "not committee-grade": "no apto para comité",
    "unknown": "desconocido",
}

SEVERITY_LABELS = {
    "critical": "critica",
    "high": "alta",
    "medium": "media",
    "low": "baja",
}


def _release_grade_label(value: str) -> str:
    return RELEASE_GRADE_LABELS.get(value, value)


def _yes_no(value: bool) -> str:
    return "Sí" if value else "No"


def _issues_for_display(issues_df: pd.DataFrame) -> pd.DataFrame:
    if issues_df.empty:
        return pd.DataFrame(
            columns=["comprobacion", "severidad", "filas_fallidas", "detalle", "correccion_recomendada"]
        )
    return issues_df.rename(
        columns={
            "check": "comprobacion",
            "severity": "severidad",
            "failed_rows": "filas_fallidas",
            "detail": "detalle",
            "recommended_fix": "correccion_recomendada",
        }
    ).assign(severidad=lambda df: df["severidad"].map(SEVERITY_LABELS).fillna(df["severidad"]))


def _read_csv(path: Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Falta archivo para validación: {path}")
    return pd.read_csv(path, parse_dates=parse_dates)


def _read_dashboard_manifest(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"dashboard_build_manifest.json no es JSON válido: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("dashboard_build_manifest.json debe contener un objeto JSON")
    return payload


def _resolve_ev_raw(table_name: str) -> Path:
    primary = EV_DATA_RAW_DIR / f"{table_name}.csv"
    if primary.exists():
        return primary
    fallback = DATA_RAW_DIR / f"{table_name}.csv"
    if fallback.exists():
        return fallback
    raise FileNotFoundError(f"No existe tabla EV de origen requerida: {primary}")


@dataclass(frozen=True)
class ValidationContext:
    raw_tables: dict[str, pd.DataFrame]
    processed_tables: dict[str, pd.DataFrame]
    inherited_kpi_summary: pd.DataFrame
    dashboard_path: Path
    dashboard_ok: bool
    dashboard_manifest: dict[str, object]


@dataclass(frozen=True)
class RawCheckMetrics:
    null_vehiculo: int
    ts_issues: int
    seq_dup: int
    patio_over: int
    sess_bad: int
    soc_bad: int
    ev_without_charge: int
    out_without_ready: int
    denom_bad: int


@dataclass(frozen=True)
class AnalyticalCheckMetrics:
    opi_unique: int
    driver_unique: int
    flat_area_metrics: int
    share_ev_gap: float
    readiness_gap: float
    delay_rate_gap: float
    delay_rate_kpi: float
    inherited_kpi_present: int
    inherited_mismatch: int


@dataclass(frozen=True)
class ArtifactCheckMetrics:
    placeholders_left: int
    scenario_spread: float


class IssueCollector:
    def __init__(self) -> None:
        self.items: list[dict[str, object]] = []

    def add(
        self,
        check: str,
        severity: str,
        failed_rows: int,
        detail: str,
        fix: str = "Sin recomendación específica",
    ) -> None:
        if failed_rows <= 0:
            return
        self.items.append(
            {
                "check": check,
                "severity": severity,
                "failed_rows": int(failed_rows),
                "detail": detail,
                "recommended_fix": fix,
            }
        )


def _load_validation_context() -> ValidationContext:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    # Base raw
    ordenes = _read_csv(_resolve_ev_raw("ordenes"), parse_dates=["fecha_programada", "fecha_real"])
    vehiculos = _read_csv(
        _resolve_ev_raw("vehiculos"),
        parse_dates=[
            "timestamp_fin_linea",
            "timestamp_entrada_patio",
            "timestamp_inicio_carga",
            "timestamp_fin_carga",
            "timestamp_salida",
        ],
    )
    bateria = _read_csv(_resolve_ev_raw("estado_bateria"), parse_dates=["timestamp"])
    sesiones = _read_csv(_resolve_ev_raw("sesiones_carga"), parse_dates=["inicio_sesion", "fin_sesion"])
    logistica = _read_csv(
        _resolve_ev_raw("logistica_salida"),
        parse_dates=["fecha_salida_planificada", "fecha_salida_real", "timestamp_readiness"],
    )
    versiones = _read_csv(_resolve_ev_raw("versiones_vehiculo"))
    raw_tables = {
        "ordenes": ordenes,
        "vehiculos": vehiculos,
        "estado_bateria": bateria,
        "sesiones_carga": sesiones,
        "logistica_salida": logistica,
        "versiones_vehiculo": versiones,
    }
    for name, df in raw_tables.items():
        require_columns(df, RAW_REQUIRED_COLUMNS[name], name)

    # Capa analítica
    vehicle_flow = _read_csv(EV_DIR / "vw_vehicle_flow_timeline.csv")
    yard_congestion = _read_csv(EV_DIR / "vw_yard_congestion.csv")
    dispatch_readiness = _read_csv(EV_DIR / "vw_dispatch_readiness.csv")
    validation_checks = _read_csv(EV_DIR / "validation_checks.csv")
    area_shift_features = _read_csv(EV_DIR / "area_shift_features.csv")
    scenarios = _read_csv(EV_DIR / "scenario_table.csv")
    scoring = _read_csv(EV_DIR / "operational_prioritization_table.csv")
    scoring_sensitivity = _read_csv(EV_DIR / "scoring_sensitivity_analysis.csv")
    scoring_rank_stability = _read_csv(EV_DIR / "scoring_rank_stability.csv")
    kpi = _read_csv(EV_DIR / "kpi_operativos.csv")
    processed_tables = {
        "vw_vehicle_flow_timeline": vehicle_flow,
        "vw_yard_congestion": yard_congestion,
        "vw_dispatch_readiness": dispatch_readiness,
        "validation_checks": validation_checks,
        "area_shift_features": area_shift_features,
        "scenario_table": scenarios,
        "operational_prioritization_table": scoring,
        "scoring_sensitivity_analysis": scoring_sensitivity,
        "scoring_rank_stability": scoring_rank_stability,
        "kpi_operativos": kpi,
    }
    for name, df in processed_tables.items():
        require_columns(df, PROCESSED_REQUIRED_COLUMNS[name], name)

    inherited_kpi_summary_path = OUTPUT_REPORTS_DIR / "kpi_summary.csv"
    inherited_kpi_summary = (
        pd.read_csv(inherited_kpi_summary_path) if inherited_kpi_summary_path.exists() else pd.DataFrame()
    )

    dashboard_path = OUTPUT_DASHBOARD_DIR / "industrial-ev-operating-command-center.html"
    dashboard_ok = dashboard_path.exists() and dashboard_path.stat().st_size > 100_000
    dashboard_manifest_path = OUTPUT_REPORTS_DIR / "dashboard_build_manifest.json"
    dashboard_manifest = _read_dashboard_manifest(dashboard_manifest_path)

    return ValidationContext(
        raw_tables=raw_tables,
        processed_tables=processed_tables,
        inherited_kpi_summary=inherited_kpi_summary,
        dashboard_path=dashboard_path,
        dashboard_ok=dashboard_ok,
        dashboard_manifest=dashboard_manifest,
    )


def _check_raw_quality(
    context: ValidationContext,
    issues: IssueCollector,
) -> RawCheckMetrics:
    ordenes = context.raw_tables["ordenes"]
    vehiculos = context.raw_tables["vehiculos"]
    bateria = context.raw_tables["estado_bateria"]
    sesiones = context.raw_tables["sesiones_carga"]
    logistica = context.raw_tables["logistica_salida"]
    versiones = context.raw_tables["versiones_vehiculo"]
    yard_congestion = context.processed_tables["vw_yard_congestion"]
    scoring = context.processed_tables["operational_prioritization_table"]
    add_issue = issues.add

    # Conteos de filas razonables
    if len(ordenes) < 1000:
        add_issue(
            "row_count_ordenes",
            "high",
            1,
            "Volumen de órdenes insuficiente para lectura industrial",
            "Ajustar generador para >1000 órdenes",
        )

    # Duplicados inesperados
    add_issue(
        "duplicados_orden_id",
        "critical",
        int(ordenes["orden_id"].duplicated().sum()),
        "orden_id debe ser único",
        "Actualizar el generador con unicidad estricta",
    )

    # Nulls problemáticos
    null_vehiculo = int(ordenes["vehiculo_id"].isna().sum())
    add_issue(
        "null_vehiculo_id_ordenes",
        "critical",
        null_vehiculo,
        "Órdenes sin vehiculo_id",
        "Imponer NOT NULL en la preparación SQL",
    )

    # Marcas temporales imposibles
    ts_issues = int(
        (
            (vehiculos["timestamp_entrada_patio"] < vehiculos["timestamp_fin_linea"])
            | (
                vehiculos["timestamp_inicio_carga"].notna()
                & (vehiculos["timestamp_inicio_carga"] < vehiculos["timestamp_entrada_patio"])
            )
            | (
                vehiculos["timestamp_fin_carga"].notna()
                & vehiculos["timestamp_inicio_carga"].notna()
                & (vehiculos["timestamp_fin_carga"] < vehiculos["timestamp_inicio_carga"])
            )
        ).sum()
    )
    add_issue(
        "timestamps_imposibles",
        "critical",
        ts_issues,
        "Secuencia temporal inválida",
        "Aplicar regla de saneamiento en la preparación SQL",
    )

    # Secuencias incoherentes
    seq_dup = int(ordenes.duplicated(subset=["fecha_turno_operativo", "turno", "secuencia_planeada"]).sum())
    add_issue(
        "secuencias_incoherentes",
        "high",
        seq_dup,
        "Colisión secuencia plan por fecha-turno",
        "Resolver empates por prioridad y marca temporal",
    )

    # Patio capacidad
    patio_over = int((yard_congestion["yard_occupancy_rate"] > 1.0).sum())
    add_issue(
        "ocupacion_patio_vs_capacidad",
        "medium",
        patio_over,
        "Ocupaciones por encima de la capacidad gobernada",
        "Revisar capacidad de zona o lógica de asignación",
    )

    # Sesiones carga coherentes
    sess_bad = int(
        ((sesiones["fin_sesion"] < sesiones["inicio_sesion"]) | (sesiones["energia_entregada_kwh"] <= 0)).sum()
    )
    add_issue(
        "sesiones_carga_incoherentes",
        "critical",
        sess_bad,
        "Sesiones con duración negativa o energía <=0",
        "Añadir restricción en generador y preparación SQL",
    )

    # SOC en rango
    soc_bad = int((~bateria["soc_pct"].between(0, 100) | ~bateria["target_soc_pct"].between(0, 100)).sum())
    add_issue("soc_fuera_rango", "critical", soc_bad, "SOC fuera de [0,100]", "Recorte de rango y validación de sensor")

    # EV requiere carga tratamiento consistente
    ev_versions = set(versiones.loc[versiones["requiere_carga_salida_flag"] == 1, "version_id"])
    ev_veh = set(vehiculos.loc[vehiculos["version_id"].isin(ev_versions), "vehiculo_id"])
    veh_with_session = set(sesiones["vehiculo_id"])
    ev_without_charge = len(ev_veh - veh_with_session)
    add_issue(
        "ev_sin_carga_consistente",
        "high",
        ev_without_charge,
        "EV que requiere carga sin sesión",
        "Forzar sesión mínima o excepción explícita",
    )

    # Preparación y salida consistentes
    salidas_reales = int(logistica["fecha_salida_real"].notna().sum())
    out_without_ready = int(
        (
            logistica["fecha_salida_real"].notna()
            & (
                logistica["timestamp_readiness"].isna()
                | (logistica["fecha_salida_real"] < logistica["timestamp_readiness"])
            )
        ).sum()
    )
    out_without_ready_rate = (out_without_ready / salidas_reales) if salidas_reales else 0.0
    add_issue(
        "salida_sin_preparacion",
        "critical",
        out_without_ready,
        f"Salidas reales anteriores a readiness (tasa={out_without_ready_rate:.2%})",
        "Bloquear expedición hasta timestamp_readiness o registrar excepción trazable",
    )

    # Métricas agregadas y denominadores
    denom_bad = int(((scoring["operational_priority_index"] < 0) | (scoring["operational_priority_index"] > 100)).sum())
    add_issue("score_fuera_rango", "medium", denom_bad, "OPI fuera de 0-100", "Normalización de puntuaciones")

    return RawCheckMetrics(
        null_vehiculo=null_vehiculo,
        ts_issues=ts_issues,
        seq_dup=seq_dup,
        patio_over=patio_over,
        sess_bad=sess_bad,
        soc_bad=soc_bad,
        ev_without_charge=ev_without_charge,
        out_without_ready=out_without_ready,
        denom_bad=denom_bad,
    )


def _check_analytical_quality(
    context: ValidationContext,
    issues: IssueCollector,
) -> AnalyticalCheckMetrics:
    scoring = context.processed_tables["operational_prioritization_table"]
    area_shift_features = context.processed_tables["area_shift_features"]
    scoring_sensitivity = context.processed_tables["scoring_sensitivity_analysis"]
    scoring_rank_stability = context.processed_tables["scoring_rank_stability"]
    vehicle_flow = context.processed_tables["vw_vehicle_flow_timeline"]
    dispatch_readiness = context.processed_tables["vw_dispatch_readiness"]
    kpi = context.processed_tables["kpi_operativos"]
    inherited_kpi_summary = context.inherited_kpi_summary
    add_issue = issues.add

    # Integridad analítica: evitar falso sentido de precisión
    opi_unique = int(scoring["operational_priority_index"].nunique(dropna=True))
    driver_unique = int(scoring["main_risk_driver"].nunique(dropna=True))
    tier_unique = int(scoring["area_priority_tier"].nunique(dropna=True))
    add_issue(
        "puntuacion_sin_discriminacion",
        "critical",
        int(opi_unique < 3),
        f"OPI con baja discriminación (valores_unicos={opi_unique})",
        "Revisar mart_area_shift, pesos y normalización",
    )
    add_issue(
        "factor_riesgo_colapsado",
        "high",
        int(driver_unique < 2),
        f"factor principal de riesgo sin variedad (valores_unicos={driver_unique})",
        "Aumentar señales por área y validación de cruces",
    )
    add_issue(
        "tiers_colapsados",
        "medium",
        int(tier_unique < 2),
        f"nivel de prioridad sin separación suficiente (valores_unicos={tier_unique})",
        "Recalibrar umbrales y dispersión de puntuaciones",
    )

    flat_area_metrics = int(
        area_shift_features.groupby("area", as_index=False)
        .agg(
            congestion_index=("congestion_index", "mean"),
            avg_wait_time=("avg_wait_time", "mean"),
            slot_utilization=("slot_utilization", "mean"),
            dispatch_risk_density=("dispatch_risk_density", "mean"),
            bottleneck_density=("bottleneck_density", "mean"),
        )[["congestion_index", "avg_wait_time", "slot_utilization", "dispatch_risk_density", "bottleneck_density"]]
        .std()
        .fillna(0)
        .eq(0)
        .sum()
    )
    add_issue(
        "metrics_area_planas",
        "high" if flat_area_metrics > 2 else "low",
        int(flat_area_metrics > 2),
        f"Se detectaron {flat_area_metrics} métricas área-turno planas entre áreas",
        "Revisar integración de turnos y cruces por área",
    )

    sensitivity_top3_unique = int(scoring_sensitivity["top3_areas"].nunique(dropna=True))
    top1_max_share = float(scoring_rank_stability["freq_share"].max()) if not scoring_rank_stability.empty else 0.0
    add_issue(
        "ranking_sensibilidad_fragil",
        "medium",
        int(top1_max_share < 0.45),
        (
            f"Estabilidad de ranking baja bajo Monte Carlo de pesos "
            f"(max_top1_share={top1_max_share:.2%}, nunique_top3={sensitivity_top3_unique})"
        ),
        "Revisar composición de riesgos y calibración de pesos",
    )

    share_ev_flow = float((vehicle_flow["tipo_propulsion"] == "EV").mean()) if not vehicle_flow.empty else np.nan
    share_ev_kpi = float(kpi["share_ev"].iloc[0]) if not kpi.empty else np.nan
    share_ev_gap = (
        abs(share_ev_flow - share_ev_kpi) if np.isfinite(share_ev_flow) and np.isfinite(share_ev_kpi) else 1.0
    )
    add_issue(
        "kpi_share_ev_inconsistente",
        "high",
        int(share_ev_gap > 0.02),
        f"KPI share_ev no consistente con el flujo (brecha={share_ev_gap:.4f})",
        "Recalcular KPI desde mart gobernado",
    )
    readiness_flow = float(vehicle_flow["readiness_final_flag"].mean() * 100) if not vehicle_flow.empty else np.nan
    readiness_kpi = float(kpi["score_readiness_global"].iloc[0]) if not kpi.empty else np.nan
    readiness_gap = (
        abs(readiness_flow - readiness_kpi) if np.isfinite(readiness_flow) and np.isfinite(readiness_kpi) else 100.0
    )
    add_issue(
        "kpi_readiness_inconsistente",
        "high",
        int(readiness_gap > 1e-9),
        f"KPI score_readiness_global no consistente con el flujo (brecha={readiness_gap:.6f})",
        "Recalcular KPI desde readiness_final_flag",
    )

    departed_dispatch = dispatch_readiness.loc[dispatch_readiness["departed_flag"].astype(bool)]
    delay_rate_detail = float(departed_dispatch["delayed_flag"].mean()) if not departed_dispatch.empty else 0.0
    delay_rate_kpi = float(kpi["ratio_salida_retrasada"].iloc[0]) if not kpi.empty else np.nan
    delay_rate_gap = abs(delay_rate_detail - delay_rate_kpi) if np.isfinite(delay_rate_kpi) else 1.0
    add_issue(
        "kpi_delay_rate_inconsistente",
        "high",
        int(delay_rate_gap > 1e-9),
        f"ratio_salida_retrasada KPI vs detalle no consistente (brecha={delay_rate_gap:.6f})",
        "Recalcular KPI sobre vehículos despachados",
    )
    add_issue(
        "ratio_salida_retrasada_implausible",
        "medium",
        int(delay_rate_kpi > 0.85),
        f"ratio_salida_retrasada excesivo ({delay_rate_kpi:.2%})",
        "Revisar umbral de atraso material o calibración del generador",
    )

    throughput_plan_flow = int(len(vehicle_flow))
    throughput_plan_kpi = int(kpi["throughput_planificado"].iloc[0]) if not kpi.empty else -1
    add_issue(
        "kpi_throughput_inconsistente",
        "high",
        int(throughput_plan_flow != throughput_plan_kpi),
        f"KPI throughput_planificado ({throughput_plan_kpi}) distinto del flujo ({throughput_plan_flow})",
        "Alinear definición de caudal base",
    )

    # Fuente única de verdad de KPI (evitar deriva de artefactos heredados)
    inherited_kpi_present = int(not inherited_kpi_summary.empty)
    inherited_mismatch = 0
    if inherited_kpi_present and not kpi.empty:
        inherited_cols = set(inherited_kpi_summary.columns)
        required_inherited = {
            "throughput_diario_unidades",
            "score_readiness_operativa",
        }
        if required_inherited.issubset(inherited_cols):
            flow_days = max(int(vehicle_flow["fecha_real"].nunique()), 1) if not vehicle_flow.empty else 1
            expected_daily = float(kpi["throughput_real"].iloc[0]) / flow_days
            observed_daily = float(inherited_kpi_summary["throughput_diario_unidades"].iloc[0])
            expected_readiness = float(kpi["score_readiness_global"].iloc[0])
            observed_readiness = float(inherited_kpi_summary["score_readiness_operativa"].iloc[0])
            inherited_mismatch = int(
                abs(observed_daily - expected_daily) > 0.5 or abs(observed_readiness - expected_readiness) > 1.0
            )
        else:
            inherited_mismatch = 1

    add_issue(
        "kpi_artifact_heredado_presente",
        "high",
        inherited_kpi_present,
        "Existe outputs/reports/kpi_summary.csv fuera de la capa KPI oficial",
        "Eliminar artefacto heredado y usar solo data/processed/ev_factory/kpi_operativos.csv",
    )
    add_issue(
        "kpi_heredado_inconsistente",
        "high",
        inherited_mismatch,
        "kpi_summary.csv no es consistente con KPI oficial gobernado",
        "Regenerar desde KPI oficial o eliminar artefacto",
    )

    return AnalyticalCheckMetrics(
        opi_unique=opi_unique,
        driver_unique=driver_unique,
        flat_area_metrics=flat_area_metrics,
        share_ev_gap=share_ev_gap,
        readiness_gap=readiness_gap,
        delay_rate_gap=delay_rate_gap,
        delay_rate_kpi=delay_rate_kpi,
        inherited_kpi_present=inherited_kpi_present,
        inherited_mismatch=inherited_mismatch,
    )


def _check_artifacts_and_scenarios(
    context: ValidationContext,
    issues: IssueCollector,
) -> ArtifactCheckMetrics:
    dashboard_ok = context.dashboard_ok
    dashboard_path = context.dashboard_path
    dashboard_manifest = context.dashboard_manifest
    scenarios = context.processed_tables["scenario_table"]
    add_issue = issues.add

    # Consistencia de outputs y panel
    placeholders_left = 0
    if dashboard_ok:
        html = dashboard_path.read_text(encoding="utf-8", errors="ignore")
        placeholders_left = int(any(tok in html for tok in ["__SEQ__", "__FILTERS__", "__CHARTJS__"]))
    add_issue(
        "dashboard_inconsistente",
        "high",
        placeholders_left,
        "Placeholder sin resolver en el panel",
        "Reconstruir panel",
    )
    add_issue(
        "dashboard_manifest_missing",
        "high",
        int(not bool(dashboard_manifest)),
        "No existe manifiesto de construcción del panel",
        "Ejecutar construcción oficial y registrar comprobaciones",
    )
    if dashboard_manifest:
        failed_manifest_checks = int(sum(1 for ok in dashboard_manifest.get("checks", {}).values() if not ok))
        add_issue(
            "dashboard_manifest_checks",
            "high",
            failed_manifest_checks,
            "El manifiesto reporta comprobaciones en alerta",
            "Corregir diseño, datos o carga embebida del panel",
        )

    # Escenarios
    if len(scenarios) != 8:
        add_issue(
            "scenario_count",
            "high",
            abs(len(scenarios) - 8),
            "No se generaron los 8 escenarios obligatorios",
            "Reejecutar gemelo de escenarios",
        )
    scenario_base = scenarios.loc[scenarios["escenario"] == "1_ramp_up_ev_base", "share_ev_estimado"]
    scenario_acc = scenarios.loc[scenarios["escenario"] == "2_ramp_up_ev_acelerado", "share_ev_estimado"]
    if not scenario_base.empty and not scenario_acc.empty:
        add_issue(
            "scenario_ev_no_monotonic",
            "high",
            int(float(scenario_acc.iloc[0]) <= float(scenario_base.iloc[0])),
            "Escenario acelerado no incrementa cuota EV respecto al base",
            "Revisar motor de escenarios y parámetros",
        )
    scenario_spread = (
        float(scenarios["decision_score"].max() - scenarios["decision_score"].min()) if not scenarios.empty else 0.0
    )
    add_issue(
        "scenario_decision_spread_bajo",
        "medium",
        int(scenario_spread < 2.0),
        f"Dispersión de puntuación de decisión baja ({scenario_spread:.2f})",
        "Aumentar sensibilidad del motor de escenarios",
    )

    return ArtifactCheckMetrics(
        placeholders_left=placeholders_left,
        scenario_spread=scenario_spread,
    )


def _write_validation_outputs(
    context: ValidationContext,
    issues: IssueCollector,
    raw_metrics: RawCheckMetrics,
    analytical_metrics: AnalyticalCheckMetrics,
    artifact_metrics: ArtifactCheckMetrics,
) -> ValidationResult:
    ordenes = context.raw_tables["ordenes"]
    validation_checks = context.processed_tables["validation_checks"]
    dashboard_ok = context.dashboard_ok
    dashboard_manifest = context.dashboard_manifest

    null_vehiculo = raw_metrics.null_vehiculo
    ts_issues = raw_metrics.ts_issues
    seq_dup = raw_metrics.seq_dup
    patio_over = raw_metrics.patio_over
    sess_bad = raw_metrics.sess_bad
    soc_bad = raw_metrics.soc_bad
    ev_without_charge = raw_metrics.ev_without_charge
    out_without_ready = raw_metrics.out_without_ready
    denom_bad = raw_metrics.denom_bad

    opi_unique = analytical_metrics.opi_unique
    driver_unique = analytical_metrics.driver_unique
    flat_area_metrics = analytical_metrics.flat_area_metrics
    share_ev_gap = analytical_metrics.share_ev_gap
    readiness_gap = analytical_metrics.readiness_gap
    delay_rate_gap = analytical_metrics.delay_rate_gap
    delay_rate_kpi = analytical_metrics.delay_rate_kpi
    inherited_kpi_present = analytical_metrics.inherited_kpi_present
    inherited_mismatch = analytical_metrics.inherited_mismatch

    placeholders_left = artifact_metrics.placeholders_left
    scenario_spread = artifact_metrics.scenario_spread

    # Riesgo de sobreinterpretación
    caveats = [
        "Dato sintético: útil para arquitectura y lógica, no para comparación real de planta.",
        "Las elasticidades del gemelo operativo son supuestos paramétricos no calibrados, no estimaciones causales.",
        "La criticidad por área depende de pesos de puntuación; revisar sensibilidad antes de uso real.",
        "No incorpora variabilidad externa real (suministro, clima, huelgas, etc.).",
    ]

    issues_df = pd.DataFrame(issues.items)
    severity_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    if not issues_df.empty:
        issues_df["severity_rank"] = issues_df["severity"].map(severity_rank).fillna(99)
        issues_df = issues_df.sort_values(["severity_rank", "failed_rows"], ascending=[True, False]).drop(
            columns=["severity_rank"]
        )

    severity_weight = {"critical": 4, "high": 3, "medium": 2, "low": 1}
    risk_points = (
        int(sum(severity_weight.get(s, 1) for s in issues_df.get("severity", []))) if not issues_df.empty else 0
    )

    critical_issues = int((issues_df["severity"] == "critical").sum()) if not issues_df.empty else 0
    high_issues = int((issues_df["severity"] == "high").sum()) if not issues_df.empty else 0
    medium_issues = int((issues_df["severity"] == "medium").sum()) if not issues_df.empty else 0

    if issues_df.empty:
        status = "PASS"
        confidence = "ALTA"
    elif critical_issues == 0 and high_issues == 0 and risk_points <= 6:
        status = "WARN"
        confidence = "MEDIA"
    else:
        status = "WARN"
        confidence = "BAJA"

    # Cross-check con validation_checks SQL
    sql_warn = int((validation_checks["status"] != "PASS").sum())
    sql_checks_total = int(len(validation_checks))
    sql_warn_ratio = float(sql_warn / sql_checks_total) if sql_checks_total else 1.0

    technically_valid = critical_issues == 0 and dashboard_ok and sql_warn == 0 and len(ordenes) >= 1000
    analytically_acceptable = (
        technically_valid
        and opi_unique >= 3
        and driver_unique >= 2
        and flat_area_metrics <= 2
        and share_ev_gap <= 0.02
        and readiness_gap <= 1e-9
        and delay_rate_gap <= 1e-9
        and delay_rate_kpi <= 0.85
        and scenario_spread >= 2.0
    )
    decision_support_only = analytically_acceptable and high_issues <= 2
    screening_grade_only = technically_valid and not analytically_acceptable
    if not technically_valid:
        release_grade = "publish-blocked"
    elif not analytically_acceptable:
        release_grade = "screening-grade only"
    elif decision_support_only:
        release_grade = "decision-support only"
    else:
        release_grade = "not committee-grade"

    # Reporte final
    lines = [
        "# Informe de Validación - Gemelo Operativo EV",
        "",
        f"- Estado global: **{status}**",
        f"- Confianza global: **{confidence}**",
        f"- Grado de publicación: **{_release_grade_label(release_grade)}**",
        f"- Problemas detectados: **{len(issues_df)}**",
        f"- Comprobaciones SQL en alerta: **{sql_warn}**",
        f"- Ratio de alertas SQL: **{sql_warn_ratio:.2%}**",
        f"- Panel presente y materializado: **{_yes_no(dashboard_ok)}**",
        "",
        "## Estados de gobernanza",
        f"- técnicamente válido: **{_yes_no(technically_valid)}**",
        f"- analíticamente aceptable: **{_yes_no(analytically_acceptable)}**",
        f"- solo apoyo a decisión: **{_yes_no(decision_support_only)}**",
        f"- solo exploración inicial: **{_yes_no(screening_grade_only)}**",
        f"- no apto para comité: **{_yes_no(technically_valid)}**",
        f"- publicación bloqueada: **{_yes_no(release_grade == 'publish-blocked')}**",
        "",
        "## Lista de validación",
        f"- conteos de filas razonables: {'OK' if len(ordenes) >= 1000 else 'ALERTA'}",
        f"- duplicados inesperados: {'OK' if ordenes['orden_id'].is_unique else 'ALERTA'}",
        f"- nulos problemáticos: {'OK' if null_vehiculo == 0 else 'ALERTA'}",
        f"- marcas temporales imposibles: {'OK' if ts_issues == 0 else 'ALERTA'}",
        f"- secuencias incoherentes: {'OK' if seq_dup == 0 else 'ALERTA'}",
        f"- ocupación patio compatible: {'OK' if patio_over == 0 else 'ALERTA'}",
        f"- sesiones carga coherentes: {'OK' if sess_bad == 0 else 'ALERTA'}",
        f"- SOC dentro de rango: {'OK' if soc_bad == 0 else 'ALERTA'}",
        f"- EV con carga consistente: {'OK' if ev_without_charge == 0 else 'ALERTA'}",
        f"- preparación y salida consistentes: {'OK' if out_without_ready == 0 else 'ALERTA'}",
        f"- métricas agregadas y denominadores: {'OK' if denom_bad == 0 else 'ALERTA'}",
        f"- consistencia outputs-panel: {'OK' if placeholders_left == 0 and dashboard_ok else 'ALERTA'}",
        f"- discriminación de puntuación: {'OK' if opi_unique >= 3 else 'ALERTA'}",
        f"- diversidad de factor de riesgo: {'OK' if driver_unique >= 2 else 'ALERTA'}",
        f"- variabilidad área-turno: {'OK' if flat_area_metrics <= 2 else 'ALERTA'}",
        f"- consistencia KPI cuota EV: {'OK' if share_ev_gap <= 0.02 else 'ALERTA'}",
        f"- consistencia KPI de preparación: {'OK' if readiness_gap <= 1e-9 else 'ALERTA'}",
        f"- consistencia KPI de tasa de atraso: {'OK' if delay_rate_gap <= 1e-9 else 'ALERTA'}",
        f"- fuente única de verdad KPI: {'OK' if inherited_kpi_present == 0 else 'ALERTA'}",
        f"- dispersión de escenarios: {'OK' if scenario_spread >= 2.0 else 'ALERTA'}",
        "- riesgo de sobreinterpretación explicitado: OK",
        "",
        "## Problemas Encontrados",
    ]

    display_issues_df = _issues_for_display(issues_df)

    if issues_df.empty:
        lines.append("No se detectaron problemas materiales en esta ejecución.")
    else:
        lines.append(to_markdown_safe(display_issues_df))

    lines.extend(
        [
            "",
            "## Advertencias Obligatorias",
        ]
    )
    lines.extend([f"- {c}" for c in caveats])

    lines.extend(
        [
            "",
            "## Evaluación Global de Confianza",
            f"Confianza **{confidence}** para demostración técnica y apoyo a discusión operativa. Para uso real de planta se requiere calibración con datos productivos y validación de negocio adicional.",
        ]
    )

    report_path = OUTPUT_REPORTS_AUDIT_DIR / "validation_report.md"
    write_text_utf8(report_path, "\n".join(lines))

    issues_path = OUTPUT_REPORTS_AUDIT_DIR / "validation_issues_found.csv"
    display_issues_df.to_csv(issues_path, index=False)

    release_json = {
        "status": status,
        "confidence": confidence,
        "release_grade": release_grade,
        "technically_valid": technically_valid,
        "analytically_acceptable": analytically_acceptable,
        "decision_support_only": decision_support_only,
        "screening_grade_only": screening_grade_only,
        "publish_blocked": release_grade == "publish-blocked",
        "issues_total": int(len(issues_df)),
        "critical_issues": critical_issues,
        "high_issues": high_issues,
        "medium_issues": medium_issues,
        "sql_warn_ratio": sql_warn_ratio,
        "kpi_single_source_of_truth": inherited_kpi_present == 0 and inherited_mismatch == 0,
        "dashboard_version": dashboard_manifest.get("dashboard_version"),
        "dashboard_html_sha256": dashboard_manifest.get("html_sha256"),
    }
    write_json_utf8(OUTPUT_REPORTS_DIR / "release_readiness.json", release_json)

    return ValidationResult(
        status=status,
        confidence=confidence,
        issues=int(len(issues_df)),
        release_grade=release_grade,
    )


def run_ev_validation() -> ValidationResult:
    context = _load_validation_context()
    issues = IssueCollector()
    raw_metrics = _check_raw_quality(context, issues)
    analytical_metrics = _check_analytical_quality(context, issues)
    artifact_metrics = _check_artifacts_and_scenarios(context, issues)
    return _write_validation_outputs(
        context,
        issues,
        raw_metrics,
        analytical_metrics,
        artifact_metrics,
    )


if __name__ == "__main__":
    result = run_ev_validation()
    print("Validación EV completada")
    print(f"- estado: {result.status}")
    print(f"- confianza: {result.confidence}")
    print(f"- grado_publicacion: {_release_grade_label(result.release_grade)}")
    print(f"- issues: {result.issues}")
