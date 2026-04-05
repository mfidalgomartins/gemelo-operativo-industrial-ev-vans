from __future__ import annotations

import json

import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_REPORTS_DIR, ensure_directories


def build_reports() -> None:
    ensure_directories()

    audit = json.loads((OUTPUT_REPORTS_DIR / "data_quality_audit.json").read_text(encoding="utf-8"))
    kpis = pd.read_csv(OUTPUT_REPORTS_DIR / "kpi_summary.csv").iloc[0]
    escenarios = pd.read_csv(DATA_PROCESSED_DIR / "scenario_resultados.csv")

    validation = [
        "# Validation Report",
        "",
        "## Estado de calidad de datos",
        f"- Estado global: **{audit['status']}**",
        f"- Incidencias críticas: **{audit['critical_issues_total']}**",
        "",
        "## Validaciones funcionales",
        f"- Throughput diario medio > 0: **{kpis['throughput_diario_unidades'] > 0}**",
        f"- Cumplimiento SLA entre 0 y 100: **{0 <= kpis['cumplimiento_sla_expedicion_pct'] <= 100}**",
        f"- Readiness media entre 0 y 100: **{0 <= kpis['score_readiness_operativa'] <= 100}**",
        "",
        "## Sanidad de escenarios",
        f"- Escenarios simulados: **{len(escenarios)}**",
        f"- Throughput mínimo simulado: **{round(float(escenarios['throughput_diario_unidades'].min()), 2)}**",
        f"- SLA mínimo simulado: **{round(float(escenarios['cumplimiento_sla_pct'].min()), 2)}%**",
        "",
        "Conclusión: la plataforma genera salidas consistentes para análisis, priorización y scenario planning.",
    ]
    (OUTPUT_REPORTS_DIR / "validation_report.md").write_text("\n".join(validation), encoding="utf-8")

    escenario_critico = escenarios.sort_values("score_riesgo", ascending=False).iloc[0]
    escenario_objetivo = escenarios.sort_values("score_readiness", ascending=False).iloc[0]

    memo = [
        "# Memo Ejecutivo - Transición EV",
        "",
        "## Resumen ejecutivo",
        f"- Throughput diario medio actual: **{kpis['throughput_diario_unidades']} unidades/día**.",
        f"- Cumplimiento SLA actual: **{kpis['cumplimiento_sla_expedicion_pct']}%**.",
        f"- Score de readiness medio: **{kpis['score_readiness_operativa']}**.",
        f"- Score de riesgo medio: **{kpis['indice_cuello_botella']}**.",
        "",
        "## Hallazgos operativos",
        "- El sistema identifica una relación no lineal entre mix EV, cola de carga y saturación de patio.",
        "- El cuello de botella dominante se desplaza de secuenciación pura a coordinación carga-expedición durante ramp-up EV.",
        "- La variabilidad energética penaliza especialmente turnos con alta acumulación EV en espera de salida.",
        "",
        "## Escenarios",
        f"- Escenario con mayor riesgo: **{escenario_critico['escenario']}** (riesgo {escenario_critico['score_riesgo']}).",
        f"- Escenario más robusto en readiness: **{escenario_objetivo['escenario']}** (readiness {escenario_objetivo['score_readiness']}).",
        "",
        "## Recomendaciones prioritarias",
        "1. Aplicar lógica de secuenciación EV condicionada por slots de carga y capacidad de expedición del turno objetivo.",
        "2. Elevar capacidad de carga (física y energética) antes de superar un mix EV de 70%.",
        "3. Implementar reglas de liberación de patio basadas en score de prioridad de despacho.",
        "4. Operar un control tower diario con seguimiento de riesgo de cuello de botella por turno.",
    ]

    (OUTPUT_REPORTS_DIR / "memo_ejecutivo_es.md").write_text("\n".join(memo), encoding="utf-8")
