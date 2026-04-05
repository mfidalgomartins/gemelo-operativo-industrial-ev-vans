from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd

from .config import PROJECT_ROOT, DATA_PROCESSED_DIR, OUTPUT_REPORTS_DIR
from .ev_sql_layer import run_ev_sql_layer
from .ev_feature_engineering import run_ev_feature_engineering
from .ev_diagnostic_analysis import run_ev_diagnostic_analysis
from .ev_scenario_twin import run_ev_scenario_twin
from .ev_scoring_framework import run_ev_scoring_framework
from .ev_create_visuals import run_ev_create_visuals
from .ev_build_dashboard import run_ev_build_dashboard
from .ev_validate_project import run_ev_validation


def _safe_read(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _list_files(path: Path, pattern: str = "*") -> List[str]:
    if not path.exists():
        return []
    return sorted([str(p.relative_to(PROJECT_ROOT)) for p in path.glob(pattern) if p.is_file()])


def run_ev_final_assembly() -> Path:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Ejecuta pipeline EV completo
    sql_res = run_ev_sql_layer()
    feat_res = run_ev_feature_engineering()
    diag_res = run_ev_diagnostic_analysis()
    scen_res = run_ev_scenario_twin()
    score_res = run_ev_scoring_framework()
    viz_res = run_ev_create_visuals()
    dash_res = run_ev_build_dashboard()
    val_res = run_ev_validation()

    ev_dir = DATA_PROCESSED_DIR / "ev_factory"

    scenario_df = _safe_read(ev_dir / "scenario_decision_comparison.csv")
    prioritization_df = _safe_read(ev_dir / "operational_prioritization_table.csv")
    validation_issues = _safe_read(OUTPUT_REPORTS_DIR / "validation_issues_found.csv")

    top_scenario = scenario_df.iloc[0]["escenario"] if not scenario_df.empty else "N/A"
    top_area = prioritization_df.iloc[0]["area"] if not prioritization_df.empty else "N/A"
    top_action = prioritization_df.iloc[0]["recommended_action"] if not prioritization_df.empty else "N/A"

    tree_lines = [
        "data/raw/",
        "data/processed/ev_factory/",
        "sql/ev_factory/",
        "src/",
        "notebooks/",
        "outputs/charts/",
        "outputs/dashboard/",
        "outputs/reports/",
        "docs/",
    ]

    sql_files = _list_files(PROJECT_ROOT / "sql" / "ev_factory", "*.sql")
    ev_processed_files = _list_files(ev_dir, "*.csv")
    chart_files = _list_files(PROJECT_ROOT / "outputs" / "charts", "ev_*.png")
    report_files = _list_files(PROJECT_ROOT / "outputs" / "reports", "*.md")

    lines = [
        "# Final Assembly Report - Gemelo Operativo EV",
        "",
        "## 1. Estructura final del repositorio",
    ]
    lines.extend([f"- {t}" for t in tree_lines])

    lines.extend(
        [
            "",
            "## 2. Lista de archivos creados (EV core)",
            "### SQL",
        ]
    )
    lines.extend([f"- {f}" for f in sql_files])
    lines.extend(["", "### Data Processed EV"])
    lines.extend([f"- {f}" for f in ev_processed_files])
    lines.extend(["", "### Reports (markdown)"])
    lines.extend([f"- {f}" for f in report_files])

    lines.extend(
        [
            "",
            "## 3. Scripts ejecutados",
            "- src/ev_sql_layer.py",
            "- src/ev_feature_engineering.py",
            "- src/ev_diagnostic_analysis.py",
            "- src/ev_scenario_twin.py",
            "- src/ev_scoring_framework.py",
            "- src/ev_create_visuals.py",
            "- src/ev_build_dashboard.py",
            "- src/ev_validate_project.py",
            "- src/ev_assemble_final.py",
        ]
    )

    lines.extend(
        [
            "",
            "## 4. Datos generados",
            f"- Tablas raw EV: {len(_list_files(PROJECT_ROOT / 'data' / 'raw' / 'ev_factory', '*.csv'))}",
            f"- Objetos SQL exportados: {len(sql_res.exported_rows)}",
            f"- Tablas de features: {len(feat_res.tables)}",
        ]
    )

    lines.extend(
        [
            "",
            "## 5. Tablas analíticas creadas",
            "- vw_vehicle_flow_timeline",
            "- vw_charging_utilization",
            "- vw_yard_congestion",
            "- vw_dispatch_readiness",
            "- vw_shift_bottleneck_summary",
            "- mart_vehicle_day",
            "- mart_area_shift",
            "- mart_dispatch_readiness",
            "- vehicle_readiness_features",
            "- area_shift_features",
            "- charging_features",
            "- yard_features",
            "- launch_transition_features",
            "- diagnostic_*",
            "- scenario_*",
            "- operational_prioritization_table",
        ]
    )

    lines.extend(
        [
            "",
            "## 6. Outputs generados",
            f"- Gráficos EV premium: {viz_res.charts_generated}",
            f"- Charts: {len(chart_files)} ficheros `ev_*.png`",
            "- Reportes en outputs/reports/* (audit, feature, diagnostic, scenario, scoring, validation)",
        ]
    )

    lines.extend(
        [
            "",
            "## 7. Dashboard HTML final",
            f"- {dash_res.path}",
        ]
    )

    lines.extend(
        [
            "",
            "## 8. Resumen ejecutivo final",
            "- El cuello dominante bajo transición EV se desplaza a carga y patio.",
            f"- Escenario recomendado: {top_scenario}",
            f"- Área prioritaria actual: {top_area}",
            f"- Acción prioritaria: {top_action}",
        ]
    )

    lines.extend(
        [
            "",
            "## 9. Hallazgos principales",
            "- La secuenciación mejora el flujo, pero sin capacidad de carga el riesgo persiste.",
            "- La saturación de patio amplifica retrasos de expedición.",
            "- La combinación de palancas es superior a acciones aisladas.",
        ]
    )

    lines.extend(
        [
            "",
            "## 10. Recomendaciones",
            "1. Ajustar secuenciación EV en ventanas de alta presión.",
            "2. Reservar/expandir capacidad de carga para unidades críticas.",
            "3. Reducir dwell y bloqueo con política de buffer por zona.",
            "4. Priorizar salida por readiness real.",
        ]
    )

    lines.extend(
        [
            "",
            "## 11. Resumen de validación",
            f"- Estado: {val_res.status}",
            f"- Confianza: {val_res.confidence}",
            f"- Release grade: {val_res.release_grade}",
            f"- Issues: {val_res.issues}",
            f"- Issues file: {str((OUTPUT_REPORTS_DIR / 'validation_issues_found.csv').relative_to(PROJECT_ROOT))}",
        ]
    )
    if not validation_issues.empty:
        lines.append("- Principales issues: " + ", ".join(validation_issues["check"].head(3).tolist()))

    lines.extend(
        [
            "",
            "## 12. Limitaciones",
            "- Dataset sintético; requiere calibración con datos reales para decisión productiva.",
            "- Elasticidades del gemelo son supuestos interpretables.",
            "- El scoring depende de pesos y gobernanza de negocio.",
            "",
            "## 13. Próximos pasos",
            "1. Calibrar con históricos reales de planta.",
            "2. Integrar restricciones energéticas intradía.",
            "3. Añadir alertado near-real-time y seguimiento de acciones.",
            "",
            "## 14. Publicación en GitHub (exacto)",
            "1. Crear branch: `git checkout -b codex/ev-operational-digital-twin`",
            "2. Ejecutar pipeline EV completo y validar outputs.",
            "3. Commit sugerido: `feat: industrial EV operational twin with SQL marts, scoring and executive dashboard`",
            "4. Subir dashboard y artefactos clave (`outputs/charts`, `outputs/dashboard`, `outputs/reports`).",
            "5. En README, incluir captura del dashboard y link directo al HTML.",
            "6. Abrir PR destacando problema, enfoque, hallazgos, decisiones y limitaciones.",
        ]
    )

    out_path = OUTPUT_REPORTS_DIR / "final_assembly_report.md"
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return out_path


if __name__ == "__main__":
    p = run_ev_final_assembly()
    print("Final assembly completado")
    print(f"- report: {p}")
