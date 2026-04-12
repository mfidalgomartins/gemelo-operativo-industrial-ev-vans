# Gemelo Operativo Industrial EV para Vans

Sistema analítico para priorizar decisiones de secuenciación, patio, carga y expedición durante un ramp-up de furgonetas eléctricas en entorno de planta.

## Qué resuelve
- Detecta dónde se pierde throughput y dónde se acumula espera interna.
- Separa cuellos coyunturales de cuellos estructurales por área y turno.
- Prioriza acciones operativas con un índice único (`operational_priority_index`).
- Compara escenarios de transición EV con trade-offs explícitos.

## Qué incluye (ruta oficial)
1. Generador sintético industrial EV (`data/raw/ev_factory`).
2. Auditoría de datos `/explore-data`.
3. Capa SQL en DuckDB (`sql/ev_factory`).
4. Feature engineering y diagnóstico interpretable (`data/processed/ev_factory`).
5. Escenarios y scoring de priorización.
6. Dashboard ejecutivo HTML único:
   `outputs/dashboard/dashboard_gemelo_operativo_ev.html`
7. Validación y release gate:
   `outputs/reports/validation_report.md`,
   `outputs/reports/release_readiness.json`.

## Arquitectura (resumen)
- `src/ev_sql_layer.py`: staging, integración, marts, KPI, validaciones SQL.
- `src/ev_feature_engineering.py`: features vehículo, área-turno, carga, patio, transición.
- `src/ev_diagnostic_analysis.py`: scores diagnósticos y drivers principales.
- `src/ev_scenario_twin.py`: simulador de escenarios EV.
- `src/ev_scoring_framework.py`: priorización operativa y sensibilidad.
- `src/ev_build_dashboard.py`: build del dashboard oficial.
- `src/ev_validate_project.py`: validación integral y clasificación de release.
- `src/run_pipeline.py`: orquestación oficial EV end-to-end.

## Estructura del repositorio
- `src/`
- `data/raw/ev_factory/`
- `data/processed/ev_factory/`
- `sql/ev_factory/`
- `docs/`
- `tests/`
- `outputs/dashboard/`
- `outputs/charts/`
- `outputs/reports/`
- `archive/legacy/` (material histórico, fuera de la ruta oficial)

## Ejecución recomendada
```bash
# opcional: regenerar datos
.venv/bin/python generate_synthetic_data.py --seed 20260328 --start-date 2025-01-01 --months 12

# pipeline oficial EV
.venv/bin/python -m src.run_pipeline

# abrir dashboard final
open outputs/dashboard/dashboard_gemelo_operativo_ev.html
```

## Criterio de uso (gobernanza)
- Estado de publicación: `outputs/reports/release_readiness.json`.
- Este proyecto está diseñado para **decision-support**; no sustituye calibración con datos reales de planta.

## Skills demostradas
- **Business/Operations**: launch readiness, flow orchestration, yard/charging operations, priorización.
- **Técnicas**: Python, SQL, DuckDB, pandas, Chart.js, pytest.

## Limitaciones explícitas
- Dataset sintético (no telemetría real).
- Escenarios con elasticidades paramétricas interpretables (no causal inference).
- Pesos de scoring requieren calibración de negocio para producción.
