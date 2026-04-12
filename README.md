# Gemelo Operativo Industrial para la Transición a Vans Eléctricas

Este proyecto construye un sistema de apoyo a decisiones para una planta de furgonetas en transición EV. El foco no está en “mostrar gráficos”, sino en entender dónde se rompe el flujo (secuenciación, patio, carga y expedición), cuantificar el impacto operativo y priorizar acciones con criterio de capacidad.

## Contexto operativo
En un ramp-up EV, el throughput deja de depender solo de la línea. La presión migra al patio, a los cargadores y a la salida: aumentan esperas internas, crecen bloqueos y sube el riesgo de expedición fuera de readiness. Si no se orquesta el flujo completo, el plan de producción pierde estabilidad.

## Qué hace realmente el sistema
- Genera datos sintéticos industriales con fases de pre-serie, ramp-up y estabilización.
- Modela el timeline completo del vehículo: orden, fin de línea, patio, carga, readiness y salida.
- Consolida una capa SQL en DuckDB con marts y KPIs operativos trazables.
- Construye features y scores interpretables para diagnóstico, priorización y riesgo.
- Ejecuta escenarios de transición EV para comparar palancas y trade-offs.
- Publica un dashboard ejecutivo único y un reporte formal de validación.

## Decisiones que habilita
- Cuándo ajustar reglas de secuenciación por mezcla EV/ICE y complejidad de versión.
- Dónde ampliar o reservar capacidad de carga antes de que aparezca congestión estructural.
- Qué zonas de patio intervenir primero para reducir dwell time, blocking y movimientos no productivos.
- Qué áreas requieren intervención inmediata y cuáles deben pasar a monitorización reforzada.

## Arquitectura, en una vista
`generate_synthetic_data.py` crea la base operativa. `src/run_pipeline.py` orquesta la ruta oficial: SQL (`src/ev_sql_layer.py`), feature engineering (`src/ev_feature_engineering.py`), diagnóstico (`src/ev_diagnostic_analysis.py`), gemelo de escenarios (`src/ev_scenario_twin.py`), scoring (`src/ev_scoring_framework.py`), dashboard (`src/ev_build_dashboard.py`) y validación/release gate (`src/ev_validate_project.py`).

## Estructura principal
```text
src/
data/raw/ev_factory/
data/processed/ev_factory/
sql/ev_factory/
docs/
tests/
outputs/charts/
outputs/dashboard/
outputs/reports/
```

## Entregables clave
- Dashboard final: `outputs/dashboard/industrial-ev-operating-command-center.html`
- Reporte de validación: `outputs/reports/validation_report.md`
- Estado de release: `outputs/reports/release_readiness.json`
- Hallazgos y drivers: `outputs/reports/diagnostic_findings.md`

## Live dashboard
[Industrial EV Operating Command Center · Live](https://mfidalgomartins.github.io/gemelo-operativo-industrial-ev-vans/outputs/dashboard/industrial-ev-operating-command-center.html)

## Por qué esta pieza destaca
Combina modelado operativo, gobierno de métricas, diagnóstico interpretable y simulación de decisiones en una sola ruta reproducible. Es un proyecto pensado para conversación de operaciones y capacidad, no solo para visualización.

## Ejecución local
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python generate_synthetic_data.py --seed 20260328 --start-date 2025-01-01 --months 12
python -m src.run_pipeline
```

## Alcance y límites
- El dato es sintético; la implantación real exige calibración con telemetría de planta.
- Los escenarios son paramétricos e interpretables; no sustituyen inferencia causal.
- Los umbrales/pesos de scoring deben ajustarse con criterio operativo local.

Herramientas: Python, SQL, DuckDB, pandas, NumPy, Matplotlib, Seaborn, Chart.js, pytest.
