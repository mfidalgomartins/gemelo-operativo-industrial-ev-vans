# Gemelo Operativo Industrial para Transición a Vans Eléctricas

Plataforma analítica para gestionar secuenciación, patio, carga y expedición durante un ramp-up EV en una planta de furgonetas.

## Problema de negocio
Cuando sube el mix EV, el flujo interno se vuelve más frágil: aumentan esperas en patio y carga, crece el riesgo de salida no lista y cae el throughput real frente al plan.

## Qué hace el sistema
- Genera un dataset industrial sintético con pre-serie, ramp-up y operación estable.
- Modela el flujo end-to-end (orden, fin de línea, patio, carga, readiness y salida).
- Construye capa SQL en DuckDB, features operativas y scoring de priorización.
- Simula escenarios de transición EV y compara impacto operativo por palanca.
- Publica dashboard ejecutivo único y reporte formal de validación.

## Decisiones que soporta
- Cuándo cambiar reglas de secuenciación por mix/versiones.
- Dónde ampliar o reservar capacidad de carga.
- Qué zonas de patio intervenir primero para reducir dwell/blocking.
- Qué áreas requieren acción inmediata vs monitorización.

## Arquitectura del proyecto
- `generate_synthetic_data.py`: generación de datos base.
- `src/run_pipeline.py`: ruta oficial end-to-end.
- `src/ev_sql_layer.py`: staging, integración, marts y KPIs.
- `src/ev_feature_engineering.py`, `src/ev_diagnostic_analysis.py`: señales y diagnóstico.
- `src/ev_scenario_twin.py`, `src/ev_scoring_framework.py`: escenarios y priorización.
- `src/ev_build_dashboard.py`, `src/ev_validate_project.py`: dashboard y release gate.

## Estructura del repositorio
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

## Outputs principales
- Dashboard final: `outputs/dashboard/dashboard_gemelo_operativo_ev.html`
- Validación: `outputs/reports/validation_report.md`
- Estado de publicación: `outputs/reports/release_readiness.json`
- Hallazgos operativos: `outputs/reports/diagnostic_findings.md`

## Por qué este proyecto es fuerte
No es un dashboard aislado: conecta modelo operativo, gobierno de métricas, simulación de escenarios y priorización accionable en un flujo reproducible.

## Cómo ejecutar
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python generate_synthetic_data.py --seed 20260328 --start-date 2025-01-01 --months 12
python -m src.run_pipeline
```

## Limitaciones
- Datos sintéticos; no sustituye calibración con datos reales de planta.
- Escenarios basados en elasticidades paramétricas interpretables.
- Umbrales y pesos de scoring requieren ajuste de negocio para producción.

## Herramientas
Python, SQL, DuckDB, pandas, NumPy, Matplotlib, Seaborn, Chart.js, pytest.
