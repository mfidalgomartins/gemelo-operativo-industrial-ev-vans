# Flujo End-to-End

1. `src/data_generation.py`
   - genera entidades raw con coherencia temporal y restricciones operativas.
2. `src/data_quality.py`
   - ejecuta profiling y validaciones de calidad/integridad.
3. `src/sql_modeling.py`
   - aplica SQL en DuckDB para staging, marts, features y scoring.
4. `src/analysis.py`
   - produce KPIs, diagnóstico de cuellos de botella y gráficos.
5. `src/scenario_engine.py`
   - simula escenarios EV (mix/cargadores/energía) y estima impacto.
6. `src/dashboard_builder.py`
   - genera dashboard HTML autocontenido.
7. `src/run_pipeline.py`
   - orquesta el proceso y deja entregables en `outputs/`.
