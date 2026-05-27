# Estructura Oficial del Repositorio

## Ruta oficial (EV)
- `src/`: código ejecutable oficial.
  - `src/utils.py`: helpers compartidos (`read_ev_csv`, `to_markdown_safe`).
  - `src/config.py`: rutas del proyecto y función `ensure_directories`.
  - `src/run_pipeline.py`: orquestador oficial del pipeline.
  - `src/ev_sql_layer.py`: carga raw, ejecución SQL y exportación a CSV.
  - `src/ev_feature_engineering.py`: construcción de features analíticas.
  - `src/ev_diagnostic_analysis.py`: scoring diagnóstico y persistencia de cuellos.
  - `src/ev_scenario_twin.py`: simulación paramétrica de escenarios de transición.
  - `src/ev_scoring_framework.py`: priorización OPI, sensibilidad y Monte Carlo.
  - `src/ev_build_dashboard.py`: construcción del dashboard ejecutivo.
  - `src/ev_validate_project.py`: validación integral y release grade.
  - `src/ev_release_gate.py`: gate de publicación basado en manifests.
  - `src/synthetic_data_gen/`: generador sintético industrial por dominio.
- `data/raw/ev_factory/`: fuente raw oficial (14 tablas CSV).
- `data/processed/ev_factory/`: tablas gobernadas de trabajo.
- `sql/ev_factory/`: capa SQL oficial (11 scripts en orden de ejecución).
- `outputs/dashboard/industrial-ev-operating-command-center.html`: dashboard final único.
- `tests/`: contratos de calidad y regresión.

## Convenciones
- No se publica más de un dashboard oficial en `outputs/dashboard/`.
- Métricas de decisión se consumen desde datasets gobernados, no desde cálculos frontend.
- El pipeline oficial se ejecuta con `python -m src.run_pipeline`.

## Artefactos generados
El pipeline puede generar salidas técnicas temporales durante la ejecución. El repositorio curado mantiene como entregable visible el dashboard oficial.
