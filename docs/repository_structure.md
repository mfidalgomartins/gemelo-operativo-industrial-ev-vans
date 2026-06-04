# Estructura Oficial del Repositorio

## Código fuente (`src/`)

| Módulo | Responsabilidad |
|--------|----------------|
| `utils.py` | Helpers compartidos (`read_ev_csv`, `to_markdown_safe`) |
| `config.py` | Rutas del proyecto y `ensure_directories` |
| `run_pipeline.py` | Orquestador oficial del pipeline |
| `ev_sql_layer.py` | Carga raw, ejecución SQL DuckDB y exportación CSV |
| `ev_feature_engineering.py` | Construcción de features analíticas por dominio |
| `ev_diagnostic_analysis.py` | Scoring diagnóstico y persistencia de cuellos de botella |
| `ev_scenario_twin.py` | Simulación paramétrica de escenarios de transición EV |
| `ev_scoring_framework.py` | Priorización OPI, análisis de sensibilidad y Monte Carlo |
| `ev_build_dashboard.py` | Construcción del dashboard ejecutivo HTML autocontenido |
| `ev_validate_project.py` | Validación integral, release grade y checklist de calidad |
| `ev_release_gate.py` | Gate de publicación basado en manifests gobernados |
| `explore_data_audit.py` | Auditoría de calidad sobre las 14 tablas raw |
| `create_notebooks.py` | Generación del notebook de revisión ejecutiva |
| `synthetic_data_gen/` | Generador sintético industrial por dominio operativo |

## Datos

- `data/raw/ev_factory/` — 14 tablas CSV base (fuente oficial).
- `data/processed/ev_factory/` — tablas gobernadas exportadas por el pipeline.
- `data/processed/gemelo_operativo_ev.duckdb` — base DuckDB de trabajo.

## SQL (`sql/ev_factory/`)

11 scripts DuckDB en orden de ejecución: staging (01–04), integration (05–06), analytical marts (07–09), KPIs (10) y validación (11).

## Scripts (`scripts/`)

- `generate_chart_pack.py` — genera 6 PNGs de análisis ejecutivo en `outputs/graphs/`.

## Outputs

- `outputs/dashboard/` — dashboard ejecutivo final (artefacto principal).
- `outputs/graphs/` — pack de gráficos PNG para portfolio.
- `outputs/reports/` — manifests, resúmenes y reportes técnicos generados por el pipeline.

## Documentación (`docs/`)

Documentación técnica de arquitectura, gobierno de métricas y convenciones del proyecto. Los ficheros `feature_dictionary.md`, `diagnostic_framework.md` y `scoring_framework.md` se regeneran en cada ejecución del pipeline; el resto son documentos de diseño estables.

## Tests (`tests/`)

Tests unitarios e integración. Los tests marcados `integration` no se ejecutan por defecto (`pytest -m not integration`); requieren los artefactos del pipeline y tardan varios minutos.

## Convenciones

- Un único dashboard oficial en `outputs/dashboard/`; versiones previas se archivan en `outputs/dashboard/legacy/`.
- Métricas de decisión consumidas desde datasets gobernados, no recalculadas en frontend.
- Pipeline oficial ejecutado con `python -m src.run_pipeline`.
