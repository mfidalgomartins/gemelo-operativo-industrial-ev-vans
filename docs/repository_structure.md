# Estructura Oficial del Repositorio

## Ruta oficial (EV)
- `src/`: código ejecutable oficial.
- `data/raw/ev_factory/`: fuente raw oficial.
- `data/processed/ev_factory/`: tablas gobernadas de trabajo.
- `sql/ev_factory/`: capa SQL oficial.
- `outputs/dashboard/industrial-ev-operating-command-center.html`: dashboard final único.
- `outputs/reports/`: validación, release gate y reportes ejecutivos.
- `tests/`: contratos de calidad y regresión.

## Convenciones
- No se publica más de un dashboard oficial en `outputs/dashboard/`.
- Métricas de decisión se consumen desde datasets gobernados, no desde cálculos frontend.
- El pipeline oficial se ejecuta con `python -m src.run_pipeline`.

## Legacy
Material histórico queda en `archive/legacy/` y no forma parte de la ruta de ejecución oficial.
