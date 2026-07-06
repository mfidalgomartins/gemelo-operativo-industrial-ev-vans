# Arranque Operativo Rápido

Guía breve para instalar, regenerar el corte analítico, validar y localizar salidas.

## Requisitos

- Python 3.10+
- Acceso de escritura a `data/processed/` y `outputs/`
- Red solo para visualizar el panel con fuentes y Chart.js desde CDN

## Instalación

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

## Ejecución recomendada

```bash
generate-data --seed 20260328 --start-date 2025-01-01 --months 12
python -m src.run_pipeline
python scripts/generate_chart_pack.py
python scripts/generate_report.py
python -m src.ev_release_gate
```

`python -m src.run_pipeline` usa por defecto los CSV de origen existentes. Para regenerar datos de origen dentro de la canalización desde Python, usar `run_pipeline(generate_data=True, seed=20260328, months=12)`.

## Orden real de la canalización

1. `explore_data_audit`: audita los CSV de origen.
2. `ev_sql_layer`: carga 14 CSV de origen en DuckDB, ejecuta 11 scripts SQL y exporta marts.
3. `ev_feature_engineering`: crea variables de preparación, área-turno, carga, patio y transición.
4. `ev_diagnostic_analysis`: calcula puntuaciones diagnósticas y rankings.
5. `ev_scenario_twin`: simula 8 escenarios paramétricos.
6. `ev_scoring_framework`: calcula OPI, sensibilidad y Monte Carlo.
7. `ev_build_dashboard`: genera el HTML oficial único del panel.
8. `ev_validate_project`: genera validación y preparación de publicación.
9. `ev_release_gate`: aprueba o bloquea publicación con base en los artefactos anteriores.

## Salidas esperadas

| Artefacto | Ruta | Uso |
|---|---|---|
| Base DuckDB | `data/processed/gemelo_operativo_ev.duckdb` | Depuración SQL local |
| Marts/variables CSV | `data/processed/ev_factory/*.csv` | Consumo analítico y panel |
| Panel oficial | `outputs/dashboard/industrial-ev-operating-command-center.html` | Interfaz ejecutiva estática |
| Paquete de gráficos | `outputs/graphs/*.png` | Gráficos para informe |
| Informe PDF | `outputs/reports/ev_transition_operating_twin_report.pdf` | Narrativa analítica |
| Manifiesto del panel | `outputs/reports/dashboard_build_manifest.json` | Contratos UI/build |
| Preparación de publicación | `outputs/reports/release_readiness.json` | Estado de publicación |
| Resumen de canalización | `outputs/reports/pipeline_run_summary.json` | Resultado agregado de la ejecución |

## Pruebas y calidad

```bash
ruff check .
ruff format --check .
pytest -q
pytest -q -m integration
```

Notas:

- `pytest -q` excluye pruebas de integración por defecto.
- `pytest -q -m integration` escribe en `data/` y `outputs/`.
- `tests/test_ev_governance.py` regenera datos y restaura el corte canónico al final.

## Comandos parciales útiles

```bash
python -m src.ev_sql_layer
python -m src.ev_feature_engineering
python -m src.ev_diagnostic_analysis
python -m src.ev_scenario_twin
python -m src.ev_scoring_framework
python -m src.ev_build_dashboard
python -m src.ev_validate_project
python -m src.ev_release_gate
```

Ejecutar comandos parciales solo cuando las entradas anteriores ya existen. Ejemplo: `ev_build_dashboard` requiere CSV procesados como `vw_vehicle_flow_timeline.csv`, `charging_features.csv`, `yard_features.csv`, `operational_prioritization_table.csv` y `scenario_table.csv`.

## Resolución rápida de problemas

- `FileNotFoundError` en origen: confirmar los 14 CSV en `data/raw/ev_factory/`.
- Panel sin estilos o gráficos: abrir con red disponible, porque Chart.js y fuentes usan CDN.
- Falla la puerta de publicación: revisar `outputs/reports/validation_report.md`, `validation_issues_found.csv` y `dashboard_build_manifest.json`.
- Salidas no deterministas: usar semilla canónica `20260328`; la capa SQL fuerza DuckDB con `PRAGMA threads=1`.
