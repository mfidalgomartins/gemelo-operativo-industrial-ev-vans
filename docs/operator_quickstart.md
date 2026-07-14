# Arranque Operativo Rápido

Guía breve para instalar, regenerar el corte analítico, validar y localizar salidas.

## Requisitos

- Python 3.10+
- Acceso de escritura a la raíz definida por `EV_TWIN_HOME`
- Red solo para visualizar el panel con fuentes y Chart.js desde CDN

## Instalación

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,service]"
```

## Ejecución recomendada

```bash
ev-twin run --generate-data --seed 20260328 --months 12
ev-twin artifacts
ev-twin release-check
ev-twin status
```

`ev-twin run` usa por defecto los CSV de origen existentes. `--generate-data` reconstruye primero el corte sintético canónico. Para fuentes conectadas, ejecutar `ev-twin ingest` antes de `ev-twin run`; la configuración y recuperación están en [production_operations.md](production_operations.md).

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
| Estado operacional | `.ev_twin/observability/latest_pipeline_run.json` | Duraciones, fallos y SLA |
| Linaje de ingesta | `.ev_twin/lineage/latest_ingestion.json` | Fuente, cardinalidad, watermark y hash |

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
- Las pruebas unitarias usan directorios temporales; las de integración regeneran el corte canónico.

## Comandos parciales útiles

```bash
python -m gemelo_operativo_ev.ev_sql_layer
python -m gemelo_operativo_ev.ev_feature_engineering
python -m gemelo_operativo_ev.ev_diagnostic_analysis
python -m gemelo_operativo_ev.ev_scenario_twin
python -m gemelo_operativo_ev.ev_scoring_framework
python -m gemelo_operativo_ev.ev_build_dashboard
python -m gemelo_operativo_ev.ev_validate_project
python -m gemelo_operativo_ev.ev_release_gate
```

Ejecutar comandos parciales solo cuando las entradas anteriores ya existen. Ejemplo: `ev_build_dashboard` requiere CSV procesados como `vw_vehicle_flow_timeline.csv`, `charging_features.csv`, `yard_features.csv`, `operational_prioritization_table.csv` y `scenario_table.csv`.

## Resolución rápida de problemas

- `FileNotFoundError` en origen: confirmar los 14 CSV en `data/raw/ev_factory/`.
- Panel sin estilos o gráficos: abrir con red disponible, porque Chart.js y fuentes usan CDN.
- Falla la puerta de publicación: revisar `outputs/reports/validation_report.md`, `validation_issues_found.csv` y `dashboard_build_manifest.json`.
- Salidas no deterministas: usar semilla canónica `20260328`; la capa SQL fuerza DuckDB con `PRAGMA threads=1`.
- API no preparada: ejecutar la canalización y comprobar `ev-twin status`; release y SLA deben estar en `PASS`.
