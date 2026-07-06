# Gemelo Operativo para la Transición a Vans Eléctricas

[![CI](https://github.com/mfidalgomartins/gemelo-operativo-industrial-ev-vans/actions/workflows/ci.yml/badge.svg)](https://github.com/mfidalgomartins/gemelo-operativo-industrial-ev-vans/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-90%25-brightgreen.svg)](#verificación)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Gemelo operativo reproducible que identifica dónde se rompe el flujo durante una rampa EV, cuánto cuesta y qué palancas lo recuperan.

| | |
|---|---|
| **Panel interactivo** | [Abrir panel](https://mfidalgomartins.github.io/gemelo-operativo-industrial-ev-vans/) |
| **Informe analítico (PDF)** | [Descargar informe](https://mfidalgomartins.github.io/gemelo-operativo-industrial-ev-vans/outputs/reports/ev_transition_operating_twin_report.pdf) |

![Comparación del escenario base y el paquete de medidas correctivas](outputs/graphs/19_before_after.png)

## Alcance analítico

El corte publicado cubre **58.697 vehículos**, **14 tablas operativas** y **13 meses**, desde el **1 de enero de 2025** hasta el **1 de enero de 2026**. Los datos sintéticos modelan tres fases de transición, desde pre-serie hasta estabilización, y conectan cada orden con su recorrido por producción, patio, carga, preparación y salida.

| Pregunta de decisión | Salida principal |
|---|---|
| ¿Dónde se rompe el flujo? | Diagnóstico por área, turno, versión y vehículo |
| ¿Qué presión introduce la cuota EV? | Comparación EV/ICE y tendencia de transición |
| ¿Qué intervención priorizar? | Operational Priority Index, sensibilidad y Monte Carlo |
| ¿Qué capacidad o regla cambiar? | Comparador paramétrico de escenarios |
| ¿Se puede publicar el resultado? | Puerta de publicación con contratos de datos, KPI y panel |

## Resultados publicados

- El caudal productivo se mantiene cerca del plan, pero la preparación y la expedición concentran el riesgo operativo.
- `LOGISTICA` y `PATIO` lideran la priorización OPI; el ranking top-1 es estable en **77,33%** de las simulaciones Monte Carlo.
- El paquete correctivo combina secuenciación, carga y gestión de patio para mejorar caudal productivo, estabilidad y tiempo interno.
- La publicación actual está **aprobada** (`PASS`), sin incidencias materiales, y limitada explícitamente a **apoyo a decisión**.

## Metodología

1. Un generador determinista crea órdenes, activos, restricciones y eventos operativos sintéticos.
2. Once scripts DuckDB construyen preparación SQL, vistas integradas, marts, KPI y comprobaciones de negocio.
3. Python calcula variables, diagnóstico, escenarios paramétricos y el Índice de Prioridad Operativa (OPI).
4. Sensibilidad de pesos y Monte Carlo prueban la estabilidad del ranking.
5. La puerta de publicación valida integridad, consistencia de métricas y contratos del panel.

Los KPI oficiales provienen de `data/processed/ev_factory/kpi_operativos.csv`. `area_throughput_loss_proxy` atribuye impacto a eventos de cuello de botella, pero no representa una estimación causal.

## Ejecución local

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"

generate-data --seed 20260328 --start-date 2025-01-01 --months 12
python -m src.run_pipeline
python scripts/generate_chart_pack.py
python scripts/generate_report.py
python -m src.ev_release_gate
```

El repositorio incluye un corte canónico de los CSV de origen, marts y la base DuckDB. Los comandos anteriores regeneran los datos y artefactos de forma determinista.

Para una guía operativa con orden real de la canalización, comandos parciales, salidas esperadas y resolución de problemas: [operator_quickstart.md](docs/operator_quickstart.md).

## Verificación

```bash
ruff check .
ruff format --check .
pytest -q
pytest -q -m integration
python -m src.ev_release_gate
```

La cobertura combinada (unitarios + integración) es ~90 %; CI ejecuta la suite
completa con un umbral mínimo del 85 % (`--cov-fail-under=85`). La base DuckDB en
`data/processed/` es un intermedio reconstruible y no se versiona; los CSV son la
salida canónica.

## Estructura

```text
data/raw/ev_factory/          14 tablas sintéticas de origen
data/processed/ev_factory/    marts, variables, puntuaciones y KPI gobernados
sql/ev_factory/               11 transformaciones DuckDB ordenadas
src/                          canalización, diagnóstico, escenarios y puerta de publicación
scripts/                      generación de 19 gráficos y del informe PDF
tests/                        pruebas unitarias, integración y contratos públicos
docs/                         arquitectura, métricas, metodología y gobernanza
outputs/dashboard/            panel HTML publicado en GitHub Pages
outputs/graphs/               gráficos analíticos curados
outputs/reports/              informe, manifiestos y validaciones
```

## Salidas principales

| Salida | Ruta |
|---|---|
| Panel oficial | `outputs/dashboard/industrial-ev-operating-command-center.html` |
| Informe PDF | `outputs/reports/ev_transition_operating_twin_report.pdf` |
| KPI gobernados | `data/processed/ev_factory/kpi_operativos.csv` |
| Priorización OPI | `data/processed/ev_factory/operational_prioritization_table.csv` |
| Escenarios | `data/processed/ev_factory/scenario_table.csv` |
| Preparación de publicación | `outputs/reports/release_readiness.json` |

## Límites de uso

- Los datos son sintéticos y no representan una planta real.
- Las elasticidades de escenarios son supuestos paramétricos, no estimaciones causales.
- Pesos, umbrales, restricciones y capacidad requieren calibración antes de uso operacional.
- El sistema sirve para arquitectura analítica, diagnóstico y apoyo a decisión; no para compromisos de inversión sin validación independiente.
- El panel es estático, pero carga Chart.js y fuentes web desde CDN.

Detalles técnicos: [arranque operativo](docs/operator_quickstart.md), [contratos de datos y advertencias](docs/data_contracts_and_caveats.md), [arquitectura SQL](docs/sql_architecture.md), [definiciones de métricas](docs/sql_metric_definitions.md), [puntuación](docs/scoring_framework.md), [gobernanza de KPI](docs/governance/kpi_governance_contract.md) y [puertas de publicación](docs/governance/release_gates.md).

Licencia: [MIT](LICENSE).
