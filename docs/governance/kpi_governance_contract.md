# KPI Governance Contract (EV)

## Objetivo
Definir fuente, fórmula y validación mínima de KPI críticos para evitar derivaciones ambiguas en análisis, scoring y dashboard.

## KPI críticos (source of truth)
- `throughput_planificado`: `kpi_operativos.csv`
- `throughput_real`: `kpi_operativos.csv`
- `throughput_gap`: `kpi_operativos.csv`
- `share_ev`: `kpi_operativos.csv`
- `ocupacion_pico_patio`: `kpi_operativos.csv`
- `utilizacion_media_cargadores`: `kpi_operativos.csv`
- `ratio_salida_retrasada`: `kpi_operativos.csv`
- `score_readiness_global`: `kpi_operativos.csv`

## Reglas de consistencia obligatorias
1. `share_ev` KPI vs `vw_vehicle_flow_timeline.tipo_propulsion`:
   - tolerancia absoluta <= 0.02.
2. `throughput_planificado` KPI vs filas de `vw_vehicle_flow_timeline`:
   - igualdad exacta en la ejecución.
3. KPI fuera de rango:
   - proporciones deben estar en [0, 1].
   - scores en [0, 100].

## Uso permitido
- Dashboard ejecutivo: permitido.
- Priorización inicial de operaciones: permitido.
- Comité de inversión: requiere además `release_grade` >= `committee-grade candidate`.
