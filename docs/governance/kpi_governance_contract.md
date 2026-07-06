# Contrato de Gobernanza KPI (EV)

## Objetivo
Definir fuente, fórmula y validación mínima de KPI críticos para evitar derivaciones ambiguas en análisis, puntuación y panel.

## KPI críticos (fuente de verdad)
- `throughput_planificado`: `kpi_operativos.csv`
- `throughput_real`: `kpi_operativos.csv`
- `throughput_gap`: `kpi_operativos.csv`
- `share_ev`: `kpi_operativos.csv`
- `ocupacion_pico_patio`: `kpi_operativos.csv`
- `utilizacion_media_cargadores`: `kpi_operativos.csv`
- `ratio_salida_retrasada`: `kpi_operativos.csv`
- `score_readiness_global`: `kpi_operativos.csv`

Definiciones:
- `score_readiness_global`: porcentaje de vehículos con preparación final, escala 0-100.
- `ratio_salida_retrasada`: porcentaje de vehículos despachados con atraso superior a 120 minutos.

## Reglas de consistencia obligatorias
1. `share_ev` KPI vs `vw_vehicle_flow_timeline.tipo_propulsion`:
   - tolerancia absoluta <= 0.02.
2. `throughput_planificado` KPI vs filas de `vw_vehicle_flow_timeline`:
   - igualdad exacta en la ejecución.
3. KPI fuera de rango:
   - proporciones deben estar en [0, 1].
   - puntuaciones en [0, 100].
4. Ninguna salida real puede tener `readiness_final_flag = false`.

## Uso permitido
- Panel ejecutivo: permitido.
- Priorización inicial de operaciones: permitido.
- Comité de inversión: no permitido sin calibración con datos reales y validación independiente.
