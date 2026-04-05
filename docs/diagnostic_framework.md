# Diagnostic Framework - Gemelo Operativo EV

## Objetivo
Identificar dónde se rompe el flujo y priorizar acciones operativas interpretables en secuenciación, patio, carga y expedición.

## Capas de análisis
1. Secuenciación: desviación plan-real y complejidad de versión.
2. Patio: espera, bloqueo y no productividad de movimientos.
3. Carga: colas, presión de slot e incumplimiento de SOC objetivo.
4. Expedición: retrasos y gap de readiness.
5. Área-turno: estrés operacional y criticidad por impacto en throughput.

## Scores principales
- `sequence_disruption_score`
- `yard_congestion_score`
- `charging_pressure_score`
- `dispatch_delay_risk_score`
- `launch_transition_stress_score`
- `area_criticality_score`

## Lógica de persistencia
- `ESTRUCTURAL`: share de periodos críticos >= 30%.
- `PICO_OCASIONAL`: share crítico bajo, pero p95 de criticality muy alto.
- `ESTABLE`: sin evidencia de tensión sostenida.

## Salidas
- `diagnostic_vehicle_scores.csv`
- `diagnostic_area_scores.csv`
- `diagnostic_area_persistence.csv`
- `diagnostic_ev_vs_non_ev.csv`
- `diagnostic_shift_comparison.csv`
- `diagnostic_area_ranking.csv`

## Uso operativo
El framework conecta síntomas con acción inicial recomendada para facilitar priorización diaria y planificación semanal durante la transición EV.
