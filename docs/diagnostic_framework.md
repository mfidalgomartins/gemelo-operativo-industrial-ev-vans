# Marco Diagnóstico - Gemelo Operativo EV

## Objetivo
Identificar dónde se rompe el flujo y priorizar acciones operativas interpretables en secuenciación, patio, carga y expedición.

## Capas de análisis
1. Secuenciación: desviación plan-real y complejidad de versión.
2. Patio: espera, bloqueo y no productividad de movimientos.
3. Carga: colas, presión de punto de carga e incumplimiento de SOC objetivo.
4. Expedición: retrasos y brecha de preparación.
5. Área-turno: tensión operacional y criticidad por impacto de caudal atribuido a eventos del área.

## Puntuaciones principales
- `sequence_disruption_score`
- `yard_congestion_score`
- `charging_pressure_score`
- `dispatch_delay_risk_score`
- `launch_transition_stress_score`
- `area_criticality_score`

## Lógica de persistencia
- `ESTRUCTURAL`: cuota de periodos críticos >= 30%.
- `PICO_OCASIONAL`: cuota crítica baja, pero p95 de criticidad muy alto.
- `ESTABLE`: sin evidencia de tensión sostenida.

## Salidas
- `diagnostic_vehicle_scores.csv`
- `diagnostic_area_scores.csv`
- `diagnostic_area_persistence.csv`
- `diagnostic_ev_vs_non_ev.csv`
- `diagnostic_shift_comparison.csv`
- `diagnostic_area_ranking.csv`

## Uso operativo
El marco conecta síntomas con acción inicial recomendada para facilitar priorización diaria y planificación semanal durante la transición EV.
