# Diccionario de Variables - Gemelo Operativo EV

## vehicle_readiness_features
- `planned_to_actual_sequence_gap` (derivada): desviación de secuencia real vs plan.
- `total_internal_lead_time` (observada/derivada): tiempo desde fin de línea a salida.
- `yard_wait_time` (observada): permanencia media en patio por vehículo.
- `charging_wait_time` (observada): espera media previa a carga.
- `charging_duration` (observada): duración de carga consolidada.
- `soc_gap_before_dispatch` (derivada): SOC objetivo menos SOC real de salida.
- `dispatch_delay_min` (observada): retraso operativo de salida.
- `non_productive_moves_count` (derivada): movimientos de patio no productivos.
- `blocking_exposure` (derivada): exposición a bloqueo en instantáneas de patio.
- `version_complexity_score` (observada): complejidad de montaje de la versión.
- `readiness_risk_score_input` (derivada): puntuación interpretable de riesgo previo a la puntuación final.

## area_shift_features
- `dispatch_gap` (derivada): diferencia entre salidas reales y planificadas por turno.
- `area_throughput_loss_proxy` (derivada): suma del impacto de caudal de eventos de cuello de botella atribuidos al área-turno.
- `congestion_index` (derivada): presión de flujo en área.
- `avg_wait_time` (derivada): espera media operativa por área-turno.
- `queue_pressure_score` (derivada): presión de cola de carga.
- `slot_utilization` (derivada): utilización media de puntos de carga en turno.
- `yard_occupancy_rate` (derivada): ocupación del patio sobre capacidad estimada.
- `bottleneck_density` (derivada): intensidad de cuellos por área-turno.
- `dispatch_risk_density` (derivada): densidad de riesgo de expedición.
- `operational_stress_score` (derivada): puntuación compuesta de tensión operativa.

## charging_features
- `sessions_per_shift` (observada agregada): volumen de sesiones por turno y punto de carga.
- `avg_wait_to_charge` (derivada): espera media antes de carga.
- `avg_energy_delivered` (observada agregada): energía media entregada.
- `interruption_rate` (derivada): ratio de sesiones interrumpidas.
- `target_soc_miss_rate` (derivada): tasa de incumplimiento de objetivo SOC.
- `charger_pressure_score` (derivada): puntuación compuesta de presión de carga.

## yard_features
- `avg_dwell_time` (observada agregada): permanencia promedio por zona.
- `p95_dwell_time` (derivada): percentil 95 de permanencia.
- `blocking_rate` (derivada): ratio de vehículos bloqueados.
- `movement_density` (observada agregada): intensidad de movimientos.
- `non_productive_move_rate` (derivada): peso de movimientos no productivos.
- `yard_saturation_score` (derivada): puntuación de saturación operativa del patio.

## launch_transition_features
- `share_ev` (observada agregada): participación EV semanal.
- `ev_operational_load_index` (derivada): carga operativa asociada a EV.
- `readiness_gap_trend` (derivada): tendencia semanal de la brecha de preparación.
- `charging_capacity_gap` (derivada): brecha presión/capacidad de carga.
- `yard_transition_stress_index` (derivada): tensión de transición en patio.
- `dispatch_stability_index` (derivada): estabilidad de expedición (100-riesgo).

## Valor operativo
Estas señales están diseñadas para decisiones interpretables de secuenciación, patio, carga y expedición durante una rampa EV sin depender de modelos de caja negra.
