# Definición de Métricas SQL

## Métricas de Flujo
- `throughput_planificado`: órdenes con fecha programada (base `stg_orders`).
- `throughput_real`: órdenes con fecha real de ejecución de producción.
- `throughput_gap`: `throughput_real - throughput_planificado`.
- `dispatch_gap`: salidas reales menos salidas planificadas por fecha-turno.
- `area_throughput_loss_proxy`: suma del impacto de caudal de cuellos de botella atribuidos al área-turno.
- `total_internal_lead_time_min`: minutos desde fin de línea hasta salida real.
- `planned_to_actual_sequence_gap`: `secuencia_real - secuencia_planeada`.

## Métricas de Patio
- `yard_wait_time_min`: permanencia promedio por vehículo.
- `yard_dwell_max_min`: permanencia máxima observada por vehículo.
- `yard_occupancy_rate`: ocupación/capacidad estimada por zona-hora.
- `blocking_rate`: proporción de instantáneas con bloqueo.
- `non_productive_move_rate`: proporción de movimientos no productivos.
- `operational_risk_score` (`vw_yard_congestion`): combinación ponderada de ocupación, bloqueo, no productivo y permanencia p95.

## Métricas de Carga
- `slot_utilization_ratio`: `(sesiones * duración media sesión) / 480` por punto de carga y turno.
- `avg_wait_time_min`: cola media antes de iniciar carga.
- `interruption_rate`: sesiones interrumpidas / sesiones totales.
- `avg_soc_gap_pct`: diferencia media `target_soc - soc`.
- `charging_bottleneck_impact`: impacto medio de cuellos de carga/energía por fecha.

## Métricas de Expedición y Preparación
- `dispatch_delay_min`: diferencia planificada vs real de salida (min).
- `turno` (`vw_dispatch_readiness`): turno derivado de la salida planificada.
- `turno_produccion`: turno original de producción, conservado para trazabilidad.
- `readiness_final_flag`: bandera final consolidada de preparación.
- `soc_gap_before_dispatch`: `target_soc_salida_pct - soc_salida_pct`.
- `dispatch_readiness_risk_score`: puntuación compuesta de brecha SOC, retraso, cola de carga, espera de patio y bloqueo.
- `readiness_rate`: proporción de vehículos listos por segmento.
- `delayed_flag`: salida real con atraso superior a 120 minutos.
- `delay_rate`: proporción de vehículos retrasados entre los efectivamente despachados.

## Métricas de Cuello de Botella y Tensión
- `eventos_cuello`: número de eventos de cuello por área-turno.
- `impacto_throughput_total`: suma de impacto de caudal por área-turno.
- `impacto_salida_total`: suma de impacto sobre salida por área-turno.
- `area_stress_score`: puntuación compuesta de severidad, impacto y presión de turno.
- `operational_stress_score` (`mart_area_shift`): puntuación compuesta para priorización táctica por área-turno.

## KPI Ejecutivos (`kpi_operativos`)
- `share_ev`
- `tiempo_medio_patio_min`
- `dwell_p95_min`
- `ocupacion_media_patio`
- `ocupacion_pico_patio`
- `utilizacion_media_cargadores`
- `tiempo_medio_espera_carga_min`
- `vehiculos_no_ready`
- `ratio_salida_retrasada`
- `causa_principal_cuello`
- `area_mayor_perdida_throughput`
- `score_readiness_global`

`score_readiness_global` es la tasa de preparación final expresada en escala 0-100. `ratio_salida_retrasada` usa únicamente vehículos efectivamente despachados.

## Validaciones (`validation_checks`)
Comprobaciones de duplicados, secuencia, orden temporal, SOC, sesiones imposibles, EV sin carga, salida sin preparación, retraso sin causa y consistencia de capacidad.
