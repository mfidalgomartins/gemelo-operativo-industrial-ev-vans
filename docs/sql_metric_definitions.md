# Definición de Métricas SQL

## Métricas de Flujo
- `throughput_planificado`: órdenes con fecha programada (base `stg_orders`).
- `throughput_real`: órdenes/vehículos con fecha real de ejecución o salida.
- `throughput_gap`: `throughput_real - throughput_planificado`.
- `total_internal_lead_time_min`: minutos desde fin de línea hasta salida real.
- `planned_to_actual_sequence_gap`: `secuencia_real - secuencia_planeada`.

## Métricas de Patio
- `yard_wait_time_min`: dwell promedio por vehículo.
- `yard_dwell_max_min`: dwell máximo observado por vehículo.
- `yard_occupancy_rate`: ocupación/capacidad estimada por zona-hora.
- `blocking_rate`: proporción de snapshots con bloqueo.
- `non_productive_move_rate`: proporción de movimientos no productivos.
- `operational_risk_score` (`vw_yard_congestion`): combinación ponderada de ocupación, blocking, no productivo y p95 dwell.

## Métricas de Carga
- `slot_utilization_ratio`: `(sesiones * duración media sesión) / 480` por slot-turno.
- `avg_wait_time_min`: cola media antes de iniciar carga.
- `interruption_rate`: sesiones interrumpidas / sesiones totales.
- `avg_soc_gap_pct`: diferencia media `target_soc - soc`.
- `charging_bottleneck_impact`: impacto medio de cuellos de carga/energía por fecha.

## Métricas de Expedición y Readiness
- `dispatch_delay_min`: diferencia planificada vs real de salida (min).
- `readiness_final_flag`: bandera final consolidada de readiness.
- `soc_gap_before_dispatch`: `target_soc_salida_pct - soc_salida_pct`.
- `dispatch_readiness_risk_score`: score compuesto de SOC gap, delay, cola carga, espera patio y bloqueo.
- `readiness_rate`: proporción de vehículos listos por segmento.
- `delay_rate`: proporción de vehículos retrasados por segmento.

## Métricas de Bottleneck y Estrés
- `eventos_cuello`: número de eventos de cuello por área-turno.
- `impacto_throughput_total`: suma de impacto de throughput por área-turno.
- `impacto_salida_total`: suma de impacto sobre salida por área-turno.
- `area_stress_score`: score compuesto de severidad, impacto y presión turno.
- `operational_stress_score` (`mart_area_shift`): score compuesto para priorización táctica por área-turno.

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

## Validaciones (`validation_checks`)
Checks de duplicados, secuencia, orden temporal, SOC, sesiones imposibles, EV sin carga, salida sin readiness, retraso sin causa y consistencia de capacidad.
