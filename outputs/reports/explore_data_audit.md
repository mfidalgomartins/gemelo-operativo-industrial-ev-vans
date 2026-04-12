# /explore-data Audit - Operational Data Readiness

## Alcance
Auditoría formal de calidad y readiness operacional sobre las 14 tablas base del gemelo operativo EV.

## Resumen por dataset
| tabla | grain | key_candidates | foreign_keys_esperadas | n_filas | n_columnas | cobertura_temporal | null_rate_pct_promedio | duplicados_pct | candidate_key_unique |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ordenes | 1 fila por orden de producción | orden_id | vehiculo_id->vehiculos.vehiculo_id; version_id->versiones_vehiculo.version_id | 43903 | 12 | 2025-01-01 06:00:00 -> 2025-10-01 07:48:00 | 0.0 | 0.0 | orden_id:True |
| versiones_vehiculo | 1 fila por versión de vehículo | version_id | N/A | 8 | 8 | N/A | 0.0 | 0.0 | version_id:True |
| vehiculos | 1 fila por vehículo | vehiculo_id | version_id->versiones_vehiculo.version_id | 43903 | 11 | 2025-01-01 13:31:00 -> 2025-10-04 00:22:00 | 13.08 | 0.0 | vehiculo_id:True |
| estado_bateria | 1 fila por lectura temporal de batería por vehículo | timestamp; vehiculo_id | vehiculo_id->vehiculos.vehiculo_id | 96329 | 8 | 2025-01-01 13:31:00 -> 2025-10-01 22:50:00 | 0.0 | 0.0 | timestamp:False; vehiculo_id:False |
| slots_carga | 1 fila por slot de carga | slot_id | N/A | 32 | 7 | N/A | 0.0 | 0.0 | slot_id:True |
| sesiones_carga | 1 fila por sesión de carga | sesion_id | vehiculo_id->vehiculos.vehiculo_id; slot_id->slots_carga.slot_id | 17236 | 9 | N/A | 0.0 | 0.0 | sesion_id:True |
| patio | 1 fila por estado temporal en patio por vehículo | timestamp; vehiculo_id; zona_patio | vehiculo_id->vehiculos.vehiculo_id | 156349 | 9 | 2025-01-01 13:36:00 -> 2025-10-04 00:22:00 | 0.0 | 0.0 | timestamp:False; vehiculo_id:False; zona_patio:False |
| movimientos_patio | 1 fila por movimiento de patio | movimiento_id | vehiculo_id->vehiculos.vehiculo_id | 87355 | 9 | 2025-01-01 21:37:00 -> 2025-10-03 23:08:00 | 0.0 | 0.0 | movimiento_id:True |
| turnos | 1 fila por fecha-turno | fecha; turno | N/A | 819 | 7 | 2025-01-01 00:00:00 -> 2025-09-30 00:00:00 | 0.0 | 0.0 | fecha:False; turno:False |
| logistica_salida | 1 fila por evento de salida por vehículo | salida_id | vehiculo_id->vehiculos.vehiculo_id | 43903 | 9 | 2025-01-02 05:45:00 -> 2025-10-04 02:07:00 | 2.488 | 0.0 | salida_id:True |
| cuellos_botella | 1 fila por evento de cuello de botella | evento_id | N/A | 1099 | 9 | 2025-01-02 11:00:00 -> 2025-10-04 19:00:00 | 0.0 | 0.0 | evento_id:True |
| recursos_operativos | 1 fila por recurso operativo | recurso_id | N/A | 13 | 6 | N/A | 0.0 | 0.0 | recurso_id:True |
| restricciones_operativas | 1 fila por restricción operativa | restriccion_id | N/A | 695 | 7 | 2025-01-01 22:02:00 -> 2025-09-30 23:46:00 | 0.0 | 0.0 | restriccion_id:True |
| escenarios_transicion | 1 fila por día de transición | fecha | N/A | 273 | 7 | 2025-01-01 00:00:00 -> 2025-09-30 00:00:00 | 0.0 | 0.0 | fecha:True |

## Issues priorizados
| issue | severity | affected_rows | rule | recommended_fix |
| --- | --- | --- | --- | --- |
| vehiculos_salen_sin_ready | critical | 2208 | No debe haber salida real con readiness_salida_flag=0 | Introducir bloqueo hard en lógica de expedición o marca explícita de override operativo. |
| ocupaciones_patio_incompatibles | high | 6 | vehículo no puede estar en dos posiciones al mismo timestamp | Deduplicar snapshots por timestamp+vehiculo y conservar estado de mayor prioridad. |
| retrasos_sin_causa | medium | 1225 | Retraso positivo requiere causa de retraso válida | Imponer catálogo de causas y fallback AUTOMATIC_CLASSIFICATION. |
| restriccion_inconsistente_capacidad | medium | 3 | Si hay restricciones severas por área, recurso debería reflejar restriccion_actual_flag | Sincronizar estado de recursos con restricciones activas por corte temporal. |

## Recomendaciones para transformación analítica
- Normalizar timestamps a UTC + timezone operacional de planta.
- Construir `vehicle_timeline_canonical` como fuente única para lead times.
- Aplicar constraints de integridad referencial en capa staging SQL.
- Mantener catálogo controlado de estados y causas para evitar ruido semántico.
- Definir reglas de override operativo para salidas sin readiness.
- Versionar reglas de scoring y validación para trazabilidad auditada.
- Prioridad inmediata: resolver issues `critical` antes de consumo ejecutivo.

## Propuesta de joins oficiales
- `ordenes.vehiculo_id` -> `vehiculos.vehiculo_id`
- `ordenes.version_id` -> `versiones_vehiculo.version_id`
- `sesiones_carga.vehiculo_id` -> `vehiculos.vehiculo_id`
- `sesiones_carga.slot_id` -> `slots_carga.slot_id`
- `estado_bateria.vehiculo_id` -> `vehiculos.vehiculo_id`
- `patio.vehiculo_id` -> `vehiculos.vehiculo_id`
- `movimientos_patio.vehiculo_id` -> `vehiculos.vehiculo_id`
- `logistica_salida.vehiculo_id` -> `vehiculos.vehiculo_id`
- `turnos(fecha, turno)` -> `ordenes(fecha_programada::date, turno)`

## Tablas candidatas para marts analíticos
- `mart_vehicle_flow_day`: flujo integral diario por vehículo (lead times, readiness, salida).
- `mart_area_shift_ops`: presión operativa y cuellos por área-turno.
- `mart_charging_readiness`: utilización, colas, SOC gap e interrupciones.
- `mart_yard_congestion`: dwell, blocking y movimientos no productivos por zona.
- `mart_dispatch_risk`: riesgo de salida por causa, turno, versión y mercado.