# Auditoría /explore-data - Preparación Operativa de Datos

## Alcance
Auditoría formal de calidad y preparación operativa sobre las 14 tablas base del gemelo operativo EV.

## Resumen por conjunto de datos
| tabla | grano | claves_candidatas | claves_foraneas_esperadas | n_filas | n_columnas | cobertura_temporal | porcentaje_nulos_promedio | duplicados_pct | clave_candidata_unica |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ordenes | 1 fila por orden de producción | orden_id | vehiculo_id->vehiculos.vehiculo_id; version_id->versiones_vehiculo.version_id | 58697 | 13 | 2025-01-01 06:00:00 -> 2026-01-01 07:36:00 | 0.0 | 0.0 | orden_id:True |
| versiones_vehiculo | 1 fila por versión de vehículo | version_id | Sin dato | 8 | 8 | Sin dato | 0.0 | 0.0 | version_id:True |
| vehiculos | 1 fila por vehículo | vehiculo_id | version_id->versiones_vehiculo.version_id | 58697 | 11 | 2025-01-01 13:07:00 -> 2026-01-03 23:16:00 | 13.553 | 0.0 | vehiculo_id:True |
| estado_bateria | 1 fila por lectura temporal de batería por vehículo | timestamp; vehiculo_id | vehiculo_id->vehiculos.vehiculo_id | 128705 | 8 | 2025-01-01 13:07:00 -> 2026-01-01 21:28:00 | 0.0 | 0.0 | timestamp:False; vehiculo_id:False |
| slots_carga | 1 fila por punto de carga | slot_id | Sin dato | 32 | 7 | Sin dato | 0.0 | 0.0 | slot_id:True |
| sesiones_carga | 1 fila por sesión de carga | sesion_id | vehiculo_id->vehiculos.vehiculo_id; slot_id->slots_carga.slot_id | 22997 | 9 | Sin dato | 0.0 | 0.0 | sesion_id:True |
| patio | 1 fila por estado temporal en patio por vehículo | timestamp; vehiculo_id; zona_patio | vehiculo_id->vehiculos.vehiculo_id | 205978 | 9 | 2025-01-01 13:35:00 -> 2026-01-03 23:16:00 | 0.0 | 0.0 | timestamp:False; vehiculo_id:False; zona_patio:False |
| movimientos_patio | 1 fila por movimiento de patio | movimiento_id | vehiculo_id->vehiculos.vehiculo_id | 116181 | 9 | 2025-01-02 00:57:00 -> 2026-01-03 22:48:00 | 0.0 | 0.0 | movimiento_id:True |
| turnos | 1 fila por fecha-turno | fecha; turno | Sin dato | 1095 | 7 | 2025-01-01 00:00:00 -> 2025-12-31 00:00:00 | 0.0 | 0.0 | fecha:False; turno:False |
| logistica_salida | 1 fila por evento de salida por vehículo | salida_id | vehiculo_id->vehiculos.vehiculo_id | 58697 | 9 | 2025-01-01 22:38:00 -> 2026-01-03 23:16:00 | 3.049 | 0.0 | salida_id:True |
| cuellos_botella | 1 fila por evento de cuello de botella | evento_id | Sin dato | 1427 | 9 | 2025-01-02 11:00:00 -> 2026-01-03 15:00:00 | 0.0 | 0.0 | evento_id:True |
| recursos_operativos | 1 fila por recurso operativo | recurso_id | Sin dato | 13 | 6 | Sin dato | 0.0 | 0.0 | recurso_id:True |
| restricciones_operativas | 1 fila por restricción operativa | restriccion_id | Sin dato | 895 | 7 | 2025-01-01 14:23:00 -> 2026-01-01 08:38:00 | 0.0 | 0.0 | restriccion_id:True |
| escenarios_transicion | 1 fila por día de transición | fecha | Sin dato | 365 | 7 | 2025-01-01 00:00:00 -> 2025-12-31 00:00:00 | 0.0 | 0.0 | fecha:True |

## Problemas priorizados
| problema | severidad | filas_afectadas | regla | correccion_recomendada |
| --- | --- | --- | --- | --- |
| ocupaciones_patio_incompatibles | alta | 10 | vehículo no puede estar en dos posiciones en la misma marca temporal | Deduplicar instantáneas por marca temporal+vehiculo y conservar el estado de mayor prioridad. |
| retrasos_sin_causa | media | 1652 | Retraso positivo requiere causa de retraso válida | Imponer catálogo de causas y clasificación automática cuando falte causa. |

## Recomendaciones para transformación analítica
- Normalizar marcas temporales a UTC y zona horaria operacional de planta.
- Construir `vehicle_timeline_canonical` como fuente única para tiempos de paso.
- Aplicar restricciones de integridad referencial en la capa de preparación SQL.
- Mantener catálogo controlado de estados y causas para evitar ruido semántico.
- Definir reglas de excepción operativa para salidas sin preparación.
- Versionar reglas de puntuación y validación para trazabilidad auditada.

## Propuesta de cruces oficiales
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
- `mart_vehicle_flow_day`: flujo integral diario por vehículo (tiempos de paso, preparación y salida).
- `mart_area_shift_ops`: presión operativa y cuellos por área-turno.
- `mart_charging_readiness`: utilización, colas, brecha SOC e interrupciones.
- `mart_yard_congestion`: permanencia, bloqueo y movimientos no productivos por zona.
- `mart_dispatch_risk`: riesgo de salida por causa, turno, versión y mercado.
