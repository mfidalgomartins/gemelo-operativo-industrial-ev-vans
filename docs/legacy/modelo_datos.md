# Modelo de Datos Completo

## Dominio operacional
El modelo representa un flujo industrial de vans con nodos: producción, test, patio, carga, liberación logística y expedición.

## Grano analítico principal
- **Unidad-orden** (`order_id`, `vehicle_id`) para ciclo operativo completo.
- **Hora-turno** para ocupación/capacidad de patio y cargadores.
- **Evento** para trazabilidad temporal.

## Entidades raw

### 1. `ordenes_produccion`
- `order_id` (PK)
- `vin`
- `fecha_plan`
- `fecha_objetivo_salida`
- `tipo_propulsion` (EV/ICE)
- `familia_van`
- `destino`
- `prioridad_cliente`
- `bateria_kwh` (EV)

### 2. `versiones_vehiculo`
- `vehicle_version_id` (PK)
- `familia_van`
- `tipo_propulsion`
- `tiempo_base_ensamblaje_min`
- `tiempo_base_test_min`
- `consumo_test_kwh`

### 3. `eventos_produccion`
- `event_id` (PK)
- `order_id`
- `estacion` (BODY/PAINT/ASSEMBLY/EOL)
- `ts_inicio`
- `ts_fin`
- `duracion_real_min`
- `turno`

### 4. `eventos_patio`
- `yard_event_id` (PK)
- `order_id`
- `tipo_evento` (ENTRY/MOVE/WAIT/EXIT)
- `ts_evento`
- `sector_patio`
- `ocupacion_sector_pct`

### 5. `sesiones_carga`
- `charge_session_id` (PK)
- `order_id`
- `charger_id`
- `ts_inicio_carga`
- `ts_fin_carga`
- `kwh_entregados`
- `soc_inicio`
- `soc_fin`
- `espera_previa_min`

### 6. `disponibilidad_energia`
- `ts_hora`
- `turno`
- `capacidad_kw_disponible`
- `demanda_kw`
- `curtailment_flag`

### 7. `eventos_expedicion`
- `dispatch_event_id` (PK)
- `order_id`
- `ts_ready_expedicion`
- `ts_salida_real`
- `modo_salida` (CAMION/TREN)
- `sla_horas`

### 8. `calendario_turnos`
- `fecha`
- `turno`
- `dotacion`
- `capacidad_teorica_unidades`

## Capa analítica SQL
- `fct_flujo_unidad`: ciclo integral y tiempos de espera.
- `fct_carga_operativa`: utilización de cargadores y colas.
- `fct_ocupacion_patio_hora`: presión de ocupación por tramo horario.
- `fct_expedicion`: readiness y cumplimiento SLA.
- `features_operativas`: variables de riesgo y priorización.
- `scores_operativos`: scoring compuesto para decisión.

## Integridad esperada
- `order_id` presente en todas las facts.
- secuencia temporal válida: producción <= patio <= carga <= expedición.
- no duraciones negativas.
- SOC entre 0 y 100.
