# Diccionario de Datos

## `data/raw/ordenes_produccion.csv`
- `order_id`: identificador de orden.
- `vin`: identificador único de vehículo.
- `fecha_plan`: fecha planificada de producción.
- `fecha_objetivo_salida`: fecha objetivo de expedición.
- `tipo_propulsion`: EV o ICE.
- `familia_van`: segmento de producto.
- `destino`: región logística destino.
- `prioridad_cliente`: prioridad comercial (1 alta - 3 baja).
- `bateria_kwh`: capacidad nominal de batería para EV.

## `data/raw/eventos_produccion.csv`
- `event_id`, `order_id`, `estacion`, `ts_inicio`, `ts_fin`, `duracion_real_min`, `turno`.

## `data/raw/eventos_patio.csv`
- `yard_event_id`, `order_id`, `tipo_evento`, `ts_evento`, `sector_patio`, `ocupacion_sector_pct`.

## `data/raw/sesiones_carga.csv`
- `charge_session_id`, `order_id`, `charger_id`, `ts_inicio_carga`, `ts_fin_carga`, `kwh_entregados`, `soc_inicio`, `soc_fin`, `espera_previa_min`.

## `data/raw/disponibilidad_energia.csv`
- `ts_hora`, `turno`, `capacidad_kw_disponible`, `demanda_kw`, `curtailment_flag`.

## `data/raw/eventos_expedicion.csv`
- `dispatch_event_id`, `order_id`, `ts_ready_expedicion`, `ts_salida_real`, `modo_salida`, `sla_horas`.

## `data/processed/features_operativas.csv`
- Feature set consolidado para scoring de readiness/riesgo/prioridad.

## `data/processed/scores_operativos.csv`
- Resultados de scores para decisiones operativas.
