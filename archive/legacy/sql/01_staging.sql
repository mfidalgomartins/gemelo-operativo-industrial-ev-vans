CREATE OR REPLACE TABLE stg_ordenes_produccion AS
SELECT
    TRIM(order_id) AS order_id,
    TRIM(vin) AS vin,
    CAST(fecha_plan AS TIMESTAMP) AS fecha_plan,
    CAST(fecha_objetivo_salida AS TIMESTAMP) AS fecha_objetivo_salida,
    UPPER(TRIM(tipo_propulsion)) AS tipo_propulsion,
    TRIM(familia_van) AS familia_van,
    TRIM(destino) AS destino,
    CAST(prioridad_cliente AS INTEGER) AS prioridad_cliente,
    CAST(bateria_kwh AS DOUBLE) AS bateria_kwh
FROM raw_ordenes_produccion;

CREATE OR REPLACE TABLE stg_versiones_vehiculo AS
SELECT
    TRIM(vehicle_version_id) AS vehicle_version_id,
    TRIM(familia_van) AS familia_van,
    UPPER(TRIM(tipo_propulsion)) AS tipo_propulsion,
    CAST(tiempo_base_ensamblaje_min AS DOUBLE) AS tiempo_base_ensamblaje_min,
    CAST(tiempo_base_test_min AS DOUBLE) AS tiempo_base_test_min,
    CAST(consumo_test_kwh AS DOUBLE) AS consumo_test_kwh
FROM raw_versiones_vehiculo;

CREATE OR REPLACE TABLE stg_eventos_produccion AS
SELECT
    TRIM(event_id) AS event_id,
    TRIM(order_id) AS order_id,
    UPPER(TRIM(estacion)) AS estacion,
    CAST(ts_inicio AS TIMESTAMP) AS ts_inicio,
    CAST(ts_fin AS TIMESTAMP) AS ts_fin,
    CAST(duracion_real_min AS DOUBLE) AS duracion_real_min,
    UPPER(TRIM(turno)) AS turno
FROM raw_eventos_produccion;

CREATE OR REPLACE TABLE stg_eventos_patio AS
SELECT
    TRIM(yard_event_id) AS yard_event_id,
    TRIM(order_id) AS order_id,
    UPPER(TRIM(tipo_evento)) AS tipo_evento,
    CAST(ts_evento AS TIMESTAMP) AS ts_evento,
    UPPER(TRIM(sector_patio)) AS sector_patio,
    CAST(ocupacion_sector_pct AS DOUBLE) AS ocupacion_sector_pct
FROM raw_eventos_patio;

CREATE OR REPLACE TABLE stg_sesiones_carga AS
SELECT
    TRIM(charge_session_id) AS charge_session_id,
    TRIM(order_id) AS order_id,
    TRIM(charger_id) AS charger_id,
    CAST(ts_inicio_carga AS TIMESTAMP) AS ts_inicio_carga,
    CAST(ts_fin_carga AS TIMESTAMP) AS ts_fin_carga,
    CAST(kwh_entregados AS DOUBLE) AS kwh_entregados,
    CAST(soc_inicio AS DOUBLE) AS soc_inicio,
    CAST(soc_fin AS DOUBLE) AS soc_fin,
    CAST(espera_previa_min AS DOUBLE) AS espera_previa_min
FROM raw_sesiones_carga;

CREATE OR REPLACE TABLE stg_disponibilidad_energia AS
SELECT
    CAST(ts_hora AS TIMESTAMP) AS ts_hora,
    UPPER(TRIM(turno)) AS turno,
    CAST(capacidad_kw_disponible AS DOUBLE) AS capacidad_kw_disponible,
    CAST(demanda_kw AS DOUBLE) AS demanda_kw,
    CAST(curtailment_flag AS INTEGER) AS curtailment_flag
FROM raw_disponibilidad_energia;

CREATE OR REPLACE TABLE stg_eventos_expedicion AS
SELECT
    TRIM(dispatch_event_id) AS dispatch_event_id,
    TRIM(order_id) AS order_id,
    CAST(ts_ready_expedicion AS TIMESTAMP) AS ts_ready_expedicion,
    CAST(ts_salida_real AS TIMESTAMP) AS ts_salida_real,
    UPPER(TRIM(modo_salida)) AS modo_salida,
    CAST(sla_horas AS DOUBLE) AS sla_horas
FROM raw_eventos_expedicion;

CREATE OR REPLACE TABLE stg_calendario_turnos AS
SELECT
    CAST(fecha AS DATE) AS fecha,
    UPPER(TRIM(turno)) AS turno,
    CAST(dotacion AS DOUBLE) AS dotacion,
    CAST(capacidad_teorica_unidades AS DOUBLE) AS capacidad_teorica_unidades
FROM raw_calendario_turnos;
