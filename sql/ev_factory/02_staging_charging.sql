-- DuckDB SQL
-- Capa staging de carga y batería

CREATE OR REPLACE VIEW stg_battery_status AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    CAST(vehiculo_id AS VARCHAR) AS vehiculo_id,
    CAST(soc_pct AS DOUBLE) AS soc_pct,
    CAST(target_soc_pct AS DOUBLE) AS target_soc_pct,
    CAST(battery_temp_proxy AS DOUBLE) AS battery_temp_proxy,
    CAST(charging_status AS VARCHAR) AS charging_status,
    CAST(energia_cargada_kwh AS DOUBLE) AS energia_cargada_kwh,
    CAST(tiempo_en_carga_min AS DOUBLE) AS tiempo_en_carga_min
FROM estado_bateria;

CREATE OR REPLACE VIEW stg_charge_slots AS
SELECT
    CAST(slot_id AS VARCHAR) AS slot_id,
    CAST(zona_carga AS VARCHAR) AS zona_carga,
    CAST(potencia_max_kw AS DOUBLE) AS potencia_max_kw,
    CAST(tipo_cargador AS VARCHAR) AS tipo_cargador,
    CASE WHEN CAST(disponibilidad_flag AS INTEGER) = 1 THEN TRUE ELSE FALSE END AS disponibilidad_flag,
    CASE WHEN CAST(mantenimiento_flag AS INTEGER) = 1 THEN TRUE ELSE FALSE END AS mantenimiento_flag,
    CASE WHEN CAST(ocupacion_actual_flag AS INTEGER) = 1 THEN TRUE ELSE FALSE END AS ocupacion_actual_flag
FROM slots_carga;

CREATE OR REPLACE VIEW stg_charge_sessions AS
SELECT
    CAST(sesion_id AS VARCHAR) AS sesion_id,
    CAST(vehiculo_id AS VARCHAR) AS vehiculo_id,
    CAST(slot_id AS VARCHAR) AS slot_id,
    CAST(inicio_sesion AS TIMESTAMP) AS inicio_sesion,
    CAST(fin_sesion AS TIMESTAMP) AS fin_sesion,
    CAST(energia_entregada_kwh AS DOUBLE) AS energia_entregada_kwh,
    CAST(tiempo_espera_previo_min AS DOUBLE) AS tiempo_espera_previo_min,
    CASE WHEN CAST(carga_interrumpida_flag AS INTEGER) = 1 THEN TRUE ELSE FALSE END AS carga_interrumpida_flag,
    CAST(causa_interrupcion AS VARCHAR) AS causa_interrupcion,
    EXTRACT(EPOCH FROM (CAST(fin_sesion AS TIMESTAMP) - CAST(inicio_sesion AS TIMESTAMP))) / 60.0 AS duracion_sesion_min
FROM sesiones_carga;
