-- DuckDB SQL
-- Capa staging de patio, movimientos y restricciones

CREATE OR REPLACE VIEW stg_yard_snapshots AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    CAST(vehiculo_id AS VARCHAR) AS vehiculo_id,
    CAST(zona_patio AS VARCHAR) AS zona_patio,
    CAST(fila AS VARCHAR) AS fila,
    CAST(posicion AS VARCHAR) AS posicion,
    CAST(estado_en_patio AS VARCHAR) AS estado_en_patio,
    CAST(dwell_time_min AS DOUBLE) AS dwell_time_min,
    CASE WHEN CAST(blocking_flag AS INTEGER) = 1 THEN TRUE ELSE FALSE END AS blocking_flag,
    CASE WHEN CAST(requiere_movimiento_flag AS INTEGER) = 1 THEN TRUE ELSE FALSE END AS requiere_movimiento_flag
FROM patio;

CREATE OR REPLACE VIEW stg_yard_movements AS
SELECT
    CAST(movimiento_id AS VARCHAR) AS movimiento_id,
    CAST(vehiculo_id AS VARCHAR) AS vehiculo_id,
    CAST(timestamp_inicio AS TIMESTAMP) AS timestamp_inicio,
    CAST(timestamp_fin AS TIMESTAMP) AS timestamp_fin,
    CAST(origen AS VARCHAR) AS origen,
    CAST(destino AS VARCHAR) AS destino,
    CAST(motivo_movimiento AS VARCHAR) AS motivo_movimiento,
    CAST(operador_turno AS VARCHAR) AS operador_turno,
    CASE WHEN CAST(movimiento_no_productivo_flag AS INTEGER) = 1 THEN TRUE ELSE FALSE END AS movimiento_no_productivo_flag,
    EXTRACT(EPOCH FROM (CAST(timestamp_fin AS TIMESTAMP) - CAST(timestamp_inicio AS TIMESTAMP))) / 60.0 AS duracion_movimiento_min
FROM movimientos_patio;

CREATE OR REPLACE VIEW stg_bottlenecks AS
SELECT
    CAST(evento_id AS VARCHAR) AS evento_id,
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    CAST(area AS VARCHAR) AS area,
    CAST(tipo_cuello_botella AS VARCHAR) AS tipo_cuello_botella,
    CAST(severidad AS DOUBLE) AS severidad,
    CAST(duracion_min AS DOUBLE) AS duracion_min,
    CAST(impacto_throughput_proxy AS DOUBLE) AS impacto_throughput_proxy,
    CAST(impacto_salida_proxy AS DOUBLE) AS impacto_salida_proxy,
    CAST(causa_probable AS VARCHAR) AS causa_probable
FROM cuellos_botella;

CREATE OR REPLACE VIEW stg_operational_resources AS
SELECT
    CAST(recurso_id AS VARCHAR) AS recurso_id,
    CAST(tipo_recurso AS VARCHAR) AS tipo_recurso,
    CAST(area AS VARCHAR) AS area,
    CAST(capacidad_nominal AS DOUBLE) AS capacidad_nominal,
    CAST(capacidad_disponible AS DOUBLE) AS capacidad_disponible,
    CASE WHEN CAST(restriccion_actual_flag AS INTEGER) = 1 THEN TRUE ELSE FALSE END AS restriccion_actual_flag
FROM recursos_operativos;

CREATE OR REPLACE VIEW stg_operational_constraints AS
SELECT
    CAST(restriccion_id AS VARCHAR) AS restriccion_id,
    CAST(timestamp_inicio AS TIMESTAMP) AS timestamp_inicio,
    CAST(timestamp_fin AS TIMESTAMP) AS timestamp_fin,
    CAST(area AS VARCHAR) AS area,
    CAST(tipo_restriccion AS VARCHAR) AS tipo_restriccion,
    CAST(severidad AS DOUBLE) AS severidad,
    CAST(impacto_capacidad_pct AS DOUBLE) AS impacto_capacidad_pct
FROM restricciones_operativas;
