-- DuckDB SQL
-- Capa staging de órdenes y maestro de vehículo

CREATE OR REPLACE VIEW stg_orders AS
WITH typed_orders AS (
    SELECT
        CAST(orden_id AS VARCHAR) AS orden_id,
        CAST(fecha_programada AS TIMESTAMP) AS fecha_programada_ts,
        CAST(fecha_real AS TIMESTAMP) AS fecha_real_ts,
        CAST(turno AS VARCHAR) AS turno,
        CAST(secuencia_planeada AS BIGINT) AS secuencia_planeada,
        CAST(secuencia_real AS BIGINT) AS secuencia_real,
        CAST(vehiculo_id AS VARCHAR) AS vehiculo_id,
        CAST(version_id AS VARCHAR) AS version_id,
        CAST(prioridad_cliente AS VARCHAR) AS prioridad_cliente,
        CAST(mercado_destino AS VARCHAR) AS mercado_destino,
        CAST(estado_orden AS VARCHAR) AS estado_orden,
        CAST(ready_for_dispatch_flag AS INTEGER) AS ready_for_dispatch_flag
    FROM ordenes
)
SELECT
    orden_id,
    fecha_programada_ts,
    fecha_real_ts,
    CAST(fecha_programada_ts AS DATE) AS fecha_programada,
    CAST(fecha_real_ts AS DATE) AS fecha_real,
    turno,
    secuencia_planeada,
    secuencia_real,
    vehiculo_id,
    version_id,
    prioridad_cliente,
    mercado_destino,
    estado_orden,
    CASE WHEN ready_for_dispatch_flag = 1 THEN TRUE ELSE FALSE END AS ready_for_dispatch_flag
FROM typed_orders;

CREATE OR REPLACE VIEW stg_versions AS
SELECT
    CAST(version_id AS VARCHAR) AS version_id,
    CAST(familia_modelo AS VARCHAR) AS familia_modelo,
    CAST(tipo_propulsion AS VARCHAR) AS tipo_propulsion,
    CAST(capacidad_bateria_kwh AS DOUBLE) AS capacidad_bateria_kwh,
    CAST(tiempo_medio_produccion AS DOUBLE) AS tiempo_medio_produccion,
    CAST(complejidad_montaje AS DOUBLE) AS complejidad_montaje,
    CASE WHEN CAST(requiere_carga_salida_flag AS INTEGER) = 1 THEN TRUE ELSE FALSE END AS requiere_carga_salida_flag,
    CAST(nivel_criticidad_logistica AS DOUBLE) AS nivel_criticidad_logistica
FROM versiones_vehiculo;

CREATE OR REPLACE VIEW stg_vehicles AS
SELECT
    CAST(vehiculo_id AS VARCHAR) AS vehiculo_id,
    CAST(vin_proxy AS VARCHAR) AS vin_proxy,
    CAST(version_id AS VARCHAR) AS version_id,
    CAST(estado_fabricacion AS VARCHAR) AS estado_fabricacion,
    CAST(timestamp_fin_linea AS TIMESTAMP) AS timestamp_fin_linea,
    CAST(timestamp_entrada_patio AS TIMESTAMP) AS timestamp_entrada_patio,
    CAST(timestamp_inicio_carga AS TIMESTAMP) AS timestamp_inicio_carga,
    CAST(timestamp_fin_carga AS TIMESTAMP) AS timestamp_fin_carga,
    CAST(timestamp_salida AS TIMESTAMP) AS timestamp_salida,
    CAST(nivel_bateria_salida AS DOUBLE) AS nivel_bateria_salida,
    CAST(readiness_score_inicial AS DOUBLE) AS readiness_score_inicial
FROM vehiculos;

CREATE OR REPLACE VIEW stg_turnos AS
SELECT
    CAST(fecha AS DATE) AS fecha,
    CAST(turno AS VARCHAR) AS turno,
    CAST(headcount_proxy AS DOUBLE) AS headcount_proxy,
    CAST(absentismo_proxy AS DOUBLE) AS absentismo_proxy,
    CAST(productividad_turno_indice AS DOUBLE) AS productividad_turno_indice,
    CAST(presion_operativa_indice AS DOUBLE) AS presion_operativa_indice,
    CASE WHEN CAST(overtime_flag AS INTEGER) = 1 THEN TRUE ELSE FALSE END AS overtime_flag
FROM turnos;
