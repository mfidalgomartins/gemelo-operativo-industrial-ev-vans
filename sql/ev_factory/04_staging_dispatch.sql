-- DuckDB SQL
-- Capa staging de expedición y escenarios de transición

CREATE OR REPLACE VIEW stg_dispatch AS
SELECT
    CAST(salida_id AS VARCHAR) AS salida_id,
    CAST(vehiculo_id AS VARCHAR) AS vehiculo_id,
    CAST(fecha_salida_planificada AS TIMESTAMP) AS fecha_salida_planificada,
    CAST(fecha_salida_real AS TIMESTAMP) AS fecha_salida_real,
    CAST(modo_salida AS VARCHAR) AS modo_salida,
    CAST(transportista_proxy AS VARCHAR) AS transportista_proxy,
    CASE WHEN CAST(readiness_salida_flag AS INTEGER) = 1 THEN TRUE ELSE FALSE END AS readiness_salida_flag,
    CAST(retraso_min AS DOUBLE) AS retraso_min,
    CAST(causa_retraso AS VARCHAR) AS causa_retraso
FROM logistica_salida;

CREATE OR REPLACE VIEW stg_transition_scenarios AS
SELECT
    CAST(fecha AS DATE) AS fecha,
    CAST(escenario AS VARCHAR) AS escenario,
    CAST(share_ev AS DOUBLE) AS share_ev,
    CAST(intensidad_ramp_up AS DOUBLE) AS intensidad_ramp_up,
    CAST(disponibilidad_slots_carga AS DOUBLE) AS disponibilidad_slots_carga,
    CAST(presion_patio_indice AS DOUBLE) AS presion_patio_indice,
    CAST(restriccion_logistica_indice AS DOUBLE) AS restriccion_logistica_indice
FROM escenarios_transicion;
