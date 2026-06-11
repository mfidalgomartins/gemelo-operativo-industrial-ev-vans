-- DuckDB SQL
-- Vista y mart de readiness de expedición

CREATE OR REPLACE VIEW vw_dispatch_readiness AS
WITH base AS (
    SELECT
        CAST(vft.fecha_salida_planificada AS DATE) AS fecha,
        CASE
            WHEN EXTRACT('hour' FROM vft.fecha_salida_planificada) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM vft.fecha_salida_planificada) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END AS turno,
        vft.turno AS turno_produccion,
        vft.orden_id,
        vft.vehiculo_id,
        vft.version_id,
        vft.familia_modelo,
        vft.tipo_propulsion,
        vft.mercado_destino,
        vft.readiness_final_flag,
        vft.fecha_salida_real IS NOT NULL AS departed_flag,
        vft.dispatch_delay_min,
        vft.causa_retraso,
        vft.soc_salida_pct,
        vft.target_soc_salida_pct,
        vft.soc_gap_before_dispatch,
        vft.requires_charge_but_missing,
        vft.charging_wait_time_min,
        vft.yard_wait_time_min,
        vft.blocking_exposure,
        vft.non_productive_moves_count,
        CASE WHEN vft.fecha_salida_real IS NOT NULL AND vft.dispatch_delay_min > 120 THEN TRUE ELSE FALSE END AS delayed_flag,
        CASE
            WHEN vft.requires_charge_but_missing THEN 'FALTA_CARGA'
            WHEN vft.soc_gap_before_dispatch > 10 THEN 'SOC_INSUFICIENTE'
            WHEN vft.blocking_exposure > 0.25 THEN 'BLOQUEO_PATIO'
            WHEN vft.dispatch_delay_min > 0 THEN COALESCE(vft.causa_retraso, 'RETRASO_NO_CLASIFICADO')
            ELSE 'OK'
        END AS readiness_gap_driver
    FROM vw_vehicle_flow_timeline vft
)
SELECT
    fecha,
    turno,
    turno_produccion,
    orden_id,
    vehiculo_id,
    version_id,
    familia_modelo,
    tipo_propulsion,
    mercado_destino,
    readiness_final_flag,
    departed_flag,
    delayed_flag,
    dispatch_delay_min,
    causa_retraso,
    soc_salida_pct,
    target_soc_salida_pct,
    soc_gap_before_dispatch,
    requires_charge_but_missing,
    charging_wait_time_min,
    yard_wait_time_min,
    blocking_exposure,
    non_productive_moves_count,
    readiness_gap_driver,
    (
        0.30 * LEAST(1.0, GREATEST(soc_gap_before_dispatch, 0.0) / 30.0)
        + 0.25 * LEAST(1.0, GREATEST(dispatch_delay_min, 0.0) / 240.0)
        + 0.20 * LEAST(1.0, charging_wait_time_min / 180.0)
        + 0.15 * LEAST(1.0, yard_wait_time_min / 240.0)
        + 0.10 * LEAST(1.0, blocking_exposure)
    ) * 100.0 AS dispatch_readiness_risk_score
FROM base;

CREATE OR REPLACE TABLE mart_dispatch_readiness AS
SELECT
    fecha,
    turno,
    tipo_propulsion,
    version_id,
    COUNT(vehiculo_id) AS total_vehiculos,
    SUM(CASE WHEN departed_flag THEN 1 ELSE 0 END) AS total_despachados,
    AVG(CASE WHEN readiness_final_flag THEN 1.0 ELSE 0.0 END) AS readiness_rate,
    AVG(CASE WHEN departed_flag THEN CASE WHEN delayed_flag THEN 1.0 ELSE 0.0 END ELSE NULL END) AS delay_rate,
    AVG(dispatch_delay_min) AS avg_dispatch_delay_min,
    AVG(soc_gap_before_dispatch) AS avg_soc_gap_before_dispatch,
    AVG(dispatch_readiness_risk_score) AS avg_dispatch_readiness_risk_score,
    SUM(CASE WHEN readiness_gap_driver = 'FALTA_CARGA' THEN 1 ELSE 0 END) AS eventos_falta_carga,
    SUM(CASE WHEN readiness_gap_driver = 'SOC_INSUFICIENTE' THEN 1 ELSE 0 END) AS eventos_soc_insuficiente,
    SUM(CASE WHEN readiness_gap_driver = 'BLOQUEO_PATIO' THEN 1 ELSE 0 END) AS eventos_bloqueo_patio
FROM vw_dispatch_readiness
GROUP BY
    fecha,
    turno,
    tipo_propulsion,
    version_id;
