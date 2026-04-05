-- DuckDB SQL
-- KPI queries materializadas para consumo ejecutivo

CREATE OR REPLACE TABLE kpi_operativos AS
WITH throughput AS (
    SELECT
        COUNT(*) AS total_ordenes,
        SUM(CASE WHEN fecha_programada IS NOT NULL THEN 1 ELSE 0 END) AS throughput_planificado,
        SUM(CASE WHEN fecha_real IS NOT NULL THEN 1 ELSE 0 END) AS throughput_real
    FROM stg_orders
),
ev_mix AS (
    SELECT
        AVG(CASE WHEN tipo_propulsion = 'EV' THEN 1.0 ELSE 0.0 END) AS share_ev
    FROM vw_vehicle_flow_timeline
),
yard AS (
    SELECT
        AVG(yard_wait_time_min) AS tiempo_medio_patio_min,
        QUANTILE_CONT(yard_dwell_max_min, 0.95) AS dwell_p95_min,
        AVG(yard_wait_time_min + charging_wait_time_min) AS tiempo_medio_hasta_readiness_min
    FROM vw_vehicle_flow_timeline
),
yard_occ AS (
    SELECT
        AVG(yard_occupancy_rate) AS ocupacion_media_patio,
        MAX(yard_occupancy_rate) AS ocupacion_pico_patio
    FROM vw_yard_congestion
),
charging AS (
    SELECT
        AVG(slot_utilization_ratio) AS utilizacion_media_cargadores,
        AVG(avg_wait_time_min) AS tiempo_medio_espera_carga_min
    FROM vw_charging_utilization
),
dispatch AS (
    SELECT
        AVG(CASE WHEN delayed_flag THEN 1.0 ELSE 0.0 END) AS ratio_salida_retrasada,
        SUM(CASE WHEN readiness_final_flag THEN 0 ELSE 1 END) AS vehiculos_no_ready,
        AVG(dispatch_readiness_risk_score) AS score_readiness_global
    FROM vw_dispatch_readiness
),
bneck AS (
    SELECT
        MAX_BY(causa_dominante, eventos_cuello) AS causa_principal_cuello,
        MAX_BY(area, impacto_throughput_total) AS area_mayor_perdida_throughput,
        SUM(impacto_throughput_total) AS perdida_throughput_total
    FROM vw_shift_bottleneck_summary
),
critical AS (
    SELECT
        COUNT(DISTINCT area) AS areas_criticas
    FROM mart_area_shift
    WHERE operational_stress_score >= 70
)
SELECT
    t.total_ordenes,
    t.throughput_planificado,
    t.throughput_real,
    (t.throughput_real - t.throughput_planificado) AS throughput_gap,
    e.share_ev,
    y.tiempo_medio_patio_min,
    y.dwell_p95_min,
    yo.ocupacion_media_patio,
    yo.ocupacion_pico_patio,
    c.utilizacion_media_cargadores,
    c.tiempo_medio_espera_carga_min,
    d.vehiculos_no_ready,
    d.ratio_salida_retrasada,
    b.causa_principal_cuello,
    b.area_mayor_perdida_throughput,
    b.perdida_throughput_total,
    d.score_readiness_global,
    y.tiempo_medio_hasta_readiness_min,
    cr.areas_criticas
FROM throughput t
CROSS JOIN ev_mix e
CROSS JOIN yard y
CROSS JOIN yard_occ yo
CROSS JOIN charging c
CROSS JOIN dispatch d
CROSS JOIN bneck b
CROSS JOIN critical cr;

CREATE OR REPLACE TABLE kpi_readiness_shift_version AS
SELECT
    dr.turno,
    dr.version_id,
    dr.tipo_propulsion,
    COUNT(dr.vehiculo_id) AS total_vehiculos,
    AVG(CASE WHEN dr.readiness_final_flag THEN 1.0 ELSE 0.0 END) AS readiness_rate,
    AVG(dr.dispatch_readiness_risk_score) AS readiness_risk_score,
    AVG(dr.dispatch_delay_min) AS avg_dispatch_delay_min
FROM vw_dispatch_readiness dr
GROUP BY dr.turno, dr.version_id, dr.tipo_propulsion
ORDER BY readiness_risk_score DESC;
