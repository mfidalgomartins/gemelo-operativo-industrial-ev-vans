-- DuckDB SQL
-- Integración principal de flujo de vehículo y vistas operativas críticas

CREATE OR REPLACE VIEW vw_vehicle_flow_timeline AS
WITH charge_agg AS (
    SELECT
        cs.vehiculo_id,
        MIN(cs.inicio_sesion) AS inicio_carga_real,
        MAX(cs.fin_sesion) AS fin_carga_real,
        SUM(cs.energia_entregada_kwh) AS energia_total_carga_kwh,
        AVG(cs.tiempo_espera_previo_min) AS espera_media_pre_carga_min,
        SUM(CASE WHEN cs.carga_interrumpida_flag THEN 1 ELSE 0 END) AS sesiones_interrumpidas
    FROM stg_charge_sessions cs
    GROUP BY cs.vehiculo_id
),
yard_agg AS (
    SELECT
        ys.vehiculo_id,
        MIN(ys.timestamp) AS entrada_patio_real,
        MAX(ys.timestamp) AS ultimo_evento_patio,
        MAX(ys.dwell_time_min) AS dwell_patio_max_min,
        AVG(ys.dwell_time_min) AS dwell_patio_avg_min,
        AVG(CASE WHEN ys.blocking_flag THEN 1.0 ELSE 0.0 END) AS exposure_blocking,
        SUM(CASE WHEN ys.requiere_movimiento_flag THEN 1 ELSE 0 END) AS snapshots_requiere_mov
    FROM stg_yard_snapshots ys
    GROUP BY ys.vehiculo_id
),
move_agg AS (
    SELECT
        ym.vehiculo_id,
        COUNT(*) AS total_movimientos_patio,
        SUM(CASE WHEN ym.movimiento_no_productivo_flag THEN 1 ELSE 0 END) AS movimientos_no_productivos,
        SUM(ym.duracion_movimiento_min) AS duracion_movimientos_min
    FROM stg_yard_movements ym
    GROUP BY ym.vehiculo_id
),
battery_last AS (
    SELECT
        b.vehiculo_id,
        FIRST(b.soc_pct ORDER BY b.timestamp DESC) AS soc_final_pct,
        FIRST(b.target_soc_pct ORDER BY b.timestamp DESC) AS target_soc_final_pct,
        FIRST(b.timestamp ORDER BY b.timestamp DESC) AS timestamp_ultimo_soc
    FROM stg_battery_status b
    GROUP BY b.vehiculo_id
)
SELECT
    o.orden_id,
    v.vehiculo_id,
    v.vin_proxy,
    o.turno,
    o.fecha_programada,
    o.fecha_real,
    o.secuencia_planeada,
    o.secuencia_real,
    (o.secuencia_real - o.secuencia_planeada) AS planned_to_actual_sequence_gap,
    o.prioridad_cliente,
    o.mercado_destino,
    vr.version_id,
    vr.familia_modelo,
    vr.tipo_propulsion,
    vr.complejidad_montaje,
    vr.requiere_carga_salida_flag,
    v.timestamp_fin_linea,
    COALESCE(y.entrada_patio_real, v.timestamp_entrada_patio) AS timestamp_entrada_patio,
    COALESCE(c.inicio_carga_real, v.timestamp_inicio_carga) AS timestamp_inicio_carga,
    COALESCE(c.fin_carga_real, v.timestamp_fin_carga) AS timestamp_fin_carga,
    v.timestamp_salida,
    d.fecha_salida_planificada,
    d.fecha_salida_real,
    d.retraso_min,
    d.causa_retraso,
    d.readiness_salida_flag,
    COALESCE(b.soc_final_pct, v.nivel_bateria_salida) AS soc_salida_pct,
    COALESCE(b.target_soc_final_pct, 80.0) AS target_soc_salida_pct,
    COALESCE(c.energia_total_carga_kwh, 0.0) AS energia_total_carga_kwh,
    COALESCE(c.espera_media_pre_carga_min, 0.0) AS charging_wait_time_min,
    COALESCE(c.sesiones_interrumpidas, 0) AS sesiones_carga_interrumpidas,
    COALESCE(y.dwell_patio_avg_min, 0.0) AS yard_wait_time_min,
    COALESCE(y.dwell_patio_max_min, 0.0) AS yard_dwell_max_min,
    COALESCE(y.exposure_blocking, 0.0) AS blocking_exposure,
    COALESCE(m.movimientos_no_productivos, 0) AS non_productive_moves_count,
    COALESCE(m.total_movimientos_patio, 0) AS total_yard_moves,
    COALESCE(m.duracion_movimientos_min, 0.0) AS yard_movement_time_min,
    CASE
        WHEN v.timestamp_salida IS NOT NULL THEN DATEDIFF('minute', v.timestamp_fin_linea, v.timestamp_salida)
        WHEN d.fecha_salida_real IS NOT NULL THEN DATEDIFF('minute', v.timestamp_fin_linea, d.fecha_salida_real)
        ELSE NULL
    END AS total_internal_lead_time_min,
    CASE
        WHEN COALESCE(c.fin_carga_real, v.timestamp_fin_carga) IS NOT NULL
             AND COALESCE(c.inicio_carga_real, v.timestamp_inicio_carga) IS NOT NULL
        THEN DATEDIFF('minute', COALESCE(c.inicio_carga_real, v.timestamp_inicio_carga), COALESCE(c.fin_carga_real, v.timestamp_fin_carga))
        ELSE 0
    END AS charging_duration_min,
    CASE
        WHEN d.fecha_salida_real IS NOT NULL AND d.fecha_salida_planificada IS NOT NULL
        THEN DATEDIFF('minute', d.fecha_salida_planificada, d.fecha_salida_real)
        ELSE d.retraso_min
    END AS dispatch_delay_min,
    COALESCE(b.target_soc_final_pct, 80.0) - COALESCE(b.soc_final_pct, v.nivel_bateria_salida) AS soc_gap_before_dispatch,
    CASE
        WHEN d.readiness_salida_flag THEN TRUE
        WHEN o.ready_for_dispatch_flag THEN TRUE
        ELSE FALSE
    END AS readiness_final_flag,
    CASE
        WHEN vr.requiere_carga_salida_flag AND COALESCE(c.energia_total_carga_kwh, 0.0) <= 0 THEN TRUE
        ELSE FALSE
    END AS requires_charge_but_missing
FROM stg_orders o
INNER JOIN stg_vehicles v
    ON o.vehiculo_id = v.vehiculo_id
INNER JOIN stg_versions vr
    ON o.version_id = vr.version_id
LEFT JOIN stg_dispatch d
    ON v.vehiculo_id = d.vehiculo_id
LEFT JOIN charge_agg c
    ON v.vehiculo_id = c.vehiculo_id
LEFT JOIN yard_agg y
    ON v.vehiculo_id = y.vehiculo_id
LEFT JOIN move_agg m
    ON v.vehiculo_id = m.vehiculo_id
LEFT JOIN battery_last b
    ON v.vehiculo_id = b.vehiculo_id;

CREATE OR REPLACE VIEW vw_charging_utilization AS
WITH sessions AS (
    SELECT
        CAST(cs.inicio_sesion AS DATE) AS fecha,
        CASE
            WHEN EXTRACT('hour' FROM cs.inicio_sesion) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM cs.inicio_sesion) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END AS turno,
        sl.zona_carga,
        cs.slot_id,
        COUNT(cs.sesion_id) AS sessions_count,
        AVG(cs.tiempo_espera_previo_min) AS avg_wait_time_min,
        AVG(cs.duracion_sesion_min) AS avg_charging_duration_min,
        SUM(cs.energia_entregada_kwh) AS energy_delivered_kwh,
        AVG(CASE WHEN cs.carga_interrumpida_flag THEN 1.0 ELSE 0.0 END) AS interruption_rate
    FROM stg_charge_sessions cs
    INNER JOIN stg_charge_slots sl
        ON cs.slot_id = sl.slot_id
    GROUP BY
        CAST(cs.inicio_sesion AS DATE),
        CASE
            WHEN EXTRACT('hour' FROM cs.inicio_sesion) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM cs.inicio_sesion) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END,
        sl.zona_carga,
        cs.slot_id
),
soc_gap AS (
    SELECT
        CAST(timestamp AS DATE) AS fecha,
        AVG(target_soc_pct - soc_pct) AS avg_soc_gap_pct
    FROM stg_battery_status
    GROUP BY CAST(timestamp AS DATE)
),
bneck AS (
    SELECT
        CAST(timestamp AS DATE) AS fecha,
        AVG(impacto_throughput_proxy) AS avg_bottleneck_impact
    FROM stg_bottlenecks
    WHERE UPPER(area) IN ('CARGA', 'ENERGIA')
    GROUP BY CAST(timestamp AS DATE)
)
SELECT
    s.fecha,
    s.turno,
    s.zona_carga,
    s.slot_id,
    s.sessions_count,
    s.avg_wait_time_min,
    s.avg_charging_duration_min,
    s.energy_delivered_kwh,
    s.interruption_rate,
    sg.avg_soc_gap_pct,
    COALESCE(b.avg_bottleneck_impact, 0.0) AS charging_bottleneck_impact,
    (s.sessions_count * s.avg_charging_duration_min) / 480.0 AS slot_utilization_ratio
FROM sessions s
LEFT JOIN soc_gap sg
    ON s.fecha = sg.fecha
LEFT JOIN bneck b
    ON s.fecha = b.fecha;

CREATE OR REPLACE VIEW vw_yard_congestion AS
WITH yard_hour AS (
    SELECT
        CAST(DATE_TRUNC('hour', ys.timestamp) AS TIMESTAMP) AS ts_hour,
        ys.zona_patio,
        COUNT(DISTINCT ys.vehiculo_id) AS occupancy_units,
        AVG(ys.dwell_time_min) AS avg_dwell_time_min,
        QUANTILE_CONT(ys.dwell_time_min, 0.95) AS p95_dwell_time_min,
        AVG(CASE WHEN ys.blocking_flag THEN 1.0 ELSE 0.0 END) AS blocking_rate,
        AVG(CASE WHEN ys.requiere_movimiento_flag THEN 1.0 ELSE 0.0 END) AS movement_required_rate
    FROM stg_yard_snapshots ys
    GROUP BY CAST(DATE_TRUNC('hour', ys.timestamp) AS TIMESTAMP), ys.zona_patio
),
move_hour AS (
    SELECT
        CAST(DATE_TRUNC('hour', ym.timestamp_inicio) AS TIMESTAMP) AS ts_hour,
        ym.destino AS zona_patio,
        COUNT(ym.movimiento_id) AS moves_count,
        AVG(CASE WHEN ym.movimiento_no_productivo_flag THEN 1.0 ELSE 0.0 END) AS non_productive_move_rate
    FROM stg_yard_movements ym
    GROUP BY CAST(DATE_TRUNC('hour', ym.timestamp_inicio) AS TIMESTAMP), ym.destino
),
zone_capacity AS (
    SELECT
        y.zona_patio,
        GREATEST(50.0, QUANTILE_CONT(y.occupancy_units, 0.98) * 1.15) AS estimated_capacity_units
    FROM yard_hour y
    GROUP BY y.zona_patio
)
SELECT
    y.ts_hour,
    y.zona_patio,
    y.occupancy_units,
    z.estimated_capacity_units,
    y.occupancy_units / NULLIF(z.estimated_capacity_units, 0.0) AS yard_occupancy_rate,
    y.avg_dwell_time_min,
    y.p95_dwell_time_min,
    y.blocking_rate,
    COALESCE(m.moves_count, 0) AS movement_density,
    COALESCE(m.non_productive_move_rate, 0.0) AS non_productive_move_rate,
    y.movement_required_rate,
    (
        0.35 * LEAST(1.5, y.occupancy_units / NULLIF(z.estimated_capacity_units, 0.0))
        + 0.25 * LEAST(1.0, y.blocking_rate * 2.0)
        + 0.20 * LEAST(1.0, COALESCE(m.non_productive_move_rate, 0.0) * 2.0)
        + 0.20 * LEAST(1.0, y.p95_dwell_time_min / 240.0)
    ) * 100.0 AS operational_risk_score
FROM yard_hour y
INNER JOIN zone_capacity z
    ON y.zona_patio = z.zona_patio
LEFT JOIN move_hour m
    ON y.ts_hour = m.ts_hour
   AND y.zona_patio = m.zona_patio;
