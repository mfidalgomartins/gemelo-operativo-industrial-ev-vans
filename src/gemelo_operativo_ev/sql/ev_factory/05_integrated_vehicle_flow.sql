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
battery_ranked AS (
    SELECT
        b.*,
        ROW_NUMBER() OVER (
            PARTITION BY b.vehiculo_id
            ORDER BY b.timestamp DESC NULLS LAST, b.soc_pct DESC NULLS LAST, b.target_soc_pct DESC NULLS LAST
        ) AS battery_rank
    FROM stg_battery_status b
),
dispatch_ranked AS (
    -- Regla de negocio: una línea de flujo por vehículo. Si aparecen varios
    -- registros de expedición, gana la última salida real; si no existe, la
    -- última salida planificada. El desempate por salida_id hace el join estable.
    SELECT
        d.*,
        ROW_NUMBER() OVER (
            PARTITION BY d.vehiculo_id
            ORDER BY d.fecha_salida_real DESC NULLS LAST, d.fecha_salida_planificada DESC NULLS LAST, d.salida_id ASC
        ) AS dispatch_rank,
        COUNT(*) OVER (PARTITION BY d.vehiculo_id) AS registros_dispatch_vehiculo
    FROM stg_dispatch d
),
battery_last AS (
    SELECT
        vehiculo_id,
        soc_pct AS soc_final_pct,
        target_soc_pct AS target_soc_final_pct,
        timestamp AS timestamp_ultimo_soc
    FROM battery_ranked
    WHERE battery_rank = 1
),
dispatch_one_row AS (
    SELECT
        vehiculo_id,
        salida_id,
        fecha_salida_planificada,
        fecha_salida_real,
        timestamp_readiness,
        modo_salida,
        transportista_proxy,
        readiness_salida_flag,
        retraso_min,
        causa_retraso,
        registros_dispatch_vehiculo
    FROM dispatch_ranked
    WHERE dispatch_rank = 1
)
SELECT
    o.orden_id,
    v.vehiculo_id,
    v.vin_proxy,
    o.turno,
    o.fecha_programada,
    o.fecha_real,
    o.fecha_turno_operativo,
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
    d.timestamp_readiness,
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
LEFT JOIN dispatch_one_row d
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
        ev_operational_date(cs.inicio_sesion) AS fecha,
        ev_operational_shift(cs.inicio_sesion) AS turno,
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
        ev_operational_date(cs.inicio_sesion),
        ev_operational_shift(cs.inicio_sesion),
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

CREATE OR REPLACE VIEW vw_yard_zone_capacity AS
WITH mapped_resources AS (
    SELECT
        CASE r.recurso_id
            WHEN 'REC_PATIO_NORTE' THEN 'NORTE'
            WHEN 'REC_PATIO_SUR' THEN 'SUR'
            WHEN 'REC_PATIO_ESTE' THEN 'ESTE'
            WHEN 'REC_PATIO_OESTE' THEN 'OESTE'
            WHEN 'REC_PATIO_BUFFER_CARGA' THEN 'BUFFER_CARGA'
            WHEN 'REC_PATIO_PRE_SALIDA' THEN 'PRE_SALIDA'
            ELSE NULL
        END AS zona_patio,
        r.capacidad_nominal,
        r.capacidad_disponible
    FROM stg_operational_resources r
    WHERE UPPER(r.tipo_recurso) = 'ESPACIO_PATIO'
),
capacity_by_zone AS (
    SELECT
        zona_patio,
        SUM(capacidad_nominal) AS physical_capacity_units,
        SUM(capacidad_disponible) AS available_capacity_units
    FROM mapped_resources
    WHERE zona_patio IS NOT NULL
    GROUP BY zona_patio
)
SELECT *
FROM capacity_by_zone;

CREATE OR REPLACE TABLE int_yard_vehicle_intervals AS
WITH observation_horizon AS (
    SELECT
        DATE_TRUNC('hour', MAX(timestamp)) + INTERVAL 1 HOUR AS observation_end
    FROM stg_yard_snapshots
),
canonical_snapshots AS (
    SELECT * EXCLUDE(snapshot_rank)
    FROM (
        SELECT
            ys.*,
            ROW_NUMBER() OVER (
                PARTITION BY ys.vehiculo_id, ys.timestamp
                ORDER BY
                    CASE ys.estado_en_patio
                        WHEN 'SALIDA' THEN 1
                        WHEN 'LISTO_EXPEDICION' THEN 2
                        WHEN 'POST_CARGA' THEN 3
                        WHEN 'ESPERA_CARGA' THEN 4
                        WHEN 'EN_ESPERA_SALIDA' THEN 5
                        ELSE 6
                    END,
                    ys.zona_patio
            ) AS snapshot_rank
        FROM stg_yard_snapshots ys
    ) ranked
    WHERE snapshot_rank = 1
),
sequenced AS (
    SELECT
        cs.*,
        LEAD(cs.timestamp) OVER (
            PARTITION BY cs.vehiculo_id
            ORDER BY cs.timestamp, cs.zona_patio
        ) AS next_snapshot_ts
    FROM canonical_snapshots cs
),
bounded AS (
    SELECT
        s.vehiculo_id,
        s.zona_patio,
        s.timestamp AS interval_start,
        LEAST(
            COALESCE(s.next_snapshot_ts, v.timestamp_salida, h.observation_end),
            COALESCE(v.timestamp_salida, h.observation_end)
        ) AS interval_end
    FROM sequenced s
    INNER JOIN stg_vehicles v
        ON s.vehiculo_id = v.vehiculo_id
    CROSS JOIN observation_horizon h
)
SELECT
    vehiculo_id,
    zona_patio,
    interval_start,
    interval_end
FROM bounded
WHERE interval_end > interval_start;

CREATE OR REPLACE VIEW vw_yard_vehicle_intervals AS
SELECT *
FROM int_yard_vehicle_intervals;

CREATE OR REPLACE TABLE int_yard_congestion AS
WITH zone_deltas AS (
    SELECT zona_patio, interval_start AS event_ts, 1 AS occupancy_delta
    FROM vw_yard_vehicle_intervals
    UNION ALL
    SELECT zona_patio, interval_end AS event_ts, -1 AS occupancy_delta
    FROM vw_yard_vehicle_intervals
),
aggregated_deltas AS (
    SELECT
        zona_patio,
        event_ts,
        SUM(occupancy_delta) AS occupancy_delta
    FROM zone_deltas
    GROUP BY zona_patio, event_ts
),
balance_points AS (
    SELECT
        zona_patio,
        event_ts,
        SUM(occupancy_delta) OVER (
            PARTITION BY zona_patio
            ORDER BY event_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS occupancy_after_event
    FROM aggregated_deltas
),
hour_bounds AS (
    SELECT
        DATE_TRUNC('hour', MIN(interval_start)) AS first_hour,
        DATE_TRUNC('hour', MAX(interval_end) - INTERVAL 1 MICROSECOND) AS last_hour
    FROM vw_yard_vehicle_intervals
),
hour_grid AS (
    SELECT
        CAST(hour_ts AS TIMESTAMP) AS ts_hour,
        c.zona_patio,
        c.physical_capacity_units,
        c.available_capacity_units
    FROM hour_bounds b
    CROSS JOIN vw_yard_zone_capacity c
    CROSS JOIN GENERATE_SERIES(b.first_hour, b.last_hour, INTERVAL 1 HOUR) hours(hour_ts)
),
hour_start_occupancy AS (
    SELECT
        g.ts_hour,
        g.zona_patio,
        g.physical_capacity_units,
        g.available_capacity_units,
        COALESCE(bp.occupancy_after_event, 0) AS occupancy_at_hour_start
    FROM hour_grid g
    ASOF LEFT JOIN balance_points bp
        ON g.zona_patio = bp.zona_patio
       AND g.ts_hour >= bp.event_ts
),
hour_event_peak AS (
    SELECT
        CAST(DATE_TRUNC('hour', event_ts) AS TIMESTAMP) AS ts_hour,
        zona_patio,
        MAX(occupancy_after_event) AS occupancy_event_peak
    FROM balance_points
    GROUP BY CAST(DATE_TRUNC('hour', event_ts) AS TIMESTAMP), zona_patio
),
yard_event_hour AS (
    SELECT
        CAST(DATE_TRUNC('hour', ys.timestamp) AS TIMESTAMP) AS ts_hour,
        ys.zona_patio,
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
hourly_state AS (
    SELECT
        h.ts_hour,
        h.zona_patio,
        GREATEST(h.occupancy_at_hour_start, COALESCE(p.occupancy_event_peak, 0)) AS occupancy_units,
        h.physical_capacity_units,
        h.available_capacity_units
    FROM hour_start_occupancy h
    LEFT JOIN hour_event_peak p
        ON h.ts_hour = p.ts_hour
       AND h.zona_patio = p.zona_patio
)
SELECT
    h.ts_hour,
    h.zona_patio,
    h.occupancy_units,
    h.physical_capacity_units,
    h.available_capacity_units,
    h.occupancy_units / NULLIF(h.physical_capacity_units, 0.0) AS yard_occupancy_rate,
    e.avg_dwell_time_min,
    e.p95_dwell_time_min,
    e.blocking_rate,
    COALESCE(m.moves_count, 0) AS movement_density,
    COALESCE(m.non_productive_move_rate, 0.0) AS non_productive_move_rate,
    e.movement_required_rate,
    (
        0.35 * LEAST(1.5, h.occupancy_units / NULLIF(h.physical_capacity_units, 0.0))
        + 0.25 * LEAST(1.0, COALESCE(e.blocking_rate, 0.0) * 2.0)
        + 0.20 * LEAST(1.0, COALESCE(m.non_productive_move_rate, 0.0) * 2.0)
        + 0.20 * LEAST(1.0, COALESCE(e.p95_dwell_time_min, 0.0) / 240.0)
    ) * 100.0 AS operational_risk_score
FROM hourly_state h
LEFT JOIN yard_event_hour e
    ON h.ts_hour = e.ts_hour
   AND h.zona_patio = e.zona_patio
LEFT JOIN move_hour m
    ON h.ts_hour = m.ts_hour
   AND h.zona_patio = m.zona_patio;

CREATE OR REPLACE VIEW vw_yard_congestion AS
SELECT *
FROM int_yard_congestion;
