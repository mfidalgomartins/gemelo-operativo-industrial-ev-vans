-- DuckDB SQL
-- Mart analítico área-turno

CREATE OR REPLACE TABLE mart_area_shift AS
WITH turnos_base AS (
    SELECT
        t.fecha,
        t.turno,
        t.headcount_proxy,
        t.absentismo_proxy,
        t.productividad_turno_indice,
        t.presion_operativa_indice,
        t.overtime_flag
    FROM stg_turnos t
),
areas AS (
    SELECT DISTINCT UPPER(area) AS area FROM stg_bottlenecks
    UNION ALL SELECT 'PATIO' AS area
    UNION ALL SELECT 'CARGA' AS area
    UNION ALL SELECT 'EXPEDICION' AS area
),
base AS (
    SELECT
        tb.fecha,
        tb.turno,
        a.area,
        tb.headcount_proxy,
        tb.absentismo_proxy,
        tb.productividad_turno_indice,
        tb.presion_operativa_indice,
        tb.overtime_flag
    FROM turnos_base tb
    CROSS JOIN areas a
),
planned AS (
    SELECT
        o.fecha_programada AS fecha,
        o.turno,
        COUNT(o.orden_id) AS throughput_plan
    FROM stg_orders o
    GROUP BY o.fecha_programada, o.turno
),
actual AS (
    SELECT
        CAST(d.fecha_salida_real AS DATE) AS fecha,
        CASE
            WHEN EXTRACT('hour' FROM d.fecha_salida_real) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM d.fecha_salida_real) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END AS turno,
        COUNT(d.salida_id) AS throughput_real
    FROM stg_dispatch d
    WHERE d.fecha_salida_real IS NOT NULL
        GROUP BY
        CAST(d.fecha_salida_real AS DATE),
        CASE
            WHEN EXTRACT('hour' FROM d.fecha_salida_real) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM d.fecha_salida_real) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END
),
yard_shift AS (
    SELECT
        CAST(y.ts_hour AS DATE) AS fecha,
        CASE
            WHEN EXTRACT('hour' FROM y.ts_hour) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM y.ts_hour) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END AS turno,
        AVG(y.yard_occupancy_rate) AS yard_occupancy_rate,
        AVG(y.operational_risk_score) AS congestion_index,
        AVG(y.avg_dwell_time_min) AS avg_wait_time,
        AVG(y.blocking_rate) AS blocking_rate
    FROM vw_yard_congestion y
    GROUP BY
        CAST(y.ts_hour AS DATE),
        CASE
            WHEN EXTRACT('hour' FROM y.ts_hour) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM y.ts_hour) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END
),
charging_shift AS (
    SELECT
        c.fecha,
        c.turno,
        AVG(c.slot_utilization_ratio) AS slot_utilization,
        AVG(c.avg_wait_time_min) AS avg_wait_to_charge,
        AVG(c.sessions_count) AS queue_pressure_score,
        AVG(c.interruption_rate) AS interruption_rate,
        AVG(c.charging_bottleneck_impact) AS charging_bottleneck_impact
    FROM vw_charging_utilization c
    GROUP BY c.fecha, c.turno
),
dispatch_shift AS (
    SELECT
        CAST(d.fecha_salida_real AS DATE) AS fecha,
        CASE
            WHEN EXTRACT('hour' FROM d.fecha_salida_real) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM d.fecha_salida_real) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END AS turno,
        AVG(CASE WHEN d.retraso_min > 0 THEN 1.0 ELSE 0.0 END) AS dispatch_risk_density,
        AVG(d.retraso_min) AS avg_dispatch_delay_min
    FROM stg_dispatch d
    WHERE d.fecha_salida_real IS NOT NULL
    GROUP BY
        CAST(d.fecha_salida_real AS DATE),
        CASE
            WHEN EXTRACT('hour' FROM d.fecha_salida_real) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM d.fecha_salida_real) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END
),
bneck_shift AS (
    SELECT
        bs.fecha,
        bs.turno,
        UPPER(bs.area) AS area,
        AVG(bs.severidad_media) AS severidad_media,
        AVG(bs.impacto_throughput_medio) AS impacto_throughput_medio,
        AVG(bs.impacto_salida_medio) AS impacto_salida_medio,
        AVG(bs.area_stress_score) AS bottleneck_density
    FROM vw_shift_bottleneck_summary bs
    GROUP BY bs.fecha, bs.turno, UPPER(bs.area)
)
SELECT
    b.fecha,
    b.turno,
    b.area,
    COALESCE(p.throughput_plan, 0) AS throughput_plan,
    COALESCE(a.throughput_real, 0) AS throughput_real,
    COALESCE(a.throughput_real, 0) - COALESCE(p.throughput_plan, 0) AS throughput_gap,
    CASE WHEN b.area = 'PATIO' THEN COALESCE(y.congestion_index, 0.0) ELSE COALESCE(bs.bottleneck_density, 0.0) END AS congestion_index,
    CASE WHEN b.area = 'PATIO' THEN COALESCE(y.avg_wait_time, 0.0)
         WHEN b.area = 'CARGA' THEN COALESCE(c.avg_wait_to_charge, 0.0)
         WHEN b.area = 'ENERGIA' THEN COALESCE(c.avg_wait_to_charge, 0.0) * 0.70
         ELSE COALESCE(d.avg_dispatch_delay_min, 0.0)
    END AS avg_wait_time,
    CASE
        WHEN b.area = 'CARGA' THEN COALESCE(c.queue_pressure_score, 0.0)
        WHEN b.area = 'ENERGIA' THEN COALESCE(c.queue_pressure_score, 0.0) * 0.65
        WHEN b.area IN ('EXPEDICION', 'LOGISTICA') THEN COALESCE(d.dispatch_risk_density, 0.0) * 100.0
        ELSE 0.0
    END AS queue_pressure_score,
    CASE
        WHEN b.area = 'CARGA' THEN COALESCE(c.slot_utilization, 0.0)
        WHEN b.area = 'ENERGIA' THEN COALESCE(c.slot_utilization, 0.0) * 0.70
        ELSE 0.0
    END AS slot_utilization,
    CASE
        WHEN b.area IN ('PATIO', 'LOGISTICA') THEN COALESCE(y.yard_occupancy_rate, 0.0)
        ELSE 0.0
    END AS yard_occupancy_rate,
    COALESCE(bs.bottleneck_density, 0.0) AS bottleneck_density,
    CASE
        WHEN b.area IN ('EXPEDICION', 'LOGISTICA') THEN COALESCE(d.dispatch_risk_density, 0.0)
        WHEN b.area = 'PRODUCCION' THEN COALESCE(d.dispatch_risk_density, 0.0) * 0.45
        ELSE 0.0
    END AS dispatch_risk_density,
    (
        0.25 * LEAST(1.0, ABS(COALESCE(a.throughput_real, 0) - COALESCE(p.throughput_plan, 0)) / 25.0)
        + 0.20 * LEAST(
            1.0,
            CASE
                WHEN b.area IN ('PATIO', 'LOGISTICA') THEN COALESCE(y.yard_occupancy_rate, 0.0)
                ELSE 0.0
            END
        )
        + 0.20 * LEAST(
            1.0,
            CASE
                WHEN b.area = 'CARGA' THEN COALESCE(c.slot_utilization, 0.0)
                WHEN b.area = 'ENERGIA' THEN COALESCE(c.slot_utilization, 0.0) * 0.70
                ELSE 0.0
            END
        )
        + 0.20 * LEAST(1.0, COALESCE(bs.bottleneck_density, 0.0) / 100.0)
        + 0.15 * LEAST(1.0, COALESCE(b.presion_operativa_indice, 0.0) / 100.0)
    ) * 100.0 AS operational_stress_score,
    b.headcount_proxy,
    b.absentismo_proxy,
    b.productividad_turno_indice,
    b.presion_operativa_indice,
    b.overtime_flag
FROM base b
LEFT JOIN planned p
    ON b.fecha = p.fecha
   AND b.turno = p.turno
LEFT JOIN actual a
    ON b.fecha = a.fecha
   AND b.turno = a.turno
LEFT JOIN yard_shift y
    ON b.fecha = y.fecha
   AND b.turno = y.turno
LEFT JOIN charging_shift c
    ON b.fecha = c.fecha
   AND b.turno = c.turno
LEFT JOIN dispatch_shift d
    ON b.fecha = d.fecha
   AND b.turno = d.turno
LEFT JOIN bneck_shift bs
    ON b.fecha = bs.fecha
   AND b.turno = bs.turno
   AND b.area = bs.area;
