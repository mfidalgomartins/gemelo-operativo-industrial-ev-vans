-- DuckDB SQL
-- Integración por área-turno para cuello de botella y presión operativa

CREATE OR REPLACE VIEW vw_shift_bottleneck_summary AS
WITH bottleneck_shift AS (
    SELECT
        CAST(b.timestamp AS DATE) AS fecha,
        CASE
            WHEN EXTRACT('hour' FROM b.timestamp) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM b.timestamp) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END AS turno,
        b.area,
        AVG(b.severidad) AS severidad_media,
        SUM(b.duracion_min) AS duracion_total_min,
        AVG(b.impacto_throughput_proxy) AS impacto_throughput_medio,
        AVG(b.impacto_salida_proxy) AS impacto_salida_medio,
        COUNT(b.evento_id) AS eventos_cuello,
        SUM(b.impacto_throughput_proxy) AS impacto_throughput_total,
        SUM(b.impacto_salida_proxy) AS impacto_salida_total
    FROM stg_bottlenecks b
    GROUP BY
        CAST(b.timestamp AS DATE),
        CASE
            WHEN EXTRACT('hour' FROM b.timestamp) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM b.timestamp) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END,
        b.area
),
dominant_cause AS (
    SELECT
        base.fecha,
        base.turno,
        base.area,
        MAX_BY(base.causa_probable, base.cnt) AS causa_dominante
    FROM (
        SELECT
            CAST(b.timestamp AS DATE) AS fecha,
            CASE
                WHEN EXTRACT('hour' FROM b.timestamp) BETWEEN 6 AND 13 THEN 'A'
                WHEN EXTRACT('hour' FROM b.timestamp) BETWEEN 14 AND 21 THEN 'B'
                ELSE 'C'
            END AS turno,
            b.area,
            b.causa_probable,
            COUNT(*) AS cnt
        FROM stg_bottlenecks b
        GROUP BY
            CAST(b.timestamp AS DATE),
            CASE
                WHEN EXTRACT('hour' FROM b.timestamp) BETWEEN 6 AND 13 THEN 'A'
                WHEN EXTRACT('hour' FROM b.timestamp) BETWEEN 14 AND 21 THEN 'B'
                ELSE 'C'
            END,
            b.area,
            b.causa_probable
    ) base
    GROUP BY base.fecha, base.turno, base.area
),
resource_stress AS (
    SELECT
        r.area,
        AVG(r.capacidad_disponible / NULLIF(r.capacidad_nominal, 0.0)) AS ratio_capacidad_disponible,
        AVG(CASE WHEN r.restriccion_actual_flag THEN 1.0 ELSE 0.0 END) AS ratio_restriccion_activa
    FROM stg_operational_resources r
    GROUP BY r.area
)
SELECT
    bs.fecha,
    bs.turno,
    bs.area,
    bs.eventos_cuello,
    bs.severidad_media,
    bs.duracion_total_min,
    bs.impacto_throughput_medio,
    bs.impacto_salida_medio,
    bs.impacto_throughput_total,
    bs.impacto_salida_total,
    t.productividad_turno_indice,
    t.presion_operativa_indice,
    t.absentismo_proxy,
    t.overtime_flag,
    rs.ratio_capacidad_disponible,
    rs.ratio_restriccion_activa,
    dc.causa_dominante,
    (
        0.35 * LEAST(1.0, bs.severidad_media / 5.0)
        + 0.25 * LEAST(1.0, bs.impacto_throughput_medio / 20.0)
        + 0.20 * LEAST(1.0, bs.impacto_salida_medio / 20.0)
        + 0.20 * LEAST(1.0, t.presion_operativa_indice / 100.0)
    ) * 100.0 AS area_stress_score
FROM bottleneck_shift bs
LEFT JOIN stg_turnos t
    ON bs.fecha = t.fecha
   AND bs.turno = t.turno
LEFT JOIN resource_stress rs
    ON UPPER(bs.area) = UPPER(rs.area)
LEFT JOIN dominant_cause dc
    ON bs.fecha = dc.fecha
   AND bs.turno = dc.turno
   AND bs.area = dc.area;
