CREATE OR REPLACE TABLE features_operativas AS
WITH util_turno AS (
    SELECT
        fecha,
        turno,
        AVG(utilizacion_cargadores_turno) AS utilizacion_cargadores_turno
    FROM fct_carga_operativa
    GROUP BY fecha, turno
),
energy_hour AS (
    SELECT
        ts_hora,
        (capacidad_kw_disponible - demanda_kw) AS buffer_energia_kwh,
        curtailment_flag
    FROM stg_disponibilidad_energia
),
base AS (
    SELECT
        f.order_id,
        f.tipo_propulsion,
        f.destino,
        f.prioridad_cliente,
        DATE(f.ts_salida_real) AS fecha_referencia,
        CASE
            WHEN EXTRACT('hour' FROM f.ts_salida_real) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM f.ts_salida_real) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END AS turno_referencia,
        f.ts_salida_real,
        COALESCE(f.espera_patio_min, 0) AS espera_patio_min,
        COALESCE(f.espera_carga_min, 0) AS espera_carga_min,
        COALESCE(f.espera_salida_min, 0) AS espera_salida_min,
        COALESCE(f.espera_patio_min, 0) + COALESCE(f.espera_carga_min, 0) + COALESCE(f.espera_salida_min, 0) AS espera_total_min,
        COALESCE(f.lead_time_total_horas, 0) AS lead_time_total_horas,
        COALESCE(f.ocupacion_sector_media_pct, 0) AS ocupacion_sector_media_pct,
        f.sla_horas,
        (f.sla_horas - (DATEDIFF('minute', f.ts_ready_expedicion, f.ts_salida_real) / 60.0)) AS slack_horas_sla,
        DATE_TRUNC('hour', COALESCE(f.ts_inicio_carga, f.ts_exit_patio, f.ts_salida_real)) AS ts_contexto
    FROM fct_flujo_unidad f
)
SELECT
    b.order_id,
    b.tipo_propulsion,
    b.destino,
    b.prioridad_cliente,
    b.fecha_referencia,
    b.turno_referencia,
    b.ts_salida_real,
    b.espera_patio_min,
    b.espera_carga_min,
    b.espera_salida_min,
    b.espera_total_min,
    b.lead_time_total_horas,
    COALESCE(u.utilizacion_cargadores_turno, 0) AS utilizacion_cargadores_turno,
    GREATEST(COALESCE(o.ocupacion_patio_pct, 0), COALESCE(b.ocupacion_sector_media_pct, 0)) / 100.0 AS indice_congestion_patio,
    COALESCE(e.buffer_energia_kwh, 0) AS buffer_energia_kwh,
    COALESCE(e.curtailment_flag, 0) AS curtailment_flag,
    b.slack_horas_sla,
    LEAST(
        100.0,
        GREATEST(
            0.0,
            25.0 * GREATEST(COALESCE(o.ocupacion_patio_pct, 0), COALESCE(b.ocupacion_sector_media_pct, 0)) / 100.0
            + 32.0 * COALESCE(u.utilizacion_cargadores_turno, 0)
            + 18.0 * CASE WHEN COALESCE(e.buffer_energia_kwh, 0) < 0 THEN 1 ELSE 0 END
            + 25.0 * LEAST(1.0, (b.espera_total_min / 720.0))
        )
    ) AS riesgo_bloqueo_salida
FROM base b
LEFT JOIN util_turno u
    ON b.fecha_referencia = u.fecha
   AND b.turno_referencia = u.turno
LEFT JOIN fct_ocupacion_patio_hora o
    ON b.ts_contexto = o.hora
LEFT JOIN energy_hour e
    ON b.ts_contexto = e.ts_hora;
