CREATE OR REPLACE TABLE scores_operativos AS
WITH base AS (
    SELECT
        f.*,
        CASE
            WHEN prioridad_cliente = 1 THEN 1.0
            WHEN prioridad_cliente = 2 THEN 0.6
            ELSE 0.3
        END AS prioridad_norm
    FROM features_operativas f
),
scored AS (
    SELECT
        b.*,
        LEAST(
            100.0,
            GREATEST(
                0.0,
                72.0
                - LEAST(42.0, b.espera_total_min / 16.0)
                - 24.0 * b.indice_congestion_patio
                - 18.0 * b.utilizacion_cargadores_turno
                - CASE
                    WHEN b.buffer_energia_kwh < 0 THEN 12.0
                    WHEN b.buffer_energia_kwh < 250 THEN 5.0
                    ELSE 0.0
                  END
                + CASE
                    WHEN b.slack_horas_sla > 8 THEN 10.0
                    WHEN b.slack_horas_sla > 0 THEN 4.0
                    WHEN b.slack_horas_sla > -4 THEN -6.0
                    ELSE -14.0
                  END
            )
        ) AS score_readiness_operativa,
        LEAST(
            100.0,
            GREATEST(
                0.0,
                18.0
                + 34.0 * b.indice_congestion_patio
                + 28.0 * b.utilizacion_cargadores_turno
                + CASE
                    WHEN b.buffer_energia_kwh < 0 THEN 15.0
                    WHEN b.buffer_energia_kwh < 250 THEN 8.0
                    ELSE 0.0
                  END
                + 14.0 * LEAST(1.0, b.espera_total_min / 480.0)
                + CASE
                    WHEN b.slack_horas_sla < 0 THEN 18.0
                    WHEN b.slack_horas_sla < 4 THEN 7.0
                    ELSE 0.0
                  END
            )
        ) AS score_riesgo_cuello_botella
    FROM base b
)
SELECT
    s.*,
    LEAST(
        100.0,
        GREATEST(
            0.0,
            0.45 * s.score_riesgo_cuello_botella
            + 0.35 * (100.0 - s.score_readiness_operativa)
            + 0.20 * (s.prioridad_norm * 100.0)
        )
    ) AS score_prioridad_despacho,
    CASE
        WHEN s.score_riesgo_cuello_botella >= 75 THEN 'Activar control tower de turno y re-secuenciación EV inmediata.'
        WHEN s.utilizacion_cargadores_turno >= 0.82 THEN 'Asignar slot de carga prioritaria y balancear demanda entre turnos.'
        WHEN s.indice_congestion_patio >= 0.85 THEN 'Aplicar drenaje de patio y priorizar expedición de unidades listas.'
        WHEN s.buffer_energia_kwh < 0 THEN 'Reducir simultaneidad de carga y coordinar ventana energética.'
        ELSE 'Mantener secuencia planificada con seguimiento estándar.'
    END AS recomendacion_operativa
FROM scored s;

CREATE OR REPLACE TABLE recomendaciones_operativas AS
SELECT
    turno_referencia,
    tipo_propulsion,
    COUNT(*) AS unidades,
    AVG(score_readiness_operativa) AS readiness_media,
    AVG(score_riesgo_cuello_botella) AS riesgo_medio,
    AVG(score_prioridad_despacho) AS prioridad_media,
    MAX(recomendacion_operativa) AS recomendacion_dominante
FROM scores_operativos
GROUP BY turno_referencia, tipo_propulsion
ORDER BY riesgo_medio DESC;
