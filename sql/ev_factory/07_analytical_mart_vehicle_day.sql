-- DuckDB SQL
-- Mart analítico vehículo-día

CREATE OR REPLACE TABLE mart_vehicle_day AS
SELECT
    CAST(vw.fecha_real AS DATE) AS fecha,
    vw.turno,
    vw.orden_id,
    vw.vehiculo_id,
    vw.version_id,
    vw.familia_modelo,
    vw.tipo_propulsion,
    vw.prioridad_cliente,
    vw.mercado_destino,
    vw.planned_to_actual_sequence_gap,
    vw.total_internal_lead_time_min,
    vw.yard_wait_time_min,
    vw.charging_wait_time_min,
    vw.charging_duration_min,
    vw.soc_gap_before_dispatch,
    vw.dispatch_delay_min,
    vw.non_productive_moves_count,
    vw.blocking_exposure,
    vw.complejidad_montaje AS version_complexity_score,
    vw.retraso_min,
    vw.readiness_final_flag,
    vw.requires_charge_but_missing,
    CASE
        WHEN vw.total_internal_lead_time_min IS NULL THEN NULL
        ELSE (
            0.20 * LEAST(1.0, ABS(COALESCE(vw.planned_to_actual_sequence_gap, 0)) / 20.0)
            + 0.20 * LEAST(1.0, COALESCE(vw.yard_wait_time_min, 0.0) / 240.0)
            + 0.20 * LEAST(1.0, COALESCE(vw.charging_wait_time_min, 0.0) / 240.0)
            + 0.15 * LEAST(1.0, GREATEST(COALESCE(vw.soc_gap_before_dispatch, 0.0), 0.0) / 40.0)
            + 0.15 * LEAST(1.0, GREATEST(COALESCE(vw.dispatch_delay_min, 0.0), 0.0) / 240.0)
            + 0.10 * LEAST(1.0, COALESCE(vw.blocking_exposure, 0.0))
        ) * 100.0
    END AS readiness_risk_score_input
FROM vw_vehicle_flow_timeline vw;
