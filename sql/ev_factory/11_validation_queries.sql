-- DuckDB SQL
-- Validaciones de consistencia de capa analítica

CREATE OR REPLACE TABLE validation_checks AS
WITH check_duplicados_ordenes AS (
    SELECT
        'duplicados_ordenes' AS check_name,
        COUNT(*) AS failed_rows
    FROM (
        SELECT orden_id, COUNT(*) AS n
        FROM stg_orders
        GROUP BY orden_id
        HAVING COUNT(*) > 1
    ) q
),
check_secuencia_incoherente AS (
    SELECT
        'secuencia_incoherente' AS check_name,
        COUNT(*) AS failed_rows
    FROM (
        SELECT fecha_programada, turno, secuencia_planeada, COUNT(*) AS n
        FROM stg_orders
        GROUP BY fecha_programada, turno, secuencia_planeada
        HAVING COUNT(*) > 1
    ) q
),
check_timestamps AS (
    SELECT
        'timestamps_fuera_orden' AS check_name,
        COUNT(*) AS failed_rows
    FROM vw_vehicle_flow_timeline
    WHERE timestamp_entrada_patio < timestamp_fin_linea
       OR (timestamp_inicio_carga IS NOT NULL AND timestamp_inicio_carga < timestamp_entrada_patio)
       OR (timestamp_fin_carga IS NOT NULL AND timestamp_inicio_carga IS NOT NULL AND timestamp_fin_carga < timestamp_inicio_carga)
),
check_soc_range AS (
    SELECT
        'soc_fuera_rango' AS check_name,
        COUNT(*) AS failed_rows
    FROM stg_battery_status
    WHERE soc_pct < 0 OR soc_pct > 100 OR target_soc_pct < 0 OR target_soc_pct > 100
),
check_sesion_imposible AS (
    SELECT
        'sesion_carga_imposible' AS check_name,
        COUNT(*) AS failed_rows
    FROM stg_charge_sessions
    WHERE fin_sesion < inicio_sesion
       OR energia_entregada_kwh <= 0
       OR tiempo_espera_previo_min < 0
),
check_ev_sin_carga AS (
    SELECT
        'ev_requiere_carga_sin_sesion' AS check_name,
        COUNT(*) AS failed_rows
    FROM vw_vehicle_flow_timeline
    WHERE requiere_carga_salida_flag
      AND energia_total_carga_kwh <= 0
),
check_salida_sin_ready AS (
    SELECT
        'salida_sin_readiness' AS check_name,
        COUNT(*) AS failed_rows
    FROM vw_dispatch_readiness
    WHERE delayed_flag = FALSE
      AND readiness_final_flag = FALSE
),
check_retraso_sin_causa AS (
    SELECT
        'retraso_sin_causa' AS check_name,
        COUNT(*) AS failed_rows
    FROM vw_dispatch_readiness
    WHERE dispatch_delay_min > 0
      AND (causa_retraso IS NULL OR causa_retraso IN ('SIN_RETRASO', 'N/A'))
),
check_restriccion_capacidad AS (
    SELECT
        'restriccion_capacidad_inconsistente' AS check_name,
        COUNT(*) AS failed_rows
    FROM stg_operational_resources r
    WHERE r.restriccion_actual_flag
      AND r.capacidad_disponible > r.capacidad_nominal
),
check_denominadores AS (
    SELECT
        'denominadores_invalidos' AS check_name,
        COUNT(*) AS failed_rows
    FROM mart_area_shift
    WHERE throughput_plan < 0
       OR throughput_real < 0
       OR slot_utilization < 0
)
SELECT
    check_name,
    failed_rows,
    CASE WHEN failed_rows = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM (
    SELECT * FROM check_duplicados_ordenes
    UNION ALL
    SELECT * FROM check_secuencia_incoherente
    UNION ALL
    SELECT * FROM check_timestamps
    UNION ALL
    SELECT * FROM check_soc_range
    UNION ALL
    SELECT * FROM check_sesion_imposible
    UNION ALL
    SELECT * FROM check_ev_sin_carga
    UNION ALL
    SELECT * FROM check_salida_sin_ready
    UNION ALL
    SELECT * FROM check_retraso_sin_causa
    UNION ALL
    SELECT * FROM check_restriccion_capacidad
    UNION ALL
    SELECT * FROM check_denominadores
) checks;
