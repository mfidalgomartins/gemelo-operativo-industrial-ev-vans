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
check_duplicados_vehiculos AS (
    SELECT
        'duplicados_vehiculos' AS check_name,
        COUNT(*) AS failed_rows
    FROM (
        SELECT vehiculo_id, COUNT(*) AS n
        FROM stg_vehicles
        GROUP BY vehiculo_id
        HAVING COUNT(*) > 1
    ) q
),
check_dispatch_duplicado_vehiculo AS (
    SELECT
        'dispatch_duplicado_vehiculo' AS check_name,
        COUNT(*) AS failed_rows
    FROM (
        SELECT vehiculo_id, COUNT(*) AS n
        FROM stg_dispatch
        GROUP BY vehiculo_id
        HAVING COUNT(*) > 1
    ) q
),
check_chaves_criticas_nulas AS (
    SELECT
        'chaves_criticas_nulas' AS check_name,
        COUNT(*) AS failed_rows
    FROM (
        SELECT orden_id AS chave
        FROM stg_orders
        WHERE orden_id IS NULL OR vehiculo_id IS NULL OR version_id IS NULL
        UNION ALL
        SELECT vehiculo_id AS chave
        FROM stg_vehicles
        WHERE vehiculo_id IS NULL OR version_id IS NULL
        UNION ALL
        SELECT vehiculo_id AS chave
        FROM stg_dispatch
        WHERE vehiculo_id IS NULL
    ) q
),
check_secuencia_incoherente AS (
    SELECT
        'secuencia_incoherente' AS check_name,
        COUNT(*) AS failed_rows
    FROM (
        SELECT fecha_turno_operativo, turno, secuencia_planeada, COUNT(*) AS n
        FROM stg_orders
        GROUP BY fecha_turno_operativo, turno, secuencia_planeada
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
    WHERE departed_flag = TRUE
      AND readiness_final_flag = FALSE
),
check_retraso_sin_causa AS (
    SELECT
        'retraso_sin_causa' AS check_name,
        COUNT(*) AS failed_rows
    FROM vw_dispatch_readiness
    WHERE departed_flag = TRUE
      AND dispatch_delay_min > 120
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
    WHERE dispatch_plan < 0
       OR dispatch_actual < 0
       OR slot_utilization < 0
),
check_cardinalidad_flujo AS (
    SELECT
        'cardinalidad_flujo_vehiculo' AS check_name,
        COUNT(*) AS failed_rows
    FROM (
        SELECT vehiculo_id, COUNT(*) AS n
        FROM vw_vehicle_flow_timeline
        GROUP BY vehiculo_id
        HAVING COUNT(*) > 1
    ) q
),
check_scores_nulos AS (
    SELECT
        'scores_riesgo_nulos' AS check_name,
        COUNT(*) AS failed_rows
    FROM (
        SELECT vehiculo_id AS chave
        FROM mart_vehicle_day
        WHERE total_internal_lead_time_min IS NOT NULL
          AND readiness_risk_score_input IS NULL
        UNION ALL
        SELECT vehiculo_id AS chave
        FROM vw_dispatch_readiness
        WHERE dispatch_readiness_risk_score IS NULL
    ) q
)
SELECT
    check_name,
    failed_rows,
    CASE WHEN failed_rows = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM (
    SELECT check_name, failed_rows FROM check_duplicados_ordenes
    UNION ALL
    SELECT check_name, failed_rows FROM check_duplicados_vehiculos
    UNION ALL
    SELECT check_name, failed_rows FROM check_dispatch_duplicado_vehiculo
    UNION ALL
    SELECT check_name, failed_rows FROM check_chaves_criticas_nulas
    UNION ALL
    SELECT check_name, failed_rows FROM check_secuencia_incoherente
    UNION ALL
    SELECT check_name, failed_rows FROM check_timestamps
    UNION ALL
    SELECT check_name, failed_rows FROM check_soc_range
    UNION ALL
    SELECT check_name, failed_rows FROM check_sesion_imposible
    UNION ALL
    SELECT check_name, failed_rows FROM check_ev_sin_carga
    UNION ALL
    SELECT check_name, failed_rows FROM check_salida_sin_ready
    UNION ALL
    SELECT check_name, failed_rows FROM check_retraso_sin_causa
    UNION ALL
    SELECT check_name, failed_rows FROM check_restriccion_capacidad
    UNION ALL
    SELECT check_name, failed_rows FROM check_denominadores
    UNION ALL
    SELECT check_name, failed_rows FROM check_cardinalidad_flujo
    UNION ALL
    SELECT check_name, failed_rows FROM check_scores_nulos
) checks;
