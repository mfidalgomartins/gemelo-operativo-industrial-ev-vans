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
        'salida_antes_readiness' AS check_name,
        COUNT(*) AS failed_rows
    FROM stg_dispatch
    WHERE fecha_salida_real IS NOT NULL
      AND (timestamp_readiness IS NULL OR fecha_salida_real < timestamp_readiness)
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
),
check_zonas_patio_sin_capacidad AS (
    SELECT
        'zonas_patio_sin_capacidad_fisica' AS check_name,
        COUNT(*) AS failed_rows
    FROM (
        SELECT DISTINCT ys.zona_patio
        FROM stg_yard_snapshots ys
        LEFT JOIN vw_yard_zone_capacity c
            ON ys.zona_patio = c.zona_patio
        WHERE c.zona_patio IS NULL
           OR c.physical_capacity_units <= 0
    ) q
),
check_ocupacion_patio_sobre_capacidad AS (
    SELECT
        'ocupacion_patio_sobre_capacidad_fisica' AS check_name,
        COUNT(*) AS failed_rows
    FROM vw_yard_congestion
    WHERE occupancy_units > physical_capacity_units
),
check_solapamiento_intervalos_patio AS (
    SELECT
        'intervalos_patio_solapados' AS check_name,
        COUNT(*) AS failed_rows
    FROM (
        SELECT
            vehiculo_id,
            interval_start,
            interval_end,
            LEAD(interval_start) OVER (
                PARTITION BY vehiculo_id
                ORDER BY interval_start, interval_end
            ) AS next_interval_start
        FROM vw_yard_vehicle_intervals
    ) q
    WHERE next_interval_start < interval_end
),
check_balance_wip_patio AS (
    SELECT
        'balance_wip_patio' AS check_name,
        CAST(ABS(expected_wip - interval_wip) AS BIGINT) AS failed_rows
    FROM (
        SELECT
            (
                SELECT COUNT(*)
                FROM stg_vehicles v
                WHERE v.timestamp_entrada_patio < horizon.observation_end
                  AND (v.timestamp_salida IS NULL OR v.timestamp_salida >= horizon.observation_end)
            ) AS expected_wip,
            (
                SELECT COUNT(DISTINCT i.vehiculo_id)
                FROM vw_yard_vehicle_intervals i
                WHERE i.interval_start < horizon.observation_end
                  AND i.interval_end >= horizon.observation_end
            ) AS interval_wip
        FROM (
            SELECT DATE_TRUNC('hour', MAX(timestamp)) + INTERVAL 1 HOUR AS observation_end
            FROM stg_yard_snapshots
        ) horizon
    ) reconciled
),
check_fecha_operativa AS (
    SELECT
        'fecha_operativa_inconsistente' AS check_name,
        COUNT(*) AS failed_rows
    FROM (
        -- Carga: cada clave fecha-turno-punto debe usar el calendario operativo.
        SELECT e.fecha
        FROM (
            SELECT DISTINCT
                ev_operational_date(cs.inicio_sesion) AS fecha,
                ev_operational_shift(cs.inicio_sesion) AS turno,
                cs.slot_id
            FROM stg_charge_sessions cs
        ) e
        LEFT JOIN vw_charging_utilization a
            ON e.fecha = a.fecha
           AND e.turno = a.turno
           AND e.slot_id = a.slot_id
        WHERE a.slot_id IS NULL

        UNION ALL

        -- Cuellos: la clave fecha-turno-área debe reconciliar con el evento.
        SELECT e.fecha
        FROM (
            SELECT DISTINCT
                ev_operational_date(b.timestamp) AS fecha,
                ev_operational_shift(b.timestamp) AS turno,
                b.area
            FROM stg_bottlenecks b
        ) e
        LEFT JOIN vw_shift_bottleneck_summary a
            ON e.fecha = a.fecha
           AND e.turno = a.turno
           AND e.area = a.area
        WHERE a.area IS NULL

        UNION ALL

        -- Expedición: la vista vehículo conserva fecha y turno de la salida planificada.
        SELECT d.fecha
        FROM vw_dispatch_readiness d
        INNER JOIN vw_vehicle_flow_timeline v
            ON d.vehiculo_id = v.vehiculo_id
        WHERE d.fecha IS DISTINCT FROM ev_operational_date(v.fecha_salida_planificada)
           OR d.turno IS DISTINCT FROM ev_operational_shift(v.fecha_salida_planificada)

        UNION ALL

        -- Mart área-turno: el agregado de patio debe reconciliar por fecha operativa.
        SELECT e.fecha
        FROM (
            SELECT
                ev_operational_date(y.ts_hour) AS fecha,
                ev_operational_shift(y.ts_hour) AS turno,
                AVG(y.yard_occupancy_rate) AS expected_occupancy
            FROM vw_yard_congestion y
            GROUP BY
                ev_operational_date(y.ts_hour),
                ev_operational_shift(y.ts_hour)
        ) e
        INNER JOIN stg_turnos t
            ON e.fecha = t.fecha
           AND e.turno = t.turno
        LEFT JOIN mart_area_shift a
            ON e.fecha = a.fecha
           AND e.turno = a.turno
           AND a.area = 'PATIO'
        WHERE a.area IS NULL
           OR ABS(e.expected_occupancy - a.yard_occupancy_rate) > 1e-9
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
    UNION ALL
    SELECT check_name, failed_rows FROM check_zonas_patio_sin_capacidad
    UNION ALL
    SELECT check_name, failed_rows FROM check_ocupacion_patio_sobre_capacidad
    UNION ALL
    SELECT check_name, failed_rows FROM check_solapamiento_intervalos_patio
    UNION ALL
    SELECT check_name, failed_rows FROM check_balance_wip_patio
    UNION ALL
    SELECT check_name, failed_rows FROM check_fecha_operativa
) checks;
