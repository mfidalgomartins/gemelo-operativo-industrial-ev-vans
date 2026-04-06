CREATE OR REPLACE TABLE dim_vehiculo AS
SELECT
    o.order_id,
    o.vin,
    o.tipo_propulsion,
    o.familia_van,
    o.destino,
    o.prioridad_cliente,
    o.bateria_kwh,
    v.vehicle_version_id,
    v.tiempo_base_ensamblaje_min,
    v.tiempo_base_test_min,
    v.consumo_test_kwh
FROM stg_ordenes_produccion o
LEFT JOIN stg_versiones_vehiculo v
    ON o.familia_van = v.familia_van
   AND o.tipo_propulsion = v.tipo_propulsion;

CREATE OR REPLACE TABLE dim_turno AS
SELECT DISTINCT
    fecha,
    turno,
    dotacion,
    capacidad_teorica_unidades
FROM stg_calendario_turnos;

CREATE OR REPLACE TABLE dim_destino AS
SELECT
    destino,
    COUNT(*) AS volumen_ordenes,
    AVG(prioridad_cliente) AS prioridad_media,
    SUM(CASE WHEN tipo_propulsion = 'EV' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS mix_ev
FROM stg_ordenes_produccion
GROUP BY destino;

CREATE OR REPLACE TABLE fct_flujo_unidad AS
WITH prod AS (
    SELECT
        order_id,
        MIN(ts_inicio) AS ts_prod_inicio,
        MAX(ts_fin) AS ts_prod_fin,
        SUM(duracion_real_min) AS duracion_total_produccion_min
    FROM stg_eventos_produccion
    GROUP BY order_id
),
yard AS (
    SELECT
        order_id,
        MIN(CASE WHEN tipo_evento = 'ENTRY' THEN ts_evento END) AS ts_entry_patio,
        MAX(CASE WHEN tipo_evento = 'EXIT' THEN ts_evento END) AS ts_exit_patio,
        AVG(ocupacion_sector_pct) AS ocupacion_sector_media_pct
    FROM stg_eventos_patio
    GROUP BY order_id
),
charge AS (
    SELECT
        order_id,
        MIN(ts_inicio_carga) AS ts_inicio_carga,
        MAX(ts_fin_carga) AS ts_fin_carga,
        SUM(kwh_entregados) AS kwh_cargados,
        AVG(espera_previa_min) AS espera_carga_min
    FROM stg_sesiones_carga
    GROUP BY order_id
),
expd AS (
    SELECT
        order_id,
        ts_ready_expedicion,
        ts_salida_real,
        modo_salida,
        sla_horas
    FROM stg_eventos_expedicion
)
SELECT
    o.order_id,
    o.tipo_propulsion,
    o.familia_van,
    o.destino,
    o.prioridad_cliente,
    p.ts_prod_inicio,
    p.ts_prod_fin,
    y.ts_entry_patio,
    y.ts_exit_patio,
    c.ts_inicio_carga,
    c.ts_fin_carga,
    e.ts_ready_expedicion,
    e.ts_salida_real,
    e.modo_salida,
    e.sla_horas,
    p.duracion_total_produccion_min,
    COALESCE(c.kwh_cargados, 0) AS kwh_cargados,
    COALESCE(c.espera_carga_min, 0) AS espera_carga_min,
    y.ocupacion_sector_media_pct,
    GREATEST(0, DATEDIFF('minute', p.ts_prod_fin, y.ts_entry_patio)) AS espera_patio_min,
    GREATEST(0, DATEDIFF('minute', COALESCE(c.ts_fin_carga, y.ts_exit_patio, e.ts_ready_expedicion), e.ts_salida_real)) AS espera_salida_min,
    DATEDIFF('minute', p.ts_prod_inicio, e.ts_salida_real) / 60.0 AS lead_time_total_horas,
    CASE
        WHEN DATEDIFF('hour', e.ts_ready_expedicion, e.ts_salida_real) <= e.sla_horas THEN 1
        ELSE 0
    END AS cumple_sla_flag
FROM stg_ordenes_produccion o
JOIN prod p ON o.order_id = p.order_id
LEFT JOIN yard y ON o.order_id = y.order_id
LEFT JOIN charge c ON o.order_id = c.order_id
LEFT JOIN expd e ON o.order_id = e.order_id;

CREATE OR REPLACE TABLE fct_carga_operativa AS
WITH base AS (
    SELECT
        DATE(ts_inicio_carga) AS fecha,
        CASE
            WHEN EXTRACT('hour' FROM ts_inicio_carga) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT('hour' FROM ts_inicio_carga) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END AS turno,
        charger_id,
        DATEDIFF('minute', ts_inicio_carga, ts_fin_carga) AS duracion_carga_min,
        kwh_entregados,
        espera_previa_min
    FROM stg_sesiones_carga
)
SELECT
    fecha,
    turno,
    COUNT(*) AS sesiones_carga,
    COUNT(DISTINCT charger_id) AS cargadores_activos,
    SUM(duracion_carga_min) AS minutos_carga_total,
    SUM(kwh_entregados) AS kwh_entregados_total,
    AVG(espera_previa_min) AS espera_previa_media_min,
    SUM(duracion_carga_min) / (24.0 * 480.0) AS utilizacion_cargadores_turno
FROM base
GROUP BY fecha, turno;

CREATE OR REPLACE TABLE fct_ocupacion_patio_hora AS
WITH yard_interval AS (
    SELECT
        order_id,
        MIN(CASE WHEN tipo_evento = 'ENTRY' THEN ts_evento END) AS ts_entry,
        MAX(CASE WHEN tipo_evento = 'EXIT' THEN ts_evento END) AS ts_exit
    FROM stg_eventos_patio
    GROUP BY order_id
),
expanded AS (
    SELECT
        yi.order_id,
        gs.hora
    FROM yard_interval yi,
    LATERAL generate_series(
        date_trunc('hour', yi.ts_entry),
        date_trunc('hour', yi.ts_exit),
        INTERVAL 1 HOUR
    ) AS gs(hora)
    WHERE yi.ts_entry IS NOT NULL
      AND yi.ts_exit IS NOT NULL
      AND yi.ts_exit >= yi.ts_entry
)
SELECT
    hora,
    DATE(hora) AS fecha,
    EXTRACT('hour' FROM hora) AS hora_dia,
    CASE
        WHEN EXTRACT('hour' FROM hora) BETWEEN 6 AND 13 THEN 'A'
        WHEN EXTRACT('hour' FROM hora) BETWEEN 14 AND 21 THEN 'B'
        ELSE 'C'
    END AS turno,
    COUNT(DISTINCT order_id) AS vehiculos_en_patio,
    900 AS capacidad_patio,
    COUNT(DISTINCT order_id) * 100.0 / 900.0 AS ocupacion_patio_pct
FROM expanded
GROUP BY hora, DATE(hora), EXTRACT('hour' FROM hora),
    CASE
        WHEN EXTRACT('hour' FROM hora) BETWEEN 6 AND 13 THEN 'A'
        WHEN EXTRACT('hour' FROM hora) BETWEEN 14 AND 21 THEN 'B'
        ELSE 'C'
    END;

CREATE OR REPLACE TABLE fct_expedicion AS
SELECT
    e.dispatch_event_id,
    e.order_id,
    o.tipo_propulsion,
    o.destino,
    o.prioridad_cliente,
    e.ts_ready_expedicion,
    e.ts_salida_real,
    e.modo_salida,
    e.sla_horas,
    DATEDIFF('minute', e.ts_ready_expedicion, e.ts_salida_real) AS espera_expedicion_min,
    CASE
        WHEN DATEDIFF('hour', e.ts_ready_expedicion, e.ts_salida_real) <= e.sla_horas THEN 1
        ELSE 0
    END AS cumple_sla_flag
FROM stg_eventos_expedicion e
JOIN stg_ordenes_produccion o
    ON e.order_id = o.order_id;
