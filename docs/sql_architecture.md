# Arquitectura SQL - Gemelo Operativo EV

## Dialecto elegido
- **DuckDB SQL**.
- Motivo: ejecución local reproducible y rápida sobre CSV, con una capa SQL visible y auditable.

## Capas
1. **Preparación** (`01` a `04`): tipado, normalización de marcas temporales, marcadores booleanos y estructura canónica.
2. **Integración** (`05` y `06`): integración de flujo vehículo, carga, patio, expedición y cuello de botella por turno-área.
3. **Marts analíticos** (`07` a `09`): materialización para consumo analítico y puntuación.
4. **Consultas KPI** (`10`): KPI ejecutivos y preparación por turno-versión.
5. **Consultas de validación** (`11`): comprobaciones de coherencia de negocio y de modelado.

## Scripts y función
- `01_staging_orders.sql`: preparación de órdenes, versiones, vehículos y turnos.
- `02_staging_charging.sql`: preparación de estado de batería, puntos de carga y sesiones.
- `03_staging_yard.sql`: preparación de instantáneas de patio, movimientos, recursos y restricciones.
- `04_staging_dispatch.sql`: preparación de expedición y escenarios de transición.
- `05_integrated_vehicle_flow.sql`: crea `vw_vehicle_flow_timeline`, `vw_charging_utilization`, `vw_yard_congestion`.
- `06_integrated_shift_operations.sql`: crea `vw_shift_bottleneck_summary`.
- `07_analytical_mart_vehicle_day.sql`: crea `mart_vehicle_day` (nivel vehículo-día).
- `08_analytical_mart_area_shift.sql`: crea `mart_area_shift` (nivel área-turno).
- `09_analytical_mart_dispatch_readiness.sql`: crea `vw_dispatch_readiness` y `mart_dispatch_readiness`.
- `10_kpi_queries.sql`: crea `kpi_operativos` y `kpi_readiness_shift_version`.
- `11_validation_queries.sql`: crea `validation_checks`.

## Orden de ejecución
1. Cargar tablas de origen en DuckDB.
2. Ejecutar scripts SQL en orden numérico.
3. Exportar vistas y marts a `data/processed/ev_factory/`.

## Runner
- Script: `src/ev_sql_layer.py`
- Entrada: `data/raw/*.csv` (14 tablas base)
- Salida:
  - DB: `data/processed/gemelo_operativo_ev.duckdb`
  - CSV analíticos: `data/processed/ev_factory/*.csv`
  - Resumen: `outputs/reports/sql_layer_execution_summary.md`

## Convenciones
- Sin `SELECT *` en SQL final de transformación.
- CTEs por bloque lógico.
- Nombres de vistas `vw_` y marts `mart_`.
- Cálculos operativos expresados en minutos para trazabilidad.
