# SQL Layer Execution Summary (DuckDB)

- Base de datos: `/Users/miguelfidalgo/Documents/gemelo-operativo-transicion-vans-electricas/data/processed/gemelo_operativo_ev.duckdb`
- Raw source EV (preferente): `/Users/miguelfidalgo/Documents/gemelo-operativo-transicion-vans-electricas/data/raw/ev_factory`
- Scripts ejecutados: 11

## Orden de ejecución
- 01_staging_orders.sql
- 02_staging_charging.sql
- 03_staging_yard.sql
- 04_staging_dispatch.sql
- 05_integrated_vehicle_flow.sql
- 06_integrated_shift_operations.sql
- 07_analytical_mart_vehicle_day.sql
- 08_analytical_mart_area_shift.sql
- 09_analytical_mart_dispatch_readiness.sql
- 10_kpi_queries.sql
- 11_validation_queries.sql

## Filas exportadas
- vw_vehicle_flow_timeline: 43903
- vw_charging_utilization: 14712
- vw_yard_congestion: 34232
- vw_dispatch_readiness: 43903
- vw_shift_bottleneck_summary: 1028
- mart_vehicle_day: 43903
- mart_area_shift: 6552
- mart_dispatch_readiness: 6165
- kpi_operativos: 1
- kpi_readiness_shift_version: 24
- validation_checks: 10