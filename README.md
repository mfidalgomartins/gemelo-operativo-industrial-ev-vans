# Gemelo Operativo de Lanzamiento Industrial para Transición a Furgonetas Eléctricas

## Executive Overview
Este proyecto construye una plataforma analítica integral para simular, monitorizar y priorizar decisiones operativas en una transición industrial hacia vans eléctricas: secuenciación, patio, carga, logística interna y expedición.

El enfoque está inspirado en un entorno de planta de automoción de alta complejidad (caso industrial tipo Vitoria), con una capa SQL profesional, feature engineering, diagnóstico interpretable, gemelo operativo de escenarios, scoring de priorización y dashboard ejecutivo HTML autocontenido.

## Business Problem
¿Cómo adaptar secuenciación, patio, capacidad de carga y lógica de salida para sostener el ramp-up EV sin pérdida de throughput ni crecimiento estructural de congestión y riesgo operativo?

## Why This Matters in an EV Van Plant
En transición EV, el cuello deja de estar solo en línea de montaje: aparece presión combinada en carga, patios intermedios y readiness de expedición. Si no se orquesta el flujo completo, el throughput cae aunque la producción nominal se mantenga.

## Project Architecture
1. Generación de datos sintéticos realistas (12 meses, fases de pre-lanzamiento/ramp-up/estabilización)
2. Auditoría `/explore-data` de calidad y readiness operacional
3. Capa SQL por niveles (`staging`, `integration`, `marts`, `kpi`, `validation`)
4. Feature engineering orientado a operaciones
5. Análisis diagnóstico interpretable
6. Gemelo operativo de escenarios (8 escenarios obligatorios)
7. Framework de scoring y priorización
8. Visualizaciones ejecutivas
9. Dashboard HTML autocontenido con filtros y what-if
10. Validación integral y caveats

## Synthetic Data Model
Tablas base industriales: `ordenes`, `vehiculos`, `versiones_vehiculo`, `estado_bateria`, `sesiones_carga`, `patio`, `movimientos_patio`, `turnos`, `logistica_salida`, `cuellos_botella`, `recursos_operativos`, `restricciones_operativas`, `escenarios_transicion`, `slots_carga`.

Incluye comportamientos realistas:
- incremento progresivo de share EV
- tensión en carga y patio durante ramp-up
- variabilidad por turno
- días estables vs días de alta presión
- retrasos de expedición por readiness/SOC

## SQL Layer
Dialecto: **DuckDB SQL**.

Ruta: `sql/ev_factory/`
- `01_staging_orders.sql`
- `02_staging_charging.sql`
- `03_staging_yard.sql`
- `04_staging_dispatch.sql`
- `05_integrated_vehicle_flow.sql`
- `06_integrated_shift_operations.sql`
- `07_analytical_mart_vehicle_day.sql`
- `08_analytical_mart_area_shift.sql`
- `09_analytical_mart_dispatch_readiness.sql`
- `10_kpi_queries.sql`
- `11_validation_queries.sql`

Pipeline legacy aislado:
- `sql/legacy/` (solo para `src/run_pipeline.py` histórico)

Vistas clave:
- `vw_vehicle_flow_timeline`
- `vw_charging_utilization`
- `vw_yard_congestion`
- `vw_dispatch_readiness`
- `vw_shift_bottleneck_summary`

## Feature Engineering
Tablas:
- `vehicle_readiness_features`
- `area_shift_features`
- `charging_features`
- `yard_features`
- `launch_transition_features`

Diccionario: [`docs/feature_dictionary.md`](docs/feature_dictionary.md)

## Diagnostic Analytics
Scores y salidas:
- `sequence_disruption_score`
- `yard_congestion_score`
- `charging_pressure_score`
- `dispatch_delay_risk_score`
- `launch_transition_stress_score`
- `area_criticality_score`
- `main_bottleneck_driver`
- `recommended_action_initial`

Framework: [`docs/diagnostic_framework.md`](docs/diagnostic_framework.md)

## Digital Twin / Scenario Engine
Escenarios simulados:
1. ramp-up EV base
2. ramp-up EV acelerado
3. aumento de slots de carga
4. mejor secuenciación EV
5. expansión/mejor uso de patio
6. mayor presión logística de salida
7. turno tensionado
8. combinación de medidas correctivas

Output principal: `data/processed/ev_factory/scenario_table.csv`

## Scoring Framework
Scores mínimos:
- `readiness_score`
- `yard_risk_score`
- `charging_risk_score`
- `dispatch_risk_score`
- `throughput_loss_score`
- `launch_transition_risk_score`
- `operational_priority_index`
- `area_priority_tier`
- `main_risk_driver`
- `recommended_action`

Documento: [`docs/scoring_framework.md`](docs/scoring_framework.md)

## Dashboard Overview
- Archivo: `outputs/dashboard/dashboard_gemelo_operativo_ev.html`
- Secciones: secuencia, patio, carga, salida, gemelo, estrategia, tabla final
- Filtros globales: fecha, turno, versión, propulsión, zonas, área, severidad y tipo de cuello
- Incluye panel what-if y bloque de decisión ejecutiva

## Key Findings
- El riesgo dominante en ramp-up EV migra a **carga + patio**, no solo a secuencia.
- El escenario de mejor desempeño combina secuenciación, refuerzo de carga y disciplina de patio.
- Persisten dos riesgos estructurales a vigilar: salidas sin readiness y picos de ocupación de patio.

## Recommendations
1. Reforzar reglas de secuenciación EV en horas de mayor presión.
2. Reservar capacidad de carga para vehículos con mayor riesgo de salida.
3. Reducir dwell no productivo en patio con buffer por zona y limpieza de bloqueos.
4. Priorizar expedición selectiva según readiness real, no solo por plan.

## Repository Structure
- `data/raw/`
- `data/processed/`
- `sql/ev_factory/`
- `src/`
- `notebooks/`
- `outputs/charts/`
- `outputs/dashboard/`
- `outputs/reports/`
- `docs/`
- `tests/`

Convenciones de estructura y contratos:
- [`docs/repository_structure.md`](docs/repository_structure.md)
- [`docs/governance/release_gates.md`](docs/governance/release_gates.md)
- [`docs/governance/kpi_governance_contract.md`](docs/governance/kpi_governance_contract.md)

## How to Run
```bash
# 1) Generar datos sintéticos industriales (ruta EV dedicada)
.venv/bin/python generate_synthetic_data.py --seed 20260328 --start-date 2025-01-01 --months 12

# 2) Auditoría explore-data
.venv/bin/python src/explore_data_audit.py

# 3) SQL layer EV
.venv/bin/python -m src.ev_sql_layer

# 4) Feature engineering
.venv/bin/python -m src.ev_feature_engineering

# 5) Diagnóstico
.venv/bin/python -m src.ev_diagnostic_analysis

# 6) Gemelo operativo
.venv/bin/python -m src.ev_scenario_twin

# 7) Scoring
.venv/bin/python -m src.ev_scoring_framework

# 8) Visualizaciones
.venv/bin/python -m src.ev_create_visuals

# 9) Dashboard
.venv/bin/python -m src.ev_build_dashboard

# 10) Validación final
.venv/bin/python -m src.ev_validate_project

# 11) Gate de release (exit code 0=aprobado)
.venv/bin/python -m src.ev_release_gate
```

## Validation Approach
- checks de integridad temporal
- checks de consistencia EV/carga/readiness
- checks de duplicados/nulls/rangos
- validación SQL (`validation_checks`)
- validación de coherencia entre outputs analíticos y dashboard

Reporte final: `outputs/reports/validation_report.md`
Release gate machine-readable: `outputs/reports/release_readiness.json`

## Limitations
- Dataset sintético, no telemetría real de planta
- Elasticidades del scenario engine basadas en supuestos interpretables
- El scoring depende de pesos definidos; requiere calibración con negocio en entorno real

## Next Steps
1. Calibrar parámetros con históricos reales por área-turno.
2. Incorporar capacidad energética y restricciones de red con granularidad intradía.
3. Integrar alertado operativo near-real-time.
4. Añadir capa de optimización de secuencia bajo restricciones de carga/patio.

## What This Project Demonstrates
- Diseño end-to-end de plataforma industrial analytics
- Capacidad de traducir problemas de operaciones a modelo analítico ejecutable
- Dominio combinado de SQL, Python, simulación y BI ejecutivo

## Business Skills Demonstrated
- framing de problema operativo complejo
- priorización basada en riesgo/capacidad
- lectura de trade-offs de inversión operativa
- comunicación ejecutiva orientada a decisión

## Technical Skills Demonstrated
- ingeniería de datos y modelado SQL en capas
- feature engineering interpretable
- scoring framework defendible
- scenario simulation
- construcción de dashboard industrial con filtros y what-if
- validación formal y trazabilidad de supuestos

## Questions This System Helps Answer
- ¿Dónde se rompe el flujo y por qué?
- ¿Qué cuello es estructural vs pico ocasional?
- ¿Qué turno/versión concentra mayor riesgo?
- ¿Qué palanca mejora más la transición EV por coste operativo?

## Why This Is Relevant for an EV Industrial Transition
La transición EV incrementa complejidad en post-línea (carga/patio/expedición). Esta solución permite escalar mix EV sin perder control de flujo ni estabilidad operativa.

## What Decisions This System Would Improve in a Real Factory
- reglas de secuenciación por ventana/turno
- asignación dinámica de slots de carga
- política de buffer y movimientos de patio
- priorización de expedición por readiness real
- inversiones de capacidad por área crítica

## Executive Decision Rules (Final)
- **Cuándo cambiar la secuencia**: cuando `sequence_disruption_score` y `throughput_loss_score` superan umbral de alerta (persistente por turno).
- **Cuándo ampliar carga**: cuando `charging_risk_score` domina el `main_risk_driver` y el wait-to-charge crece en escenarios EV acelerados.
- **Cuándo reorganizar patio**: cuando `yard_risk_score` y `p95_dwell_time` muestran saturación recurrente.
- **Qué cuello atacar primero**: el área con mayor `operational_priority_index` y tier `intervenir ahora`.
- **Riesgo si sube share EV sin adaptar operación**: aumento de cola de carga, congestión de patio, caída de readiness y crecimiento de retrasos de salida.
