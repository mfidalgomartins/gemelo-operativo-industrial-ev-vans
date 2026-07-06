# Contratos de Datos y Advertencias de Producción

Referencia breve para esquema, granularidad y límites de uso.

## Esquema de origen oficial

Fuente canónica: `data/raw/ev_factory/`.

| Tabla | Grano | Clave práctica | Contenido |
|---|---|---|---|
| `ordenes` | orden | `orden_id` | plan/real, turno, secuencia, prioridad, mercado y preparación inicial |
| `vehiculos` | vehículo | `vehiculo_id` | marcas temporales del flujo físico, versión y SOC de salida |
| `versiones_vehiculo` | versión | `version_id` | familia, propulsión, batería, complejidad y marcadores EV |
| `estado_bateria` | lectura batería | `vehiculo_id`, `timestamp` | SOC, SOC objetivo, estado de carga y energía |
| `slots_carga` | punto de carga | `slot_id` | zona, potencia, disponibilidad y mantenimiento |
| `sesiones_carga` | sesión carga | `sesion_id` | inicio/fin, energía, espera e interrupción |
| `patio` | instantánea de patio | `vehiculo_id`, `timestamp` | zona, posición, permanencia, bloqueo y movimiento requerido |
| `movimientos_patio` | movimiento | `movimiento_id` | origen/destino, motivo, operador y movimiento improductivo |
| `turnos` | día-turno | `fecha`, `turno` | dotación, absentismo, productividad y presión |
| `logistica_salida` | salida | `salida_id` | plan/real de expedición, transportista, preparación y atraso |
| `cuellos_botella` | evento | `evento_id` | área, severidad, duración e impactos proxy |
| `recursos_operativos` | recurso | `recurso_id` | capacidad nominal/disponible y restricción actual |
| `restricciones_operativas` | restricción | `restriccion_id` | ventana, área, tipo, severidad e impacto en capacidad |
| `escenarios_transicion` | día | `fecha` | cuota EV, intensidad de rampa y presión de patio/carga/logística |

## Esquema procesado principal

Fuente: `data/processed/ev_factory/`.

| Artefacto | Grano | Uso principal |
|---|---|---|
| `vw_vehicle_flow_timeline.csv` | orden/vehículo | flujo completo fin de línea -> patio -> carga -> salida |
| `vw_charging_utilization.csv` | fecha-turno-zona-punto | utilización, colas, interrupciones y brecha SOC |
| `vw_yard_congestion.csv` | hora-zona patio | ocupación, permanencia, bloqueo y riesgo operativo |
| `vw_dispatch_readiness.csv` | vehículo | preparación final, atraso, causa, SOC y riesgo de expedición |
| `vw_shift_bottleneck_summary.csv` | fecha-turno-área | eventos de cuello, severidad e impacto |
| `mart_vehicle_day.csv` | vehículo-día | mart analítico para variables de vehículo |
| `mart_area_shift.csv` | fecha-turno-área | mart táctico de estrés operativo |
| `mart_dispatch_readiness.csv` | fecha-turno-propulsión-versión | preparación y retraso por segmento |
| `kpi_operativos.csv` | instantánea única | fuente de verdad de KPI ejecutivos |
| `vehicle_readiness_features.csv` | vehículo | entradas de puntuación y diagnóstico |
| `area_shift_features.csv` | fecha-turno-área | entradas de OPI por área |
| `charging_features.csv` | fecha-turno-zona-punto | presión de carga |
| `yard_features.csv` | hora-zona patio | saturación de patio |
| `launch_transition_features.csv` | semana | presión de transición EV |
| `operational_prioritization_table.csv` | área | OPI, factor principal, nivel y acción recomendada |
| `scenario_table.csv` | escenario | simulación paramétrica de 8 escenarios |
| `validation_checks.csv` | comprobación | validaciones SQL de negocio |

Definiciones detalladas: `docs/sql_metric_definitions.md` y `docs/feature_dictionary.md`.

## Contratos de calidad

- `orden_id` debe ser único.
- Secuencias por `fecha_turno_operativo`, `turno`, `secuencia_planeada` no deben duplicarse.
- Las marcas temporales de flujo no pueden retroceder.
- `soc_pct` y `target_soc_pct` deben estar en `[0, 100]`.
- Una sesión de carga requiere `fin_sesion >= inicio_sesion` y energía positiva.
- Un vehículo con salida real no puede tener `readiness_salida_flag = 0`.
- KPI críticos del panel deben venir de `kpi_operativos.csv`.
- El panel oficial debe ser único en `outputs/dashboard/`.

## Arquitectura en una página

```text
data/raw/ev_factory/*.csv
        |
        v
Scripts DuckDB SQL en sql/ev_factory/
        |
        v
data/processed/gemelo_operativo_ev.duckdb
data/processed/ev_factory/{views,marts,kpi}.csv
        |
        v
Analítica Python: variables -> diagnóstico -> escenarios -> puntuación
        |
        v
Panel HTML, gráficos PNG, PDF, informe de validación y puerta de publicación
```

## Advertencias de producción

- Los datos actuales son sintéticos; no usarlos para compromisos operativos sin calibración con histórico real.
- Las elasticidades de escenarios son parámetros, no estimaciones causales.
- `area_throughput_loss_proxy` atribuye impacto por evento observado; no mide causalidad incremental.
- El OPI es interpretable, pero depende de pesos y umbrales que deben aprobarse por operaciones.
- El panel es estático; no tiene autenticación, servicio servidor, linaje en tiempo de ejecución ni refresco incremental.
- Las dependencias visuales del panel usan CDN; los entornos cerrados deben empaquetar localmente Chart.js y fuentes.
- La ejecución completa sobrescribe CSV e informes en `data/processed/` y `outputs/`.
- Antes de uso real, añadir contratos de PII, control de acceso, observabilidad, versionado de conjuntos de datos y pruebas de reconciliación con sistemas fuente.
