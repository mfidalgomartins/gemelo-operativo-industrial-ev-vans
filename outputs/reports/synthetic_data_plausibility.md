# Validaciones de Plausibilidad - Datos Sintéticos Industriales

Estado global: **PASS**

## Periodo generado
- Inicio: `2025-01-01 06:00:00`
- Fin: `2025-10-01 06:07:00`
- Meses: `10`

## Validaciones
- [PASS] `columnas_ordenes` -> ok
- [PASS] `columnas_versiones_vehiculo` -> ok
- [PASS] `columnas_vehiculos` -> ok
- [PASS] `columnas_estado_bateria` -> ok
- [PASS] `columnas_slots_carga` -> ok
- [PASS] `columnas_sesiones_carga` -> ok
- [PASS] `columnas_patio` -> ok
- [PASS] `columnas_movimientos_patio` -> ok
- [PASS] `columnas_turnos` -> ok
- [PASS] `columnas_logistica_salida` -> ok
- [PASS] `columnas_cuellos_botella` -> ok
- [PASS] `columnas_recursos_operativos` -> ok
- [PASS] `columnas_restricciones_operativas` -> ok
- [PASS] `columnas_escenarios_transicion` -> ok
- [PASS] `horizonte_meses_9_15` -> meses=10
- [PASS] `unicidad_orden_id` -> duplicados=0
- [PASS] `unicidad_vehiculo_id` -> duplicados=0
- [PASS] `integridad_ordenes_vehiculos` -> orphan=0
- [PASS] `integridad_sesiones_vehiculos` -> orphan=0
- [PASS] `rango_soc` -> soc dentro de [0,100]
- [PASS] `espera_carga_no_trivial` -> media_espera=61.15
- [PASS] `progresion_share_ev` -> inicio=0.064, fin=0.732
- [PASS] `ready_ratio_plausible` -> ready_ratio=0.726
- [WARN] `retraso_salida_plausible` -> delay_mean=27101.8

## Cardinalidades
- `ordenes`: 43903
- `versiones_vehiculo`: 8
- `vehiculos`: 43903
- `estado_bateria`: 96329
- `slots_carga`: 32
- `sesiones_carga`: 17236
- `patio`: 156349
- `movimientos_patio`: 87355
- `turnos`: 819
- `logistica_salida`: 43903
- `cuellos_botella`: 1099
- `recursos_operativos`: 13
- `restricciones_operativas`: 695
- `escenarios_transicion`: 273