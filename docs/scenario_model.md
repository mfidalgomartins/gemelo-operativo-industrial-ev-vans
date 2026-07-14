# Modelo Paramétrico de Escenarios

## Decisión que soporta

Comparar una rampa EV sin medidas correctivas con cambios de secuenciación, carga y patio, además de dos escenarios de tensión. El modelo ordena opciones para diseñar pilotos; no estima causalidad, ROI ni necesidad de inversión.

## Contrato de entrada

| Parámetro | Rango | Interpretación |
|---|---:|---|
| `share_ev_delta` | 0-1 | aumento absoluto de cuota EV sobre la media observada |
| `sequencing_gain` | 0-1 | intensidad relativa de mejora de secuenciación |
| `charging_gain` | 0-1 | intensidad relativa de mejora de capacidad/asignación de carga |
| `yard_gain` | 0-1 | intensidad relativa de mejora de capacidad/gestión de patio |
| `dispatch_pressure` | 0-1 | tensión adicional sobre expedición |
| `shift_loss` | 0-1 | pérdida relativa de disponibilidad del turno |

Los parámetros deben ser finitos y permanecer en `[0, 1]`. Las métricas base se leen de los marts canónicos; riesgos y cuota EV deben estar en `[0, 1]`, estabilidad en `[0, 100]` y tiempos/volúmenes no pueden ser negativos.

## Escenarios publicados

| Escenario | Δ EV | Secuencia | Carga | Patio | Presión salida | Pérdida turno |
|---|---:|---:|---:|---:|---:|---:|
| Rampa EV base | 0,25 | 0,00 | 0,00 | 0,00 | 0,00 | 0,00 |
| Rampa acelerada | 0,35 | 0,05 | 0,00 | 0,00 | 0,05 | 0,00 |
| Aumento de carga | 0,18 | 0,05 | 0,35 | 0,00 | 0,00 | 0,00 |
| Mejor secuenciación | 0,18 | 0,35 | 0,05 | 0,05 | 0,00 | 0,00 |
| Mejor uso de patio | 0,18 | 0,05 | 0,00 | 0,35 | 0,00 | 0,00 |
| Presión logística | 0,18 | 0,05 | 0,00 | 0,00 | 0,35 | 0,00 |
| Turno tensionado | 0,18 | 0,00 | 0,00 | 0,00 | 0,10 | 0,35 |
| Paquete correctivo | 0,25 | 0,35 | 0,30 | 0,30 | 0,05 | 0,00 |

## Respuesta del modelo

Sin calibración aprobada, cada métrica se obtiene multiplicando la baseline por elasticidades declaradas en `ev_scenario_twin.py`. Ejemplos:

```text
throughput = base × (1 - 0,10 × ΔEV)
                  × (1 + 0,06 × secuencia)
                  × (1 + 0,05 × carga)
                  × (1 + 0,04 × patio)
                  × (1 - 0,08 × presión_salida)
                  × (1 - 0,10 × pérdida_turno)

espera_carga = base × (1 + 1,20 × ΔEV)
                    × (1 - 0,30 × carga)
                    × (1 - 0,05 × secuencia)
                    × (1 + 0,05 × pérdida_turno)
```

Los riesgos se truncan a `[0, 1]`. La estabilidad combina riesgo de preparación, congestión, retraso y desviación de espera de carga. `decision_score` pondera caudal (30%), tiempo interno (20%), ocupación pico (15%), espera de carga (15%), riesgo de congestión (10%) y estabilidad (10%).

## Priors de palancas

`scenario_lever_ranking.csv` registra un índice relativo `[0, 1]` para ordenar pilotos. En el modo por defecto, todos los registros llevan `clase_evidencia = supuesto_parametrico_no_calibrado`; no deben presentarse como elasticidades estimadas, retorno financiero o ahorro esperado.

## Contrato de calibración

`ev-twin calibrate` recibe un CSV largo con una fila por observación y métrica:

| Campo | Regla |
|---|---|
| `observation_id`, `metric` | combinación única |
| `unit_id` | unidad independiente usada para agrupar errores estándar |
| `period` | timestamp válido para efectos fijos temporales |
| `metric` | una de las ocho métricas calibrables |
| `baseline_value`, `observed_value` | valores finitos y estrictamente positivos |
| seis palancas | intensidades finitas en `[0, 1]` |

Para cada métrica se estima:

```text
log(observado / baseline) = efectos_unidad + efectos_periodo + β · palancas + error
```

La covarianza se agrupa por `unit_id`. La ejecución se bloquea ante falta de variación, menos de 60 observaciones, menos de cinco unidades, más parámetros que grados de libertad, rango incompleto o condición numérica superior al umbral. El output contiene coeficiente, error estándar, intervalo, p-value, R², tamaño muestral, clusters, número de condición, método y estado.

El gemelo solo carga un fichero que cubra las 48 combinaciones de ocho métricas por seis palancas, sin duplicados, con estimaciones finitas y `calibration_status = approved`. En ese modo aplica `baseline × exp(β · palancas)`, restringe riesgos a `[0, 1]` y etiqueta la evidencia como calibrada.

## Uso real

1. Estimar elasticidades con pilotos controlados o histórico operacional.
2. Revisar identificación, intervalos, estabilidad temporal y signo operativo con los responsables de proceso.
3. Vincular efectos a costes de implementación mediante un modelo económico independiente.
4. Ejecutar sensibilidad sobre elasticidades, no solo sobre pesos de decisión.
5. Validar restricciones físicas con capacidad por hora, reglas de secuenciación y ventanas logísticas reales.
