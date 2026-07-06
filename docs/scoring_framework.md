# Marco de Puntuación - Priorización Operativa EV

## Objetivo
Priorizar acciones de secuenciación, patio, carga, expedición y capacidad para sostener la rampa EV.

## Puntuaciones mínimas
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

## Regla de tier
- >=80: intervenir ahora
- 65-79: estabilizar en la siguiente ola
- 50-64: monitorizar muy de cerca
- 35-49: mantener bajo observación
- <35: sin prioridad inmediata

## Lógica de decisión
El `operational_priority_index` combina riesgo de patio, carga, expedición, impacto de caudal atribuido al área y tensión de transición EV. `throughput_loss_score` usa `area_throughput_loss_proxy`; no repite la brecha global de planta ni pretende estimar causalidad.

## Sensibilidad
Se aplica perturbación de pesos (+/-20%) para verificar estabilidad del ranking de áreas críticas.
Se añade prueba Monte Carlo de estabilidad del primer puesto bajo ruido de pesos.
