# Scoring Framework - Priorización Operativa EV

## Objetivo
Priorizar acciones de secuenciación, patio, carga, expedición y capacidad para sostener el ramp-up EV.

## Scores mínimos
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
El `operational_priority_index` combina riesgo de patio, carga, expedición, impacto de throughput atribuido al área y tensión de transición EV. `throughput_loss_score` usa `area_throughput_loss_proxy`; no repite el gap global de planta ni pretende estimar causalidad.

## Sensibilidad
Se aplica perturbación de pesos (+/-20%) para verificar estabilidad del ranking de áreas críticas.
Se añade test Monte Carlo de estabilidad de top-1 bajo ruido de pesos.
