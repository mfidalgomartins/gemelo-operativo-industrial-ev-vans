# Scoring y Priorización - Resumen

## Top áreas críticas
- LOGISTICA: OPI=67.2, tier=estabilizar en la siguiente ola, driver=throughput_loss_score, acción=ajustar turnos o recursos
- PATIO: OPI=65.1, tier=estabilizar en la siguiente ola, driver=yard_risk_score, acción=revisar política de buffer en patio
- CARGA: OPI=35.8, tier=mantener bajo observación, driver=charging_risk_score, acción=ampliar infraestructura de carga
- PRODUCCION: OPI=35.0, tier=mantener bajo observación, driver=dispatch_risk_score, acción=priorizar expedición selectiva
- EXPEDICION: OPI=29.4, tier=sin prioridad inmediata, driver=dispatch_risk_score, acción=priorizar expedición selectiva
- ENERGIA: OPI=19.9, tier=sin prioridad inmediata, driver=charging_risk_score, acción=ampliar infraestructura de carga

## Top acciones
- ajustar turnos o recursos: prioridad_media=67.2, áreas_afectadas=1
- revisar política de buffer en patio: prioridad_media=65.1, áreas_afectadas=1
- priorizar expedición selectiva: prioridad_media=32.2, áreas_afectadas=2
- ampliar infraestructura de carga: prioridad_media=27.9, áreas_afectadas=2

## Governance checks
- opi_diversity: PASS (valor=6.00, umbral=3.00)
- risk_driver_diversity: PASS (valor=4.00, umbral=2.00)
- tier_diversity: PASS (valor=3.00, umbral=2.00)
- opi_dispersion: PASS (valor=17.81, umbral=1.00)
- rank_stability_top1_share: PASS (valor=0.77, umbral=0.45)

## Estabilidad Monte Carlo (top-1)
- Área dominante: LOGISTICA
- Frecuencia top-1: 77.33%
