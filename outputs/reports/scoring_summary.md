# Scoring y Priorización - Resumen

## Top áreas críticas
- PATIO: OPI=68.5, tier=estabilizar en la siguiente ola, driver=yard_risk_score, acción=revisar política de buffer en patio
- LOGISTICA: OPI=68.1, tier=estabilizar en la siguiente ola, driver=throughput_loss_score, acción=ajustar turnos o recursos
- EXPEDICION: OPI=54.8, tier=monitorizar muy de cerca, driver=throughput_loss_score, acción=ajustar turnos o recursos
- PRODUCCION: OPI=50.6, tier=monitorizar muy de cerca, driver=throughput_loss_score, acción=ajustar turnos o recursos
- CARGA: OPI=48.7, tier=mantener bajo observación, driver=throughput_loss_score, acción=ajustar turnos o recursos
- ENERGIA: OPI=40.8, tier=mantener bajo observación, driver=throughput_loss_score, acción=ajustar turnos o recursos

## Top acciones
- revisar política de buffer en patio: prioridad_media=68.5, áreas_afectadas=1
- ajustar turnos o recursos: prioridad_media=52.6, áreas_afectadas=5

## Governance checks
- opi_diversity: PASS (valor=6.00, umbral=3.00)
- risk_driver_diversity: PASS (valor=2.00, umbral=2.00)
- tier_diversity: PASS (valor=3.00, umbral=2.00)
- opi_dispersion: PASS (valor=10.12, umbral=1.00)
- rank_stability_top1_share: PASS (valor=0.53, umbral=0.45)

## Estabilidad Monte Carlo (top-1)
- Área dominante: PATIO
- Frecuencia top-1: 53.33%