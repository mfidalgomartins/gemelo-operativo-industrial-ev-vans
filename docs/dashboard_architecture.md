# Arquitectura del Dashboard EV (Official)

## Build Path oficial
- `python -m src.ev_build_dashboard`
- `python -m src.run_pipeline`
- Output oficial único: `outputs/dashboard/industrial-ev-operating-command-center.html`

## Principios técnicos
- KPI críticos consumidos desde dataset gobernado (`kpi_operativos.csv`).
- Sin lógica de scoring crítica en frontend.
- Payload agregado para rendimiento y legibilidad.
- Filtros aplicados por contrato de dataset.
- QA de build con manifest y reporte dedicado.
