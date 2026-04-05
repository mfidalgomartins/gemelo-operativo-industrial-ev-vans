# Repository Structure (Current Contract)

## Principios
- Separación explícita entre pipeline EV y legacy.
- Una única salida oficial de dashboard en raíz de `outputs/dashboard/`.
- Artefactos legacy aislados en subcarpetas dedicadas.

## Estructura operativa
- `data/raw/ev_factory/`: fuente raw oficial para pipeline EV.
- `data/raw/`: espacio legacy/histórico (no usar como fuente EV principal).
- `data/processed/ev_factory/`: tablas EV gobernadas.
- `sql/ev_factory/`: capa SQL oficial EV.
- `sql/legacy/`: capa SQL legacy (pipeline histórico).
- `docs/legacy/`: documentación legacy/histórica aislada.
- `src/`: código fuente.
- `outputs/dashboard/dashboard_gemelo_operativo_ev.html`: dashboard oficial.
- `outputs/dashboard/legacy/`: dashboards legacy archivados.
- `outputs/reports/`: reportes de validación, governance y ejecución.

## Contratos de navegación
- Dashboard oficial: `outputs/dashboard/dashboard_gemelo_operativo_ev.html`
- Build dashboard oficial: `python -m src.ev_build_dashboard`
- Validación oficial: `python -m src.ev_validate_project`
- Release gate machine-readable: `outputs/reports/release_readiness.json`
- Release gate ejecutable: `python -m src.ev_release_gate`

## Anti-patterns bloqueados
- mezclar datasets EV con legacy en la misma ruta raw sin partición.
- publicar múltiples dashboards “oficiales” en la raíz de `outputs/dashboard/`.
- usar métricas de decisión calculadas en frontend.
