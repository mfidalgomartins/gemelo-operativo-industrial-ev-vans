# Arquitectura del Dashboard EV

## Artefacto oficial

`outputs/dashboard/industrial-ev-operating-command-center.html`

Fichero HTML estático con payload de datos embebido. Chart.js y las fuentes Geist se cargan desde CDN, por lo que la visualización requiere acceso de red.

## Build path

```bash
python -m src.run_pipeline       # pipeline completo
python -m src.ev_build_dashboard # solo dashboard, si los CSVs ya existen
```

## Composición técnica

- **Librería de gráficos**: Chart.js 4.4.3, cargada desde CDN.
- **Tipografía**: Geist (sans) + Geist Mono (numérica), cargadas desde Google Fonts.
- **Gráficos**: 17 canvas Chart.js — throughput, secuencia, patio, carga, expedición, escenarios, priorización OPI y diagnóstico EV/ICE.
- **KPI strip**: 7 indicadores above-the-fold desde `kpi_operativos.csv`.
- **Filtros interactivos**: fecha, turno, propulsión, versión, área, zona patio, zona carga y severidad — aplicados en cliente sin llamadas al servidor.
- **Temas**: light/dark con persistencia en `localStorage`.

## Principios de diseño

- KPIs críticos consumidos desde dataset gobernado (`kpi_operativos.csv`); no se recalculan en el frontend.
- Cero lógica de scoring en el HTML — el scoring ocurre en el pipeline Python.
- Payload JSON embebido en el HTML; las únicas llamadas de red son las dependencias visuales declaradas.
- Build validado con manifest técnico (`dashboard_build_manifest.json`) que verifica tamaño, presencia de contratos de layout y ausencia de placeholders.

## Manifest de build

`outputs/reports/dashboard_build_manifest.json` registra:
- ruta oficial del artefacto
- tamaño en bytes
- checks estructurales (filtros, gráficos, temas, KPI strip)
