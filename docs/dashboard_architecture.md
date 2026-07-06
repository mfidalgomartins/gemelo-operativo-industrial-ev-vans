# Arquitectura del Panel EV

## Artefacto oficial

`outputs/dashboard/industrial-ev-operating-command-center.html`

Fichero HTML estático con carga de datos embebida. Chart.js y las fuentes Geist se cargan desde CDN, por lo que la visualización requiere acceso de red.

## Ruta de construcción

```bash
python -m src.run_pipeline       # canalización completa
python -m src.ev_build_dashboard # solo panel, si los CSVs ya existen
```

## Composición técnica

- **Librería de gráficos**: Chart.js 4.4.3, cargada desde CDN.
- **Tipografía**: Geist (sans) + Geist Mono (numérica), cargadas desde Google Fonts.
- **Gráficos**: 17 canvas Chart.js — caudal, secuencia, patio, carga, expedición, escenarios, priorización OPI y diagnóstico EV/ICE.
- **Banda KPI**: 7 indicadores en primera vista desde `kpi_operativos.csv`.
- **Filtros interactivos**: fecha, turno, propulsión, versión, área, zona patio, zona carga y severidad — aplicados en cliente sin llamadas al servidor.
- **Temas**: claro/oscuro con persistencia en `localStorage`.

## Principios de diseño

- KPI críticos consumidos desde conjunto de datos gobernado (`kpi_operativos.csv`); no se recalculan en el cliente.
- Cero lógica de puntuación en el HTML; la puntuación ocurre en la canalización Python.
- JSON embebido en el HTML; las únicas llamadas de red son las dependencias visuales declaradas.
- Construcción validada con manifiesto técnico (`dashboard_build_manifest.json`) que verifica tamaño, presencia de contratos de diseño y ausencia de marcadores pendientes.

## Manifiesto de construcción

`outputs/reports/dashboard_build_manifest.json` registra:
- ruta oficial del artefacto
- tamaño en bytes
- comprobaciones estructurales (filtros, gráficos, temas, banda KPI)
