# Uso del Panel Ejecutivo

## Apertura

1. Ejecutar la canalización oficial: `python -m gemelo_operativo_ev.run_pipeline`
2. Abrir `outputs/dashboard/industrial-ev-operating-command-center.html` en el navegador.

O usar el enlace público si el repositorio está desplegado en GitHub Pages (ver README).

## Filtros disponibles

| Filtro | Descripción |
|--------|-------------|
| Fecha desde / hasta | Rango temporal del análisis |
| Turno | A, B o C |
| Propulsión | EV, ICE o ambas |
| Versión | Versión de vehículo específica |
| Área | Área operativa (PATIO, CARGA, EXPEDICION…) |
| Zona patio | Zona geográfica del patio |
| Zona carga | Zona de puntos de carga |
| Severidad | Nivel de criticidad del evento |

Los filtros se aplican en el cliente sobre la carga de datos embebida; no requieren conexión al servidor.

## Lectura recomendada

1. **Banda KPI** (arriba del todo): caudal productivo, cuota EV, preparación global, tiempo de patio y carga.
2. **Tabla de priorización OPI**: áreas ordenadas por riesgo operativo compuesto.
3. **Gráficos de flujo**: caudal diario, cuota EV semanal y tiempos internos por propulsión.
4. **Comparador de escenarios**: palancas de transición ordenadas por puntuación de decisión.
5. **Diagnóstico EV vs ICE**: diferencial de presión por factor operativo.

## Trazabilidad

- Manifiesto de construcción: `outputs/reports/dashboard_build_manifest.json`
- Estado de publicación: `outputs/reports/release_readiness.json`
- Validación completa: `outputs/reports/validation_report.md`
