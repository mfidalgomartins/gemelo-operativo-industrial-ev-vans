# Uso del Dashboard Ejecutivo

## Apertura

1. Ejecutar pipeline oficial: `python -m src.run_pipeline`
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
| Zona carga | Zona de slots de carga |
| Severidad | Nivel de criticidad del evento |

Los filtros se aplican en cliente sobre el payload embebido; no requieren conexión al servidor.

## Lectura recomendada

1. **KPI strip** (arriba del todo): throughput, share EV, readiness global, tiempo de patio y carga.
2. **Tabla de priorización OPI**: áreas ordenadas por riesgo operativo compuesto.
3. **Gráficos de flujo**: throughput diario, mix EV semanal, lead times por propulsión.
4. **Comparador de escenarios**: palancas de transición rankeadas por decision score.
5. **Diagnóstico EV vs ICE**: diferencial de presión por driver operativo.

## Trazabilidad

- Manifest de build: `outputs/reports/dashboard_build_manifest.json`
- Estado de release: `outputs/reports/release_readiness.json`
- Validación completa: `outputs/reports/validation_report.md`
