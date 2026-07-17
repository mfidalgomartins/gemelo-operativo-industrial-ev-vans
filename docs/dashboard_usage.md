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

Seleccionar una etapa en la espina de flujo equivale a fijar el filtro de área: volver a pulsarla lo libera.

## Lectura recomendada

1. **Lectura ejecutiva**: el veredicto y la cifra que lo sostiene, compuestos con los números del corte.
2. **Espina de flujo**: dónde se acumula la presión a lo largo del recorrido del vehículo, de la línea a la puerta.
3. **Banda KPI**: caudal, brecha frente al plan, salidas retrasadas, preparación, carga, patio y cuota EV, cada uno con su serie y su estado.
4. **Secciones 01–03**: la evidencia detrás del veredicto — flujo y cuota EV, patio y carga, riesgo y expedición.
5. **Tabla de priorización OPI**: áreas ordenadas por riesgo operativo compuesto, con la acción que ataca cada factor.

## Alcance de los indicadores

Sobre el período completo y sin filtros, la banda KPI muestra los valores gobernados de `kpi_operativos.csv`. Al acotar el contexto, los indicadores se recalculan sobre el subconjunto visible y el panel lo indica bajo la banda. Las dos lecturas no son intercambiables: la recalculada opera sobre datos ya agregados y puede diferir de la cifra publicada.

## Datos detrás de cada gráfico

Cada tarjeta expone un botón **Datos** que abre la serie representada en forma de tabla. Es la vía de lectura equivalente cuando el color o el trazo no bastan.

## Trazabilidad

- Manifiesto de construcción: `outputs/reports/dashboard_build_manifest.json`
- Estado de publicación: `outputs/reports/release_readiness.json`
- Validación completa: `outputs/reports/validation_report.md`
