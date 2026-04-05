# Lógica del Generador Sintético Industrial

## Objetivo
Construir un ecosistema de datos operativo realista para transición ICE->EV en entorno de fabricación de vans, incluyendo secuenciación, patio, carga, logística de salida, restricciones y cuellos de botella.

## Diseño general
El generador opera por dominios:
1. Planificación de transición (`escenarios_transicion`).
2. Condiciones de operación por turno (`turnos`).
3. Restricciones físicas/operativas (`restricciones_operativas`).
4. Datos maestros de producto y recursos (`versiones_vehiculo`, `slots_carga`, `recursos_operativos`).
5. Operación unitaria completa (`ordenes`, `vehiculos`, `estado_bateria`, `sesiones_carga`, `patio`, `movimientos_patio`, `logistica_salida`).
6. Detección de eventos de saturación (`cuellos_botella`).
7. Validación de plausibilidad y cardinalidades.

## Comportamientos simulados
- Evolución diaria de share EV con fases: pre-lanzamiento, pre-serie, ramp-up y estable.
- Variación de headcount, absentismo y productividad por turno A/B/C.
- Secuencias real vs plan con desviaciones por presión operativa.
- Tiempo de producción sensible a complejidad de versión y restricciones.
- Colas de carga EV por disponibilidad de slots, potencia efectiva y saturación.
- Interrupciones de sesión de carga por incidencias operativas.
- Dwell en patio, bloqueos y movimientos no productivos.
- Retrasos de salida por readiness, congestión y restricciones logísticas.
- Eventos de cuello de botella detectados por señales operativas observadas.

## Reproducibilidad
- Uso de seed configurable.
- Parámetros fijos por dominio y distribución controlada.
- Salida determinista dado `seed`, `start_date` y `months`.

## Validación integrada
Se ejecutan comprobaciones de:
- columnas obligatorias por tabla,
- horizonte temporal de 9-15 meses,
- unicidad e integridad referencial,
- rangos de SOC,
- progresión de share EV,
- plausibilidad de esperas de carga y retrasos de salida.

## Salidas
- CSV por tabla en `data/raw/`.
- Reportes de plausibilidad y cardinalidades en `outputs/reports/`.
