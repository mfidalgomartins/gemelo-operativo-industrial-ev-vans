# Memo Ejecutivo - Transición EV

## Resumen ejecutivo
- Throughput diario medio actual: **30.62 unidades/día**.
- Cumplimiento SLA actual: **100.0%**.
- Score de readiness medio: **33.54**.
- Score de riesgo medio: **57.28**.

## Hallazgos operativos
- El sistema identifica una relación no lineal entre mix EV, cola de carga y saturación de patio.
- El cuello de botella dominante se desplaza de secuenciación pura a coordinación carga-expedición durante ramp-up EV.
- La variabilidad energética penaliza especialmente turnos con alta acumulación EV en espera de salida.

## Escenarios
- Escenario con mayor riesgo: **EV70_sin_refuerzo** (riesgo 76.36).
- Escenario más robusto en readiness: **Base actual** (readiness 63.79).

## Recomendaciones prioritarias
1. Aplicar lógica de secuenciación EV condicionada por slots de carga y capacidad de expedición del turno objetivo.
2. Elevar capacidad de carga (física y energética) antes de superar un mix EV de 70%.
3. Implementar reglas de liberación de patio basadas en score de prioridad de despacho.
4. Operar un control tower diario con seguimiento de riesgo de cuello de botella por turno.