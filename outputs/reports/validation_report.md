# Validation Report - Gemelo Operativo EV

- Estado global: **PASS**
- Confianza global: **ALTA**
- Release grade: **decision-support only**
- Issues detectados: **0**
- Checks SQL en WARN: **0**
- Ratio WARN SQL: **0.00%**
- Dashboard presente y materializado: **SI**

## Estados de gobernanza
- technically valid: **YES**
- analytically acceptable: **YES**
- decision-support only: **YES**
- screening-grade only: **NO**
- not committee-grade: **YES**
- publish-blocked: **NO**

## Checklist de validación
- row counts razonables: OK
- duplicados inesperados: OK
- nulls problemáticos: OK
- timestamps imposibles: OK
- secuencias incoherentes: OK
- ocupación patio compatible: OK
- sesiones carga coherentes: OK
- SOC dentro de rango: OK
- EV con carga consistente: OK
- readiness y salida consistentes: OK
- métricas agregadas y denominadores: OK
- consistencia outputs-dashboard: OK
- discriminación de scoring: OK
- diversidad de driver de riesgo: OK
- variabilidad área-turno: OK
- consistencia KPI share_ev: OK
- consistencia KPI readiness: OK
- consistencia KPI delay rate: OK
- single source of truth KPI: OK
- spread de escenarios: OK
- riesgo de sobreinterpretación explicitado: OK

## Issues Found
No se detectaron issues materiales en esta ejecución.

## Caveats Obligatorios
- Dato sintético: útil para arquitectura y lógica, no para benchmark real de planta.
- Las elasticidades del gemelo operativo son supuestos calibrados, no estimación causal.
- La criticidad por área depende de pesos de scoring; revisar sensibilidad antes de uso real.
- No incorpora variabilidad externa real (suministro, clima, huelgas, etc.).

## Overall Confidence Assessment
Confianza **ALTA** para demostración técnica y apoyo a discusión operativa. Para uso real de planta se requiere calibración con datos productivos y validación de negocio adicional.
