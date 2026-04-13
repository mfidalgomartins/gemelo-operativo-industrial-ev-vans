# Validation Report - Gemelo Operativo EV

- Estado global: **PASS**
- Confianza global: **ALTA**
- Release grade: **decision-support only**
- Issues detectados: **2**
- Checks SQL en WARN: **2**
- Ratio WARN SQL: **20.00%**
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
- ocupación patio compatible: WARN
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
- single source of truth KPI: OK
- spread de escenarios: OK
- riesgo de sobreinterpretación explicitado: OK

## Issues Found
| check | severity | failed_rows | detail | fix_applied |
| --- | --- | --- | --- | --- |
| salida_sin_readiness | medium | 2208 | Salidas reales sin readiness (rate=6.48%) | Bloqueo en lógica de expedición o excepción trazable por causa |
| ocupacion_patio_vs_capacidad | medium | 99 | Ocupaciones por encima de capacidad estimada | Ajustar buffers y zonas dinámicas |

## Fixes Applied
- Fallback markdown sin `tabulate` en auditoría /explore-data.
- Capa SQL dedicada `ev_factory` separada del pipeline legacy.
- Corrección de rutas de escritura en feature engineering y dashboard.

## Caveats Obligatorios
- Dato sintético: útil para arquitectura y lógica, no para benchmark real de planta.
- Las elasticidades del gemelo operativo son supuestos calibrados, no estimación causal.
- La criticidad por área depende de pesos de scoring; revisar sensibilidad antes de uso real.
- No incorpora variabilidad externa real (suministro, clima, huelgas, etc.).

## Overall Confidence Assessment
Confianza **ALTA** para uso de portfolio y discusión técnica/operativa. Para uso real de planta se requiere calibración con datos productivos y validación de negocio adicional.