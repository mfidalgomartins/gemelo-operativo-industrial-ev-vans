# Release Gates y Niveles de Uso

## Objetivo
Evitar falsa confianza y clasificar explícitamente la calidad del resultado antes de usarlo para decisiones.

## Estados obligatorios
- `technically valid`: integridad técnica mínima superada (sin issues críticos, artefactos principales presentes y cero WARN SQL).
- `analytically acceptable`: señales con discriminación suficiente, consistencia KPI y escenarios defendibles.
- `decision-support only`: apto para priorización operativa inicial con caveats explícitos.
- `screening-grade only`: útil para triage y exploración, no para compromisos de capacidad.
- `not committee-grade`: no cumple exigencia de comité de inversión/transformación.
- `publish-blocked`: no publicar como versión final.

## Reglas de clasificación
Fuente de verdad: `outputs/reports/release_readiness.json`.

1. `publish-blocked`:
   - existe al menos un issue `critical`, o
   - artefactos técnicos clave ausentes, o
   - ratio de WARN SQL fuera de umbral técnico.
2. `screening-grade only`:
   - técnicamente válido pero no analíticamente aceptable.
3. `decision-support only`:
   - técnicamente válido y analíticamente aceptable, con riesgo residual controlado.
El máximo nivel permitido para este repositorio sintético es `decision-support only`.

## Blocking checks (mínimo)
- unicidad de claves críticas (`orden_id`).
- orden temporal coherente.
- sesiones de carga válidas.
- SOC en rango.
- ninguna salida real sin readiness.
- dashboard oficial consistente y con manifest en PASS.

## Warn checks (operacionales/metodológicos)
- sobre-ocupación de patio.
- sensibilidad de scoring poco reactiva.
- spread bajo de escenarios.

## Disciplina de release
Antes de publicación:
1. Ejecutar pipeline EV completo.
2. Ejecutar validación EV.
3. Confirmar `release_grade != publish-blocked`.
4. Revisar `outputs/reports/validation_report.md` y `outputs/reports/release_readiness.json`.
