# Puertas de Publicación y Niveles de Uso

## Objetivo
Evitar falsa confianza y clasificar explícitamente la calidad del resultado antes de usarlo para decisiones.

## Estados obligatorios
- `technically valid`: integridad técnica mínima superada (sin problemas críticos, artefactos principales presentes y cero alertas SQL).
- `analytically acceptable`: señales con discriminación suficiente, consistencia KPI y escenarios defendibles.
- `decision-support only`: solo apoyo a decisión; apto para priorización operativa inicial con advertencias explícitas.
- `screening-grade only`: solo screening; útil para triaje y exploración, no para compromisos de capacidad.
- `not committee-grade`: no apto para comité de inversión/transformación.
- `publish-blocked`: publicación bloqueada.

## Reglas de clasificación
Fuente de verdad: `outputs/reports/release_readiness.json`.

1. `publish-blocked`:
   - existe al menos un problema `critica`, o
   - artefactos técnicos clave ausentes, o
   - ratio de alertas SQL fuera de umbral técnico.
2. `screening-grade only`:
   - técnicamente válido pero no analíticamente aceptable.
3. `decision-support only`:
   - técnicamente válido y analíticamente aceptable, con riesgo residual controlado.
El máximo nivel permitido para este repositorio sintético es `decision-support only`.

## Comprobaciones bloqueantes (mínimo)
- unicidad de claves críticas (`orden_id`).
- orden temporal coherente.
- sesiones de carga válidas.
- SOC en rango.
- ninguna salida real sin preparación.
- panel oficial consistente y con manifiesto en estado OK.
- versión y hash SHA-256 del panel idénticos en el manifiesto y en la validación de publicación.

## Comprobaciones de aviso (operacionales/metodológicas)
- sobre-ocupación de patio.
- sensibilidad de puntuación poco reactiva.
- dispersión baja de escenarios.

## Disciplina de publicación
Antes de publicación:
1. Ejecutar canalización EV completa.
2. Ejecutar validación EV.
3. Confirmar que `release_grade` no sea `publish-blocked`.
4. Revisar `outputs/reports/pipeline_audit/validation_report.md` y `outputs/reports/release_readiness.json`.
