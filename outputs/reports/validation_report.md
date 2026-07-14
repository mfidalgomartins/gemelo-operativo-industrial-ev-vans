# Informe de Validación - Gemelo Operativo EV

- Estado global: **PASS**
- Confianza global: **ALTA**
- Grado de publicación: **solo apoyo a decisión**
- Problemas detectados: **0**
- Comprobaciones SQL en alerta: **0**
- Ratio de alertas SQL: **0.00%**
- Panel presente y materializado: **Sí**

## Estados de gobernanza
- técnicamente válido: **Sí**
- analíticamente aceptable: **Sí**
- solo apoyo a decisión: **Sí**
- solo exploración inicial: **No**
- no apto para comité: **Sí**
- publicación bloqueada: **No**

## Lista de validación
- conteos de filas razonables: OK
- duplicados inesperados: OK
- nulos problemáticos: OK
- marcas temporales imposibles: OK
- secuencias incoherentes: OK
- ocupación patio compatible: OK
- sesiones carga coherentes: OK
- SOC dentro de rango: OK
- EV con carga consistente: OK
- preparación y salida consistentes: OK
- métricas agregadas y denominadores: OK
- consistencia outputs-panel: OK
- discriminación de puntuación: OK
- diversidad de factor de riesgo: OK
- variabilidad área-turno: OK
- consistencia KPI cuota EV: OK
- consistencia KPI de preparación: OK
- consistencia KPI de tasa de atraso: OK
- fuente única de verdad KPI: OK
- dispersión de escenarios: OK
- riesgo de sobreinterpretación explicitado: OK

## Problemas Encontrados
No se detectaron problemas materiales en esta ejecución.

## Advertencias Obligatorias
- Dato sintético: útil para arquitectura y lógica, no para comparación real de planta.
- Las elasticidades del gemelo operativo son supuestos paramétricos no calibrados, no estimaciones causales.
- La criticidad por área depende de pesos de puntuación; revisar sensibilidad antes de uso real.
- No incorpora variabilidad externa real (suministro, clima, huelgas, etc.).

## Evaluación Global de Confianza
Confianza **ALTA** para demostración técnica y apoyo a discusión operativa. Para uso real de planta se requiere calibración con datos productivos y validación de negocio adicional.
