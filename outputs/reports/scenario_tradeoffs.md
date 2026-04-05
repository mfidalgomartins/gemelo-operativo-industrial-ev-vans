# Trade-offs de Escenarios - Gemelo Operativo EV

## Lectura general
- Escalar EV sin medidas correctivas desplaza el cuello hacia carga y patio.
- La combinación de secuenciación + carga + patio mejora simultáneamente throughput y estabilidad.
- Bajo presión logística, el riesgo de expedición crece más rápido que la pérdida de throughput.

## Trade-offs principales
- Acelerar EV sin refuerzo incrementa congestión y espera de carga.
- Mejor secuenciación reduce tiempo interno, pero no elimina riesgo si falta capacidad de carga.
- Expandir patio estabiliza picos, pero sin disciplina de salida puede cronificar inventario interno.

## Ranking de palancas
- capacidad_carga: impacto esperado 0.37
- secuenciacion_ev: impacto esperado 0.31
- gestion_patio: impacto esperado 0.29
- disciplina_expedicion: impacto esperado 0.22
- resiliencia_turno: impacto esperado 0.20