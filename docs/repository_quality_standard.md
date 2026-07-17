# Estándar de Calidad del Repositorio

## Objetivo

Mantener un gemelo operativo reproducible, auditable y defendible en una entrevista técnica, con separación explícita entre datos sintéticos, supuestos paramétricos y resultados observados en la simulación.

## Criterios de aceptación

- La generación sintética es determinista para una semilla y un horizonte dados.
- La capa SQL valida claves, calendario operativo, balances de patio, capacidad física y coherencia temporal.
- KPI, escenarios y puntuaciones declaran fórmula, grano, denominador y límites de uso.
- La publicación queda bloqueada ante fallos críticos o artefactos que no coincidan con su manifiesto.
- Lint, formato, pruebas, cobertura combinada, empaquetado y escáneres de seguridad pasan en CI.
- Panel, gráficos e informe se regeneran desde código sin cambios manuales sobre los artefactos.
- La ingesta conectada valida contratos, soporta incremental idempotente y registra checkpoint y linaje.
- Cada ejecución deja duración por etapa, resultado de SLA y error tipado sin exponer secretos.
- El wheel contiene SQL y plantilla del panel, se instala en un entorno aislado y expone CLI estables.
- La API opcional aplica autenticación fail-closed, RBAC, contratos de respuesta y límites de exposición.
- La calibración solo se activa con cobertura completa, soporte identificador y coeficientes aprobados.
- Datos y bases reconstruibles quedan fuera de Git; el portfolio conserva solo artefactos finales de revisión.
- La documentación es breve, específica y coherente con el comportamiento ejecutable.

## Límite de uso

El máximo nivel de decisión permitido es `decision-support only`. Los datos sintéticos y las elasticidades paramétricas no sustentan compromisos de inversión ni inferencia causal sin calibración externa.
