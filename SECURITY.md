# Seguridad

## Alcance

Este repositorio es un proyecto analítico de apoyo a decisión construido íntegramente con datos sintéticos. No contiene sistemas productivos, datos operativos reales ni secretos. La superficie principal de seguridad es la cadena de suministro de software y el código que genera los artefactos publicados.

CI ejecuta dos comprobaciones automatizadas en cada push y pull request:

- **bandit**: análisis estático de seguridad sobre `src/`.
- **pip-audit**: auditoría estricta de vulnerabilidades de dependencias.

## Versiones soportadas

La última versión en `main` es la única soportada.

| Versión | Soporte |
|---|---|
| `main` | Sí |
| Otras ramas | No |

## Reporte de vulnerabilidades

Si encuentras un problema de seguridad, por ejemplo una dependencia vulnerable, una ruta de código explotable o exposición accidental de secretos:

1. No abras una incidencia pública para nada explotable.
2. Usa el reporte privado de vulnerabilidades de GitHub en la pestaña **Security**, o contacta al mantenedor mediante el correo del perfil de GitHub.
3. Incluye pasos de reproducción y la versión o commit afectado.
