# Seguridad

## Alcance

El corte publicado usa datos sintéticos y no contiene secretos. El paquete admite, de forma opcional, fuentes HTTP/CSV y una API de lectura; por eso la superficie incluye cadena de suministro, configuración de conectores, autenticación, exposición de artefactos y ejecución de la canalización.

CI ejecuta dos comprobaciones automatizadas en cada push y pull request:

- **bandit**: análisis estático sobre todo el paquete Python.
- **pip-audit**: auditoría estricta de vulnerabilidades de dependencias.

## Controles implementados

- Los conectores HTTP de producción exigen HTTPS, host permitido, rutas relativas, token por variable de entorno, límites de respuesta, timeout y paginación acotada. No siguen redirecciones.
- Los contratos de origen bloquean columnas inesperadas, claves nulas o duplicadas y timestamps inválidos antes de publicar datos.
- La API falla cerrada si faltan credenciales, separa roles `viewer` y `operator`, valida `Host`, limita el tamaño de petición y desactiva CORS y documentación interactiva en producción.
- Los tokens se comparan en tiempo constante, no se registran y deben ser distintos y de al menos 32 caracteres.
- Locks de ejecución impiden escrituras concurrentes; manifiestos y checkpoints se escriben de forma atómica.

## Responsabilidades de despliegue

- Entregar tokens desde un gestor de secretos y rotarlos según la política del entorno.
- Desplegar la API detrás de TLS, HSTS y rate limiting. El servidor no confía en cabeceras de proxy por defecto.
- Restringir permisos sobre `EV_TWIN_HOME`, el fichero de conectores y `.ev_twin/`.
- Clasificar y minimizar cualquier dato real antes de activar integraciones; este repositorio no define una política universal de PII.
- No habilitar `--allow-http-development` fuera de un entorno local aislado.

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
