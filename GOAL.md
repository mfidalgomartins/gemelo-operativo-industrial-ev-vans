# Objetivo del Repositorio

## Objetivo activo

Asegurar que el informe, el panel, los informes generados, la documentación y los textos visibles del repositorio estén en español, manteniendo la canalización reproducible y los contratos técnicos intactos.

## Estado de referencia

| Área | Estado |
|---|---|
| Pruebas unitarias | Suite completa en CI |
| Cobertura combinada | Umbral CI mínimo del 85% |
| Artefactos | Panel HTML, paquete de gráficos e informe PDF |
| Datos | Sintéticos, deterministas y regenerables |
| Gobierno | Puerta de publicación, validaciones y contratos de KPI |

## Criterios de finalización

- Documentación principal en español.
- Plantillas GitHub y documentos de comunidad en español.
- Informe PDF generado desde código fuente en español.
- Informes Markdown generados sin cabeceras ni narrativa en inglés.
- Panel HTML con etiquetas, ayudas, filtros y mensajes en español.
- Código funcional sin cambiar nombres de columnas, rutas o contratos que consume la canalización.
- Validación con `ruff`, pruebas relevantes y puerta de publicación.

## Notas de alcance

Los nombres técnicos de columnas, ficheros, paquetes, comandos, claves YAML, rutas y estados contractuales como `PASS` pueden mantenerse cuando forman parte de una API, un estándar o un contrato de datos.

El texto dirigido a lectores, operadores o revisores debe estar en español claro, directo y sin relleno artificial.
