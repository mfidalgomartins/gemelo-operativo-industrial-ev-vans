# Registro de Cambios

Todos los cambios relevantes del proyecto se documentan aquí. El formato sigue la estructura de Keep a Changelog y el versionado sigue SemVer.

## [1.0.2] - 2026-07-06

### Cambiado

- Consolidación completa a español: documentación principal, plantillas de
  comunidad de GitHub, flujo de trabajo de CI, metadatos de `pyproject.toml`
  y `CITATION.cff`, comentarios de código, pruebas e informe PDF generado
  (`scripts/generate_report.py`). Los nombres técnicos de columnas, rutas,
  paquetes, comandos y estados contractuales (`PASS`, etc.) se mantienen sin
  cambios para no romper la canalización ni los contratos de datos.
- El informe PDF se corrigió en dos pasadas de diseño: las tablas ya no se
  dividen entre páginas dejando filas huérfanas (`KeepTogether`), los
  encabezados del apéndice quedan unidos a su tabla, y las últimas fugas de
  texto sin traducir en el apéndice (causa de cuello de botella, palancas de
  capacidad, patrón de sensibilidad Monte Carlo) quedan traducidas de forma
  consistente con el resto del documento.
- Corregido un error de agregación en `p95_dwell` a nivel de zona de patio
  que divergía silenciosamente entre el informe y el paquete de gráficos.

## [1.0.1] - 2026-06-26

### Cambiado

- Se dejó de versionar el binario reconstruible `data/processed/gemelo_operativo_ev.duckdb`.
- Los CSV de marts siguen siendo las salidas canónicas versionadas y la base DuckDB se regenera con la canalización.

### Añadido

- Documentación de comunidad: `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, `CITATION.cff`, plantillas de incidencias y plantilla de pull request.
- Umbral de cobertura en CI: el trabajo de integración ejecuta la suite completa con `--cov-fail-under=85`.
- Insignias de CI, licencia, Python y cobertura en el README.
- Ganchos de `pre-commit` para `ruff check`, `ruff format` y control de ficheros grandes.
- Dependabot semanal para dependencias `pip` y GitHub Actions.

## [1.0.0] - 2026-06-24

### Añadido

- Primera versión pública del gemelo operativo digital para transición EV.
- Generador determinista de datos sintéticos: órdenes, activos, restricciones y eventos operativos.
- Capa SQL DuckDB con preparación, vistas integradas, marts, KPI y validaciones de negocio.
- Capa analítica en Python: variables, diagnóstico, gemelo de escenarios, OPI, sensibilidad de pesos y estabilidad Monte Carlo.
- Puerta de publicación con contratos de datos, consistencia de KPI y contratos del panel.
- Entregables publicados: paquete de 19 gráficos, panel HTML interactivo e informe PDF analítico.
- CI con análisis estático, formato, pruebas multiversión, empaquetado, integración y seguridad.
