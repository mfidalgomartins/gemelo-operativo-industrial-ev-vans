# Guía de Contribución

Gracias por mejorar el gemelo operativo EV. Este repositorio es una canalización analítica reproducible, así que el criterio principal es simple: la ejecución debe seguir siendo determinista, las pruebas deben pasar y los artefactos publicados deben poder regenerarse desde el código fuente.

## Configuración local

```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install -e ".[dev,service]"  # añadir ".[security]" para los escáneres
```

Python soportado: **3.10-3.12** en CI.

Opcionalmente, instalar hooks locales para ejecutar las mismas comprobaciones que CI:

```bash
pre-commit install
```

## Canalización

```bash
ev-twin run --generate-data --seed 20260328 --months 12
ev-twin artifacts
ev-twin release-check
```

Los CSV de `data/`, el fichero DuckDB y `.ev_twin/` son estado reconstruible y no se versionan. Los artefactos visuales publicados se actualizan solo después de superar la puerta de publicación.

## Calidad

```bash
ruff check .
ruff format --check .
pytest -q
pytest -m integration                 # canalización completa; escribe en data/ y outputs/
```

- Las pruebas unitarias no deben escribir fuera de `tmp_path`.
- Si una prueba ejecuta la canalización completa o toca `data/`, marcarla con `@pytest.mark.integration`.
- Mantener la cobertura combinada por encima del umbral de CI: **85%**.
- Nuevos auxiliares analíticos deben incluir pruebas unitarias para su lógica pura.

## Commits y PRs

- Usar asuntos imperativos y claros, por ejemplo `fix(informe): ...` o `docs(ci): ...`.
- Mantener artefactos generados, gráficos, informe y marts sincronizados con el código que los produce.
- Completar la plantilla de pull request con cambios, verificación y límites.

## Límites de datos

Este proyecto usa datos sintéticos. Toda figura debe poder reproducirse desde la semilla canónica `20260328`. No introducir datos reales de planta, secretos ni dependencias de red en la canalización.
