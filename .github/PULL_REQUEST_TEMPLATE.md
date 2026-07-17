## Qué

Descripción breve del cambio y de la pregunta de decisión o incidencia que resuelve.

## Por qué

Contexto y motivación. Enlazar cualquier incidencia relacionada (`Closes #...`).

## Verificación

- [ ] `ruff check .` limpio
- [ ] `ruff format --check .` limpio
- [ ] `pytest` verde
- [ ] `pytest -m integration` verde si cambia la canalización o los artefactos
- [ ] Cobertura igual o superior al umbral de CI (85%)
- [ ] Artefactos generados, gráficos, informe y marts regenerados si aplica

## Notas

Aspectos que los revisores deban saber: compensaciones, seguimientos o elementos fuera de alcance.
