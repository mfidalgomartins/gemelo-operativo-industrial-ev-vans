# Governance e Disciplina de Release (EV)

## Fonte de verdade
- KPI oficiales: `data/processed/ev_factory/kpi_operativos.csv`
- Validación integral: `outputs/reports/validation_report.md`
- Estado de release: `outputs/reports/release_readiness.json`
- Dashboard oficial: `outputs/dashboard/industrial-ev-operating-command-center.html`

## Niveles de uso
- `technically valid`
- `analytically acceptable`
- `decision-support only`
- `screening-grade only`
- `not committee-grade`
- `publish-blocked`

## Regla de publicación
Se publica únicamente si:
1. `release_grade != publish-blocked`
2. existe dashboard oficial único
3. no hay issues críticos abiertos en validación

## Riesgo de sobreinterpretación (obligatorio)
- Dataset sintético: no usar como benchmark real de planta.
- Escenarios: simulación paramétrica, no inferencia causal.
- Scoring: depende de pesos; revisar sensibilidad antes de decisión real.

## Checklist de release
1. `python -m src.run_pipeline`
2. Revisar `outputs/reports/release_readiness.json`
3. Confirmar coherencia de hallazgos en dashboard + memo ejecutivo
4. Publicar solo artefactos oficiales
