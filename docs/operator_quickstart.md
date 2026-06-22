# Operator Quickstart

Guia curto para instalar, regenerar o snapshot analitico, validar e localizar outputs.

## Requisitos

- Python 3.10+
- Acesso de escrita a `data/processed/` e `outputs/`
- Rede apenas para visualizar o dashboard com fontes/Chart.js via CDN

## Instalação

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

## Execução recomendada

```bash
generate-data --seed 20260328 --start-date 2025-01-01 --months 12
python -m src.run_pipeline
python scripts/generate_chart_pack.py
python scripts/generate_report.py
python -m src.ev_release_gate
```

`python -m src.run_pipeline` usa os CSV raw existentes por defeito. Para regenerar raw dentro do pipeline a partir de Python, usar `run_pipeline(generate_data=True, seed=20260328, months=12)`.

## Ordem real do pipeline

1. `explore_data_audit`: auditoria dos CSV raw.
2. `ev_sql_layer`: carrega 14 CSV raw em DuckDB, executa 11 scripts SQL e exporta marts.
3. `ev_feature_engineering`: cria features de readiness, area-turno, carga, patio e transição.
4. `ev_diagnostic_analysis`: calcula scores diagnosticos e rankings.
5. `ev_scenario_twin`: simula 8 cenarios parametricos.
6. `ev_scoring_framework`: calcula OPI, sensibilidade e Monte Carlo.
7. `ev_build_dashboard`: gera o HTML oficial unico.
8. `ev_validate_project`: gera validacao e release readiness.
9. `ev_release_gate`: aprova/bloqueia publicacao com base nos artefactos anteriores.

## Outputs esperados

| Artefacto | Caminho | Uso |
|---|---|---|
| Base DuckDB | `data/processed/gemelo_operativo_ev.duckdb` | Debug SQL local |
| Marts/features CSV | `data/processed/ev_factory/*.csv` | Consumo analitico e dashboard |
| Dashboard oficial | `outputs/dashboard/industrial-ev-operating-command-center.html` | Interface executiva estatica |
| Chart pack | `outputs/graphs/*.png` | Graficos para relatorio |
| Relatorio PDF | `outputs/reports/ev_transition_operating_twin_report.pdf` | Narrativa analitica |
| Manifest dashboard | `outputs/reports/dashboard_build_manifest.json` | Contratos de UI/build |
| Release readiness | `outputs/reports/release_readiness.json` | Estado de publicacao |
| Sumario pipeline | `outputs/reports/pipeline_run_summary.json` | Resultado agregado da execução |

## Testes e qualidade

```bash
ruff check .
ruff format --check .
pytest -q
pytest -q -m integration
```

Notas:
- `pytest -q` exclui testes de integração por defeito.
- `pytest -q -m integration` escreve em `data/` e `outputs/`.
- `tests/test_ev_governance.py` regenera dados e restaura o snapshot canonico no fim.

## Comandos parciais uteis

```bash
python -m src.ev_sql_layer
python -m src.ev_feature_engineering
python -m src.ev_diagnostic_analysis
python -m src.ev_scenario_twin
python -m src.ev_scoring_framework
python -m src.ev_build_dashboard
python -m src.ev_validate_project
python -m src.ev_release_gate
```

Executar comandos parciais apenas quando as entradas anteriores ja existem. Exemplo: `ev_build_dashboard` requer CSV processados como `vw_vehicle_flow_timeline.csv`, `charging_features.csv`, `yard_features.csv`, `operational_prioritization_table.csv` e `scenario_table.csv`.

## Troubleshooting rapido

- `FileNotFoundError` em raw: confirmar os 14 CSV em `data/raw/ev_factory/`.
- Dashboard sem estilos/graficos: abrir com rede disponivel, porque Chart.js e fontes usam CDN.
- Release gate falha: rever `outputs/reports/validation_report.md`, `validation_issues_found.csv` e `dashboard_build_manifest.json`.
- Outputs nao deterministas: usar seed canonica `20260328`; a layer SQL força DuckDB com `PRAGMA threads=1`.
