# Data Contracts and Production Caveats

Referencia curta para schema, granularidade e limites de uso.

## Raw schema oficial

Fonte canonica: `data/raw/ev_factory/`.

| Tabela | Grao | Chave pratica | Conteudo |
|---|---|---|---|
| `ordenes` | ordem | `orden_id` | plano/real, turno, sequencia, prioridade, mercado, readiness inicial |
| `vehiculos` | veiculo | `vehiculo_id` | timestamps do fluxo fisico, versao, SOC saida |
| `versiones_vehiculo` | versao | `version_id` | familia, propulsao, bateria, complexidade, flags EV |
| `estado_bateria` | leitura bateria | `vehiculo_id`, `timestamp` | SOC, target SOC, estado de carga, energia |
| `slots_carga` | slot | `slot_id` | zona, potencia, disponibilidade, manutencao |
| `sesiones_carga` | sessao carga | `sesion_id` | inicio/fim, energia, espera, interrupcao |
| `patio` | snapshot patio | `vehiculo_id`, `timestamp` | zona, posicao, dwell, bloqueio, movimento requerido |
| `movimientos_patio` | movimento | `movimiento_id` | origem/destino, motivo, operador, movimento improdutivo |
| `turnos` | dia-turno | `fecha`, `turno` | headcount, absentismo, produtividade, pressao |
| `logistica_salida` | saida | `salida_id` | plano/real de expedicao, transportista, readiness, atraso |
| `cuellos_botella` | evento | `evento_id` | area, severidade, duracao, impactos proxy |
| `recursos_operativos` | recurso | `recurso_id` | capacidade nominal/disponivel e restricao atual |
| `restricciones_operativas` | restricao | `restriccion_id` | janela, area, tipo, severidade, impacto capacidade |
| `escenarios_transicion` | dia | `fecha` | share EV, intensidade ramp-up, pressao patio/carga/logistica |

## Processed schema principal

Fonte: `data/processed/ev_factory/`.

| Artefacto | Grao | Uso principal |
|---|---|---|
| `vw_vehicle_flow_timeline.csv` | ordem/veiculo | fluxo completo fim de linha -> patio -> carga -> saida |
| `vw_charging_utilization.csv` | data-turno-zona-slot | utilizacao, filas, interrupcoes e gap SOC |
| `vw_yard_congestion.csv` | hora-zona patio | ocupacao, dwell, bloqueio e risco operacional |
| `vw_dispatch_readiness.csv` | veiculo | readiness final, atraso, causa, SOC e risco de expedicao |
| `vw_shift_bottleneck_summary.csv` | data-turno-area | eventos de cuello, severidade e impacto |
| `mart_vehicle_day.csv` | veiculo-dia | mart analitico para features veiculo |
| `mart_area_shift.csv` | data-turno-area | mart tatico de stress operacional |
| `mart_dispatch_readiness.csv` | data-turno-propulsao-versao | readiness e delay por segmento |
| `kpi_operativos.csv` | snapshot unico | source of truth dos KPI executivos |
| `vehicle_readiness_features.csv` | veiculo | inputs de scoring e diagnostico |
| `area_shift_features.csv` | data-turno-area | inputs de OPI por area |
| `charging_features.csv` | data-turno-zona-slot | pressao de carga |
| `yard_features.csv` | hora-zona patio | saturacao de patio |
| `launch_transition_features.csv` | semana | pressao de transicao EV |
| `operational_prioritization_table.csv` | area | OPI, driver principal, tier e acao recomendada |
| `scenario_table.csv` | cenario | simulacao parametrica de 8 cenarios |
| `validation_checks.csv` | check | validacoes SQL de negocio |

Definicoes detalhadas: `docs/sql_metric_definitions.md` e `docs/feature_dictionary.md`.

## Contratos de qualidade

- `orden_id` deve ser unico.
- Sequencias por `fecha_turno_operativo`, `turno`, `secuencia_planeada` nao devem duplicar.
- Timestamps de fluxo nao podem andar para tras.
- `soc_pct` e `target_soc_pct` ficam em `[0, 100]`.
- Sessao de carga requer `fin_sesion >= inicio_sesion` e energia positiva.
- Veiculo com saida real nao pode ter `readiness_salida_flag = 0`.
- KPI criticos do dashboard devem vir de `kpi_operativos.csv`.
- Dashboard oficial deve ser unico em `outputs/dashboard/`.

## Arquitetura em uma pagina

```text
data/raw/ev_factory/*.csv
        |
        v
DuckDB SQL scripts em sql/ev_factory/
        |
        v
data/processed/gemelo_operativo_ev.duckdb
data/processed/ev_factory/{views,marts,kpi}.csv
        |
        v
Python analytics: features -> diagnostico -> cenarios -> scoring
        |
        v
Dashboard HTML, graficos PNG, PDF, validation report, release gate
```

## Caveats para producao

- Dados atuais sao sinteticos; nao usar para compromissos operacionais sem calibracao com historico real.
- Elasticidades dos cenarios sao parametros, nao estimativas causais.
- `area_throughput_loss_proxy` atribui impacto por evento observado; nao mede causalidade incremental.
- O OPI e interpretavel, mas depende de pesos e thresholds que devem ser aprovados por operacoes.
- O dashboard e estatico; nao tem autenticacao, backend, lineage runtime nem refresh incremental.
- Dependencias visuais do dashboard usam CDN; ambientes fechados precisam vendorizar Chart.js e fontes.
- A execucao completa sobrescreve CSV/relatorios em `data/processed/` e `outputs/`.
- Antes de uso real, adicionar contratos de PII, controlo de acesso, observabilidade, versionamento de datasets e testes de reconciliacao com sistemas fonte.
