# Operación en Producción

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install ".[service]"
```

Fijar `EV_TWIN_HOME` a un directorio persistente con permisos de escritura. Los secretos se entregan exclusivamente mediante variables de entorno o un gestor de secretos; nunca se incluyen en JSON, argumentos de proceso o Git.

## Ingesta conectada

El fichero JSON de conectores exige `schema_version: 1` y una entrada para `mes`, `wms`, `ems`, `planning` y `master_data`. Cada sistema admite:

- `csv_directory`: directorio de entrega controlado.
- `http_json`: API HTTPS paginada con host permitido y token referenciado por nombre de variable de entorno.

Ejemplo mínimo para un sistema CSV:

```json
{
  "kind": "csv_directory",
  "source_dir": "/srv/ev-twin/drop/mes",
  "name": "mes_csv"
}
```

Ejemplo mínimo para un sistema HTTP:

```json
{
  "kind": "http_json",
  "base_url": "https://integration.example.net/api/",
  "allowed_hosts": ["integration.example.net"],
  "token_env": "EV_TWIN_MES_TOKEN",
  "endpoints": {"ordenes": "v1/orders", "vehiculos": "v1/vehicles"}
}
```

Los endpoints son rutas relativas; no se siguen redirecciones. En producción se exige TLS, allowlist de host, token de al menos 32 caracteres, timeout, límite de respuesta y máximo de páginas.

```bash
ev-twin ingest --config /etc/ev-twin/connectors.json --mode full
ev-twin ingest --config /etc/ev-twin/connectors.json --mode incremental
```

El incremental parte del último watermark confirmado y relee un día para absorber correcciones tardías. La fusión conserva el último registro por clave primaria y es idempotente. Si una tabla falla el contrato, no se actualizan checkpoints; la siguiente ejecución recupera el lote.

## Ejecución y scheduling

```bash
ev-twin run
ev-twin release-check
ev-twin status
```

El scheduler externo debe lanzar una sola instancia, respetar el código de salida y alertar si `status != PASS` o `sla.status != PASS` en `.ev_twin/observability/latest_pipeline_run.json`. El lock local evita solapamientos accidentales. El SLA máximo se configura con `EV_TWIN_SLA_MAX_RUN_SECONDS`; el valor por defecto es 300 segundos.

Política recomendada:

1. ingesta incremental;
2. canalización completa;
3. puerta de publicación;
4. publicación de artefactos solo con código de salida cero;
5. retención externa de logs y manifiestos según la política del entorno.

## Calibración

El comando `ev-twin calibrate` recibe el contrato largo documentado en [scenario_model.md](scenario_model.md). La estimación solo se aprueba si todas las métricas tienen soporte, rango completo de palancas, grados de libertad, rango matricial y número mínimo de unidades.

```bash
ev-twin calibrate \
  --input /srv/ev-twin/calibration/observations.csv \
  --output /srv/ev-twin/calibration/scenario_coefficients.csv

export EV_TWIN_CALIBRATION_FILE=/srv/ev-twin/calibration/scenario_coefficients.csv
ev-twin run
```

Sin `EV_TWIN_CALIBRATION_FILE`, el gemelo utiliza priors paramétricos y conserva el grado `decision-support only`.

## API y RBAC

Variables obligatorias:

```bash
export EV_TWIN_ENV=production
export EV_TWIN_TRUSTED_HOSTS=api.example.net
export EV_TWIN_VIEWER_TOKEN="$(openssl rand -hex 32)"
export EV_TWIN_OPERATOR_TOKEN="$(openssl rand -hex 32)"
ev-twin-api
```

El rol `viewer` accede a estado, KPI, prioridades y linaje. El rol `operator` puede ejecutar la comprobación de release. Los tokens deben ser distintos. La documentación interactiva está desactivada en producción; no existe CORS abierto; el host, el tamaño de petición y las respuestas se restringen.

Desplegar detrás de un proxy TLS que añada HSTS, rote tokens, limite tasa y registre el `X-Request-ID`. El proceso no confía en cabeceras de proxy por defecto. Endpoints:

| Método y ruta | Acceso |
|---|---|
| `GET /health/live` | público, sin datos |
| `GET /health/ready` | público, estado mínimo |
| `GET /v1/status` | viewer u operator |
| `GET /v1/kpis` | viewer u operator |
| `GET /v1/priorities?limit=10` | viewer u operator |
| `GET /v1/lineage` | viewer u operator |
| `POST /v1/operator/release-check` | operator |

## Recuperación

- Lock abandonado: se recupera automáticamente cuando supera el umbral de obsolescencia; investigar antes de reducirlo.
- Ingesta fallida: corregir la fuente y reejecutar; no editar checkpoints manualmente.
- Release bloqueado: revisar `outputs/reports/pipeline_audit/validation_report.md`, `outputs/reports/release_readiness.json` y el manifiesto del panel.
- Artefacto incoherente: regenerar desde datos de origen; no editar CSV, HTML o PDF a mano.
- API no preparada: comprobar que existe una ejecución PASS y que release y SLA están aprobados.
