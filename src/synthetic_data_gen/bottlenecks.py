from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd

from .utils import clamp


def generate_cuellos_botella(
    rng: np.random.Generator,
    sesiones_carga: pd.DataFrame,
    patio: pd.DataFrame,
    logistica_salida: pd.DataFrame,
    turnos: pd.DataFrame,
    restricciones_operativas: pd.DataFrame,
) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    event_counter = 1

    if not sesiones_carga.empty:
        sc = sesiones_carga.copy()
        sc["fecha"] = pd.to_datetime(sc["inicio_sesion"]).dt.date
        agg = sc.groupby("fecha", as_index=False).agg(
            espera_media=("tiempo_espera_previo_min", "mean"),
            espera_p95=("tiempo_espera_previo_min", lambda s: float(np.percentile(s, 95))),
            interrupciones=("carga_interrumpida_flag", "sum"),
        )
        for row in agg.itertuples(index=False):
            if row.espera_media > 55 or row.espera_p95 > 130 or row.interrupciones > 8:
                sev = int(np.clip(round(1.7 + row.espera_p95 / 85 + row.interrupciones / 22), 1, 5))
                dur = int(max(35, rng.normal(95 + 38 * sev, 22)))
                rows.append(
                    {
                        "evento_id": f"BOT_{event_counter:08d}",
                        "timestamp": pd.Timestamp(row.fecha) + pd.Timedelta(hours=int(rng.integers(7, 22))),
                        "area": "CARGA",
                        "tipo_cuello_botella": "COLA_CARGA",
                        "severidad": sev,
                        "duracion_min": dur,
                        "impacto_throughput_proxy": round(clamp(0.08 * sev + row.espera_media / 900, 0.05, 0.95), 4),
                        "impacto_salida_proxy": round(clamp(0.10 * sev + row.espera_p95 / 700, 0.06, 0.98), 4),
                        "causa_probable": "SATURACION_SLOTS_CARGA",
                    }
                )
                event_counter += 1

    if not patio.empty:
        py = patio.copy()
        py["fecha"] = pd.to_datetime(py["timestamp"]).dt.date
        agg = py.groupby("fecha", as_index=False).agg(
            blocking_rate=("blocking_flag", "mean"),
            dwell_p90=("dwell_time_min", lambda s: float(np.percentile(s, 90))),
            movimientos=("requiere_movimiento_flag", "sum"),
        )
        for row in agg.itertuples(index=False):
            if row.blocking_rate > 0.24 or row.dwell_p90 > 420:
                sev = int(np.clip(round(1.3 + 3.2 * row.blocking_rate + row.dwell_p90 / 260), 1, 5))
                rows.append(
                    {
                        "evento_id": f"BOT_{event_counter:08d}",
                        "timestamp": pd.Timestamp(row.fecha) + pd.Timedelta(hours=int(rng.integers(9, 23))),
                        "area": "PATIO",
                        "tipo_cuello_botella": "CONGESTION_PATIO",
                        "severidad": sev,
                        "duracion_min": int(max(40, rng.normal(110 + 35 * sev, 25))),
                        "impacto_throughput_proxy": round(clamp(0.06 * sev + row.blocking_rate * 0.6, 0.04, 0.95), 4),
                        "impacto_salida_proxy": round(clamp(0.09 * sev + row.dwell_p90 / 900, 0.05, 0.99), 4),
                        "causa_probable": "BLOQUEO_INTERNO_Y_REUBICACION",
                    }
                )
                event_counter += 1

    if not logistica_salida.empty:
        lg = logistica_salida.copy()
        lg["fecha"] = pd.to_datetime(lg["fecha_salida_planificada"]).dt.date
        agg = lg.groupby("fecha", as_index=False).agg(
            retraso_medio=("retraso_min", "mean"),
            pct_no_ready=("readiness_salida_flag", lambda x: 1 - float(np.mean(x))),
        )
        for row in agg.itertuples(index=False):
            if row.retraso_medio > 130 or row.pct_no_ready > 0.14:
                sev = int(np.clip(round(1.8 + row.retraso_medio / 110 + 2.2 * row.pct_no_ready), 1, 5))
                rows.append(
                    {
                        "evento_id": f"BOT_{event_counter:08d}",
                        "timestamp": pd.Timestamp(row.fecha) + pd.Timedelta(hours=int(rng.integers(10, 23))),
                        "area": "LOGISTICA",
                        "tipo_cuello_botella": "BLOQUEO_EXPEDICION",
                        "severidad": sev,
                        "duracion_min": int(max(30, rng.normal(90 + 28 * sev, 30))),
                        "impacto_throughput_proxy": round(clamp(0.04 * sev + row.retraso_medio / 1400, 0.04, 0.90), 4),
                        "impacto_salida_proxy": round(clamp(0.09 * sev + row.pct_no_ready, 0.05, 0.99), 4),
                        "causa_probable": "DESALINEACION_READINESS_TRANSPORTE",
                    }
                )
                event_counter += 1

    if not turnos.empty:
        t = turnos.copy()
        t["fecha"] = pd.to_datetime(t["fecha"])
        for row in t.itertuples(index=False):
            if row.presion_operativa_indice > 0.82 and row.productividad_turno_indice < 0.86:
                sev = int(np.clip(round(2 + 3 * row.presion_operativa_indice), 1, 5))
                rows.append(
                    {
                        "evento_id": f"BOT_{event_counter:08d}",
                        "timestamp": pd.Timestamp(row.fecha) + pd.Timedelta(hours={"A": 9, "B": 17, "C": 1}[row.turno]),
                        "area": "PRODUCCION",
                        "tipo_cuello_botella": "DEGRADACION_TURNO",
                        "severidad": sev,
                        "duracion_min": int(max(25, rng.normal(75 + 30 * sev, 25))),
                        "impacto_throughput_proxy": round(clamp(0.07 * sev + (1 - row.productividad_turno_indice), 0.05, 0.98), 4),
                        "impacto_salida_proxy": round(clamp(0.05 * sev + row.absentismo_proxy, 0.04, 0.95), 4),
                        "causa_probable": "ABSENTISMO_Y_VARIABILIDAD_SECUENCIA",
                    }
                )
                event_counter += 1

    if not restricciones_operativas.empty:
        severe = restricciones_operativas[restricciones_operativas["severidad"] >= 4].copy()
        for row in severe.itertuples(index=False):
            rows.append(
                {
                    "evento_id": f"BOT_{event_counter:08d}",
                    "timestamp": pd.Timestamp(row.timestamp_inicio),
                    "area": str(row.area),
                    "tipo_cuello_botella": "RESTRICCION_CRITICA",
                    "severidad": int(row.severidad),
                    "duracion_min": int((pd.Timestamp(row.timestamp_fin) - pd.Timestamp(row.timestamp_inicio)).total_seconds() // 60),
                    "impacto_throughput_proxy": round(clamp(0.08 * row.severidad + 0.4 * row.impacto_capacidad_pct, 0.08, 0.99), 4),
                    "impacto_salida_proxy": round(clamp(0.09 * row.severidad + 0.45 * row.impacto_capacidad_pct, 0.08, 0.99), 4),
                    "causa_probable": str(row.tipo_restriccion),
                }
            )
            event_counter += 1

    if not rows:
        rows.append(
            {
                "evento_id": "BOT_00000001",
                "timestamp": pd.Timestamp(turnos["fecha"]).min() + pd.Timedelta(hours=8),
                "area": "PRODUCCION",
                "tipo_cuello_botella": "SIN_EVENTOS_CRITICOS",
                "severidad": 1,
                "duracion_min": 30,
                "impacto_throughput_proxy": 0.05,
                "impacto_salida_proxy": 0.05,
                "causa_probable": "OPERACION_ESTABLE",
            }
        )

    out = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)
    return out
