from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd

from .utils import clamp, ordered_phase, scenario_curve


def generate_escenarios_transicion(
    start_date: str,
    months: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    start = pd.Timestamp(start_date)
    end = start + pd.DateOffset(months=months) - pd.Timedelta(days=1)
    fechas = pd.date_range(start, end, freq="D")

    records = []
    phase_positions: Dict[str, int] = {"pre_lanzamiento": 0, "pre_serie": 0, "ramp_up": 0, "estable": 0}
    phase_totals = {"pre_lanzamiento": 1, "pre_serie": 1, "ramp_up": 1, "estable": 1}

    assigned_phases = [ordered_phase(i, len(fechas)) for i in range(len(fechas))]
    phase_series = pd.Series(assigned_phases)
    for phase, count in phase_series.value_counts().to_dict().items():
        phase_totals[phase] = int(count)

    for idx, fecha in enumerate(fechas):
        phase = assigned_phases[idx]
        phase_positions[phase] += 1
        pos = phase_positions[phase] / max(phase_totals[phase], 1)
        curve = scenario_curve(phase, pos, rng)

        records.append(
            {
                "fecha": fecha.normalize(),
                "escenario": phase,
                **curve,
            }
        )

    escenarios = pd.DataFrame(records)
    weekend = escenarios["fecha"].dt.dayofweek >= 5
    escenarios.loc[weekend, "presion_patio_indice"] = escenarios.loc[weekend, "presion_patio_indice"].apply(
        lambda x: clamp(x * 0.92, 0.1, 1.0)
    )
    escenarios.loc[weekend, "restriccion_logistica_indice"] = escenarios.loc[
        weekend, "restriccion_logistica_indice"
    ].apply(lambda x: clamp(x * 0.95, 0.05, 1.0))

    return escenarios


def generate_turnos(escenarios: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    shift_base_headcount = {"A": 122, "B": 114, "C": 95}
    shift_efficiency = {"A": 1.00, "B": 0.96, "C": 0.86}

    records = []
    for row in escenarios.itertuples(index=False):
        weekend = pd.Timestamp(row.fecha).dayofweek >= 5
        for turno in ["A", "B", "C"]:
            absentismo = clamp(
                float(rng.normal(0.052 + 0.07 * row.intensidad_ramp_up + (0.018 if weekend else 0), 0.018)),
                0.01,
                0.24,
            )
            base_headcount = shift_base_headcount[turno] * (0.88 if weekend else 1.0)
            headcount = int(max(48, rng.normal(base_headcount * (1.0 - absentismo * 0.65), 6)))

            productividad = clamp(
                shift_efficiency[turno]
                * (1.08 - absentismo)
                * (1.03 - 0.16 * row.intensidad_ramp_up)
                * float(rng.normal(1.0, 0.04)),
                0.58,
                1.18,
            )
            presion = clamp(
                0.25
                + 0.45 * row.presion_patio_indice
                + 0.30 * row.restriccion_logistica_indice
                + (0.07 if turno == "C" else 0)
                + float(rng.normal(0, 0.04)),
                0.05,
                1.0,
            )
            overtime = int(presion > 0.72 or absentismo > 0.11)

            records.append(
                {
                    "fecha": pd.Timestamp(row.fecha).date(),
                    "turno": turno,
                    "headcount_proxy": headcount,
                    "absentismo_proxy": round(absentismo, 4),
                    "productividad_turno_indice": round(productividad, 4),
                    "presion_operativa_indice": round(presion, 4),
                    "overtime_flag": overtime,
                }
            )

    return pd.DataFrame(records)


def generate_restricciones_operativas(
    escenarios: pd.DataFrame,
    turnos: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    restriction_types = {
        "PRODUCCION": ["BAJA_DOTACION", "MICROPARADAS_LINEA", "RETRABAJO_ELEVADO"],
        "PATIO": ["SATURACION_ZONA", "REORGANIZACION_FLUJO", "BLOQUEO_POSICIONES"],
        "CARGA": ["CAPACIDAD_CARGA_LIMITADA", "AVERIA_CARGADOR", "MANTENIMIENTO_CARGA"],
        "LOGISTICA": ["FALTA_SLOT_TRANSPORTE", "LIMITACION_TRANSPORTISTA", "INCIDENCIA_DOCUMENTAL"],
        "ENERGIA": ["RESTRICCION_POTENCIA", "CURTAILMENT_REDES", "ALERTA_TERMICA"],
    }
    area_weights = [0.24, 0.23, 0.24, 0.21, 0.08]
    areas = list(restriction_types.keys())

    turnos_local = turnos.copy()
    escenarios_local = escenarios.copy()
    turnos_local["fecha"] = pd.to_datetime(turnos_local["fecha"]).dt.date
    escenarios_local["fecha"] = pd.to_datetime(escenarios_local["fecha"]).dt.date

    turnos_enriched = turnos_local.merge(
        escenarios_local[["fecha", "intensidad_ramp_up", "presion_patio_indice", "restriccion_logistica_indice"]],
        on="fecha",
        how="left",
    )

    records = []
    rid = 1
    for row in turnos_enriched.itertuples(index=False):
        p_restriction = clamp(
            0.05
            + 0.22 * row.intensidad_ramp_up
            + 0.18 * row.presion_operativa_indice
            + 0.16 * row.restriccion_logistica_indice
            + (0.07 if row.turno == "C" else 0),
            0.02,
            0.75,
        )
        n_events = int(rng.poisson(2 * p_restriction))

        if n_events == 0:
            continue

        shift_hour = {"A": 6, "B": 14, "C": 22}[row.turno]
        shift_start = pd.Timestamp(row.fecha) + pd.Timedelta(hours=shift_hour)

        for _ in range(n_events):
            area = str(rng.choice(areas, p=area_weights))
            severity = int(np.clip(np.round(rng.normal(2.2 + 2.1 * p_restriction, 1.1)), 1, 5))
            duration = int(max(25, rng.normal(55 + 45 * severity, 30)))
            start_offset = int(rng.integers(0, 8 * 60))
            ts_ini = shift_start + pd.Timedelta(minutes=start_offset)
            ts_fin = ts_ini + pd.Timedelta(minutes=duration)
            impact = clamp(0.04 * severity + float(rng.normal(0.06, 0.04)), 0.05, 0.70)

            records.append(
                {
                    "restriccion_id": f"RST_{rid:08d}",
                    "timestamp_inicio": ts_ini,
                    "timestamp_fin": ts_fin,
                    "area": area,
                    "tipo_restriccion": str(rng.choice(restriction_types[area])),
                    "severidad": severity,
                    "impacto_capacidad_pct": round(impact, 4),
                }
            )
            rid += 1

    return pd.DataFrame(records)


def build_daily_restriction_map(restricciones: pd.DataFrame) -> pd.DataFrame:
    if restricciones.empty:
        return pd.DataFrame(columns=["fecha", "area", "impacto_medio"])

    df = restricciones.copy()
    df["fecha"] = pd.to_datetime(df["timestamp_inicio"]).dt.date
    out = (
        df.groupby(["fecha", "area"], as_index=False)["impacto_capacidad_pct"]
        .mean()
        .rename(columns={"impacto_capacidad_pct": "impacto_medio"})
    )
    return out
