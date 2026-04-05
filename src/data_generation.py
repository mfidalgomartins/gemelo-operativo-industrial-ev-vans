from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import pandas as pd

from .config import (
    AVG_ORDERS_PER_DAY,
    CHARGER_POWER_KW,
    DATA_RAW_DIR,
    RANDOM_SEED,
    SIM_DAYS,
    TOTAL_CHARGERS,
    ensure_directories,
)


@dataclass
class GenerationConfig:
    seed: int = RANDOM_SEED
    days: int = SIM_DAYS
    avg_orders_per_day: int = AVG_ORDERS_PER_DAY
    start_date: str = "2025-01-01"


def _shift_from_timestamp(ts: pd.Timestamp) -> str:
    hour = ts.hour
    if 6 <= hour < 14:
        return "A"
    if 14 <= hour < 22:
        return "B"
    return "C"


def _ev_share(day_idx: int, total_days: int) -> float:
    return 0.25 + 0.35 * (day_idx / max(total_days - 1, 1))


def _next_charge_slot(ts: pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    if ts.hour < 6:
        return ts.normalize() + pd.Timedelta(hours=6)
    if ts.hour >= 22:
        return (ts + pd.Timedelta(days=1)).normalize() + pd.Timedelta(hours=6)
    return ts


def _build_version_table() -> pd.DataFrame:
    records: List[Dict[str, object]] = []
    families = ["MidVan", "LargeVan", "CrewVan"]
    params = {
        "ICE": {"ensamblaje": [190, 220, 205], "test": [38, 44, 40], "kwh_test": [8, 10, 9]},
        "EV": {"ensamblaje": [220, 250, 235], "test": [46, 54, 50], "kwh_test": [14, 16, 15]},
    }

    version_id = 1
    for propulsion in ["ICE", "EV"]:
        for idx, family in enumerate(families):
            records.append(
                {
                    "vehicle_version_id": f"VV{version_id:03d}",
                    "familia_van": family,
                    "tipo_propulsion": propulsion,
                    "tiempo_base_ensamblaje_min": params[propulsion]["ensamblaje"][idx],
                    "tiempo_base_test_min": params[propulsion]["test"][idx],
                    "consumo_test_kwh": params[propulsion]["kwh_test"][idx],
                }
            )
            version_id += 1

    return pd.DataFrame(records)


def _generate_orders(cfg: GenerationConfig, rng: np.random.Generator) -> pd.DataFrame:
    fechas = pd.date_range(cfg.start_date, periods=cfg.days, freq="D")
    families = ["MidVan", "LargeVan", "CrewVan"]
    destinos = ["Iberia", "Francia", "Alemania", "Benelux", "Italia"]

    records: List[Dict[str, object]] = []
    order_idx = 1

    for day_idx, day in enumerate(fechas):
        n_orders = max(35, int(rng.poisson(cfg.avg_orders_per_day)))
        ev_prob = _ev_share(day_idx, cfg.days)

        for _ in range(n_orders):
            tipo_propulsion = "EV" if rng.random() < ev_prob else "ICE"
            bateria = int(rng.choice([70, 85, 95])) if tipo_propulsion == "EV" else 0
            family = rng.choice(families, p=[0.44, 0.33, 0.23])

            plan_ts = day + pd.Timedelta(minutes=int(rng.integers(0, 24 * 60)))
            objetivo_salida = plan_ts + pd.Timedelta(hours=int(rng.integers(22, 84)))

            records.append(
                {
                    "order_id": f"ORD{order_idx:06d}",
                    "vin": f"W1V{rng.integers(10000000000000, 99999999999999)}",
                    "fecha_plan": plan_ts,
                    "fecha_objetivo_salida": objetivo_salida,
                    "tipo_propulsion": tipo_propulsion,
                    "familia_van": family,
                    "destino": rng.choice(destinos, p=[0.30, 0.21, 0.22, 0.15, 0.12]),
                    "prioridad_cliente": int(rng.choice([1, 2, 3], p=[0.18, 0.57, 0.25])),
                    "bateria_kwh": bateria,
                }
            )
            order_idx += 1

    return pd.DataFrame(records)


def _generate_production_events(
    orders: pd.DataFrame,
    versions: pd.DataFrame,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, pd.Series]:
    version_map = versions.set_index(["familia_van", "tipo_propulsion"]).to_dict("index")
    events: List[Dict[str, object]] = []
    eol_finish: Dict[str, pd.Timestamp] = {}

    stations = ["BODY", "PAINT", "ASSEMBLY", "EOL"]
    event_idx = 1

    for row in orders.itertuples(index=False):
        cfg = version_map[(row.familia_van, row.tipo_propulsion)]

        start_ts = pd.Timestamp(row.fecha_plan) + pd.Timedelta(minutes=int(rng.integers(5, 360)))
        body = int(max(65, rng.normal(95 if row.tipo_propulsion == "ICE" else 108, 14)))
        paint = int(max(72, rng.normal(104, 16)))
        assembly = int(max(120, rng.normal(cfg["tiempo_base_ensamblaje_min"], 24)))
        eol = int(max(25, rng.normal(cfg["tiempo_base_test_min"], 7)))
        durations = [body, paint, assembly, eol]

        cursor = start_ts
        for station, duration_min in zip(stations, durations):
            end_ts = cursor + pd.Timedelta(minutes=int(duration_min))
            events.append(
                {
                    "event_id": f"PRD{event_idx:08d}",
                    "order_id": row.order_id,
                    "estacion": station,
                    "ts_inicio": cursor,
                    "ts_fin": end_ts,
                    "duracion_real_min": int(duration_min),
                    "turno": _shift_from_timestamp(cursor),
                }
            )
            event_idx += 1
            cursor = end_ts + pd.Timedelta(minutes=int(rng.integers(3, 22)))

        eol_finish[row.order_id] = cursor

    return pd.DataFrame(events), pd.Series(eol_finish, name="ts_fin_eol")


def _generate_yard_charge_dispatch(
    orders: pd.DataFrame,
    eol_finish: pd.Series,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    yard_events: List[Dict[str, object]] = []
    charge_sessions: List[Dict[str, object]] = []
    dispatch_events: List[Dict[str, object]] = []

    initial_slot = _next_charge_slot(pd.Timestamp(orders["fecha_plan"].min()))
    charger_available = {f"CHG{idx:03d}": initial_slot for idx in range(1, TOTAL_CHARGERS + 1)}

    ordered = orders.copy()
    ordered["ts_fin_eol"] = ordered["order_id"].map(eol_finish)
    ordered = ordered.sort_values("ts_fin_eol")

    yard_idx = 1
    charge_idx = 1
    dispatch_idx = 1

    sla_map = {"Iberia": 24, "Francia": 30, "Alemania": 42, "Benelux": 36, "Italia": 48}

    for i, row in enumerate(ordered.itertuples(index=False), start=1):
        progress = i / len(ordered)
        ts_entry = pd.Timestamp(row.ts_fin_eol) + pd.Timedelta(minutes=int(rng.integers(18, 95)))
        sector = rng.choice(["NORTE", "SUR", "ESTE", "OESTE"], p=[0.28, 0.26, 0.24, 0.22])

        base_occ = 58 + 30 * progress + (8 if row.tipo_propulsion == "EV" else 0)
        occ_entry = float(np.clip(rng.normal(base_occ, 7), 45, 98))

        if row.tipo_propulsion == "EV":
            pre_charge_wait = int(max(10, rng.normal(78 + 65 * progress, 35)))
            ready_for_charge = _next_charge_slot(ts_entry + pd.Timedelta(minutes=pre_charge_wait))
            queue_operativa = int(max(0, rng.normal(10 + 95 * progress, 28)))

            charger_id, available_ts = min(charger_available.items(), key=lambda item: item[1])
            ts_start_charge = _next_charge_slot(
                max(ready_for_charge + pd.Timedelta(minutes=queue_operativa), _next_charge_slot(available_ts))
            )
            queue_min = int((ts_start_charge - ready_for_charge).total_seconds() // 60)

            soc_start = float(np.clip(rng.normal(28, 8), 12, 58))
            soc_target = float(np.clip(rng.normal(91, 3), 83, 97))
            energy_needed = row.bateria_kwh * (soc_target - soc_start) / 100
            kwh = float(np.clip(energy_needed * rng.uniform(0.93, 1.08), 8, 82))

            effective_power = float(np.clip(rng.normal(38, 6), 24, 50))
            duration_charge = int(max(35, (kwh / effective_power) * 60))
            ts_end_charge = ts_start_charge + pd.Timedelta(minutes=duration_charge)
            charger_available[charger_id] = _next_charge_slot(
                ts_end_charge + pd.Timedelta(minutes=int(rng.integers(8, 22)))
            )

            ts_yard_exit = ts_end_charge + pd.Timedelta(minutes=int(rng.integers(30, 140)))

            charge_sessions.append(
                {
                    "charge_session_id": f"CHS{charge_idx:08d}",
                    "order_id": row.order_id,
                    "charger_id": charger_id,
                    "ts_inicio_carga": ts_start_charge,
                    "ts_fin_carga": ts_end_charge,
                    "kwh_entregados": round(kwh, 2),
                    "soc_inicio": round(soc_start, 1),
                    "soc_fin": round(soc_target, 1),
                    "espera_previa_min": queue_min,
                }
            )
            charge_idx += 1

            ts_wait = ready_for_charge
        else:
            ts_wait = ts_entry + pd.Timedelta(minutes=int(rng.integers(45, 170)))
            ts_yard_exit = ts_entry + pd.Timedelta(minutes=int(max(90, rng.normal(275 + 60 * progress, 75))))

        occ_wait = float(np.clip(rng.normal(base_occ + 5, 8), 46, 99))
        occ_exit = float(np.clip(rng.normal(base_occ - 3, 9), 38, 97))

        yard_events.extend(
            [
                {
                    "yard_event_id": f"YRD{yard_idx:08d}",
                    "order_id": row.order_id,
                    "tipo_evento": "ENTRY",
                    "ts_evento": ts_entry,
                    "sector_patio": sector,
                    "ocupacion_sector_pct": round(occ_entry, 2),
                },
                {
                    "yard_event_id": f"YRD{yard_idx + 1:08d}",
                    "order_id": row.order_id,
                    "tipo_evento": "WAIT",
                    "ts_evento": ts_wait,
                    "sector_patio": sector,
                    "ocupacion_sector_pct": round(occ_wait, 2),
                },
                {
                    "yard_event_id": f"YRD{yard_idx + 2:08d}",
                    "order_id": row.order_id,
                    "tipo_evento": "EXIT",
                    "ts_evento": ts_yard_exit,
                    "sector_patio": sector,
                    "ocupacion_sector_pct": round(occ_exit, 2),
                },
            ]
        )
        yard_idx += 3

        ts_ready_dispatch = ts_yard_exit + pd.Timedelta(minutes=int(rng.integers(20, 170)))
        mode = rng.choice(["CAMION", "TREN"], p=[0.67, 0.33]) if row.destino != "Iberia" else "CAMION"
        delay_dispatch = int(max(20, rng.normal(190 + 120 * progress, 85)))
        ts_dispatch = ts_ready_dispatch + pd.Timedelta(minutes=delay_dispatch)

        dispatch_events.append(
            {
                "dispatch_event_id": f"DSP{dispatch_idx:08d}",
                "order_id": row.order_id,
                "ts_ready_expedicion": ts_ready_dispatch,
                "ts_salida_real": ts_dispatch,
                "modo_salida": mode,
                "sla_horas": int(sla_map[row.destino]),
            }
        )
        dispatch_idx += 1

    return pd.DataFrame(yard_events), pd.DataFrame(charge_sessions), pd.DataFrame(dispatch_events)


def _generate_shift_calendar(cfg: GenerationConfig, rng: np.random.Generator) -> pd.DataFrame:
    records: List[Dict[str, object]] = []

    for day in pd.date_range(cfg.start_date, periods=cfg.days, freq="D"):
        weekend_factor = 0.86 if day.dayofweek >= 5 else 1.0
        for shift, base_staff, base_capacity in [("A", 112, 74), ("B", 104, 70), ("C", 82, 52)]:
            staff = int(max(58, rng.normal(base_staff * weekend_factor, 7)))
            cap = int(max(32, rng.normal(base_capacity * weekend_factor, 5)))
            records.append(
                {
                    "fecha": day.date(),
                    "turno": shift,
                    "dotacion": staff,
                    "capacidad_teorica_unidades": cap,
                }
            )

    return pd.DataFrame(records)


def _generate_energy_availability(
    orders: pd.DataFrame,
    charge_sessions: pd.DataFrame,
    dispatch_events: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    ts_start = pd.Timestamp(orders["fecha_plan"].min()).floor("h")
    ts_end = pd.Timestamp(dispatch_events["ts_salida_real"].max()).ceil("h")
    hourly_index = pd.date_range(ts_start, ts_end, freq="h")

    sessions = charge_sessions.copy()
    sessions["ts_inicio_carga"] = pd.to_datetime(sessions["ts_inicio_carga"])
    sessions["ts_fin_carga"] = pd.to_datetime(sessions["ts_fin_carga"])

    rows: List[Dict[str, object]] = []
    for ts in hourly_index:
        active = (
            (sessions["ts_inicio_carga"] <= ts) & (sessions["ts_fin_carga"] > ts)
        ).sum() if not sessions.empty else 0

        charging_demand = float(active * CHARGER_POWER_KW)
        shift = _shift_from_timestamp(ts)
        shift_load = {"A": 420, "B": 320, "C": 180}[shift]
        process_demand = float(max(950, rng.normal(1280 + shift_load, 110)))
        demand_kw = process_demand + charging_demand

        base_capacity = float(max(2100, rng.normal(3420, 190)))
        planned_curtailment = rng.random() < 0.014
        if planned_curtailment:
            base_capacity *= rng.uniform(0.72, 0.88)

        curtailment_flag = int(planned_curtailment or demand_kw > base_capacity * 0.95)

        rows.append(
            {
                "ts_hora": ts,
                "turno": shift,
                "capacidad_kw_disponible": round(base_capacity, 2),
                "demanda_kw": round(demand_kw, 2),
                "curtailment_flag": curtailment_flag,
            }
        )

    return pd.DataFrame(rows)


def generate_raw_data(cfg: GenerationConfig | None = None) -> Dict[str, pd.DataFrame]:
    ensure_directories()
    cfg = cfg or GenerationConfig()
    rng = np.random.default_rng(cfg.seed)

    versions = _build_version_table()
    orders = _generate_orders(cfg, rng)
    production_events, eol_finish = _generate_production_events(orders, versions, rng)
    yard_events, charge_sessions, dispatch_events = _generate_yard_charge_dispatch(orders, eol_finish, rng)
    shift_calendar = _generate_shift_calendar(cfg, rng)
    energy = _generate_energy_availability(orders, charge_sessions, dispatch_events, rng)

    tables = {
        "ordenes_produccion": orders,
        "versiones_vehiculo": versions,
        "eventos_produccion": production_events,
        "eventos_patio": yard_events,
        "sesiones_carga": charge_sessions,
        "disponibilidad_energia": energy,
        "eventos_expedicion": dispatch_events,
        "calendario_turnos": shift_calendar,
    }

    for name, df in tables.items():
        out_path = DATA_RAW_DIR / f"{name}.csv"
        df.to_csv(out_path, index=False)

    return tables


if __name__ == "__main__":
    generate_raw_data()
