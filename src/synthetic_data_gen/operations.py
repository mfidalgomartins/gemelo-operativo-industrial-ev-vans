from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .utils import clamp, get_shift_start, shift_from_timestamp


@dataclass
class OperationalOutputs:
    ordenes: pd.DataFrame
    vehiculos: pd.DataFrame
    estado_bateria: pd.DataFrame
    sesiones_carga: pd.DataFrame
    patio: pd.DataFrame
    movimientos_patio: pd.DataFrame
    logistica_salida: pd.DataFrame
    slots_carga_actualizados: pd.DataFrame


def _select_version(
    rng: np.random.Generator,
    versiones: pd.DataFrame,
    share_ev: float,
) -> str:
    if rng.random() < share_ev:
        subset = versiones[versiones["tipo_propulsion"] == "EV"]
        probs = np.array([0.30, 0.30, 0.24, 0.16])
    else:
        subset = versiones[versiones["tipo_propulsion"] == "ICE"]
        probs = np.array([0.35, 0.30, 0.20, 0.15])

    probs = probs[: len(subset)]
    probs = probs / probs.sum()
    return str(rng.choice(subset["version_id"].values, p=probs))


def _build_impact_lookup(daily_restriction_map: pd.DataFrame) -> Dict[Tuple[pd.Timestamp, str], float]:
    if daily_restriction_map.empty:
        return {}

    lookup: Dict[Tuple[pd.Timestamp, str], float] = {}
    tmp = daily_restriction_map.copy()
    tmp["fecha"] = pd.to_datetime(tmp["fecha"]).dt.normalize()
    for row in tmp.itertuples(index=False):
        lookup[(pd.Timestamp(row.fecha), str(row.area))] = float(row.impacto_medio)
    return lookup


def generate_operational_tables(
    rng: np.random.Generator,
    escenarios: pd.DataFrame,
    turnos: pd.DataFrame,
    versiones: pd.DataFrame,
    slots_carga: pd.DataFrame,
    daily_restriction_map: pd.DataFrame,
) -> OperationalOutputs:
    scenario_map = {
        pd.Timestamp(row.fecha).normalize(): {
            "share_ev": float(row.share_ev),
            "intensidad": float(row.intensidad_ramp_up),
            "slots": float(row.disponibilidad_slots_carga),
            "presion_patio": float(row.presion_patio_indice),
            "restriccion_logistica": float(row.restriccion_logistica_indice),
            "escenario": str(row.escenario),
        }
        for row in escenarios.itertuples(index=False)
    }
    impact_lookup = _build_impact_lookup(daily_restriction_map)

    version_map = versiones.set_index("version_id").to_dict("index")

    market_probs = {
        "Iberia": 0.33,
        "Francia": 0.17,
        "Alemania": 0.16,
        "Italia": 0.12,
        "Benelux": 0.08,
        "EuropaNorte": 0.07,
        "EuropaEste": 0.07,
    }
    markets = list(market_probs.keys())
    market_p = np.array(list(market_probs.values()))

    planned_hours_market = {
        "Iberia": (16, 32),
        "Francia": (20, 40),
        "Alemania": (28, 52),
        "Italia": (26, 54),
        "Benelux": (24, 46),
        "EuropaNorte": (40, 70),
        "EuropaEste": (42, 72),
    }

    turnos_df = turnos.copy()
    turnos_df["fecha"] = pd.to_datetime(turnos_df["fecha"]).dt.normalize()
    turnos_df["shift_start"] = turnos_df.apply(lambda r: get_shift_start(r["fecha"], r["turno"]), axis=1)
    turnos_df = turnos_df.sort_values(["fecha", "turno"]).reset_index(drop=True)

    base_units_shift = {"A": 76, "B": 68, "C": 54}

    order_rows: List[Dict[str, object]] = []
    vehicle_rows: List[Dict[str, object]] = []

    order_counter = 1
    vehicle_counter = 1

    for shift_row in turnos_df.itertuples(index=False):
        scn = scenario_map[pd.Timestamp(shift_row.fecha)]

        prod_impact = impact_lookup.get((pd.Timestamp(shift_row.fecha), "PRODUCCION"), 0.0)
        patio_impact = impact_lookup.get((pd.Timestamp(shift_row.fecha), "PATIO"), 0.0)

        vol_factor = (
            base_units_shift[shift_row.turno]
            * shift_row.productividad_turno_indice
            * (1.0 - shift_row.absentismo_proxy * 0.55)
            * (0.86 if pd.Timestamp(shift_row.fecha).dayofweek >= 5 else 1.0)
            * (0.90 + 0.22 * scn["intensidad"])
        )

        n_orders = max(18, int(rng.poisson(max(12, vol_factor))))

        shift_orders_tmp = []
        for seq in range(1, n_orders + 1):
            version_id = _select_version(rng, versiones, scn["share_ev"])
            version = version_map[version_id]

            tact_minutes = clamp(float(rng.normal(7.1 + 0.4 * version["complejidad_montaje"], 1.2)), 4.2, 12.0)
            fecha_programada = shift_row.shift_start + pd.Timedelta(minutes=int((seq - 1) * tact_minutes))

            desvio_real = clamp(
                float(
                    rng.normal(
                        10
                        + 42 * shift_row.presion_operativa_indice
                        + 35 * prod_impact
                        + 8 * version["complejidad_montaje"]
                        + 12 * patio_impact,
                        15,
                    )
                ),
                -20,
                180,
            )
            fecha_real = fecha_programada + pd.Timedelta(minutes=int(desvio_real))

            mercado = str(rng.choice(markets, p=market_p))
            prioridad_cliente = int(rng.choice([1, 2, 3], p=[0.22, 0.56, 0.22]))

            order_id = f"ORD_{order_counter:07d}"
            vehiculo_id = f"VEH_{vehicle_counter:07d}"

            tiempo_prod = clamp(
                float(
                    rng.normal(
                        version["tiempo_medio_produccion"]
                        * (1.0 + 0.30 * prod_impact + 0.10 * shift_row.presion_operativa_indice),
                        32,
                    )
                ),
                version["tiempo_medio_produccion"] * 0.82,
                version["tiempo_medio_produccion"] * 1.55,
            )
            ts_fin_linea = fecha_real + pd.Timedelta(minutes=int(tiempo_prod))
            ts_entrada_patio = ts_fin_linea + pd.Timedelta(
                minutes=int(max(5, rng.normal(22 + 35 * patio_impact, 10)))
            )

            readiness_inicial = clamp(
                76
                - 7.5 * version["complejidad_montaje"]
                - 18 * shift_row.absentismo_proxy
                - 24 * shift_row.presion_operativa_indice
                + float(rng.normal(0, 6)),
                5,
                95,
            )

            shift_orders_tmp.append(
                {
                    "orden_id": order_id,
                    "fecha_programada": fecha_programada,
                    "fecha_real": fecha_real,
                    "turno": shift_row.turno,
                    "secuencia_planeada": seq,
                    "vehiculo_id": vehiculo_id,
                    "version_id": version_id,
                    "prioridad_cliente": prioridad_cliente,
                    "mercado_destino": mercado,
                    "fecha_turno_aux": shift_row.fecha,
                    "presion_turno_aux": float(shift_row.presion_operativa_indice),
                    "escenario_aux": scn["escenario"],
                }
            )

            vehicle_rows.append(
                {
                    "vehiculo_id": vehiculo_id,
                    "vin_proxy": f"W1V{rng.integers(10**13, 10**14 - 1)}",
                    "version_id": version_id,
                    "estado_fabricacion": "EN_PATIO",
                    "timestamp_fin_linea": ts_fin_linea,
                    "timestamp_entrada_patio": ts_entrada_patio,
                    "timestamp_inicio_carga": pd.NaT,
                    "timestamp_fin_carga": pd.NaT,
                    "timestamp_salida": pd.NaT,
                    "nivel_bateria_salida": np.nan,
                    "readiness_score_inicial": round(readiness_inicial, 2),
                }
            )

            order_counter += 1
            vehicle_counter += 1

        shift_df = pd.DataFrame(shift_orders_tmp)
        shift_df = shift_df.sort_values("fecha_real").reset_index(drop=True)
        shift_df["secuencia_real"] = np.arange(1, len(shift_df) + 1)
        order_rows.extend(shift_df.to_dict("records"))

    ordenes_df = pd.DataFrame(order_rows)
    vehiculos_df = pd.DataFrame(vehicle_rows)

    vehiculos_df = vehiculos_df.merge(
        ordenes_df[
            ["orden_id", "vehiculo_id", "fecha_programada", "fecha_turno_aux", "presion_turno_aux", "mercado_destino"]
        ],
        on="vehiculo_id",
        how="left",
    )

    active_slots = slots_carga[(slots_carga["disponibilidad_flag"] == 1) & (slots_carga["mantenimiento_flag"] == 0)].copy()
    if active_slots.empty:
        active_slots = slots_carga.copy()

    slot_power = dict(zip(active_slots["slot_id"], active_slots["potencia_max_kw"]))
    slot_zone = dict(zip(active_slots["slot_id"], active_slots["zona_carga"]))

    start_slot_clock = pd.to_datetime(ordenes_df["fecha_programada"]).min().floor("h") + pd.Timedelta(hours=5)
    slot_next_free = {slot_id: start_slot_clock for slot_id in active_slots["slot_id"]}

    sessions_rows: List[Dict[str, object]] = []
    battery_rows: List[Dict[str, object]] = []

    ev_vehicle_idx = vehiculos_df[
        vehiculos_df["version_id"].map(lambda x: int(version_map[x]["requiere_carga_salida_flag"]) == 1)
    ].sort_values("timestamp_entrada_patio")

    session_counter = 1
    for v in ev_vehicle_idx.itertuples(index=True):
        v_date = pd.Timestamp(v.timestamp_entrada_patio).normalize()
        scn = scenario_map.get(v_date, list(scenario_map.values())[-1])

        charge_impact = impact_lookup.get((v_date, "CARGA"), 0.0)
        patio_impact = impact_lookup.get((v_date, "PATIO"), 0.0)

        pre_charge_wait = int(
            max(
                8,
                rng.normal(
                    24 + 95 * scn["presion_patio"] + 125 * (1 - scn["slots"]) + 85 * charge_impact + 25 * patio_impact,
                    32,
                ),
            )
        )
        ready_for_charge = pd.Timestamp(v.timestamp_entrada_patio) + pd.Timedelta(minutes=pre_charge_wait)
        queue_structural = int(
            max(0, rng.normal(6 + 50 * scn["presion_patio"] + 70 * (1 - scn["slots"]) + 40 * charge_impact, 18))
        )

        slot_id, next_free = min(slot_next_free.items(), key=lambda x: x[1])
        start_charge = max(ready_for_charge + pd.Timedelta(minutes=queue_structural), next_free)
        wait_prev = int((start_charge - ready_for_charge).total_seconds() // 60)

        version = version_map[v.version_id]
        cap = float(version["capacidad_bateria_kwh"])
        soc_inicio = clamp(float(rng.normal(32 - 6 * charge_impact, 10)), 10, 65)
        target_soc = clamp(float(rng.normal(91 + 1.2 * (v.presion_turno_aux > 0.75), 3)), 84, 98)

        energia_objetivo = cap * (target_soc - soc_inicio) / 100.0
        power = float(slot_power[slot_id])
        effective_power = max(28.0, power * clamp(float(rng.normal(0.72 - 0.22 * charge_impact, 0.10)), 0.35, 0.95))

        duracion = int(max(22, (energia_objetivo / effective_power) * 60 + rng.normal(10, 6)))

        p_interrupt = clamp(0.03 + 0.16 * charge_impact + 0.08 * scn["presion_patio"] + 0.06 * (1 - scn["slots"]), 0.01, 0.50)
        interrupted = int(rng.random() < p_interrupt)
        cause = "SIN_INTERRUPCION"
        energia_entregada = energia_objetivo
        if interrupted:
            cause = str(rng.choice(["MICROCORTE_ENERGIA", "REASIGNACION_SLOT", "FALLO_CONECTOR", "ALERTA_TERMICA"]))
            energia_entregada = energia_objetivo * float(rng.uniform(0.68, 0.90))

        soc_fin = clamp(soc_inicio + (energia_entregada / max(cap, 1)) * 100.0, soc_inicio, 99.0)

        end_charge = start_charge + pd.Timedelta(minutes=duracion)
        slot_next_free[slot_id] = end_charge + pd.Timedelta(minutes=int(rng.integers(6, 22)))

        sessions_rows.append(
            {
                "sesion_id": f"SES_{session_counter:08d}",
                "vehiculo_id": v.vehiculo_id,
                "slot_id": slot_id,
                "inicio_sesion": start_charge,
                "fin_sesion": end_charge,
                "energia_entregada_kwh": round(float(energia_entregada), 3),
                "tiempo_espera_previo_min": wait_prev,
                "carga_interrumpida_flag": interrupted,
                "causa_interrupcion": cause,
            }
        )

        battery_rows.append(
            {
                "timestamp": pd.Timestamp(v.timestamp_entrada_patio),
                "vehiculo_id": v.vehiculo_id,
                "soc_pct": round(soc_inicio, 2),
                "target_soc_pct": round(target_soc, 2),
                "battery_temp_proxy": round(float(rng.normal(23 + 9 * charge_impact, 2.5)), 2),
                "charging_status": "EN_ESPERA",
                "energia_cargada_kwh": 0.0,
                "tiempo_en_carga_min": 0,
            }
        )

        checkpoints = max(3, int(duracion // 28))
        for s in range(1, checkpoints + 1):
            progress = s / checkpoints
            ts = start_charge + pd.Timedelta(minutes=int(progress * duracion))
            bateria = clamp(soc_inicio + (soc_fin - soc_inicio) * progress + float(rng.normal(0, 0.8)), 0, 100)
            energy_loaded = max(0.0, energia_entregada * progress)
            battery_rows.append(
                {
                    "timestamp": ts,
                    "vehiculo_id": v.vehiculo_id,
                    "soc_pct": round(bateria, 2),
                    "target_soc_pct": round(target_soc, 2),
                    "battery_temp_proxy": round(float(rng.normal(28 + 8 * progress, 2.8)), 2),
                    "charging_status": "CARGANDO" if s < checkpoints else ("INTERRUMPIDA" if interrupted else "COMPLETADA"),
                    "energia_cargada_kwh": round(float(energy_loaded), 3),
                    "tiempo_en_carga_min": int(progress * duracion),
                }
            )

        vehiculos_df.at[v.Index, "timestamp_inicio_carga"] = start_charge
        vehiculos_df.at[v.Index, "timestamp_fin_carga"] = end_charge
        vehiculos_df.at[v.Index, "nivel_bateria_salida"] = round(soc_fin, 2)

        session_counter += 1

    ice_idx = vehiculos_df[vehiculos_df["timestamp_inicio_carga"].isna()].index
    for idx in ice_idx:
        ts = pd.Timestamp(vehiculos_df.at[idx, "timestamp_fin_linea"])
        battery_rows.append(
            {
                "timestamp": ts,
                "vehiculo_id": vehiculos_df.at[idx, "vehiculo_id"],
                "soc_pct": 0.0,
                "target_soc_pct": 0.0,
                "battery_temp_proxy": round(float(rng.normal(24, 1.2)), 2),
                "charging_status": "NO_APLICA",
                "energia_cargada_kwh": 0.0,
                "tiempo_en_carga_min": 0,
            }
        )

    logistics_rows: List[Dict[str, object]] = []
    patio_rows: List[Dict[str, object]] = []
    move_rows: List[Dict[str, object]] = []

    salida_counter = 1
    mov_counter = 1

    last_reference_ts = pd.to_datetime(ordenes_df["fecha_real"]).max() + pd.Timedelta(days=4)

    zone_list = ["NORTE", "SUR", "ESTE", "OESTE", "BUFFER_CARGA", "PRE_SALIDA"]

    for veh in vehiculos_df.itertuples(index=True):
        v_date = pd.Timestamp(veh.fecha_turno_aux).normalize()
        scn = scenario_map.get(v_date, list(scenario_map.values())[-1])

        logistic_impact = impact_lookup.get((v_date, "LOGISTICA"), 0.0)
        patio_impact = impact_lookup.get((v_date, "PATIO"), 0.0)

        low_h, high_h = planned_hours_market[str(veh.mercado_destino)]
        fecha_planificada = pd.Timestamp(veh.fecha_programada) + pd.Timedelta(hours=int(rng.integers(low_h, high_h + 1)))

        is_ev_charge_flow = pd.notna(veh.timestamp_fin_carga)

        if is_ev_charge_flow:
            base_ready = pd.Timestamp(veh.timestamp_fin_carga) + pd.Timedelta(minutes=int(rng.integers(20, 140)))
            # Umbral operativo pragmático: evita bloquear de forma artificial >50% del flujo EV.
            readiness_batt = float(veh.nivel_bateria_salida) >= 78.0
        else:
            base_ready = pd.Timestamp(veh.timestamp_entrada_patio) + pd.Timedelta(minutes=int(rng.integers(90, 340)))
            readiness_batt = True
            if np.isnan(veh.nivel_bateria_salida):
                vehiculos_df.at[veh.Index, "nivel_bateria_salida"] = 0.0

        readiness_flag = int(readiness_batt and (veh.readiness_score_inicial >= 28))

        extra_delay = max(
            0,
            int(
                rng.normal(
                    18
                    + 160 * logistic_impact
                    + 130 * patio_impact
                    + 105 * veh.presion_turno_aux
                    + (220 if not readiness_flag else 0),
                    60,
                )
            ),
        )

        fecha_real_salida = max(fecha_planificada, base_ready) + pd.Timedelta(minutes=extra_delay)

        if fecha_real_salida > last_reference_ts and rng.random() < 0.60:
            fecha_real_salida = pd.NaT

        # Si no alcanza readiness, la mayoría de unidades deben quedar retenidas (no salir en falso).
        if readiness_flag == 0 and rng.random() < (0.80 if is_ev_charge_flow else 0.92):
            fecha_real_salida = pd.NaT

        if pd.isna(fecha_real_salida):
            retraso = int(max(0, (last_reference_ts - fecha_planificada).total_seconds() // 60))
        else:
            retraso = int(max(0, (fecha_real_salida - fecha_planificada).total_seconds() // 60))

        if not readiness_flag:
            causa = "READINESS_BAJO_BATERIA" if pd.notna(veh.timestamp_fin_carga) else "READINESS_INCOMPLETO"
        elif retraso <= 20:
            causa = "SIN_RETRASO"
        elif logistic_impact >= max(patio_impact, 0.25):
            causa = "FALTA_SLOT_LOGISTICO"
        elif patio_impact > 0.20:
            causa = "CONGESTION_PATIO"
        else:
            causa = "VARIABILIDAD_OPERATIVA"

        modo = "CAMION" if veh.mercado_destino in {"Iberia", "Francia", "Italia"} else str(
            rng.choice(["CAMION", "TREN"], p=[0.58, 0.42])
        )

        logistics_rows.append(
            {
                "salida_id": f"SAL_{salida_counter:08d}",
                "vehiculo_id": veh.vehiculo_id,
                "fecha_salida_planificada": fecha_planificada,
                "fecha_salida_real": fecha_real_salida,
                "modo_salida": modo,
                "transportista_proxy": str(rng.choice(["Carrier_A", "Carrier_B", "Carrier_C", "Carrier_D"])),
                "readiness_salida_flag": readiness_flag,
                "retraso_min": retraso,
                "causa_retraso": causa,
            }
        )
        salida_counter += 1

        vehiculos_df.at[veh.Index, "timestamp_salida"] = fecha_real_salida

        if pd.isna(fecha_real_salida):
            estado_fab = "EN_PATIO" if pd.notna(veh.timestamp_entrada_patio) else "EN_LINEA"
        elif retraso > 420 or readiness_flag == 0:
            estado_fab = "EXPEDIDO_TARDIO"
        else:
            estado_fab = "EXPEDIDO"
        vehiculos_df.at[veh.Index, "estado_fabricacion"] = estado_fab

        ts_entry = pd.Timestamp(veh.timestamp_entrada_patio)
        zone_entry = str(rng.choice(["NORTE", "SUR", "ESTE", "OESTE"], p=[0.30, 0.27, 0.23, 0.20]))

        patio_points: List[Tuple[pd.Timestamp, str, str]] = [(ts_entry, zone_entry, "INGRESO_PATIO")]

        if pd.notna(veh.timestamp_inicio_carga):
            ts_wait = max(ts_entry + pd.Timedelta(minutes=8), pd.Timestamp(veh.timestamp_inicio_carga) - pd.Timedelta(minutes=int(rng.integers(4, 45))))
            patio_points.append((ts_wait, "BUFFER_CARGA", "ESPERA_CARGA"))
            patio_points.append((pd.Timestamp(veh.timestamp_fin_carga), "BUFFER_CARGA", "POST_CARGA"))

        if pd.notna(fecha_real_salida):
            ts_preout = max(ts_entry, pd.Timestamp(fecha_real_salida) - pd.Timedelta(minutes=int(rng.integers(15, 210))))
            patio_points.append((ts_preout, "PRE_SALIDA", "LISTO_EXPEDICION"))
            patio_points.append((pd.Timestamp(fecha_real_salida), "PRE_SALIDA", "SALIDA"))
        else:
            ts_block = ts_entry + pd.Timedelta(minutes=int(rng.integers(240, 980)))
            patio_points.append((ts_block, str(rng.choice(zone_list)), "EN_ESPERA_SALIDA"))

        patio_points = sorted({(a, b, c) for a, b, c in patio_points}, key=lambda x: x[0])

        for ts, zone, state in patio_points:
            dwell = int(max(0, (ts - ts_entry).total_seconds() // 60))
            blocking = int(dwell > 480 or (scn["presion_patio"] > 0.80 and dwell > 220))
            requires_move = int(blocking or (state == "ESPERA_CARGA" and dwell > 90))

            patio_rows.append(
                {
                    "timestamp": ts,
                    "vehiculo_id": veh.vehiculo_id,
                    "zona_patio": zone,
                    "fila": int(rng.integers(1, 28)),
                    "posicion": int(rng.integers(1, 48)),
                    "estado_en_patio": state,
                    "dwell_time_min": dwell,
                    "blocking_flag": blocking,
                    "requiere_movimiento_flag": requires_move,
                }
            )

            if requires_move and state != "SALIDA":
                mstart = ts + pd.Timedelta(minutes=int(rng.integers(5, 90)))
                mdur = int(rng.integers(4, 28))
                mend = mstart + pd.Timedelta(minutes=mdur)
                destino = str(rng.choice([z for z in zone_list if z != zone]))

                if blocking:
                    motivo = str(rng.choice(["REUBICACION_BLOQUEO", "LIBERACION_PASILLO"]))
                elif state == "ESPERA_CARGA":
                    motivo = "ACCESO_ZONA_CARGA"
                else:
                    motivo = "PREPARACION_SALIDA"

                move_rows.append(
                    {
                        "movimiento_id": f"MOV_{mov_counter:09d}",
                        "vehiculo_id": veh.vehiculo_id,
                        "timestamp_inicio": mstart,
                        "timestamp_fin": mend,
                        "origen": zone,
                        "destino": destino,
                        "motivo_movimiento": motivo,
                        "operador_turno": shift_from_timestamp(mstart),
                        "movimiento_no_productivo_flag": int(motivo in {"REUBICACION_BLOQUEO", "LIBERACION_PASILLO"}),
                    }
                )
                mov_counter += 1

                if blocking and rng.random() < 0.35:
                    m2_start = mend + pd.Timedelta(minutes=int(rng.integers(15, 80)))
                    m2_dur = int(rng.integers(5, 24))
                    m2_end = m2_start + pd.Timedelta(minutes=m2_dur)
                    move_rows.append(
                        {
                            "movimiento_id": f"MOV_{mov_counter:09d}",
                            "vehiculo_id": veh.vehiculo_id,
                            "timestamp_inicio": m2_start,
                            "timestamp_fin": m2_end,
                            "origen": destino,
                            "destino": str(rng.choice([z for z in zone_list if z != destino])),
                            "motivo_movimiento": "RESECUENCIACION_PATIO",
                            "operador_turno": shift_from_timestamp(m2_start),
                            "movimiento_no_productivo_flag": 1,
                        }
                    )
                    mov_counter += 1

    logistica_df = pd.DataFrame(logistics_rows)

    order_out = ordenes_df.merge(
        logistica_df[["vehiculo_id", "readiness_salida_flag", "retraso_min", "fecha_salida_real"]],
        on="vehiculo_id",
        how="left",
    )

    order_out["ready_for_dispatch_flag"] = order_out["readiness_salida_flag"].fillna(0).astype(int)
    order_out["estado_orden"] = np.select(
        [
            order_out["fecha_salida_real"].isna(),
            order_out["ready_for_dispatch_flag"].eq(0),
            order_out["retraso_min"].gt(360),
            order_out["retraso_min"].gt(120),
        ],
        ["EN_PATIO", "BLOQUEADA", "RETRASADA_CRITICA", "RETRASADA"],
        default="COMPLETADA",
    )

    order_out = order_out[
        [
            "orden_id",
            "fecha_programada",
            "fecha_real",
            "turno",
            "secuencia_planeada",
            "secuencia_real",
            "vehiculo_id",
            "version_id",
            "prioridad_cliente",
            "mercado_destino",
            "estado_orden",
            "ready_for_dispatch_flag",
        ]
    ].sort_values("orden_id")

    vehiculos_out = vehiculos_df[
        [
            "vehiculo_id",
            "vin_proxy",
            "version_id",
            "estado_fabricacion",
            "timestamp_fin_linea",
            "timestamp_entrada_patio",
            "timestamp_inicio_carga",
            "timestamp_fin_carga",
            "timestamp_salida",
            "nivel_bateria_salida",
            "readiness_score_inicial",
        ]
    ].sort_values("vehiculo_id")

    estado_bateria_df = pd.DataFrame(battery_rows).sort_values(["timestamp", "vehiculo_id"])
    sesiones_df = pd.DataFrame(sessions_rows).sort_values("inicio_sesion")
    patio_df = pd.DataFrame(patio_rows).sort_values(["timestamp", "vehiculo_id"])
    movimientos_df = pd.DataFrame(move_rows).sort_values("timestamp_inicio")

    last_ts = vehiculos_out[["timestamp_salida", "timestamp_fin_carga", "timestamp_entrada_patio"]].stack().dropna().max()
    active_last_slots = set()
    if not sesiones_df.empty:
        active_last_slots = set(
            sesiones_df[
                (sesiones_df["inicio_sesion"] <= last_ts)
                & (sesiones_df["fin_sesion"] >= (last_ts - pd.Timedelta(minutes=90)))
            ]["slot_id"]
        )

    slots_out = slots_carga.copy()
    slots_out["ocupacion_actual_flag"] = slots_out["slot_id"].map(lambda x: int(x in active_last_slots))

    return OperationalOutputs(
        ordenes=order_out,
        vehiculos=vehiculos_out,
        estado_bateria=estado_bateria_df,
        sesiones_carga=sesiones_df,
        patio=patio_df,
        movimientos_patio=movimientos_df,
        logistica_salida=logistica_df.sort_values("fecha_salida_planificada"),
        slots_carga_actualizados=slots_out.sort_values("slot_id"),
    )
