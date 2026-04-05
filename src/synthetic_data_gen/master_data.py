from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd

from .utils import clamp


def generate_versiones_vehiculo() -> pd.DataFrame:
    records = [
        {
            "version_id": "VAN_ICE_MID_01",
            "familia_modelo": "MidVan",
            "tipo_propulsion": "ICE",
            "capacidad_bateria_kwh": 0,
            "tiempo_medio_produccion": 390,
            "complejidad_montaje": 2.4,
            "requiere_carga_salida_flag": 0,
            "nivel_criticidad_logistica": 2,
        },
        {
            "version_id": "VAN_ICE_MID_02",
            "familia_modelo": "MidVan",
            "tipo_propulsion": "ICE",
            "capacidad_bateria_kwh": 0,
            "tiempo_medio_produccion": 410,
            "complejidad_montaje": 2.7,
            "requiere_carga_salida_flag": 0,
            "nivel_criticidad_logistica": 3,
        },
        {
            "version_id": "VAN_ICE_LARGE_01",
            "familia_modelo": "LargeVan",
            "tipo_propulsion": "ICE",
            "capacidad_bateria_kwh": 0,
            "tiempo_medio_produccion": 445,
            "complejidad_montaje": 3.0,
            "requiere_carga_salida_flag": 0,
            "nivel_criticidad_logistica": 3,
        },
        {
            "version_id": "VAN_EV_MID_01",
            "familia_modelo": "MidVan",
            "tipo_propulsion": "EV",
            "capacidad_bateria_kwh": 74,
            "tiempo_medio_produccion": 460,
            "complejidad_montaje": 3.4,
            "requiere_carga_salida_flag": 1,
            "nivel_criticidad_logistica": 4,
        },
        {
            "version_id": "VAN_EV_MID_02",
            "familia_modelo": "MidVan",
            "tipo_propulsion": "EV",
            "capacidad_bateria_kwh": 84,
            "tiempo_medio_produccion": 485,
            "complejidad_montaje": 3.8,
            "requiere_carga_salida_flag": 1,
            "nivel_criticidad_logistica": 4,
        },
        {
            "version_id": "VAN_EV_LARGE_01",
            "familia_modelo": "LargeVan",
            "tipo_propulsion": "EV",
            "capacidad_bateria_kwh": 96,
            "tiempo_medio_produccion": 515,
            "complejidad_montaje": 4.1,
            "requiere_carga_salida_flag": 1,
            "nivel_criticidad_logistica": 5,
        },
        {
            "version_id": "VAN_EV_CREW_01",
            "familia_modelo": "CrewVan",
            "tipo_propulsion": "EV",
            "capacidad_bateria_kwh": 90,
            "tiempo_medio_produccion": 505,
            "complejidad_montaje": 4.0,
            "requiere_carga_salida_flag": 1,
            "nivel_criticidad_logistica": 5,
        },
        {
            "version_id": "VAN_ICE_CREW_01",
            "familia_modelo": "CrewVan",
            "tipo_propulsion": "ICE",
            "capacidad_bateria_kwh": 0,
            "tiempo_medio_produccion": 430,
            "complejidad_montaje": 2.9,
            "requiere_carga_salida_flag": 0,
            "nivel_criticidad_logistica": 3,
        },
    ]
    return pd.DataFrame(records)


def generate_slots_carga(rng: np.random.Generator, n_slots: int = 32) -> pd.DataFrame:
    zones = ["NORTE", "SUR", "ESTE", "OESTE"]
    charger_types = ["DC_FAST", "HPC", "DC_STANDARD"]

    records = []
    for idx in range(1, n_slots + 1):
        tipo = rng.choice(charger_types, p=[0.45, 0.25, 0.30])
        if tipo == "HPC":
            power = int(rng.choice([180, 220, 300], p=[0.4, 0.45, 0.15]))
        elif tipo == "DC_FAST":
            power = int(rng.choice([90, 120, 150], p=[0.35, 0.5, 0.15]))
        else:
            power = int(rng.choice([50, 60, 80], p=[0.3, 0.5, 0.2]))

        mantenimiento = int(rng.random() < 0.07)
        disponibilidad = int((not mantenimiento) and (rng.random() > 0.05))

        records.append(
            {
                "slot_id": f"SLOT_{idx:03d}",
                "zona_carga": str(rng.choice(zones, p=[0.30, 0.26, 0.24, 0.20])),
                "potencia_max_kw": power,
                "tipo_cargador": tipo,
                "disponibilidad_flag": disponibilidad,
                "mantenimiento_flag": mantenimiento,
                "ocupacion_actual_flag": 0,
            }
        )

    return pd.DataFrame(records)


def generate_recursos_operativos(
    rng: np.random.Generator,
    turnos: pd.DataFrame,
    restricciones_operativas: pd.DataFrame,
) -> pd.DataFrame:
    latest_date = pd.to_datetime(turnos["fecha"]).max()
    latest_restrictions = restricciones_operativas[
        pd.to_datetime(restricciones_operativas["timestamp_inicio"]).dt.date == latest_date.date()
    ]
    latest_flags: Dict[str, bool] = {
        area: not latest_restrictions[latest_restrictions["area"] == area].empty
        for area in ["PRODUCCION", "PATIO", "CARGA", "LOGISTICA", "ENERGIA"]
    }

    resource_templates = [
        ("REC_PRD_LINEA_1", "LINEA_PRODUCCION", "PRODUCCION", 78),
        ("REC_PRD_LINEA_2", "LINEA_PRODUCCION", "PRODUCCION", 72),
        ("REC_PRD_LINEA_3", "LINEA_PRODUCCION", "PRODUCCION", 64),
        ("REC_PATIO_NORTE", "ESPACIO_PATIO", "PATIO", 260),
        ("REC_PATIO_SUR", "ESPACIO_PATIO", "PATIO", 240),
        ("REC_PATIO_ESTE", "ESPACIO_PATIO", "PATIO", 220),
        ("REC_PATIO_OESTE", "ESPACIO_PATIO", "PATIO", 210),
        ("REC_CARGA_CLUSTER_A", "CARGADORES", "CARGA", 12),
        ("REC_CARGA_CLUSTER_B", "CARGADORES", "CARGA", 10),
        ("REC_CARGA_CLUSTER_C", "CARGADORES", "CARGA", 10),
        ("REC_LOG_MUELLES_CAMION", "MUELLE_SALIDA", "LOGISTICA", 18),
        ("REC_LOG_TERMINAL_TREN", "TERMINAL_SALIDA", "LOGISTICA", 6),
        ("REC_ENERGIA_SUBESTACION", "POTENCIA", "ENERGIA", 4200),
    ]

    records = []
    for recurso_id, tipo, area, nominal in resource_templates:
        restriction_hit = latest_flags.get(area, False)
        reduction = rng.uniform(0.04, 0.18) if restriction_hit else rng.uniform(0.01, 0.08)
        disponible = nominal * (1.0 - reduction)
        if area == "PRODUCCION":
            disponible *= clamp(float(turnos["productividad_turno_indice"].mean()), 0.75, 1.12)

        records.append(
            {
                "recurso_id": recurso_id,
                "tipo_recurso": tipo,
                "area": area,
                "capacidad_nominal": round(float(nominal), 2),
                "capacidad_disponible": round(float(disponible), 2),
                "restriccion_actual_flag": int(restriction_hit),
            }
        )

    return pd.DataFrame(records)
