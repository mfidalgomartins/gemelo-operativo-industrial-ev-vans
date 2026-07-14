from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType

import pandas as pd


class ContractViolationError(ValueError):
    """El lote recibido no cumple el contrato de una tabla de origen."""


class SourceSystem(str, Enum):
    MES = "mes"
    WMS = "wms"
    EMS = "ems"
    PLANNING = "planning"
    MASTER_DATA = "master_data"


@dataclass(frozen=True)
class TableContract:
    name: str
    source_system: SourceSystem
    primary_key: tuple[str, ...]
    columns: tuple[str, ...]
    watermark_column: str | None = None

    @property
    def filename(self) -> str:
        return f"{self.name}.csv"

    def validate(self, frame: pd.DataFrame, *, allow_empty: bool = False) -> None:
        if frame.empty and not allow_empty:
            raise ContractViolationError(f"{self.name}: el lote no puede estar vacío")

        actual = tuple(str(column) for column in frame.columns)
        missing = [column for column in self.columns if column not in actual]
        unexpected = [column for column in actual if column not in self.columns]
        if missing or unexpected:
            raise ContractViolationError(
                f"{self.name}: esquema inválido; faltan={missing or '[]'}, sobran={unexpected or '[]'}"
            )

        null_keys = frame.loc[:, list(self.primary_key)].isna().any(axis=1)
        if null_keys.any():
            raise ContractViolationError(f"{self.name}: {int(null_keys.sum())} filas con clave nula")
        duplicate_keys = frame.duplicated(subset=list(self.primary_key), keep=False)
        if duplicate_keys.any():
            raise ContractViolationError(f"{self.name}: {int(duplicate_keys.sum())} filas con clave duplicada")

        if self.watermark_column is not None and not frame.empty:
            parsed = pd.to_datetime(frame[self.watermark_column], errors="coerce")
            invalid = parsed.isna() & frame[self.watermark_column].notna()
            if invalid.any():
                raise ContractViolationError(
                    f"{self.name}: {int(invalid.sum())} valores temporales inválidos en {self.watermark_column}"
                )

    def project(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Devuelve las columnas contractuales en orden estable."""
        return frame.loc[:, list(self.columns)].copy()


def _contract(
    name: str,
    source_system: SourceSystem,
    primary_key: tuple[str, ...],
    columns: tuple[str, ...],
    watermark_column: str | None = None,
) -> TableContract:
    return TableContract(
        name=name,
        source_system=source_system,
        primary_key=primary_key,
        columns=columns,
        watermark_column=watermark_column,
    )


_CONTRACTS = {
    "ordenes": _contract(
        "ordenes",
        SourceSystem.MES,
        ("orden_id",),
        (
            "orden_id",
            "fecha_programada",
            "fecha_real",
            "fecha_turno_operativo",
            "turno",
            "secuencia_planeada",
            "secuencia_real",
            "vehiculo_id",
            "version_id",
            "prioridad_cliente",
            "mercado_destino",
            "estado_orden",
            "ready_for_dispatch_flag",
        ),
        "fecha_programada",
    ),
    "versiones_vehiculo": _contract(
        "versiones_vehiculo",
        SourceSystem.MASTER_DATA,
        ("version_id",),
        (
            "version_id",
            "familia_modelo",
            "tipo_propulsion",
            "capacidad_bateria_kwh",
            "tiempo_medio_produccion",
            "complejidad_montaje",
            "requiere_carga_salida_flag",
            "nivel_criticidad_logistica",
        ),
    ),
    "vehiculos": _contract(
        "vehiculos",
        SourceSystem.MES,
        ("vehiculo_id",),
        (
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
        ),
        "timestamp_fin_linea",
    ),
    "estado_bateria": _contract(
        "estado_bateria",
        SourceSystem.EMS,
        ("timestamp", "vehiculo_id"),
        (
            "timestamp",
            "vehiculo_id",
            "soc_pct",
            "target_soc_pct",
            "battery_temp_proxy",
            "charging_status",
            "energia_cargada_kwh",
            "tiempo_en_carga_min",
        ),
        "timestamp",
    ),
    "slots_carga": _contract(
        "slots_carga",
        SourceSystem.EMS,
        ("slot_id",),
        (
            "slot_id",
            "zona_carga",
            "potencia_max_kw",
            "tipo_cargador",
            "disponibilidad_flag",
            "mantenimiento_flag",
            "ocupacion_actual_flag",
        ),
    ),
    "sesiones_carga": _contract(
        "sesiones_carga",
        SourceSystem.EMS,
        ("sesion_id",),
        (
            "sesion_id",
            "vehiculo_id",
            "slot_id",
            "inicio_sesion",
            "fin_sesion",
            "energia_entregada_kwh",
            "tiempo_espera_previo_min",
            "carga_interrumpida_flag",
            "causa_interrupcion",
        ),
        "inicio_sesion",
    ),
    "patio": _contract(
        "patio",
        SourceSystem.WMS,
        ("timestamp", "vehiculo_id"),
        (
            "timestamp",
            "vehiculo_id",
            "zona_patio",
            "fila",
            "posicion",
            "estado_en_patio",
            "dwell_time_min",
            "blocking_flag",
            "requiere_movimiento_flag",
        ),
        "timestamp",
    ),
    "movimientos_patio": _contract(
        "movimientos_patio",
        SourceSystem.WMS,
        ("movimiento_id",),
        (
            "movimiento_id",
            "vehiculo_id",
            "timestamp_inicio",
            "timestamp_fin",
            "origen",
            "destino",
            "motivo_movimiento",
            "operador_turno",
            "movimiento_no_productivo_flag",
        ),
        "timestamp_inicio",
    ),
    "turnos": _contract(
        "turnos",
        SourceSystem.MES,
        ("fecha", "turno"),
        (
            "fecha",
            "turno",
            "headcount_proxy",
            "absentismo_proxy",
            "productividad_turno_indice",
            "presion_operativa_indice",
            "overtime_flag",
        ),
        "fecha",
    ),
    "logistica_salida": _contract(
        "logistica_salida",
        SourceSystem.WMS,
        ("salida_id",),
        (
            "salida_id",
            "vehiculo_id",
            "fecha_salida_planificada",
            "fecha_salida_real",
            "timestamp_readiness",
            "modo_salida",
            "transportista_proxy",
            "readiness_salida_flag",
            "retraso_min",
            "causa_retraso",
        ),
        "fecha_salida_planificada",
    ),
    "cuellos_botella": _contract(
        "cuellos_botella",
        SourceSystem.MES,
        ("evento_id",),
        (
            "evento_id",
            "timestamp",
            "area",
            "tipo_cuello_botella",
            "severidad",
            "duracion_min",
            "impacto_throughput_proxy",
            "impacto_salida_proxy",
            "causa_probable",
        ),
        "timestamp",
    ),
    "recursos_operativos": _contract(
        "recursos_operativos",
        SourceSystem.WMS,
        ("recurso_id",),
        (
            "recurso_id",
            "tipo_recurso",
            "area",
            "capacidad_nominal",
            "capacidad_disponible",
            "restriccion_actual_flag",
        ),
    ),
    "restricciones_operativas": _contract(
        "restricciones_operativas",
        SourceSystem.MES,
        ("restriccion_id",),
        (
            "restriccion_id",
            "timestamp_inicio",
            "timestamp_fin",
            "area",
            "tipo_restriccion",
            "severidad",
            "impacto_capacidad_pct",
        ),
        "timestamp_inicio",
    ),
    "escenarios_transicion": _contract(
        "escenarios_transicion",
        SourceSystem.PLANNING,
        ("fecha", "escenario"),
        (
            "fecha",
            "escenario",
            "share_ev",
            "intensidad_ramp_up",
            "disponibilidad_slots_carga",
            "presion_patio_indice",
            "restriccion_logistica_indice",
        ),
        "fecha",
    ),
}

SOURCE_CONTRACTS: Mapping[str, TableContract] = MappingProxyType(_CONTRACTS)
