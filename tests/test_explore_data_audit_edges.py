from __future__ import annotations

import pandas as pd

from src.explore_data_audit import _build_recommendations_md, _classify_column, _table_temporal_coverage


def test_classify_column_covers_expected_semantic_buckets() -> None:
    assert _classify_column(pd.Series(["V1"]), "vehiculo_id") == "identificadores"
    assert _classify_column(pd.Series([1, 0]), "readiness_flag") == "booleanas"
    assert _classify_column(pd.Series(pd.to_datetime(["2025-01-01"])), "fecha_real") == "temporales"
    assert _classify_column(pd.Series([1.5, 2.0]), "lead_time_min") == "metricas"
    assert _classify_column(pd.Series(["A", "B"]), "turno") == "dimensiones"
    assert _classify_column(pd.Series(["texto"]), "comentario") == "estructurales"


def test_table_temporal_coverage_returns_na_without_valid_temporal_data() -> None:
    no_time = pd.DataFrame({"orden_id": ["O1"]})
    all_null_time = pd.DataFrame({"fecha": pd.to_datetime([None])})

    assert _table_temporal_coverage(no_time) == "N/A"
    assert _table_temporal_coverage(all_null_time) == "N/A"


def test_table_temporal_coverage_spans_all_datetime_columns() -> None:
    df = pd.DataFrame(
        {
            "fecha_inicio": pd.to_datetime(["2025-01-03", "2025-01-04"]),
            "timestamp_fin": pd.to_datetime(["2025-01-05 10:00", "2025-01-06 12:00"]),
        }
    )

    assert _table_temporal_coverage(df) == "2025-01-03 00:00:00 -> 2025-01-06 12:00:00"


def test_build_recommendations_md_adds_critical_priority_when_needed() -> None:
    issues = pd.DataFrame({"severity": ["medium", "critical"]})

    md = _build_recommendations_md(issues)

    assert "Prioridad inmediata" in md
    assert "`mart_vehicle_flow_day`" in md
