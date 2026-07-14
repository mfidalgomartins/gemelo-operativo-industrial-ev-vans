from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from gemelo_operativo_ev.reporting.chart_pack import format_period_es, prepare_daily_throughput, scenario_label

ROOT = Path(__file__).resolve().parents[1]


def _vehicle_rows(day_counts: list[tuple[str, int]]) -> pd.DataFrame:
    rows = []
    for day, count in day_counts:
        rows.extend({"fecha_real": day, "vehiculo_id": f"{day}-{idx}"} for idx in range(count))
    return pd.DataFrame(rows)


def test_daily_throughput_removes_only_partial_coverage_at_edges() -> None:
    frame = _vehicle_rows(
        [
            ("2025-01-01", 2),
            ("2025-01-02", 10),
            ("2025-01-03", 2),
            ("2025-01-04", 11),
            ("2025-01-05", 2),
        ]
    )

    daily = prepare_daily_throughput(frame)

    assert daily["fecha"].dt.strftime("%Y-%m-%d").tolist() == ["2025-01-02", "2025-01-03", "2025-01-04"]
    assert daily["real"].tolist() == [10, 2, 11]
    assert daily.loc[1, "coverage_ratio"] < 0.5


def test_daily_throughput_validates_schema_and_threshold() -> None:
    with pytest.raises(ValueError, match="Faltan columnas requeridas"):
        prepare_daily_throughput(pd.DataFrame({"fecha_real": ["2025-01-01"]}))

    frame = _vehicle_rows([("2025-01-01", 1)])
    with pytest.raises(ValueError, match="intervalo"):
        prepare_daily_throughput(frame, min_coverage_ratio=0)


def test_period_and_scenario_labels_are_business_facing() -> None:
    dates = pd.Series(pd.to_datetime(["2025-01-02", "2025-12-31"]))

    assert format_period_es(dates) == "enero de 2025 a diciembre de 2025"
    assert scenario_label("1_ramp_up_ev_base") == "Rampa EV base"
    assert scenario_label("8_combinacion_medidas_correctivas") == "Combinación de medidas correctivas"


def test_public_portfolio_contracts() -> None:
    attributes = (ROOT / ".gitattributes").read_text(encoding="utf-8")
    for pattern in ["outputs/dashboard/**", "outputs/graphs/**", "outputs/reports/**", "data/**"]:
        assert f"{pattern} linguist-generated=true" in attributes

    chart_source = (ROOT / "src" / "gemelo_operativo_ev" / "reporting" / "chart_pack.py").read_text(encoding="utf-8")
    for stale_copy in ["Synthetic factory data", "Operational Priority Index", "Ramp-up base", "iloc[:-2]"]:
        assert stale_copy not in chart_source

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "incluye un corte canónico de los CSV de origen, marts y la base DuckDB" not in readme
    assert "base DuckDB y estado operacional son reconstruibles" in readme
    assert "Operational Priority Index" not in readme
