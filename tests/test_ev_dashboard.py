from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.config import OUTPUT_DASHBOARD_DIR, OUTPUT_REPORTS_DIR
from src.ev_build_dashboard import (
    OFFICIAL_DASHBOARD_NAME,
    _build_meta,
    _build_payload,
    _records,
    run_ev_build_dashboard,
)


@pytest.mark.integration
def test_ev_dashboard_official_build_manifest_and_single_html() -> None:
    OUTPUT_DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    inherited_candidate = OUTPUT_DASHBOARD_DIR / "panel_heredado_tmp.html"
    inherited_candidate.write_text("<html><body>heredado</body></html>", encoding="utf-8")

    result = run_ev_build_dashboard()
    output_path = Path(result.path)
    assert output_path.exists()
    assert output_path.name == OFFICIAL_DASHBOARD_NAME

    html_files = list(OUTPUT_DASHBOARD_DIR.glob("*.html"))
    assert len(html_files) == 1
    assert html_files[0].name == OFFICIAL_DASHBOARD_NAME

    assert not inherited_candidate.exists()

    manifest_path = OUTPUT_REPORTS_DIR / "dashboard_build_manifest.json"
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["official_dashboard"] == "outputs/dashboard/industrial-ev-operating-command-center.html"
    assert manifest["html_size_bytes"] < 6_000_000
    assert all(manifest["checks"].values())


def test_ev_dashboard_html_structure_filters_and_visual_safety_contracts() -> None:
    html_path = OUTPUT_DASHBOARD_DIR / OFFICIAL_DASHBOARD_NAME
    assert html_path.exists()
    html = html_path.read_text(encoding="utf-8")

    assert "__PAYLOAD__" not in html
    assert "__FILTERS__" not in html
    assert "__CHARTJS__" not in html

    # Layout safety contracts — executive redesign (Geist typography, KPI strip,
    # restrained palette, neutral surfaces, no gradients/glassmorphism).
    assert '"Geist"' in html, "Display typeface must be Geist"
    assert '"Geist Mono"' in html, "Tabular numerals must use Geist Mono"
    assert "--font-sans:" in html and "--font-mono:" in html
    assert "kpi-strip" in html, "Above-the-fold KPI strip must be present"
    assert "min-height:340px" in html or "card tall" in html, "Chart cards must reserve adequate vertical room"
    assert "maxTicksLimit: 8" in html
    assert "html[data-theme='dark']" in html
    assert 'id="theme_toggle"' in html
    assert 'id="btn_toggle_filters"' in html
    assert 'id="filters_shell"' in html
    assert "setFilterPanelCollapsed(false);" in html, "Filters are inline; start visible"
    assert "const THEME_KEY = 'ev_dashboard_theme';" in html
    # Design guardrails: no decorative gradients, glassmorphism, or excessive eyebrow chips.
    assert "linear-gradient(135deg" not in html, "No decorative hero gradients"
    assert 'class="eyebrow"' not in html or html.count('class="eyebrow"') <= 2, "Eyebrow chips must be minimal"
    assert "Iowan Old Style" not in html, "Legacy serif must be removed"
    assert "IBM Plex Sans" not in html, "Legacy sans must be removed"

    # Filter wiring contracts
    for fid in [
        "f_date_from",
        "f_date_to",
        "f_turno",
        "f_prop",
        "f_version",
        "f_area",
        "f_yard",
        "f_charge",
        "f_severity",
    ]:
        assert f'id="{fid}"' in html
    assert (
        "const filterIds = ['f_date_from','f_date_to','f_turno','f_prop','f_version','f_area','f_yard','f_charge','f_severity'];"
        in html
    )
    assert "el.addEventListener('input', updateCharts);" in html
    assert 'id="btn_apply"' in html
    assert "document.getElementById('btn_apply').addEventListener('click', updateCharts);" in html
    assert "document.getElementById('btn_toggle_filters').addEventListener('click'" in html
    assert "updateFilterSummary();" in html
    assert "scen.innerHTML = '';" in html
    for key in ["from:", "to:", "turno:", "prop:", "version:", "area:", "yard:", "charge:", "severity:"]:
        assert key in html
    assert 'id="f_severity"' in html
    assert "severity:'severidad'" in html
    assert "filterRows(DATA.throughput" in html
    assert "filterRows(DATA.seq_gap" in html
    assert "filterRows(DATA.yard_daily" in html
    assert "filterRows(DATA.charge_daily" in html
    assert "filterRows(DATA.dispatch_base" in html
    assert "filterRows(DATA.b_detail" in html
    assert "kpi_official" in html
    assert "Official dashboard build path" not in html
    assert "<strong>Version:</strong>" not in html
    assert "Dashboard Version" not in html
    assert "Actualizado" not in html

    # Chart contracts: expected number of canvases and chart initializers
    assert html.count("<canvas id=") == 17
    assert html.count("makeChart('ch_") == 17


def test_dashboard_records_serializes_dates_numbers_and_nulls_deterministically() -> None:
    df = pd.DataFrame(
        {
            "fecha": [pd.Timestamp("2025-01-01"), pd.NaT],
            "score": [np.float64(1.23456), np.nan],
            "count": [np.int64(3), 4],
            "label": ["EV", None],
        }
    )

    result = _records(df)

    assert result == [
        {"fecha": "2025-01-01", "score": 1.2346, "count": 3, "label": "EV"},
        {"fecha": None, "score": None, "count": 4, "label": None},
    ]


def test_dashboard_meta_kpi_validation_passes_with_consistent_inputs() -> None:
    flow = pd.DataFrame(
        {
            "fecha_programada": pd.to_datetime(["2024-12-31", "2025-01-01", "2025-01-02"]),
            "fecha_real": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]),
            "turno": ["A", "B", "C"],
            "orden_id": ["O1", "O2", "O3"],
            "vehiculo_id": ["V1", "V2", "V3"],
            "tipo_propulsion": ["EV", "ICE", "EV"],
            "version_id": ["VER1", "VER1", "VER2"],
            "planned_to_actual_sequence_gap": [0, 1, -1],
            "total_internal_lead_time_min": [120.0, 130.0, 140.0],
            "yard_wait_time_min": [10.0, 20.0, 30.0],
            "charging_wait_time_min": [15.0, 0.0, 25.0],
            "dispatch_delay_min": [5.0, 0.0, 10.0],
            "readiness_final_flag": [1, 0, 1],
        }
    )
    dispatch = pd.DataFrame(
        {
            "fecha": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]),
            "turno": ["A", "B", "C"],
            "tipo_propulsion": ["EV", "ICE", "EV"],
            "vehiculo_id": ["V1", "V2", "V3"],
            "departed_flag": [True, True, False],
            "delayed_flag": [True, False, True],
            "readiness_final_flag": [1, 0, 1],
            "dispatch_delay_min": [5.0, 0.0, 10.0],
            "soc_salida_pct": [80.0, 70.0, 90.0],
            "target_soc_salida_pct": [80.0, 70.0, 90.0],
            "causa_retraso": ["SOC", "SIN_RETRASO", "SIN_SALIDA"],
        }
    )
    kpi = pd.DataFrame(
        {
            "throughput_planificado": [3],
            "throughput_real": [3],
            "throughput_gap": [0],
            "share_ev": [2 / 3],
            "score_readiness_global": [float(flow["readiness_final_flag"].mean() * 100)],
            "ratio_salida_retrasada": [0.5],
            "ocupacion_media_patio": [0.55],
            "ocupacion_pico_patio": [0.75],
            "utilizacion_media_cargadores": [0.60],
        }
    )
    priorities = pd.DataFrame(
        {
            "area": ["PATIO", "CARGA"],
            "operational_priority_index": [70.0, 95.0],
            "recommended_action": ["Reducir dwell", "Reservar slots"],
        }
    )
    scenarios = pd.DataFrame(
        {
            "escenario": ["base", "acelerado"],
            "throughput": [3.0, 3.5],
            "espera_carga": [30.0, 20.0],
            "ocupacion_pico_patio": [0.75, 0.70],
            "riesgo_salida_baja_readiness": [0.20, 0.15],
            "estabilidad_operativa": [70.0, 78.0],
            "decision_score": [70.0, 81.0],
        }
    )

    meta = _build_meta(
        flow=flow,
        yard=pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2025-01-01 08:00", "2025-01-01 09:00"]),
                "zona_patio": ["NORTE", "SUR"],
                "yard_occupancy_rate": [0.5, 0.7],
                "avg_dwell_time": [30.0, 40.0],
                "p95_dwell_time": [60.0, 80.0],
                "blocking_rate": [0.1, 0.2],
                "non_productive_move_rate": [0.05, 0.10],
            }
        ),
        charging=pd.DataFrame(
            {
                "fecha": pd.to_datetime(["2025-01-01", "2025-01-02"]),
                "turno": ["A", "B"],
                "zona_carga": ["Z1", "Z2"],
                "charger_pressure_score": [40.0, 50.0],
                "avg_wait_to_charge": [10.0, 20.0],
                "interruption_rate": [0.01, 0.02],
                "target_soc_miss_rate": [0.0, 0.1],
                "sessions_per_shift": [4, 5],
            }
        ),
        dispatch=dispatch,
        priorities=priorities,
        scenarios=scenarios,
        kpi=kpi,
    )

    assert meta["coverage"] == "2025-01-01 a 2025-01-03"
    assert meta["orders"] == 3
    assert meta["vehicles"] == 3
    assert meta["executive_snapshot"]["top_area"] == "CARGA"
    assert meta["executive_snapshot"]["top_scenario"] == "acelerado"
    assert all(meta["kpi_validation"].values())


def test_dashboard_meta_kpi_validation_flags_mismatched_official_kpis() -> None:
    flow = pd.DataFrame(
        {
            "fecha_programada": pd.to_datetime(["2024-12-31", "2025-01-01"]),
            "fecha_real": pd.to_datetime(["2025-01-01", "2025-01-02"]),
            "turno": ["A", "B"],
            "orden_id": ["O1", "O2"],
            "vehiculo_id": ["V1", "V2"],
            "tipo_propulsion": ["EV", "ICE"],
            "version_id": ["VER1", "VER2"],
            "planned_to_actual_sequence_gap": [0, 1],
            "total_internal_lead_time_min": [120.0, 130.0],
            "yard_wait_time_min": [10.0, 20.0],
            "charging_wait_time_min": [15.0, 0.0],
            "dispatch_delay_min": [0.0, 0.0],
            "readiness_final_flag": [1, 1],
        }
    )
    dispatch = pd.DataFrame(
        {
            "fecha": pd.to_datetime(["2025-01-01", "2025-01-02"]),
            "turno": ["A", "B"],
            "tipo_propulsion": ["EV", "ICE"],
            "vehiculo_id": ["V1", "V2"],
            "departed_flag": [True, True],
            "delayed_flag": [False, False],
            "readiness_final_flag": [1, 1],
            "dispatch_delay_min": [0.0, 0.0],
            "soc_salida_pct": [80.0, 70.0],
            "target_soc_salida_pct": [80.0, 70.0],
            "causa_retraso": ["SIN_RETRASO", "SIN_RETRASO"],
        }
    )
    kpi = pd.DataFrame(
        {
            "throughput_planificado": [99],
            "throughput_real": [2],
            "throughput_gap": [97],
            "share_ev": [0.5],
            "score_readiness_global": [10.0],
            "ratio_salida_retrasada": [0.0],
            "ocupacion_media_patio": [0.2],
            "ocupacion_pico_patio": [0.3],
            "utilizacion_media_cargadores": [0.4],
        }
    )

    meta = _build_meta(
        flow=flow,
        yard=pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2025-01-01 08:00"]),
                "zona_patio": ["NORTE"],
                "yard_occupancy_rate": [0.5],
                "avg_dwell_time": [30.0],
                "p95_dwell_time": [60.0],
                "blocking_rate": [0.1],
                "non_productive_move_rate": [0.05],
            }
        ),
        charging=pd.DataFrame(
            {
                "fecha": pd.to_datetime(["2025-01-01"]),
                "turno": ["A"],
                "zona_carga": ["Z1"],
                "charger_pressure_score": [40.0],
                "avg_wait_to_charge": [10.0],
                "interruption_rate": [0.01],
                "target_soc_miss_rate": [0.0],
                "sessions_per_shift": [4],
            }
        ),
        dispatch=dispatch,
        priorities=pd.DataFrame(
            {"area": ["PATIO"], "operational_priority_index": [80.0], "recommended_action": ["Actuar"]}
        ),
        scenarios=pd.DataFrame(
            {
                "escenario": ["base"],
                "throughput": [3.0],
                "espera_carga": [30.0],
                "ocupacion_pico_patio": [0.75],
                "riesgo_salida_baja_readiness": [0.20],
                "estabilidad_operativa": [70.0],
                "decision_score": [70.0],
            }
        ),
        kpi=kpi,
    )

    assert not meta["kpi_validation"]["throughput_planificado_matches_flow"]
    assert not meta["kpi_validation"]["readiness_matches_flow"]
    assert not meta["kpi_validation"]["throughput_gap_matches_components"]


def test_dashboard_payload_filters_are_sorted_and_records_serialized() -> None:
    datasets = {
        "throughput": pd.DataFrame({"turno": ["C", "A"], "fecha": pd.to_datetime(["2025-01-02", "2025-01-01"])}),
        "seq_gap": pd.DataFrame({"tipo_propulsion": ["ICE", "EV"]}),
        "lead_version": pd.DataFrame({"version_id": ["VER2", "VER1"]}),
        "priorities": pd.DataFrame({"area": ["PATIO", "CARGA"]}),
        "yard_daily": pd.DataFrame({"zona_patio": ["SUR", "NORTE"]}),
        "charge_daily": pd.DataFrame({"zona_carga": ["Z2", "Z1"]}),
        "b_detail": pd.DataFrame({"severidad": ["media", "alta"]}),
        "scenarios": pd.DataFrame({"escenario": ["base"]}),
        "dispatch_base": pd.DataFrame({"departed_flag": [True]}),
        "dispatch_cause": pd.DataFrame({"causa_retraso": ["SOC"]}),
        "ev_share_week": pd.DataFrame({"week": [pd.Timestamp("2025-01-06")]}),
        "flow_prop_daily": pd.DataFrame({"fecha": [pd.Timestamp("2025-01-01")]}),
        "charge_daily_extra": pd.DataFrame({"unused": []}),
        "kpi_readiness": pd.DataFrame({"score": [1.23456]}),
    }

    payload = _build_payload(meta={"coverage": "test"}, datasets=datasets)

    assert payload["filters"]["turno"] == ["A", "C"]
    assert payload["filters"]["propulsion"] == ["EV", "ICE"]
    assert payload["filters"]["version"] == ["VER1", "VER2"]
    assert payload["filters"]["area"] == ["CARGA", "PATIO"]
    assert payload["filters"]["zona_patio"] == ["NORTE", "SUR"]
    assert payload["filters"]["zona_carga"] == ["Z1", "Z2"]
    assert payload["filters"]["severidad"] == ["alta", "media"]
    assert payload["data"]["throughput"][0]["fecha"] == "2025-01-02"
    assert payload["data"]["kpi_readiness"][0]["score"] == 1.2346
