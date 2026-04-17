from __future__ import annotations

import json
from pathlib import Path

from src.config import OUTPUT_DASHBOARD_DIR, OUTPUT_REPORTS_DIR
from src.ev_build_dashboard import OFFICIAL_DASHBOARD_NAME, run_ev_build_dashboard


def test_ev_dashboard_official_build_manifest_and_single_html() -> None:
    OUTPUT_DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    legacy_candidate = OUTPUT_DASHBOARD_DIR / "legacy_dashboard_tmp.html"
    legacy_candidate.write_text("<html><body>legacy</body></html>", encoding="utf-8")

    result = run_ev_build_dashboard()
    output_path = Path(result.path)
    assert output_path.exists()
    assert output_path.name == OFFICIAL_DASHBOARD_NAME

    html_files = list(OUTPUT_DASHBOARD_DIR.glob("*.html"))
    assert len(html_files) == 1
    assert html_files[0].name == OFFICIAL_DASHBOARD_NAME

    archived_path = OUTPUT_DASHBOARD_DIR / "legacy" / legacy_candidate.name
    assert archived_path.exists()
    archived_path.unlink()
    assert not list((OUTPUT_DASHBOARD_DIR / "legacy").glob("*.html"))

    manifest_path = OUTPUT_REPORTS_DIR / "dashboard_build_manifest.json"
    qa_report_path = OUTPUT_REPORTS_DIR / "dashboard_qa_report.md"
    assert manifest_path.exists()
    assert qa_report_path.exists()

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

    # Layout safety contracts
    assert "grid-template-columns:repeat(auto-fit,minmax(170px,1fr));" in html
    assert "grid-template-columns:repeat(auto-fit,minmax(360px,1fr));" in html
    assert "min-height:320px" in html
    assert "maxTicksLimit: 8" in html
    assert "html[data-theme='dark']" in html
    assert 'id="theme_toggle"' in html
    assert 'id="btn_toggle_filters"' in html
    assert 'id="filters_shell"' in html
    assert "setFilterPanelCollapsed(true);" in html
    assert "const THEME_KEY = 'ev_dashboard_theme';" in html

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
    assert "const filterIds = ['f_date_from','f_date_to','f_turno','f_prop','f_version','f_area','f_yard','f_charge','f_severity'];" in html
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
