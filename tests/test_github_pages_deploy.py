from pathlib import Path

OFFICIAL_DASHBOARD = Path("outputs/dashboard/dashboard_gemelo_operativo_ev.html")
PAGES_INDEX = Path("docs/index.html")


def test_github_pages_entry_exists_and_is_html() -> None:
    assert PAGES_INDEX.exists(), "docs/index.html no existe"
    html = PAGES_INDEX.read_text(encoding="utf-8")
    assert "<!doctype html>" in html.lower()
    assert "<canvas id=\"ch_throughput\"" in html


def test_github_pages_dashboard_contracts() -> None:
    html = PAGES_INDEX.read_text(encoding="utf-8")

    # Contratos críticos de interacción
    for fid in [
        "f_date_from", "f_date_to", "f_turno", "f_prop", "f_version",
        "f_area", "f_yard", "f_charge", "f_severity", "btn_apply", "btn_reset",
        "table_search", "btn_export", "scenario_select", "theme_toggle", "btn_print"
    ]:
        assert f'id="{fid}"' in html

    # Gráficos esperados
    assert html.count("<canvas id=") == 17
    assert html.count("makeChart('ch_") == 17

    # Debe ser autocontenido para despliegue estático
    assert "file:///" not in html
    assert "C:\\" not in html
    assert "__PAYLOAD__" not in html


def test_github_pages_entry_is_in_sync_with_official_dashboard_size() -> None:
    assert OFFICIAL_DASHBOARD.exists(), "No existe dashboard oficial en outputs/dashboard"
    official_size = OFFICIAL_DASHBOARD.stat().st_size
    pages_size = PAGES_INDEX.stat().st_size

    # Permite pequeñas diferencias futuras, pero evita drift fuerte accidental.
    assert abs(official_size - pages_size) < 200_000
