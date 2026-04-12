from pathlib import Path

OFFICIAL_DASHBOARD = Path("outputs/dashboard/industrial-ev-operating-command-center.html")
PAGES_INDEX = Path("docs/index.html")
PAGES_DASHBOARD = Path("docs/industrial-ev-operating-command-center.html")


def test_github_pages_entrypoint_redirect_exists() -> None:
    assert PAGES_INDEX.exists(), "docs/index.html no existe"
    html = PAGES_INDEX.read_text(encoding="utf-8")
    assert "<!doctype html>" in html.lower()
    assert "industrial-ev-operating-command-center.html" in html
    assert "http-equiv=\"refresh\"" in html


def test_github_pages_dashboard_contracts() -> None:
    assert PAGES_DASHBOARD.exists(), "docs/industrial-ev-operating-command-center.html no existe"
    html = PAGES_DASHBOARD.read_text(encoding="utf-8")

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
    pages_size = PAGES_DASHBOARD.stat().st_size

    # Permite pequeñas diferencias futuras, pero evita drift fuerte accidental.
    assert abs(official_size - pages_size) < 200_000
