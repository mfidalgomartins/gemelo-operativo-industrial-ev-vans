import re
from pathlib import Path

OFFICIAL_DASHBOARD = Path("outputs/dashboard/industrial-ev-operating-command-center.html")
DUPLICATED_DASHBOARD = Path("docs/industrial-ev-operating-command-center.html")
ROOT_INDEX = Path("index.html")


def test_single_final_dashboard_artifact() -> None:
    assert OFFICIAL_DASHBOARD.exists(), "No existe panel oficial en outputs/dashboard"
    assert not DUPLICATED_DASHBOARD.exists(), "Existe una copia duplicada en docs/"


def test_github_pages_dashboard_contracts() -> None:
    html = OFFICIAL_DASHBOARD.read_text(encoding="utf-8")

    # Contratos críticos de interacción
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
        "btn_apply",
        "btn_reset",
        "btn_toggle_filters",
        "filters_shell",
        "table_search",
        "btn_export",
        "scenario_select",
        "theme_toggle",
        "btn_print",
    ]:
        assert f'id="{fid}"' in html

    # Gráficos esperados: cada lienzo declarado tiene que quedar inicializado.
    assert html.count("<canvas id=") == 19
    assert set(re.findall(r'<canvas id="(ch_[a-z_]+)"', html)) == set(
        re.findall(r"make(?:Rank)?Chart\('(ch_[a-z_]+)'", html)
    )

    # No debe contener rutas locales; las dependencias visuales de CDN son explícitas.
    assert "file:///" not in html
    assert "C:\\" not in html
    assert "__PAYLOAD__" not in html
    assert "cdn.jsdelivr.net/npm/chart.js" in html

    # La vista de escenarios debe usar etiquetas de negocio y distinguir el
    # escenario seleccionado del recomendado por la puntuación multiobjetivo.
    for label in [
        "Rampa EV acelerada",
        "Aumentar slots de carga",
        "Mejorar secuenciación EV",
        "Optimizar uso de patio",
        "Mayor presión logística",
        "Turno con menor disponibilidad",
    ]:
        assert label in html
    assert "Mejor escenario modelado:" in html
    assert "Escenario recomendado: ' + scenarioLabel(state.scenario" not in html
    assert "selectedScenario" in html
    assert '<details class="advanced-filters">' in html
    assert html.count('role="img" aria-label=') == 19
    assert '<caption class="sr-only">' in html
    assert html.count('scope="col"') == 7
    assert "score-badge" not in html
    assert "tier-badge" not in html

    # La espina de flujo es diagnóstico y navegación: cada etapa acota el panel
    # a su área, así que debe existir y estar cableada al filtro de área.
    assert 'id="spine_track"' in html
    assert "const STAGE_ORDER = " in html
    assert "select.value = (select.value === area) ? 'ALL' : area;" in html


def test_github_pages_entry_is_in_sync_with_official_dashboard_size() -> None:
    assert OFFICIAL_DASHBOARD.exists(), "No existe panel oficial en outputs/dashboard"
    official_size = OFFICIAL_DASHBOARD.stat().st_size
    assert official_size > 100_000


def test_root_index_redirects_to_official_dashboard() -> None:
    assert ROOT_INDEX.exists(), "Falta index.html en la raíz para entrada GitHub Pages"
    html = ROOT_INDEX.read_text(encoding="utf-8")
    relative_dashboard = "./outputs/dashboard/industrial-ev-operating-command-center.html"
    assert relative_dashboard in html
    assert "/gemelo-operativo-industrial-ev-vans/outputs/" not in html
