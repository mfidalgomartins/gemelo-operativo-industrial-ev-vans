from __future__ import annotations

from reportlab.lib.units import mm
from reportlab.platypus import BalancedColumns, CondPageBreak, PageBreak

from gemelo_operativo_ev.reporting import chart_pack, report


def test_report_uses_an_embedded_editorial_type_pair() -> None:
    assert report.S["body"].fontName == "ReportSerif"
    assert report.S["h1"].fontName == "ReportSans-Bold"
    assert report.S["cover_title"].fontName == "ReportSans-Bold"
    assert report.S["caption"].fontName == "ReportSerif-Italic"
    assert report.S["body"].fontSize == 8.7
    assert report.S["body"].leading == 11.8


def test_chart_system_uses_the_reference_inspired_editorial_palette() -> None:
    assert chart_pack.INK == "#111111"
    assert chart_pack.ACCENT == "#6bcb45"
    assert chart_pack.ACCENT_TEXT == "#357d28"
    assert chart_pack.EV_COLOR == "#2aa7c7"
    assert chart_pack.SURFACE == "#f2f2f0"
    assert chart_pack.plt.rcParams["font.family"][0] == "DejaVu Sans"


def test_accent_text_uses_the_accessible_green_tone() -> None:
    assert hasattr(report, "ACCENT_TEXT")
    assert report.S["eyebrow"].textColor == report.ACCENT_TEXT
    assert report.S["kpi_num"].textColor == report.ACCENT_TEXT


def test_section_title_is_a_toc_aware_editorial_flowable() -> None:
    assert hasattr(report, "SectionTitle")
    title = report.h1("Título de prueba", "Sección 7")
    assert len(title) == 1
    assert isinstance(title[0], report.SectionTitle)
    assert title[0].getPlainText() == "Título de prueba"
    assert title[0].style.name == "h1"


def test_narrative_blocks_are_composed_as_balanced_columns() -> None:
    assert hasattr(report, "editorialize")
    narrative = [report.p("Primera columna."), report.h2("Subtítulo"), report.p("Segunda columna.")]
    result = report.editorialize(narrative)
    assert len(result) == 1
    assert isinstance(result[0], BalancedColumns)
    assert result[0]._nCols == 2


def test_tables_use_rules_instead_of_black_header_bands() -> None:
    table = report._table_flowables(["Métrica"], [["Valor"]], [120])[1]
    assert ("BACKGROUND", (0, 0), (-1, 0), report.PAPER) in table._bkgrndcmds
    assert any(command[0] == "LINEABOVE" and command[3] == 1.2 for command in table._linecmds)
    assert not any(command[0] == "BACKGROUND" and command[3] == report.INK for command in table._bkgrndcmds)


def test_kpi_strip_uses_vertical_separators_not_card_accents() -> None:
    strip = report.kpi_strip([("41%", "Salida limpia"), ("72%", "Preparación")])
    assert any(command[0] == "LINEAFTER" for command in strip._linecmds)


def test_contents_page_lists_sections_without_subsection_clutter() -> None:
    toc = report.toc_page()[-1]
    assert len(toc.levelStyles) == 1
    assert toc.levelStyles[0].name == "toc1"


def test_section_composition_replaces_terminal_hard_break_with_space_gate() -> None:
    assert hasattr(report, "compose_section")
    result = report.compose_section([report.p("Cierre de sección."), PageBreak()], is_first=False)
    assert isinstance(result[0], CondPageBreak)
    assert not any(isinstance(flowable, PageBreak) for flowable in result)


def test_cover_art_stays_clear_of_the_metadata_column() -> None:
    assert hasattr(report, "COVER_META_WIDTH")
    assert hasattr(report, "COVER_ART_X0")
    assert report.COVER_ART_X0 >= report.MARGIN + report.COVER_META_WIDTH + 2 * mm
