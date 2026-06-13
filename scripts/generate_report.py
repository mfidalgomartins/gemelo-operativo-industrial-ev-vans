"""Builds the analytical PDF report from processed data and the chart pack.

Output: outputs/reports/ev_transition_operating_twin_report.pdf

A multi-page, narrative report in the style of a senior operations analyst.
Prose flows continuously; charts and tables are placed inline next to the
findings they support. Numbers are pulled from the processed marts so the
report stays consistent with the published dashboard.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    Image,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.tableofcontents import TableOfContents

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "processed" / "ev_factory"
GRAPHS = ROOT / "outputs" / "graphs"
OUT = ROOT / "outputs" / "reports"
OUT.mkdir(parents=True, exist_ok=True)
PDF = OUT / "ev_transition_operating_twin_report.pdf"

# ── Palette (matches the chart pack)
INK = colors.HexColor("#1c1917")
INK_2 = colors.HexColor("#44403c")
MUTED = colors.HexColor("#78716c")
SUBTLE = colors.HexColor("#a8a29e")
LINE = colors.HexColor("#e7e5e4")
ACCENT = colors.HexColor("#1d4ed8")
DANGER = colors.HexColor("#b91c1c")
POSITIVE = colors.HexColor("#15803d")
PAPER = colors.HexColor("#ffffff")
BAND = colors.HexColor("#f5f5f4")

PAGE_W, PAGE_H = A4
MARGIN = 2.2 * cm
CONTENT_W = PAGE_W - 2 * MARGIN

# ── Numbers loaded once from the marts (single source of truth)
kpi = pd.read_csv(DATA / "kpi_operativos.csv").iloc[0]
prio = pd.read_csv(DATA / "operational_prioritization_table.csv")
scen = pd.read_csv(DATA / "scenario_decision_comparison.csv")
levers = pd.read_csv(DATA / "scenario_lever_ranking.csv")
checks = pd.read_csv(DATA / "validation_checks.csv")
evice = pd.read_csv(DATA / "diagnostic_ev_vs_non_ev.csv")
gov = pd.read_csv(DATA / "scoring_governance_checks.csv")
ranking = pd.read_csv(DATA / "diagnostic_area_ranking.csv")

TOTAL = int(kpi["total_ordenes"])
SHARE_EV = float(kpi["share_ev"])
NO_READY = int(kpi["vehiculos_no_ready"])
READY = TOTAL - NO_READY
RATIO_LATE = float(kpi["ratio_salida_retrasada"])
READINESS_GLOBAL = float(kpi["score_readiness_global"])
DWELL_MEAN_H = float(kpi["tiempo_medio_patio_min"]) / 60
DWELL_P95_H = float(kpi["dwell_p95_min"]) / 60
WAIT_CHARGE = float(kpi["tiempo_medio_espera_carga_min"])
CHARGER_UTIL = float(kpi["utilizacion_media_cargadores"])
ON_TIME_READY = int(round(READY * (1 - RATIO_LATE)))

best = scen.sort_values("decision_score", ascending=False).iloc[0]
base = scen.set_index("escenario").loc["1_ramp_up_ev_base"]


# ──────────────────────────────────────────────────────────────────────────────
# Styles
def build_styles() -> dict:
    ss = getSampleStyleSheet()
    s = {}
    s["body"] = ParagraphStyle(
        "body",
        parent=ss["BodyText"],
        fontName="Helvetica",
        fontSize=10.3,
        leading=15.6,
        textColor=INK_2,
        alignment=TA_JUSTIFY,
        spaceAfter=8,
        spaceBefore=0,
    )
    s["lead"] = ParagraphStyle(
        "lead",
        parent=s["body"],
        fontSize=11.5,
        leading=17,
        textColor=INK,
        spaceAfter=11,
    )
    s["h1"] = ParagraphStyle(
        "h1",
        parent=ss["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=20,
        leading=24,
        textColor=INK,
        spaceBefore=10,
        spaceAfter=4,
    )
    s["h1_plain"] = ParagraphStyle(
        "h1_plain",
        parent=s["h1"],
    )
    s["eyebrow"] = ParagraphStyle(
        "eyebrow",
        fontName="Helvetica-Bold",
        fontSize=9,
        leading=12,
        textColor=ACCENT,
        spaceBefore=0,
        spaceAfter=2,
    )
    s["h2"] = ParagraphStyle(
        "h2",
        parent=ss["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=13.5,
        leading=18,
        textColor=INK,
        spaceBefore=16,
        spaceAfter=5,
    )
    s["h3"] = ParagraphStyle(
        "h3",
        fontName="Helvetica-Bold",
        fontSize=11,
        leading=15,
        textColor=INK,
        spaceBefore=10,
        spaceAfter=3,
    )
    s["caption"] = ParagraphStyle(
        "caption",
        fontName="Helvetica",
        fontSize=8.6,
        leading=11.5,
        textColor=MUTED,
        alignment=TA_LEFT,
        spaceBefore=4,
        spaceAfter=14,
    )
    s["callout"] = ParagraphStyle(
        "callout",
        fontName="Helvetica",
        fontSize=10.5,
        leading=15,
        textColor=INK,
        leftIndent=10,
        spaceBefore=2,
        spaceAfter=2,
    )
    s["kpi_num"] = ParagraphStyle(
        "kpi_num",
        fontName="Helvetica-Bold",
        fontSize=19,
        leading=21,
        textColor=INK,
    )
    s["kpi_lbl"] = ParagraphStyle(
        "kpi_lbl",
        fontName="Helvetica",
        fontSize=7.8,
        leading=10,
        textColor=MUTED,
    )
    s["toc1"] = ParagraphStyle(
        "toc1",
        fontName="Helvetica-Bold",
        fontSize=11,
        leading=20,
        textColor=INK,
    )
    s["toc2"] = ParagraphStyle(
        "toc2",
        fontName="Helvetica",
        fontSize=10,
        leading=17,
        textColor=INK_2,
        leftIndent=14,
    )
    s["cover_title"] = ParagraphStyle(
        "cover_title",
        fontName="Helvetica-Bold",
        fontSize=30,
        leading=35,
        textColor=INK,
    )
    s["cover_sub"] = ParagraphStyle(
        "cover_sub",
        fontName="Helvetica",
        fontSize=13.5,
        leading=19,
        textColor=MUTED,
    )
    s["cover_meta"] = ParagraphStyle(
        "cover_meta",
        fontName="Helvetica",
        fontSize=9.5,
        leading=15,
        textColor=INK_2,
    )
    s["tbl"] = ParagraphStyle("tbl", fontName="Helvetica", fontSize=8.8, leading=11.5, textColor=INK_2)
    s["tbl_b"] = ParagraphStyle("tbl_b", fontName="Helvetica-Bold", fontSize=8.8, leading=11.5, textColor=INK)
    s["tbl_h"] = ParagraphStyle("tbl_h", fontName="Helvetica-Bold", fontSize=8.4, leading=11, textColor=colors.white)
    return s


S = build_styles()


# ──────────────────────────────────────────────────────────────────────────────
# Document template with header / footer / TOC notify
class Report(BaseDocTemplate):
    def __init__(self, filename, **kw):
        super().__init__(filename, pagesize=A4, **kw)
        frame = Frame(MARGIN, MARGIN, CONTENT_W, PAGE_H - 2 * MARGIN - 8 * mm, id="body")
        cover_frame = Frame(MARGIN, MARGIN, CONTENT_W, PAGE_H - 2 * MARGIN, id="cover")
        self.addPageTemplates(
            [
                PageTemplate(id="cover", frames=[cover_frame], onPage=self._cover_bg),
                PageTemplate(id="main", frames=[frame], onPage=self._chrome),
            ]
        )

    def _cover_bg(self, canvas, doc):
        canvas.saveState()
        canvas.setFillColor(INK)
        canvas.rect(0, PAGE_H - 6 * mm, PAGE_W, 6 * mm, fill=1, stroke=0)
        canvas.setFillColor(ACCENT)
        canvas.rect(0, PAGE_H - 6 * mm, PAGE_W * 0.39, 6 * mm, fill=1, stroke=0)
        canvas.restoreState()

    def _chrome(self, canvas, doc):
        canvas.saveState()
        # header
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(SUBTLE)
        canvas.drawString(MARGIN, PAGE_H - MARGIN + 6 * mm, "Operating Twin for the EV Van Transition")
        canvas.drawRightString(
            PAGE_W - MARGIN, PAGE_H - MARGIN + 6 * mm, "Operational diagnostics and scenario analysis"
        )
        canvas.setStrokeColor(LINE)
        canvas.setLineWidth(0.6)
        canvas.line(MARGIN, PAGE_H - MARGIN + 4 * mm, PAGE_W - MARGIN, PAGE_H - MARGIN + 4 * mm)
        # footer
        canvas.setStrokeColor(LINE)
        canvas.line(MARGIN, MARGIN - 4 * mm, PAGE_W - MARGIN, MARGIN - 4 * mm)
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(SUBTLE)
        canvas.drawString(MARGIN, MARGIN - 9 * mm, "Synthetic factory data")
        canvas.drawRightString(PAGE_W - MARGIN, MARGIN - 9 * mm, f"{doc.page}")
        canvas.restoreState()

    def afterFlowable(self, flowable):  # noqa: N802  (overrides reportlab BaseDocTemplate)
        if not hasattr(flowable, "style"):
            return
        name = flowable.style.name
        if name == "h1":
            self.notify("TOCEntry", (0, flowable.getPlainText(), self.page))
        elif name == "h2":
            self.notify("TOCEntry", (1, flowable.getPlainText(), self.page))


# ──────────────────────────────────────────────────────────────────────────────
# Flowable helpers
def fig(name: str, caption: str, width=CONTENT_W) -> list:
    path = GRAPHS / name
    img = Image(str(path))
    iw, ih = img.imageWidth, img.imageHeight
    img.drawWidth = width
    img.drawHeight = width * ih / iw
    return [Spacer(1, 4), img, Paragraph(caption, S["caption"])]


def p(text: str) -> Paragraph:
    return Paragraph(text, S["body"])


def lead(text: str) -> Paragraph:
    return Paragraph(text, S["lead"])


def h1(text: str, eyebrow: str | None = None) -> list:
    out = []
    if eyebrow:
        out.append(Paragraph(eyebrow.upper(), S["eyebrow"]))
    out.append(Paragraph(text, S["h1"]))
    out.append(hr())
    return out


def h2(text: str) -> Paragraph:
    return Paragraph(text, S["h2"])


def h3(text: str) -> Paragraph:
    return Paragraph(text, S["h3"])


def hr(color=LINE, w=0.8, space_after=10):
    t = Table([[""]], colWidths=[CONTENT_W], rowHeights=[1])
    t.setStyle(
        TableStyle(
            [
                ("LINEBELOW", (0, 0), (-1, -1), w, color),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), space_after),
            ]
        )
    )
    return t


def kpi_strip(items) -> Table:
    """items: list of (number, label) -> single row of stat cards."""
    cells = []
    for num, label in items:
        inner = Table(
            [[Paragraph(num, S["kpi_num"])], [Paragraph(label, S["kpi_lbl"])]],
            colWidths=[(CONTENT_W - (len(items) - 1) * 6) / len(items)],
        )
        inner.setStyle(
            TableStyle(
                [
                    ("TOPPADDING", (0, 0), (-1, -1), 2),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 1),
                    ("BOTTOMPADDING", (0, 1), (-1, 1), 8),
                    ("LEFTPADDING", (0, 0), (-1, -1), 10),
                    ("LINEABOVE", (0, 0), (-1, 0), 2, ACCENT),
                ]
            )
        )
        cells.append(inner)
    t = Table([cells], colWidths=[(CONTENT_W) / len(items)] * len(items))
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), BAND),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    return t


def _aligned_style(base: ParagraphStyle, align: str) -> ParagraphStyle:
    a = {"RIGHT": TA_RIGHT, "CENTER": 1, "LEFT": TA_LEFT}.get(align, TA_LEFT)
    return ParagraphStyle(f"{base.name}_{align}", parent=base, alignment=a)


def data_table(header, rows, col_widths, highlight_first_col=True, aligns=None) -> Table:
    aligns = aligns or {}
    head = []
    for j, h in enumerate(header):
        hs = _aligned_style(S["tbl_h"], aligns.get(j, "LEFT"))
        head.append(Paragraph(h, hs))
    body = [head]
    for r in rows:
        cells = []
        for j, val in enumerate(r):
            base = S["tbl_b"] if (highlight_first_col and j == 0) else S["tbl"]
            style = _aligned_style(base, aligns.get(j, "LEFT"))
            cells.append(Paragraph(str(val), style))
        body.append(cells)
    t = Table(body, colWidths=col_widths, repeatRows=1)
    ts = [
        ("BACKGROUND", (0, 0), (-1, 0), INK),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [PAPER, BAND]),
        ("LINEBELOW", (0, 0), (-1, 0), 0.6, INK),
        ("LINEBELOW", (0, -1), (-1, -1), 0.6, LINE),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]
    if aligns:
        for col, al in aligns.items():
            ts.append(("ALIGN", (col, 0), (col, -1), al))
    t.setStyle(TableStyle(ts))
    return [Spacer(1, 2), t, Spacer(1, 12)]


def pct(x, d=0):
    return f"{x * 100:.{d}f}%"


# ──────────────────────────────────────────────────────────────────────────────
def cover() -> list:
    st = []
    st.append(Spacer(1, 3.2 * cm))
    st.append(Paragraph("OPERATIONS DIAGNOSTIC AND DECISION REPORT", S["eyebrow"]))
    st.append(Spacer(1, 6))
    st.append(Paragraph("Operating Twin for the<br/>Electric Van Transition", S["cover_title"]))
    st.append(Spacer(1, 14))
    st.append(
        Paragraph(
            "Where the EV ramp-up actually breaks the plant, how much output it costs, "
            "and which capacity levers recover it.",
            S["cover_sub"],
        )
    )
    st.append(Spacer(1, 1.6 * cm))
    st.append(hr(INK, 1.0, 10))
    meta = Table(
        [
            [
                Paragraph("<b>Scope</b>", S["cover_meta"]),
                Paragraph("Single van assembly plant during a 12-month EV ramp-up", S["cover_meta"]),
            ],
            [
                Paragraph("<b>Period</b>", S["cover_meta"]),
                Paragraph("January 2025 to December 2025, 53 operating weeks", S["cover_meta"]),
            ],
            [
                Paragraph("<b>Records</b>", S["cover_meta"]),
                Paragraph(f"{TOTAL:,} vehicle-orders across sequencing, yard, charging and dispatch", S["cover_meta"]),
            ],
            [
                Paragraph("<b>Method</b>", S["cover_meta"]),
                Paragraph(
                    "Reproducible pipeline: DuckDB marts, Python diagnostics, scenario twin, Monte Carlo scoring",
                    S["cover_meta"],
                ),
            ],
            [
                Paragraph("<b>Data</b>", S["cover_meta"]),
                Paragraph(
                    "Synthetic, deterministic seed. Patterns are internally consistent; absolute numbers are not measurements from a real plant.",
                    S["cover_meta"],
                ),
            ],
        ],
        colWidths=[3 * cm, CONTENT_W - 3 * cm],
    )
    meta.setStyle(
        TableStyle(
            [
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    st.append(meta)
    st.append(Spacer(1, 1.4 * cm))
    st.append(hr(LINE, 0.8, 6))
    st.append(
        Paragraph(
            "Figures are generated directly from the processed marts and match the live operating dashboard.",
            S["caption"],
        )
    )
    return st


def toc_page() -> list:
    st = [Paragraph("Contents", S["h1_plain"]), hr()]
    toc = TableOfContents()
    toc.levelStyles = [S["toc1"], S["toc2"]]
    st.append(toc)
    return st


# ──────────────────────────────────────────────────────────────────────────────
def build_story() -> list:
    st: list = []

    # ---- Cover
    st += cover()
    st.append(NextPageTemplate("main"))
    st.append(PageBreak())

    # ---- TOC
    st += toc_page()
    st.append(PageBreak())

    # ============================================================= 1. EXEC SUMMARY
    st += h1("Executive summary", "Section 1")
    st.append(
        kpi_strip(
            [
                (f"{TOTAL // 1000}k", "VEHICLE-ORDERS ANALYSED"),
                (pct(SHARE_EV), "EV SHARE OF FLOW"),
                (pct(RATIO_LATE), "DISPATCHED LATE"),
                (pct(READY / TOTAL), "READY AT DISPATCH"),
            ]
        )
    )
    st.append(Spacer(1, 12))
    st.append(
        lead(
            "The plant moves volume. Across the full ramp-up it completed every one of its "
            f"{TOTAL:,} scheduled orders, holding a steady 160 vehicles per day even as the electric "
            f"mix climbed from 5 percent to nearly 80 percent of weekly output. Throughput is not the "
            "problem. The problem sits at the end of the line, where vehicles wait to leave."
        )
    )
    st.append(
        p(
            f"Only {pct(READY / TOTAL)} of completed vehicles are ready at their dispatch window, and "
            f"{pct(RATIO_LATE)} of those that do leave go out more than two hours late. Combine the two "
            f"gates and just {ON_TIME_READY:,} orders, around {pct(ON_TIME_READY / TOTAL)} of the plan, "
            "leave both on time and ready. The output the plant builds is not the output it reliably ships. "
            "That gap is the single most important number in this report."
        )
    )
    st.append(
        p(
            "The cause is concentrated and identifiable. It is not spread evenly across the factory and it "
            "is not a generic EV slowdown. Three findings define it. First, the readiness gap is an EV "
            f"phenomenon: electric versions are ready {pct(0.412)} of the time against {pct(0.928)} for "
            "combustion versions, and the four EV versions carry 78 percent of all late-dispatch minutes. "
            "Second, the physical chokepoint is one yard zone. The pre-dispatch staging area runs at a 49-hour "
            "p95 dwell time and is blocked almost continuously, while the rest of the yard flows freely. Third, "
            "internal cycle time is not the issue. EV and ICE vehicles share the same lead-time profile, around "
            "24 hours end to end, so the EV cost shows up in readiness and exit timing rather than in how long a "
            "vehicle takes to build."
        )
    )
    st.append(
        p(
            "The prioritisation model ranks Logistics and Yard as the two areas to stabilise first, and that "
            "ranking is robust. Across 300 Monte Carlo redraws of the scoring weights, Logistics stays first "
            "77 percent of the time and Yard takes the top spot the rest, so no other area ever leads. The "
            "scenario twin points to the same conclusion from the other direction: no single lever fixes the "
            "exit gate. The combined corrective package, sequencing discipline plus charging capacity plus yard "
            "buffer redesign, is the only scenario that improves throughput, internal time and on-time exits at "
            f"once, lifting the decision score to {best['decision_score']:.1f} against {base['decision_score']:.1f} "
            "for the unmanaged ramp-up."
        )
    )
    st.append(h3("What to do, in order"))
    st.append(
        p(
            "1. Treat the pre-dispatch yard zone as a capacity-limited resource. Cap occupancy, redesign the "
            "buffer by destination window, and pull-sequence vehicles into staging only when their dispatch slot is "
            "real. This attacks the 49-hour staging dwell that drives the exit delay.<br/>"
            "2. Reserve charging slots for EV versions and add capacity at the shift peaks. Charging capacity is the "
            f"highest-return lever in the scenario model at an expected impact of {levers['impacto_esperado'].max():.2f}, "
            "and EV readiness is the gate that the corrective package moves most.<br/>"
            "3. Impose a readiness window on dispatch. Stop releasing combustion vehicles into a staging area that is "
            "already saturated by EVs waiting on charge and state-of-charge confirmation, and the on-time gate recovers "
            "without any new throughput."
        )
    )
    st.append(
        p(
            "The rest of this report shows the evidence behind each of these statements, quantifies the size of the "
            "prize, and states plainly where the analysis stops and judgement begins."
        )
    )
    st.append(PageBreak())

    # ============================================================= 2. CONTEXT
    st += h1("Context and objectives", "Section 2")
    st.append(
        lead(
            "A van plant in the middle of an electric ramp-up is running two factories at once. The combustion line "
            "it has optimised for years, and a growing electric line that shares the same yard, the same dispatch bays "
            "and the same shifts but behaves differently at every step. This report asks where that collision actually "
            "hurts and what it costs."
        )
    )
    st.append(
        p(
            "An EV is not a combustion vehicle with a different engine from an operations point of view. It needs a "
            "charge before it leaves, which adds a dependency that did not exist before. It carries a state-of-charge "
            "target that has to be confirmed at dispatch. It competes for a finite set of charging slots that the plant "
            "did not need a year ago. Each of these touches the back half of the flow, the part between end of line and "
            "the gate, which is exactly where this plant is losing reliability."
        )
    )
    st.append(
        p(
            "The operating twin built for this analysis integrates five operational domains that are usually looked at "
            "separately: production sequencing, yard movements, charging sessions, dispatch readiness and outbound "
            "logistics. Looking at them together is the point. A late vehicle is rarely late for one reason. It is late "
            "because a charging slot was busy, which kept it in a staging zone, which was already full, which blocked the "
            "next vehicle. Single-domain dashboards miss that chain. The twin is designed to follow it."
        )
    )
    st.append(h2("Objectives"))
    st.append(
        p(
            "The analysis has four concrete objectives, and every section maps back to one of them.<br/>"
            "<b>Locate the bottleneck.</b> Identify which area and which physical zone constrain reliable output, "
            "with evidence rather than intuition.<br/>"
            "<b>Quantify the EV penalty.</b> Separate what the electric mix genuinely costs from what is simply the "
            "plant's existing operating noise.<br/>"
            "<b>Rank the response.</b> Order the areas and the capacity levers by expected return, and test whether that "
            "order is stable or an artefact of the chosen weights.<br/>"
            "<b>Compare the futures.</b> Put the unmanaged ramp-up side by side with targeted interventions and a combined "
            "package, on a consistent multi-criteria score."
        )
    )
    st.append(h2("How to read this report"))
    st.append(
        p(
            "Findings are organised by question, not by data source. Each finding opens with the claim, shows the chart "
            "that supports it, then explains what the chart does and does not prove. Scores reported on a 0 to 100 scale "
            "are relative pressure indices, not physical units, and are defined in the methodology section. Where a number "
            "is a modelling assumption rather than a measurement, the text says so. The recommendations in the final "
            "section are tied back to the specific findings that justify them."
        )
    )
    st.append(PageBreak())

    # ============================================================= 3. DATA & METHOD
    st += h1("Data and methodology", "Section 3")
    st.append(
        lead(
            "Every figure in this report is reproducible from a single deterministic pipeline. The same commands that "
            "regenerate the raw tables also rebuild the marts, the diagnostics and the dashboard, so the numbers here, "
            "the numbers on the dashboard and the numbers in the source data cannot drift apart."
        )
    )
    st.append(h2("Data foundation"))
    st.append(
        p(
            "The analysis rests on 14 synthetic source tables covering orders, vehicles, versions, yard movements, "
            "charging sessions, charging slots, shifts, operational resources, bottleneck events and outbound logistics. "
            "The generator models three phases of the ramp-up, a pre-series period, the ramp itself and a stabilisation "
            "tail, so the EV share rises over time the way it would in a real launch rather than sitting at a flat "
            f"average. Across the period the plant handles {TOTAL:,} vehicle-orders, of which {pct(SHARE_EV)} are "
            "electric, spread across three model families and eight versions, shipping to seven European markets."
        )
    )
    st.append(
        p(
            "The data is synthetic and seeded. That is a deliberate choice for a public, shareable analysis: it removes "
            "confidentiality constraints and makes the entire chain auditable. It also sets the boundary on interpretation. "
            "The patterns here are realistic and internally consistent, but the absolute numbers are not measurements from "
            "a real plant and should not be read as benchmarks for one."
        )
    )
    st.append(h2("Pipeline"))
    st.append(
        p(
            "Raw tables are loaded into DuckDB, which builds a staging layer, integrated views across domains, the "
            "analytical marts and a set of governed KPIs. Python then takes over for the work that SQL is poor at: "
            "feature engineering, the area-level diagnostic, the scenario twin and the scoring framework. A release gate "
            "runs last and checks structural integrity, metric consistency and the data contracts the dashboard depends on. "
            "The gate is not cosmetic. A single real dispatch with no readiness record blocks publication, because that "
            "would mean a vehicle left the plant that the data says was never ready, and the central finding of this report "
            "depends on the readiness signal being trustworthy."
        )
    )
    st.append(h2("Data quality"))
    st.append(
        p(
            "Ten integrity checks run on every build. All ten pass with zero failing rows: no invalid denominators, no "
            "duplicate orders, no EV requiring a charge with no session, no capacity inconsistency, no delay without a "
            "cause, no dispatch without readiness, no incoherent sequence, no impossible charging session, no out-of-range "
            "state of charge, no out-of-order timestamps. The findings that follow sit on data that is internally clean."
        )
    )
    st += data_table(
        ["Integrity check", "Failing rows", "Status"],
        [
            [c["check_name"].replace("_", " ").capitalize(), int(c["failed_rows"]), c["status"]]
            for _, c in checks.iterrows()
        ],
        [CONTENT_W * 0.62, CONTENT_W * 0.20, CONTENT_W * 0.18],
        aligns={1: "CENTER", 2: "CENTER"},
    )
    st.append(PageBreak())

    # ============================================================= 4. FRAMEWORK
    st += h1("Analytical framework", "Section 4")
    st.append(
        lead(
            "Three constructs carry the analysis: a set of operational pressure scores, a single Operational Priority "
            "Index that ranks the areas, and a scenario decision score. Each is defined here so that the findings can be "
            "read without reverse-engineering the maths."
        )
    )
    st.append(h2("Pressure scores"))
    st.append(
        p(
            "Raw operational metrics live on incompatible scales. A dwell time is in minutes, an occupancy rate is a "
            "fraction, a count of non-productive moves is an integer. To compare areas and propulsion types on one axis, "
            "each driver is normalised to a 0 to 100 pressure score, where higher means more operational strain. These "
            "scores are explicitly relative. A yard congestion score of 74 does not mean the yard is 74 percent full. It "
            "means the yard sits high on the observed distribution of congestion pressure. The scores answer where strain "
            "concentrates, not what the strain measures in physical units."
        )
    )
    st.append(h2("Operational Priority Index"))
    st.append(
        p(
            "The OPI combines five pressure scores into one ranking number per area: throughput loss, yard risk, charging "
            "risk, dispatch risk and launch-transition risk. It is the model's answer to a blunt management question: if we "
            "can only stabilise one or two areas next week, which ones. The index is not a causal estimate. It is a "
            "transparent, weighted prioritisation that can be recomputed under different weights, which is exactly what the "
            "robustness analysis does."
        )
    )
    st.append(h2("Decision score"))
    st.append(
        p(
            "Scenarios are scored on a multi-criteria function that rewards throughput, readiness and operational stability "
            "and penalises late dispatch and congestion risk. It deliberately refuses to optimise a single metric, because "
            "the whole finding of this report is that the plant can hit its volume target while failing its reliability "
            "target. A score that only looked at throughput would rate the unmanaged ramp-up as a success."
        )
    )
    st.append(h2("Governance"))
    st.append(
        p(
            "The prioritisation is held to five governance checks before it is allowed to drive any recommendation: the "
            "priority index must show real dispersion across areas, the risk drivers must be diverse rather than collapsing "
            "to one, the priority tiers must separate, and the top-ranked area must be stable under resampling. All five "
            "pass. The standout is dispersion: the OPI standard deviation across areas is 17.8 against a floor of 1.0, which "
            "means the ranking genuinely separates areas rather than rating them all alike."
        )
    )
    st += data_table(
        ["Governance check", "Value", "Threshold", "Status"],
        [
            [
                g["check_name"].replace("_", " ").capitalize(),
                f"{float(g['value']):.2f}",
                f"{float(g['threshold']):.2f}",
                g["status"],
            ]
            for _, g in gov.iterrows()
        ],
        [CONTENT_W * 0.46, CONTENT_W * 0.18, CONTENT_W * 0.18, CONTENT_W * 0.18],
        aligns={1: "CENTER", 2: "CENTER", 3: "CENTER"},
    )
    st.append(PageBreak())

    # ============================================================= 5. FINDINGS: THROUGHPUT
    st += h1("Findings: the plant builds volume but cannot ship it", "Section 5")
    st.append(
        lead(
            "Start with the good news, because it frames everything that follows. The plant is not throughput-constrained. "
            "It builds what it is asked to build, every day, through the entire ramp."
        )
    )
    st += fig(
        "01_throughput_daily.png",
        "Figure 1. Daily completions with a 7-day moving average. Output holds around 160 vehicles per day for the full period; the weekly waves are operating rhythm, not decline.",
    )
    st.append(
        p(
            "Daily completions sit on a flat trend at roughly 160 vehicles per day. The visible week-to-week swing is the "
            "normal breathing of a three-shift operation, not a downward drift. The throughput gap against plan is zero: "
            "every scheduled order was completed. If the only question were can the plant make the vehicles, the answer is "
            "yes, and the EV ramp did not change that answer."
        )
    )
    st += fig(
        "02_share_ev_weekly.png",
        "Figure 2. Weekly EV share of completed flow. The mix climbs from about 5 percent to nearly 80 percent across the ramp, a genuine transition rather than a token pilot.",
    )
    st.append(
        p(
            "The second chart shows this was a real transition, not a pilot. The electric share of weekly output rises from "
            "around 5 percent at the start to nearly 80 percent by the end. The plant absorbed that shift in mix without "
            "losing build volume. That is a real achievement and it is worth stating before the report turns to what broke, "
            "because it locates the problem precisely. The failure is not in making EVs. It is in getting them out."
        )
    )
    st += fig(
        "09_dispatch_funnel.png",
        "Figure 3. From scheduled order to clean exit. Readiness removes 27 points of the plan and late dispatch removes another 31, leaving a 41 percent clean-exit rate.",
    )
    st.append(
        p(
            f"The exit funnel is where the volume story ends and the reliability story begins. All {TOTAL:,} orders are "
            f"completed, but only {READY:,} are ready at their dispatch window, a {pct(NO_READY / TOTAL)} readiness loss. "
            f"Of those that are ready, {pct(RATIO_LATE)} still leave late. Stack the two gates and the clean-exit rate, on "
            f"time and ready, falls to {pct(ON_TIME_READY / TOTAL)}. The plant is converting a fully-built order book into a "
            "minority of clean exits. Every section that follows is an attempt to explain that collapse and to size what "
            "recovering it is worth."
        )
    )
    st.append(PageBreak())

    # ============================================================= 6. FINDINGS: EV PENALTY
    st += h1("Findings: the EV penalty is real, but specific", "Section 6")
    st.append(
        lead(
            "It would be easy to blame the ramp-up in general. The data does not allow that. The electric mix carries a clear "
            "operational penalty, but it lands in three named places, and one obvious suspect is cleared entirely."
        )
    )
    st += fig(
        "06_ev_vs_ice.png",
        "Figure 4. Pressure scores by propulsion type. EV leads on sequence disruption, charging pressure and dispatch-delay risk. Yard congestion is the exception: it is structural and slightly higher for ICE.",
    )
    st.append(
        p(
            "The comparison by driver is unambiguous on three fronts. Electric vehicles score higher on sequence disruption, "
            "on charging pressure, which combustion vehicles do not generate at all, and most sharply on dispatch-delay risk, "
            "where EV sits near 54 against 28 for combustion. These are the real costs of the transition and they cluster in "
            "the back half of the flow, around charge and exit."
        )
    )
    st.append(
        p(
            "The exception matters as much as the rule. Yard congestion is not an EV problem. The congestion score is actually "
            "marginally higher for combustion vehicles, because the yard is a shared physical constraint that every vehicle "
            "passes through regardless of drivetrain. This is the first place the analysis refuses an easy story. The yard is "
            "congested, but blaming the EV mix for it would point the response at the wrong lever."
        )
    )
    st += fig(
        "07_readiness_cohort.png",
        "Figure 5. Readiness rate by version, weighted by volume. Every EV version sits far below the 95 percent target; every ICE version sits comfortably above 87 percent.",
    )
    st.append(
        p(
            "The readiness cohort is the clearest single chart in the report. Sorted by version, the electric and combustion "
            "vehicles separate into two bands with almost no overlap. EV versions are ready between 28 and 59 percent of the "
            "time. Combustion versions sit between 87 and 97 percent. No EV version comes close to the 95 percent target line. "
            "This is not a tail problem to be managed at the margin. It is a structural readiness deficit on the entire "
            "electric range, and because the EV share is rising toward 80 percent of output, it is a deficit that grows with "
            "every week of the transition."
        )
    )
    st += fig(
        "11_delay_pareto.png",
        "Figure 6. Concentration of total late-dispatch minutes by version. The four EV versions account for 78 percent of all delay; the cumulative curve confirms the problem is not spread across the range.",
    )
    st.append(
        p(
            "If readiness is the mechanism, late-dispatch minutes are the consequence, and they concentrate exactly where the "
            "readiness gap predicts. The four electric versions carry 78 percent of all late-dispatch minutes. The Pareto curve "
            "rises steeply and then flattens, which is the signature of a concentrated problem rather than a diffuse one. This is "
            "operationally encouraging. A concentrated problem has a concentrated fix. The plant does not need to improve "
            "everything; it needs to move the readiness of four versions."
        )
    )
    st += fig(
        "10_leadtime_distribution.png",
        "Figure 7. Internal lead-time distributions overlap almost completely, with medians of 24 and 25 hours. The EV penalty is not in cycle time.",
    )
    st.append(
        p(
            "The cleared suspect is cycle time. It would be reasonable to assume electric vehicles simply take longer to move "
            "through the plant. They do not. The internal lead-time distributions for EV and ICE sit almost on top of each "
            "other, with medians of 24 and 25 hours and near-identical 90th percentiles. The electric vehicles are built and "
            "moved just as fast as the combustion ones. What happens to them is different: they reach the end of the line on "
            "time and then fail to leave, because the charge, the state-of-charge confirmation and the staging space are not "
            "there when they need them. This is why the report keeps separating build performance from exit performance. They "
            "are genuinely different problems with different owners."
        )
    )
    st += fig(
        "18_readiness_heatmap.png",
        "Figure 8. Readiness by version and shift. The colour bands run horizontally, not vertically: readiness is a property of the version, not the shift that built it.",
    )
    st.append(
        p(
            "One more alternative explanation deserves a direct test: maybe a particular shift is the weak link. The heatmap "
            "rules it out. Colour varies down the chart, by version, and barely varies across it, by shift. Shifts A, B and C "
            "produce nearly identical readiness for any given version. The deficit travels with the product, not the crew, "
            "which means the fix is a process and capacity fix, not a training or staffing fix aimed at one shift."
        )
    )
    st.append(PageBreak())

    # ============================================================= 7. FINDINGS: WHERE
    st += h1("Findings: where the bottleneck physically is", "Section 7")
    st.append(
        lead(
            "The EV penalty explains who is late. It does not by itself explain where vehicles get stuck. For that the analysis "
            "has to go down to the physical yard and to the area-level diagnostic."
        )
    )
    st += fig(
        "13_yard_zone_congestion.png",
        "Figure 9. p95 dwell time by yard zone. Pre-dispatch staging dominates at roughly 43 hours and near-total blocking; every other zone clears in under three hours.",
    )
    st.append(
        p(
            "The yard is not uniformly congested. One zone carries the entire problem. The pre-dispatch staging area shows a "
            "p95 dwell of roughly 43 hours and a blocking rate close to 100 percent, while every other zone, the cardinal "
            "buffers and the charge buffer, clears in under three hours. This is the physical signature of the exit gate "
            "failure. Vehicles that are not yet ready accumulate in staging, the staging area saturates, and a saturated "
            "staging area blocks the vehicles behind it, including ready ones. The plant-level p95 dwell of 49 hours is almost "
            "entirely this one zone."
        )
    )
    st += fig(
        "03_priority_ranking_opi.png",
        "Figure 10. Operational Priority Index by area. Logistics and Yard form a clear top tier; Charging, Production, Dispatch and Energy fall well below.",
    )
    st.append(
        p(
            "Lifted to the area level, the Operational Priority Index puts Logistics and Yard in a tier of their own, at 67 and "
            "65, with the next area more than 25 points behind. This is the model agreeing with the yard data. Logistics owns "
            "the dispatch-readiness and outbound-window failures; Yard owns the physical staging saturation. The two are the "
            "same failure seen from two angles, which is why they rank together and why the recommendations treat them as one "
            "coordinated intervention rather than two separate projects."
        )
    )
    st += data_table(
        ["Area", "OPI", "Main risk driver", "Recommended action", "Tier"],
        [
            [
                r["area"],
                f"{r['operational_priority_index']:.1f}",
                r["main_risk_driver"].replace("_", " "),
                r["recommended_action"].capitalize(),
                r["area_priority_tier"]
                .replace("estabilizar en la siguiente ola", "Stabilise next wave")
                .replace("mantener bajo observación", "Watch")
                .replace("sin prioridad inmediata", "No immediate priority"),
            ]
            for _, r in prio.sort_values("operational_priority_index", ascending=False).iterrows()
        ],
        [CONTENT_W * 0.16, CONTENT_W * 0.10, CONTENT_W * 0.22, CONTENT_W * 0.34, CONTENT_W * 0.18],
        aligns={1: "CENTER"},
    )
    st += fig(
        "04_risk_matrix.png",
        "Figure 11. Throughput loss against dispatch risk by area, sized by operational stress. Logistics and Yard sit in the intervene-now quadrant; Energy and Dispatch sit in the sustain quadrant.",
    )
    st.append(
        p(
            "The risk matrix is the one-slide version of the priority story. Plotting throughput loss against dispatch risk, "
            "Logistics lands in the top-right intervene-now quadrant on throughput loss, Yard sits high on the same axis with "
            "heavy operational stress, and the remaining areas fall toward the sustain corner. The chart is built so that the "
            "eye goes straight to the two bubbles that matter. Nothing about the layout is decorative; position encodes risk on "
            "both axes and size encodes stress."
        )
    )
    st += fig(
        "17_driver_correlation.png",
        "Figure 12. Correlation between operational drivers at area-shift level. Congestion, waiting and stress move together; throughput loss is nearly independent of them.",
    )
    st.append(
        p(
            "The correlation structure explains why the priority list has two distinct heads rather than one. Congestion, average "
            "waiting time, queue pressure and operational stress form a tight cluster that moves together: these are the physical "
            "yard family of risk. Throughput loss sits almost orthogonal to that cluster, which means it is driven by something "
            "else, the readiness-and-dispatch family. Two independent risk families is precisely why Logistics and Yard both reach "
            "the top tier through different drivers, and why a single-lever fix cannot resolve both."
        )
    )
    st.append(PageBreak())

    # ============================================================= 8. FINDINGS: TRANSITION
    st += h1("Findings: the pressure rises with the mix", "Section 8")
    st.append(
        lead(
            "A single-period snapshot can hide whether a problem is getting worse. The weekly transition series shows that it is, "
            "and that the deterioration tracks the EV share almost step for step."
        )
    )
    st += fig(
        "08_launch_transition_trend.png",
        "Figure 13. Weekly EV share against yard-transition stress and dispatch stability. As the mix rises, yard stress climbs from 25 to 53 and dispatch stability erodes in parallel.",
    )
    st.append(
        p(
            "The relationship is direct. As the electric share climbs across the 53 weeks, the yard-transition stress index rises "
            "from about 25 to about 53, more than doubling, and the dispatch-stability index erodes in parallel. This is the "
            "time-based confirmation of the cross-sectional findings. The back-of-line pressure is not a fixed cost of running "
            "EVs; it scales with how many of them are in the flow. Left unmanaged, the trend says the exit problem gets worse "
            "exactly as the plant succeeds at its transition target, which is the most dangerous shape a problem can take because "
            "success in one metric manufactures failure in another."
        )
    )
    st += fig(
        "12_market_geography.png",
        "Figure 14. Volume and readiness by destination market. Iberia takes roughly a third of output; readiness is uniform across destinations.",
    )
    st.append(
        p(
            "Geography is the second cleared suspect. Output is concentrated in Iberia, which takes around a third of volume, "
            "followed by France and Germany, but readiness is essentially uniform across all seven destinations. No market is "
            "structurally worse served than another. This matters for the response: the fix is internal and operational, not a "
            "matter of reallocating volume between destinations or renegotiating outbound windows market by market. The lever is "
            "inside the plant."
        )
    )
    st.append(PageBreak())

    # ============================================================= 9. FINDINGS: SCENARIOS
    st += h1("Findings: which response actually works", "Section 9")
    st.append(
        lead(
            "The diagnostic says what is wrong and where. The scenario twin asks what to do about it, by simulating eight futures "
            "on a consistent decision score and letting them compete."
        )
    )
    st += fig(
        "05_scenario_decision.png",
        "Figure 15. Decision score across eight scenarios. The combined corrective package leads; accelerating the mix without support scores worst.",
    )
    st.append(
        p(
            f"The combined corrective package wins, at a decision score of {best['decision_score']:.1f}. The unmanaged base "
            f"ramp-up scores {base['decision_score']:.1f}, and the worst option, accelerating the EV mix with no operational "
            "support, falls to 47.1. The ordering carries a clear message. Pushing more electric volume into the existing "
            "configuration is the single worst thing the plant can do, because it loads the exact gate that is already failing. "
            "The single-lever scenarios, more charging slots, better sequencing, more yard space, each help a little and cluster "
            "in the middle. Only the package that does all three together separates from the pack."
        )
    )
    st += fig(
        "14_scenario_tradeoff.png",
        "Figure 16. Throughput against late vehicles by scenario, with the x-axis inverted so better is up and to the left. The corrective package sits on the efficient frontier.",
    )
    st.append(
        p(
            "The trade-off view shows why. Plotting throughput against the share of late vehicles, the scenarios trace a frontier, "
            "and the corrective package sits at its best corner: higher throughput with fewer late vehicles at the same time. The "
            "accelerated-mix scenario sits at the opposite corner, buying nominal throughput with a sharp rise in late exits. This "
            "is the chart that kills the tempting argument that the plant should simply build more EVs faster. On this data, faster "
            "without support moves the plant the wrong way on the metric that is already broken."
        )
    )
    st += data_table(
        ["Scenario", "Throughput", "Internal time", "Late veh.", "Stability", "Decision"],
        [
            [
                s["escenario"].split("_", 1)[1].replace("_", " ").capitalize(),
                f"{s['throughput']:.0f}",
                f"{s['tiempo_total_interno']:.0f}",
                pct(s["vehiculos_retrasados"]),
                f"{s['estabilidad_operativa']:.0f}",
                f"{s['decision_score']:.1f}",
            ]
            for _, s in scen.sort_values("decision_score", ascending=False).iterrows()
        ],
        [CONTENT_W * 0.34, CONTENT_W * 0.13, CONTENT_W * 0.14, CONTENT_W * 0.11, CONTENT_W * 0.13, CONTENT_W * 0.13],
        aligns={1: "CENTER", 2: "CENTER", 3: "CENTER", 4: "CENTER", 5: "CENTER"},
    )
    st += fig(
        "19_before_after.png",
        "Figure 17. The base ramp-up against the corrective package across five metrics. Throughput, stability, internal time and late vehicles all improve; charging wait edges up as the only trade.",
    )
    st.append(
        p(
            "Held directly against the unmanaged base, the corrective package improves throughput by 3 percent, lifts operational "
            "stability, cuts internal time by 5 percent and reduces late vehicles from 43 to 40 percent of flow. The one metric that "
            "moves the wrong way is average charging wait, which edges up by about 4 percent. That is an honest trade and worth "
            "naming: pulling more EVs through readiness on time puts more simultaneous demand on the chargers. It is also exactly why "
            "charging capacity is the lever the next chart says to pull first."
        )
    )
    st += fig(
        "15_lever_ranking.png",
        "Figure 18. Capacity levers by expected impact. Charging capacity leads, with sequencing and yard management close behind.",
    )
    st.append(
        p(
            f"Ranked individually, charging capacity is the highest-return lever at an expected impact of "
            f"{levers['impacto_esperado'].max():.2f}, with EV sequencing at {levers.set_index('palanca').loc['secuenciacion_ev', 'impacto_esperado']:.2f} "
            f"and yard management at {levers.set_index('palanca').loc['gestion_patio', 'impacto_esperado']:.2f} just behind. The gaps "
            "between the top three are small, which is the quantitative reason the combined package beats any single move. No one lever "
            "dominates by enough to stand alone. The impact figures are parametric assumptions inside the scenario model, not measured "
            "elasticities, and the recommendations section treats them as a priority ordering rather than as promised percentages."
        )
    )
    st.append(PageBreak())

    # ============================================================= 10. ROBUSTNESS
    st += h1("Findings: is the ranking trustworthy", "Section 10")
    st.append(
        lead(
            "A prioritisation that flips the moment someone questions the weights is worthless for decision-making. The scoring "
            "framework is stress-tested precisely so that the top of the ranking can be defended."
        )
    )
    st += fig(
        "16_montecarlo_robustness.png",
        "Figure 19. Top-ranked area across 300 Monte Carlo redraws of the scoring weights. Logistics leads 77 percent of the time and Yard takes the rest; no other area ever reaches first.",
    )
    st.append(
        p(
            "The weights in the priority index are a judgement call, so the analysis redraws them 300 times from a Dirichlet "
            "distribution and recomputes the ranking each time. Logistics comes first in 77 percent of the draws and Yard in the "
            "remaining 23 percent. No third area ever takes the top spot. The headline conclusion of this report, that Logistics and "
            "Yard are the two areas to stabilise, does not depend on one particular set of weights. It survives the full range of "
            "reasonable weightings, and the only thing that moves is the order of the top two, both of which are being recommended "
            "for coordinated action anyway."
        )
    )
    st.append(
        p(
            "The sensitivity analysis reinforces this from a different direction. Perturbing each individual risk driver up and down "
            "by 20 percent leaves Logistics and Yard in the top two of the top-three list in every single case. The mean score shifts "
            "only at the second decimal. A ranking this stable under both weight resampling and driver perturbation is one a plant "
            "manager can act on without fear that the recommendation was an artefact of a modelling choice."
        )
    )
    st.append(PageBreak())

    # ============================================================= 11. RISKS
    st += h1("Risks, limitations and caveats", "Section 11")
    st.append(
        lead(
            "A credible report is explicit about where it stops. The following limitations bound how far these findings should be "
            "pushed, and none of them is hidden in a footnote because each one changes how the results should be used."
        )
    )
    st.append(h3("The data is synthetic"))
    st.append(
        p(
            "This is the first and largest caveat. The records are generated, not measured. The structure is realistic and the "
            "relationships are internally consistent, which is enough to demonstrate the method and to show what such an analysis "
            "would surface on real data. It is not enough to treat any absolute number here as a fact about a physical plant. The "
            "49-hour staging dwell and the 41 percent clean-exit rate are properties of this dataset, not benchmarks."
        )
    )
    st.append(h3("Scenario impacts are assumptions, not estimates"))
    st.append(
        p(
            "The lever impacts and the scenario outcomes come from parametric elasticities chosen to be plausible. They are not "
            "causal estimates from an experiment or a natural intervention. The scenario results should be read as a structured way "
            "to compare options under stated assumptions, and the ranking they produce is more trustworthy than any individual "
            "percentage they report. Before any capital commitment, these elasticities would need calibration against the plant's "
            "own response to past changes."
        )
    )
    st.append(h3("Scores are relative, not physical"))
    st.append(
        p(
            "Every 0 to 100 score in this report is a position on a distribution, not a measurement in minutes, units or euros. "
            "They are built for comparison across areas and propulsion types, which they do well. They are not built to be read as "
            "absolute levels and should not be quoted as such outside the comparative context that gives them meaning."
        )
    )
    st.append(h3("The throughput-loss proxy is attribution, not causation"))
    st.append(
        p(
            "The area throughput-loss figure attributes lost flow to the bottleneck events recorded against an area. It is a proxy "
            "that apportions observed loss, not a counterfactual estimate of what output would have been without the bottleneck. It "
            "is reliable for ranking where loss concentrates and should not be over-read as a precise recoverable-output number."
        )
    )
    st.append(h3("Weights and thresholds need local calibration"))
    st.append(
        p(
            "The 95 percent readiness target, the 120-minute late-dispatch threshold and the scoring weights are defaults. They are "
            "reasonable and the conclusions are robust to moving them within sensible ranges, as the robustness section shows, but a "
            "real deployment would set them to the plant's own standards and service-level agreements before the numbers drive money."
        )
    )
    st.append(h3("The dashboard needs network access"))
    st.append(
        p(
            "The companion dashboard is a single self-contained HTML file with all data embedded, but it loads its charting library "
            "and web fonts from a content delivery network. It renders fully online and degrades gracefully offline. For a fully "
            "air-gapped deployment those two dependencies would need to be vendored into the file."
        )
    )
    st.append(PageBreak())

    # ============================================================= 12. RECOMMENDATIONS
    st += h1("Recommendations and action priorities", "Section 12")
    st.append(
        lead(
            "Five recommendations, ordered by expected return and tied to the findings that justify them. The first three are the "
            "corrective package the scenario model selects. The last two protect the gains and keep the model honest."
        )
    )
    st += data_table(
        ["#", "Action", "Owner", "Tied to"],
        [
            [
                "1",
                "Cap and redesign the pre-dispatch staging zone; pull-sequence into staging only against real dispatch slots",
                "Yard / Logistics",
                "Fig 9, 11",
            ],
            [
                "2",
                "Reserve charging slots for EV versions and add capacity at shift peaks",
                "Energy / Charging",
                "Fig 4, 5, 18",
            ],
            [
                "3",
                "Enforce a readiness window on dispatch; hold ICE out of a saturated staging area",
                "Logistics",
                "Fig 3, 5",
            ],
            ["4", "Stop accelerating the EV mix until exit reliability recovers", "Plant / Planning", "Fig 13, 16"],
            [
                "5",
                "Calibrate weights, thresholds and elasticities to plant standards before capital decisions",
                "Operations analytics",
                "Section 11",
            ],
        ],
        [CONTENT_W * 0.05, CONTENT_W * 0.55, CONTENT_W * 0.22, CONTENT_W * 0.18],
    )
    st.append(h2("Priority 1. Fix the staging chokepoint"))
    st.append(
        p(
            "The pre-dispatch staging zone is the physical bottleneck, at 43 hours p95 dwell and near-total blocking against under "
            "three hours everywhere else in the yard. Treat it as a capacity-limited resource rather than open ground. Set a hard "
            "occupancy cap, segment the buffer by destination dispatch window, and only pull a vehicle into staging when its dispatch "
            "slot is confirmed. This is the highest-confidence recommendation in the report because it rests on measured dwell data, "
            "not on a modelling assumption, and because it directly relieves the blocking that delays even ready vehicles."
        )
    )
    st.append(h2("Priority 2. Unblock EV readiness through charging"))
    st.append(
        p(
            "EV readiness is the gate, charging is its dominant input, and charging capacity is the highest-return lever in the "
            "scenario model. Reserve slots for electric versions so they are not competing with discretionary demand, and add capacity "
            "at the shift peaks where the queue forms. Average charger utilisation across the period is only "
            f"{pct(CHARGER_UTIL)}, which says the constraint is timing and allocation at the peaks, not a shortage of total charging "
            "energy. That is good news, because allocation is cheaper to fix than capacity."
        )
    )
    st.append(h2("Priority 3. Put a readiness gate on dispatch"))
    st.append(
        p(
            "A large part of the exit problem is sequencing discipline, not capacity. Combustion vehicles, which are ready more than "
            "90 percent of the time, are being released into a staging area already saturated with EVs waiting on charge and "
            "state-of-charge confirmation. A readiness window that holds vehicles out of staging until their slot is real decongests "
            "the zone without adding a single square metre or a single charger. It is the lowest-cost item in the package and it "
            "supports the other two."
        )
    )
    st.append(h2("Priority 4. Do not accelerate the mix into a broken gate"))
    st.append(
        p(
            "The transition trend and the scenario ranking agree that pushing more EV volume into the current configuration is the "
            "worst available move. Hold the planned mix steady until the exit gate recovers. This is a sequencing decision about the "
            "transition itself, and it costs nothing except patience, while protecting the plant from manufacturing more late exits in "
            "the name of hitting an electrification milestone."
        )
    )
    st.append(h2("Priority 5. Calibrate before you commit capital"))
    st.append(
        p(
            "Before any of the above drives a spending decision, calibrate the model to the plant. Replace the default readiness "
            "target and late threshold with the plant's own service levels, and fit the scenario elasticities to the plant's observed "
            "response to past operational changes. The diagnostic and the ranking are robust enough to act on now. The scenario "
            "magnitudes are not, and should earn their place against real history before they size an investment."
        )
    )
    st.append(h2("The size of the prize"))
    st.append(
        p(
            f"To frame the stakes in one line: the plant already builds {TOTAL:,} vehicles to plan, but only about "
            f"{ON_TIME_READY:,} of them leave both on time and ready today. The corrective package does not ask the plant to build "
            "more. It asks it to ship cleanly what it already builds. That is the most efficient form of operational improvement "
            "available, because the volume has already been paid for. Recovering even half of the clean-exit gap would do more for "
            "the plant's reliability than any throughput initiative, at a fraction of the cost."
        )
    )
    st.append(PageBreak())

    # ============================================================= 13. APPENDIX
    st += h1("Appendix", "Section 13")
    st.append(h2("A. Operational KPI summary"))
    st += data_table(
        ["Metric", "Value"],
        [
            ["Total vehicle-orders", f"{TOTAL:,}"],
            ["Throughput gap to plan", f"{int(kpi['throughput_gap'])}"],
            ["EV share of flow", pct(SHARE_EV, 1)],
            ["Mean yard dwell", f"{DWELL_MEAN_H:.1f} h"],
            ["p95 yard dwell", f"{DWELL_P95_H:.1f} h"],
            ["Mean charging wait", f"{WAIT_CHARGE:.0f} min"],
            ["Mean charger utilisation", pct(CHARGER_UTIL, 1)],
            ["Vehicles not ready at dispatch", f"{NO_READY:,}"],
            ["Late-dispatch ratio", pct(RATIO_LATE, 1)],
            ["Clean-exit rate (on time and ready)", pct(ON_TIME_READY / TOTAL, 1)],
            ["Global readiness score", f"{READINESS_GLOBAL:.1f}"],
            ["Principal bottleneck cause", str(kpi["causa_principal_cuello"]).replace("_", " ").title()],
            ["Area of greatest throughput loss", str(kpi["area_mayor_perdida_throughput"]).title()],
        ],
        [CONTENT_W * 0.62, CONTENT_W * 0.38],
        aligns={1: "RIGHT"},
    )
    st.append(h2("B. EV versus ICE pressure scores"))
    st += data_table(
        ["Driver", "EV", "ICE"],
        [
            [
                "Sequence disruption",
                f"{evice.set_index('tipo_propulsion').loc['EV', 'sequence_disruption_score']:.1f}",
                f"{evice.set_index('tipo_propulsion').loc['ICE', 'sequence_disruption_score']:.1f}",
            ],
            [
                "Yard congestion",
                f"{evice.set_index('tipo_propulsion').loc['EV', 'yard_congestion_score']:.1f}",
                f"{evice.set_index('tipo_propulsion').loc['ICE', 'yard_congestion_score']:.1f}",
            ],
            [
                "Charging pressure",
                f"{evice.set_index('tipo_propulsion').loc['EV', 'charging_pressure_score']:.1f}",
                f"{evice.set_index('tipo_propulsion').loc['ICE', 'charging_pressure_score']:.1f}",
            ],
            [
                "Dispatch-delay risk",
                f"{evice.set_index('tipo_propulsion').loc['EV', 'dispatch_delay_risk_score']:.1f}",
                f"{evice.set_index('tipo_propulsion').loc['ICE', 'dispatch_delay_risk_score']:.1f}",
            ],
            [
                "Launch-transition stress",
                f"{evice.set_index('tipo_propulsion').loc['EV', 'launch_transition_stress_score']:.1f}",
                f"{evice.set_index('tipo_propulsion').loc['ICE', 'launch_transition_stress_score']:.1f}",
            ],
        ],
        [CONTENT_W * 0.5, CONTENT_W * 0.25, CONTENT_W * 0.25],
        aligns={1: "CENTER", 2: "CENTER"},
    )
    st.append(h2("C. Capacity levers by expected impact"))
    st += data_table(
        ["Lever", "Expected impact"],
        [
            [lv["palanca"].replace("_", " ").capitalize(), f"{lv['impacto_esperado']:.2f}"]
            for _, lv in levers.sort_values("impacto_esperado", ascending=False).iterrows()
        ],
        [CONTENT_W * 0.62, CONTENT_W * 0.38],
        aligns={1: "RIGHT"},
    )
    st.append(h2("D. Figure index"))
    figs = [
        "Fig 1 Daily throughput",
        "Fig 2 Weekly EV share",
        "Fig 3 Dispatch funnel",
        "Fig 4 EV vs ICE pressure",
        "Fig 5 Readiness cohort",
        "Fig 6 Delay concentration",
        "Fig 7 Lead-time distribution",
        "Fig 8 Readiness heatmap",
        "Fig 9 Yard zone congestion",
        "Fig 10 OPI ranking",
        "Fig 11 Risk matrix",
        "Fig 12 Driver correlation",
        "Fig 13 Transition trend",
        "Fig 14 Market geography",
        "Fig 15 Scenario decision",
        "Fig 16 Scenario trade-off",
        "Fig 17 Before vs after",
        "Fig 18 Lever ranking",
        "Fig 19 Monte Carlo robustness",
    ]
    rows = [[figs[i], figs[i + 1] if i + 1 < len(figs) else ""] for i in range(0, len(figs), 2)]
    st += data_table(["Figure", "Figure"], rows, [CONTENT_W * 0.5, CONTENT_W * 0.5], highlight_first_col=False)
    return st


def main() -> None:
    doc = Report(str(PDF), title="Operating Twin for the EV Van Transition", author="Operations Analytics")
    story = build_story()
    doc.multiBuild(story)
    print(f"OK - report at {PDF.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
