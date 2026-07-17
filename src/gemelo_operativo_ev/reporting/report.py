"""Construye el informe analítico PDF desde los datos procesados y el paquete de gráficos.

Salida: outputs/reports/ev_transition_operating_twin_report.pdf

Informe narrativo multipágina con estilo de analista sénior de operaciones.
El texto fluye de forma continua; gráficos y tablas se colocan junto a los
hallazgos que sustentan. Las cifras se leen desde los marts procesados para que
el informe se mantenga consistente con el panel publicado.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from matplotlib import get_data_path
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BalancedColumns,
    BaseDocTemplate,
    CondPageBreak,
    Flowable,
    Frame,
    Image,
    KeepTogether,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.tableofcontents import TableOfContents

from ..config import DATA_PROCESSED_DIR, OUTPUT_GRAPHS_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT

ROOT = PROJECT_ROOT
DATA = DATA_PROCESSED_DIR / "ev_factory"
GRAPHS = OUTPUT_GRAPHS_DIR
OUT = OUTPUT_REPORTS_DIR
PDF = OUT / "ev_transition_operating_twin_report.pdf"

# ── Sistema editorial inspirado en la referencia
INK = colors.HexColor("#111111")
INK_2 = colors.HexColor("#343638")
MUTED = colors.HexColor("#62676b")
SUBTLE = colors.HexColor("#6f7478")
LINE = colors.HexColor("#d7d9da")
ACCENT = colors.HexColor("#6bcb45")
ACCENT_TEXT = colors.HexColor("#357d28")
ACCENT_2 = colors.HexColor("#2aa7c7")
WARM = colors.HexColor("#d47745")
DANGER = colors.HexColor("#c44938")
POSITIVE = colors.HexColor("#4f9f3a")
PAPER = colors.HexColor("#ffffff")
BAND = colors.HexColor("#f2f2f0")

PAGE_W, PAGE_H = A4
MARGIN = 1.8 * cm
CONTENT_W = PAGE_W - 2 * MARGIN
COVER_META_WIDTH = 8.9 * cm
COVER_ART_X0 = 11.2 * cm


def _register_editorial_fonts() -> None:
    """Registra una pareja serif/sans incluida con Matplotlib."""
    font_dir = Path(get_data_path()) / "fonts" / "ttf"
    fonts = {
        "ReportSans": "DejaVuSans.ttf",
        "ReportSans-Bold": "DejaVuSans-Bold.ttf",
        "ReportSerif": "DejaVuSerif.ttf",
        "ReportSerif-Bold": "DejaVuSerif-Bold.ttf",
        "ReportSerif-Italic": "DejaVuSerif-Italic.ttf",
    }
    for name, filename in fonts.items():
        if name not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont(name, str(font_dir / filename)))
    pdfmetrics.registerFontFamily(
        "ReportSans",
        normal="ReportSans",
        bold="ReportSans-Bold",
    )
    pdfmetrics.registerFontFamily(
        "ReportSerif",
        normal="ReportSerif",
        bold="ReportSerif-Bold",
        italic="ReportSerif-Italic",
    )


_register_editorial_fonts()


def weighted_avg(df: pd.DataFrame, value_col: str, weight_col: str) -> float:
    weight = df[weight_col].sum()
    if weight == 0:
        return 0.0
    return float((df[value_col] * df[weight_col]).sum() / weight)


def _load_report_data() -> None:
    """Carga los marts únicamente cuando se solicita construir el informe."""
    global kpi, prio, scen, levers, checks, evice, gov, ranking, readiness
    global yard, charging, transition, scenario_delta, rank_stability, sensitivity
    global TOTAL, SHARE_EV, NO_READY, READY, RATIO_LATE, READINESS_GLOBAL
    global DWELL_MEAN_H, DWELL_P95_H, WAIT_CHARGE, CHARGER_UTIL, ON_TIME_READY
    global CLEAN_EXIT_RATE, CLEAN_EXIT_GAP, READY_LOSS_RATE, LATE_GATE_LOSS_RATE
    global best, base, worst, ev_readiness, ice_readiness, EV_READY_RATE, ICE_READY_RATE
    global EV_ORDERS, ICE_ORDERS, READINESS_GAP_PP, version_readiness
    global worst_ev_version, best_ice_version, yard_zone, pre_salida, next_yard_zone
    global charging_zone, highest_charge_zone, transition_start, transition_end
    global EV_SHARE_DELTA_PP, YARD_STRESS_DELTA, DISPATCH_STABILITY_DELTA, CHARGE_GAP_DELTA
    global scenario_delta_by_metric, DECISION_SCORE_UPLIFT, ACCELERATION_SCORE_PENALTY
    global CORRECTIVE_THROUGHPUT_DELTA, CORRECTIVE_INTERNAL_TIME_DELTA
    global CORRECTIVE_LATE_PP_REDUCTION, CORRECTIVE_CHARGE_WAIT_DELTA
    global TOP1_LOGISTICS, TOP1_YARD

    kpi = pd.read_csv(DATA / "kpi_operativos.csv").iloc[0]
    prio = pd.read_csv(DATA / "operational_prioritization_table.csv")
    scen = pd.read_csv(DATA / "scenario_decision_comparison.csv")
    levers = pd.read_csv(DATA / "scenario_lever_ranking.csv")
    checks = pd.read_csv(DATA / "validation_checks.csv")
    evice = pd.read_csv(DATA / "diagnostic_ev_vs_non_ev.csv")
    gov = pd.read_csv(DATA / "scoring_governance_checks.csv")
    ranking = pd.read_csv(DATA / "diagnostic_area_ranking.csv")
    readiness = pd.read_csv(DATA / "kpi_readiness_shift_version.csv")
    yard = pd.read_csv(DATA / "yard_features.csv")
    charging = pd.read_csv(DATA / "charging_features.csv")
    transition = pd.read_csv(DATA / "launch_transition_features.csv")
    scenario_delta = pd.read_csv(DATA / "scenario_base_vs_mejorado.csv")
    rank_stability = pd.read_csv(DATA / "scoring_rank_stability.csv")
    sensitivity = pd.read_csv(DATA / "scoring_sensitivity_analysis.csv")

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
    CLEAN_EXIT_RATE = ON_TIME_READY / TOTAL
    CLEAN_EXIT_GAP = TOTAL - ON_TIME_READY
    READY_LOSS_RATE = NO_READY / TOTAL
    LATE_GATE_LOSS_RATE = READY * RATIO_LATE / TOTAL

    best = scen.sort_values("decision_score", ascending=False).iloc[0]
    base = scen.set_index("escenario").loc["1_ramp_up_ev_base"]
    worst = scen.sort_values("decision_score", ascending=True).iloc[0]

    ev_readiness = readiness[readiness["tipo_propulsion"] == "EV"]
    ice_readiness = readiness[readiness["tipo_propulsion"] == "ICE"]
    EV_READY_RATE = weighted_avg(ev_readiness, "readiness_rate", "total_vehiculos")
    ICE_READY_RATE = weighted_avg(ice_readiness, "readiness_rate", "total_vehiculos")
    EV_ORDERS = int(ev_readiness["total_vehiculos"].sum())
    ICE_ORDERS = int(ice_readiness["total_vehiculos"].sum())
    READINESS_GAP_PP = (ICE_READY_RATE - EV_READY_RATE) * 100

    version_readiness = (
        readiness.groupby(["tipo_propulsion", "version_id"], as_index=False)
        .apply(
            lambda group: pd.Series(
                {
                    "vehicles": group["total_vehiculos"].sum(),
                    "readiness": weighted_avg(group, "readiness_rate", "total_vehiculos"),
                }
            )
        )
        .reset_index(drop=True)
    )
    worst_ev_version = version_readiness[version_readiness["tipo_propulsion"] == "EV"].sort_values("readiness").iloc[0]
    best_ice_version = (
        version_readiness[version_readiness["tipo_propulsion"] == "ICE"]
        .sort_values("readiness", ascending=False)
        .iloc[0]
    )

    yard_zone = (
        yard.groupby("zona_patio")
        .agg(
            avg_dwell=("avg_dwell_time", "mean"),
            p95_dwell=("p95_dwell_time", "mean"),
            blocking_rate=("blocking_rate", "mean"),
            avg_occupancy=("yard_occupancy_rate", "mean"),
        )
        .reset_index()
    )
    pre_salida = yard_zone[yard_zone["zona_patio"] == "PRE_SALIDA"].iloc[0]
    next_yard_zone = (
        yard_zone[yard_zone["zona_patio"] != "PRE_SALIDA"].sort_values("avg_dwell", ascending=False).iloc[0]
    )

    charging_zone = (
        charging.groupby("zona_carga")
        .agg(
            sessions=("sessions_per_shift", "sum"),
            avg_wait=("avg_wait_to_charge", "mean"),
            interruption_rate=("interruption_rate", "mean"),
            target_miss_rate=("target_soc_miss_rate", "mean"),
            pressure=("charger_pressure_score", "mean"),
        )
        .reset_index()
    )
    highest_charge_zone = charging_zone.sort_values("pressure", ascending=False).iloc[0]

    transition_start = transition.iloc[0]
    transition_end = transition.iloc[-1]
    EV_SHARE_DELTA_PP = (transition_end["share_ev"] - transition_start["share_ev"]) * 100
    YARD_STRESS_DELTA = (
        transition_end["yard_transition_stress_index"] - transition_start["yard_transition_stress_index"]
    )
    DISPATCH_STABILITY_DELTA = transition_end["dispatch_stability_index"] - transition_start["dispatch_stability_index"]
    CHARGE_GAP_DELTA = transition_end["charging_capacity_gap"] - transition_start["charging_capacity_gap"]

    scenario_delta_by_metric = scenario_delta.set_index("metrica")
    DECISION_SCORE_UPLIFT = float(best["decision_score"] - base["decision_score"])
    ACCELERATION_SCORE_PENALTY = float(base["decision_score"] - worst["decision_score"])
    CORRECTIVE_THROUGHPUT_DELTA = float(scenario_delta_by_metric.loc["throughput", "delta_abs"])
    CORRECTIVE_INTERNAL_TIME_DELTA = float(scenario_delta_by_metric.loc["tiempo_total_interno", "delta_pct"])
    CORRECTIVE_LATE_PP_REDUCTION = -float(scenario_delta_by_metric.loc["vehiculos_retrasados", "delta_abs"]) * 100
    CORRECTIVE_CHARGE_WAIT_DELTA = float(scenario_delta_by_metric.loc["espera_carga", "delta_pct"])
    TOP1_LOGISTICS = float(rank_stability.set_index("top1_area").loc["LOGISTICA", "freq_share"])
    TOP1_YARD = float(rank_stability.set_index("top1_area").loc["PATIO", "freq_share"])


AREA_NAME_ES = {
    "LOGISTICA": "Logística",
    "PATIO": "Patio",
    "CARGA": "Carga",
    "PRODUCCION": "Producción",
    "EXPEDICION": "Expedición",
    "ENERGIA": "Energía",
}

VALIDATION_CHECK_ES = {
    "cardinalidad_flujo_vehiculo": "Cardinalidad del flujo de vehículos",
    "chaves_criticas_nulas": "Claves críticas nulas",
    "denominadores_invalidos": "Denominadores inválidos",
    "dispatch_duplicado_vehiculo": "Vehículo duplicado en expedición",
    "duplicados_ordenes": "Órdenes duplicadas",
    "duplicados_vehiculos": "Vehículos duplicados",
    "ev_requiere_carga_sin_sesion": "EV con carga requerida sin sesión",
    "restriccion_capacidad_inconsistente": "Restricción de capacidad inconsistente",
    "retraso_sin_causa": "Retraso sin causa",
    "salida_sin_readiness": "Salida sin preparación",
    "scores_riesgo_nulos": "Puntuaciones de riesgo nulas",
    "secuencia_incoherente": "Secuencia incoherente",
    "sesion_carga_imposible": "Sesión de carga imposible",
    "soc_fuera_rango": "SOC fuera de rango",
    "timestamps_fuera_orden": "Marcas temporales fuera de orden",
}

GOVERNANCE_CHECK_ES = {
    "opi_diversity": "Diversidad del OPI",
    "risk_driver_diversity": "Diversidad de factores de riesgo",
    "tier_diversity": "Diversidad de niveles",
    "opi_dispersion": "Dispersión del OPI",
    "rank_stability_top1_share": "Estabilidad del primer puesto",
}

RISK_DRIVER_ES = {
    "throughput_loss_score": "Pérdida de caudal productivo",
    "yard_risk_score": "Riesgo de patio",
    "charging_risk_score": "Riesgo de carga",
    "dispatch_risk_score": "Riesgo de expedición",
    "launch_transition_risk_score": "Riesgo de transición",
}

SCENARIO_NAME_ES = {
    "1_ramp_up_ev_base": "Rampa EV base",
    "2_ramp_up_ev_acelerado": "Rampa EV acelerada",
    "3_aumento_slots_carga": "Aumento de puntos de carga",
    "4_mejor_secuenciacion_ev": "Mejor secuenciación EV",
    "5_expansion_o_mejor_uso_patio": "Expansión o mejor uso de patio",
    "6_mas_presion_logistica_salida": "Más presión en logística de salida",
    "7_turno_tensionado_menor_disponibilidad": "Turno tensionado con menor disponibilidad",
    "8_combinacion_medidas_correctivas": "Combinación de medidas correctivas",
}

STATUS_ES = dict(zip(("PASS", "FAIL", "WARN"), ("OK", "FALLA", "AVISO"), strict=True))
YARD_ZONE_ES = {
    "BUFFER_CARGA": "Pulmón de carga",
    "PRE_SALIDA": "Preexpedición",
}


def label_from_code(value: str, mapping: dict[str, str]) -> str:
    return mapping.get(value, value.replace("_", " ").capitalize())


def yard_zone_label(value: str) -> str:
    return label_from_code(str(value), YARD_ZONE_ES)


# ──────────────────────────────────────────────────────────────────────────────
# Styles
def build_styles() -> dict:
    ss = getSampleStyleSheet()
    s = {}
    s["body"] = ParagraphStyle(
        "body",
        parent=ss["BodyText"],
        fontName="ReportSerif",
        fontSize=8.7,
        leading=11.8,
        textColor=INK_2,
        alignment=TA_JUSTIFY,
        spaceAfter=6,
        spaceBefore=0,
    )
    s["lead"] = ParagraphStyle(
        "lead",
        parent=s["body"],
        fontName="ReportSans",
        fontSize=10.2,
        leading=13.8,
        textColor=INK,
        spaceAfter=10,
    )
    s["h1"] = ParagraphStyle(
        "h1",
        parent=ss["Heading1"],
        fontName="ReportSans-Bold",
        fontSize=23,
        leading=25.5,
        textColor=INK,
        spaceBefore=6,
        spaceAfter=3,
    )
    s["h1_plain"] = ParagraphStyle(
        "h1_plain",
        parent=s["h1"],
    )
    s["eyebrow"] = ParagraphStyle(
        "eyebrow",
        fontName="ReportSans-Bold",
        fontSize=7.4,
        leading=9.5,
        textColor=ACCENT_TEXT,
        spaceBefore=0,
        spaceAfter=3,
    )
    s["h2"] = ParagraphStyle(
        "h2",
        parent=ss["Heading2"],
        fontName="ReportSans-Bold",
        fontSize=11.2,
        leading=13.8,
        textColor=INK,
        spaceBefore=11,
        spaceAfter=4,
    )
    s["h3"] = ParagraphStyle(
        "h3",
        fontName="ReportSans-Bold",
        fontSize=9.2,
        leading=11.5,
        textColor=INK,
        spaceBefore=10,
        spaceAfter=3,
    )
    s["caption"] = ParagraphStyle(
        "caption",
        fontName="ReportSerif-Italic",
        fontSize=7.3,
        leading=9.6,
        textColor=MUTED,
        alignment=TA_LEFT,
        spaceBefore=4,
        spaceAfter=14,
    )
    s["callout"] = ParagraphStyle(
        "callout",
        fontName="ReportSerif",
        fontSize=9.2,
        leading=12.6,
        textColor=INK,
        leftIndent=10,
        spaceBefore=2,
        spaceAfter=2,
    )
    s["kpi_num"] = ParagraphStyle(
        "kpi_num",
        fontName="ReportSans-Bold",
        fontSize=18,
        leading=20,
        textColor=ACCENT_TEXT,
    )
    s["kpi_lbl"] = ParagraphStyle(
        "kpi_lbl",
        fontName="ReportSans",
        fontSize=7.1,
        leading=9.2,
        textColor=MUTED,
    )
    s["toc1"] = ParagraphStyle(
        "toc1",
        fontName="ReportSans-Bold",
        fontSize=9.6,
        leading=14,
        textColor=INK,
    )
    s["toc2"] = ParagraphStyle(
        "toc2",
        fontName="ReportSerif",
        fontSize=8.6,
        leading=12.5,
        textColor=INK_2,
        leftIndent=14,
    )
    s["cover_title"] = ParagraphStyle(
        "cover_title",
        fontName="ReportSans-Bold",
        fontSize=25,
        leading=28.5,
        textColor=INK,
    )
    s["cover_sub"] = ParagraphStyle(
        "cover_sub",
        fontName="ReportSerif-Italic",
        fontSize=10.5,
        leading=14.5,
        textColor=INK_2,
    )
    s["cover_meta"] = ParagraphStyle(
        "cover_meta",
        fontName="ReportSerif",
        fontSize=8.2,
        leading=11.4,
        textColor=INK_2,
    )
    s["tbl"] = ParagraphStyle("tbl", fontName="ReportSerif", fontSize=7.2, leading=9.4, textColor=INK_2)
    s["tbl_b"] = ParagraphStyle("tbl_b", fontName="ReportSerif-Bold", fontSize=7.2, leading=9.4, textColor=INK)
    s["tbl_h"] = ParagraphStyle("tbl_h", fontName="ReportSans-Bold", fontSize=6.8, leading=8.8, textColor=INK)
    return s


S = build_styles()


# ──────────────────────────────────────────────────────────────────────────────
# Apertura editorial de sección
class SectionTitle(Flowable):
    """Título de sección con punto de color y regla vertical de navegación."""

    def __init__(self, text: str, eyebrow: str | None = None):
        super().__init__()
        self.text = text
        self.eyebrow = eyebrow or ""
        self.style = S["h1"]
        self._eyebrow_flowable = Paragraph(self.eyebrow, S["eyebrow"]) if self.eyebrow else None
        self._title_flowable = Paragraph(text, S["h1"])
        self._content_width = 0.0
        self._eyebrow_height = 0.0
        self._title_height = 0.0

    def getPlainText(self) -> str:  # noqa: N802 - API de ReportLab
        return self.text

    def wrap(self, avail_width, avail_height):
        self.width = avail_width
        self._content_width = max(avail_width - 14 * mm, 1)
        if self._eyebrow_flowable:
            _, self._eyebrow_height = self._eyebrow_flowable.wrap(self._content_width, avail_height)
        _, self._title_height = self._title_flowable.wrap(self._content_width, avail_height)
        self.height = max(28 * mm, self._eyebrow_height + self._title_height + 8 * mm)
        return self.width, self.height

    def draw(self):
        canvas = self.canv
        marker_x = 4.2 * mm
        marker_y = self.height - 5.2 * mm
        canvas.saveState()
        canvas.setFillColor(ACCENT)
        canvas.circle(marker_x, marker_y, 2.8 * mm, fill=1, stroke=0)
        canvas.setStrokeColor(INK)
        canvas.setLineWidth(0.7)
        canvas.line(marker_x, marker_y - 4.2 * mm, marker_x, 0)
        canvas.restoreState()

        y = self.height - 2 * mm
        if self._eyebrow_flowable:
            y -= self._eyebrow_height
            self._eyebrow_flowable.drawOn(canvas, 14 * mm, y)
        y -= self._title_height + 1.5 * mm
        self._title_flowable.drawOn(canvas, 14 * mm, y)


# ──────────────────────────────────────────────────────────────────────────────
# Plantilla del documento con cabecera, pie e índice
class Report(BaseDocTemplate):
    def __init__(self, filename, **kw):
        kw.setdefault("invariant", True)
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
        canvas.setFillColor(BAND)
        canvas.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)

        # Sistema vectorial original: flujo, capacidad y transición convergen.
        canvas.setStrokeColor(colors.HexColor("#c7cacc"))
        canvas.setLineWidth(0.7)
        for offset in range(6):
            path = canvas.beginPath()
            path.moveTo(COVER_ART_X0, (5.4 + offset * 1.15) * cm)
            path.curveTo(
                13.2 * cm,
                (4.2 + offset * 1.35) * cm,
                16.2 * cm,
                (6.2 + offset * 0.95) * cm,
                20.2 * cm,
                (5.0 + offset * 1.2) * cm,
            )
            canvas.drawPath(path, fill=0, stroke=1)

        slat_colors = (PAPER, ACCENT_2, PAPER, WARM, PAPER, colors.HexColor("#bfc2c4"))
        for index in range(15):
            x = COVER_ART_X0 + index * 0.58 * cm
            y = (6.2 + (index % 5) * 0.82 + index * 0.12) * cm
            canvas.saveState()
            canvas.translate(x, y)
            canvas.rotate(-24 + index * 3.2)
            canvas.setFillColor(slat_colors[index % len(slat_colors)])
            canvas.setStrokeColor(colors.HexColor("#b6b9bb"))
            canvas.setLineWidth(0.45)
            canvas.roundRect(-3.2 * mm, -23 * mm, 6.4 * mm, 46 * mm, 3.2 * mm, fill=1, stroke=1)
            canvas.restoreState()

        for x, y, radius, color in (
            (11.3, 8.7, 3.0, ACCENT),
            (12.2, 6.6, 2.6, INK),
            (13.4, 10.9, 2.4, ACCENT_2),
            (16.2, 7.4, 2.8, PAPER),
            (18.5, 11.8, 2.5, WARM),
            (19.8, 8.8, 2.3, ACCENT),
        ):
            canvas.setFillColor(color)
            canvas.circle(x * cm, y * cm, radius * mm, fill=1, stroke=0)

        canvas.setFillColor(INK)
        canvas.rect(MARGIN, 13 * mm, 20 * mm, 1.2 * mm, fill=1, stroke=0)
        canvas.setFillColor(ACCENT)
        canvas.rect(MARGIN, 13 * mm, 7 * mm, 1.2 * mm, fill=1, stroke=0)
        canvas.restoreState()

    def _chrome(self, canvas, doc):
        canvas.saveState()
        # Navegación editorial mínima.
        canvas.setFont("ReportSans", 6.5)
        canvas.setFillColor(SUBTLE)
        canvas.drawString(MARGIN, PAGE_H - MARGIN + 5 * mm, "GEMELO OPERATIVO · TRANSICIÓN EV")
        canvas.drawRightString(
            PAGE_W - MARGIN, PAGE_H - MARGIN + 5 * mm, "DIAGNÓSTICO · ESCENARIOS · DECISIÓN"
        )
        canvas.setStrokeColor(LINE)
        canvas.setLineWidth(0.45)
        canvas.line(MARGIN, PAGE_H - MARGIN + 3 * mm, MARGIN + 28 * mm, PAGE_H - MARGIN + 3 * mm)

        outer_x = PAGE_W - 10 * mm
        canvas.line(outer_x, 16 * mm, outer_x, PAGE_H - 16 * mm)
        canvas.saveState()
        canvas.translate(PAGE_W - 6.5 * mm, PAGE_H / 2)
        canvas.rotate(90)
        canvas.setFont("ReportSans", 5.8)
        canvas.drawCentredString(0, 0, "OPERACIONES INDUSTRIALES · DATOS SINTÉTICOS · INFORME REPRODUCIBLE")
        canvas.restoreState()

        canvas.setFillColor(ACCENT)
        canvas.circle(MARGIN + 1.5 * mm, MARGIN - 6.4 * mm, 1.5 * mm, fill=1, stroke=0)
        canvas.setFont("ReportSans", 6.3)
        canvas.setFillColor(SUBTLE)
        canvas.drawString(MARGIN + 5 * mm, MARGIN - 8 * mm, "DATOS SINTÉTICOS DE FÁBRICA")
        canvas.setFont("ReportSans-Bold", 7.2)
        canvas.setFillColor(INK)
        canvas.drawRightString(PAGE_W - MARGIN, MARGIN - 8 * mm, f"{doc.page}")
        canvas.restoreState()

    def afterFlowable(self, flowable):  # noqa: N802  (overrides reportlab BaseDocTemplate)
        if not hasattr(flowable, "style"):
            return
        name = flowable.style.name
        if name == "h1":
            self.notify("TOCEntry", (0, flowable.getPlainText(), self.page))


# ──────────────────────────────────────────────────────────────────────────────
# Helpers de flowables
def fig(name: str, caption: str, width=CONTENT_W) -> list:
    path = GRAPHS / name
    img = Image(str(path))
    iw, ih = img.imageWidth, img.imageHeight
    img.drawWidth = width
    img.drawHeight = width * ih / iw
    caption_band = Table([[Paragraph(caption, S["caption"])]], colWidths=[width])
    caption_band.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), BAND),
                ("LEFTPADDING", (0, 0), (-1, -1), 7),
                ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return [Spacer(1, 3), KeepTogether([img, caption_band]), Spacer(1, 4)]


def p(text: str) -> Paragraph:
    return Paragraph(text, S["body"])


def lead(text: str) -> Paragraph:
    return Paragraph(text, S["lead"])


def h1(text: str, eyebrow: str | None = None) -> list:
    return [SectionTitle(text, eyebrow)]


def h2(text: str) -> Paragraph:
    return Paragraph(text, S["h2"])


def h3(text: str) -> Paragraph:
    return Paragraph(text, S["h3"])


def editorialize(flowables: list) -> list:
    """Compone bloques narrativos en dos columnas y preserva visuales a ancho completo."""
    result: list = []
    narrative: list = []

    def flush() -> None:
        if not narrative:
            return
        result.append(
            BalancedColumns(
                list(narrative),
                nCols=2,
                needed=52 * mm,
                innerPadding=7 * mm,
                spaceBefore=1.5 * mm,
                spaceAfter=3 * mm,
                endSlack=0.08,
            )
        )
        narrative.clear()

    for flowable in flowables:
        if isinstance(flowable, Paragraph) and flowable.style.name in {"body", "h2", "h3"}:
            narrative.append(flowable)
        else:
            flush()
            result.append(flowable)
    flush()
    return result


def compose_section(flowables: list, *, is_first: bool) -> list:
    """Substitui a quebra rígida final por uma porta de espaço para a secção seguinte."""
    section = list(flowables)
    if section and isinstance(section[-1], PageBreak):
        section.pop()
    composed = editorialize(section)
    if not is_first:
        composed.insert(0, CondPageBreak(15 * cm))
    return composed


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
    """items: lista de (número, etiqueta) -> fila única de tarjetas KPI."""
    cells = []
    for num, label in items:
        inner = Table(
            [[Paragraph(num, S["kpi_num"])], [Paragraph(label, S["kpi_lbl"])]],
            colWidths=[(CONTENT_W - (len(items) - 1) * 6) / len(items)],
        )
        inner.setStyle(
            TableStyle(
                [
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 1),
                    ("BOTTOMPADDING", (0, 1), (-1, 1), 7),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ]
            )
        )
        cells.append(inner)
    t = Table([cells], colWidths=[(CONTENT_W) / len(items)] * len(items))
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), BAND),
                ("LINEAFTER", (0, 0), (-2, -1), 0.6, LINE),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    return t


def _aligned_style(base: ParagraphStyle, align: str) -> ParagraphStyle:
    a = {"RIGHT": TA_RIGHT, "CENTER": 1, "LEFT": TA_LEFT}.get(align, TA_LEFT)
    return ParagraphStyle(f"{base.name}_{align}", parent=base, alignment=a)


def _table_flowables(header, rows, col_widths, highlight_first_col=True, aligns=None) -> list:
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
        ("BACKGROUND", (0, 0), (-1, 0), PAPER),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [PAPER, BAND]),
        ("LINEABOVE", (0, 0), (-1, 0), 1.2, INK),
        ("LINEBELOW", (0, 0), (-1, 0), 0.45, INK),
        ("LINEBELOW", (0, -1), (-1, -1), 0.55, INK),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]
    if aligns:
        for col, al in aligns.items():
            ts.append(("ALIGN", (col, 0), (col, -1), al))
    t.setStyle(TableStyle(ts))
    return [Spacer(1, 2), t, Spacer(1, 12)]


def data_table(header, rows, col_widths, highlight_first_col=True, aligns=None) -> list:
    # KeepTogether evita dividir una tabla a mitad del cuerpo y dejar pocas
    # filas aisladas en la página anterior con una cabecera repetida debajo.
    return [KeepTogether(_table_flowables(header, rows, col_widths, highlight_first_col, aligns))]


def h2_table(title: str, header, rows, col_widths, **kwargs) -> list:
    """Subsección unida a la tabla que la sigue inmediatamente.

    Sin esto, un encabezado puede quedar huérfano al final de una página
    mientras la tabla salta entera a la siguiente, dejando un título aislado y
    espacio en blanco. Se usa cuando no hay prosa antes de la tabla, como en las
    subsecciones densas del apéndice. Envuelve encabezado y tabla en un único
    KeepTogether; anidar dos KeepTogether confunde el cálculo de espacio de
    ReportLab y fuerza cada tabla a una página propia.
    """
    return [KeepTogether([h2(title), *_table_flowables(header, rows, col_widths, **kwargs)])]


def pct(x, d=0):
    return f"{x * 100:.{d}f}%"


# ──────────────────────────────────────────────────────────────────────────────
def cover() -> list:
    st = []
    st.append(Spacer(1, 0.7 * cm))
    title_block = Table(
        [
            [
                [
                    Paragraph("INFORME DE DIAGNÓSTICO OPERATIVO Y DECISIÓN", S["eyebrow"]),
                    Spacer(1, 5),
                    Paragraph("Gemelo Operativo para la<br/>Transición a Vans Eléctricas", S["cover_title"]),
                ],
                Paragraph(
                    "Dónde rompe la rampa EV el modelo operativo, cuánta fiabilidad pierde "
                    "y qué controles recuperan la puerta de salida.",
                    S["cover_sub"],
                ),
            ]
        ],
        colWidths=[CONTENT_W * 0.60, CONTENT_W * 0.40],
    )
    title_block.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (0, 0), 18),
                ("RIGHTPADDING", (1, 0), (1, 0), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )
    st.append(title_block)
    st.append(Spacer(1, 9.2 * cm))
    meta = Table(
        [
            [
                Paragraph("<b>Alcance</b>", S["cover_meta"]),
                Paragraph("Planta única de ensamblaje de vans durante una rampa EV de 12 meses", S["cover_meta"]),
            ],
            [
                Paragraph("<b>Periodo</b>", S["cover_meta"]),
                Paragraph("Enero de 2025 a diciembre de 2025, 53 semanas operativas", S["cover_meta"]),
            ],
            [
                Paragraph("<b>Registros</b>", S["cover_meta"]),
                Paragraph(f"{TOTAL:,} órdenes-vehículo en secuenciación, patio, carga y expedición", S["cover_meta"]),
            ],
            [
                Paragraph("<b>Método</b>", S["cover_meta"]),
                Paragraph(
                    "Canalización reproducible: marts DuckDB, diagnóstico Python, gemelo de escenarios y puntuación Monte Carlo",
                    S["cover_meta"],
                ),
            ],
            [
                Paragraph("<b>Datos</b>", S["cover_meta"]),
                Paragraph(
                    "Sintéticos, con semilla determinista. Los patrones son consistentes internamente; las cifras absolutas no son mediciones de una planta real.",
                    S["cover_meta"],
                ),
            ],
        ],
        colWidths=[2.1 * cm, COVER_META_WIDTH - 2.1 * cm],
    )
    meta.setStyle(
        TableStyle(
            [
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("LINEABOVE", (0, 0), (-1, 0), 1.0, INK),
                ("LINEBELOW", (0, -1), (-1, -1), 0.5, INK),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    st.append(Table([[meta, ""]], colWidths=[COVER_META_WIDTH, CONTENT_W - COVER_META_WIDTH]))
    st.append(Spacer(1, 0.8 * cm))
    st.append(
        Table(
            [[Paragraph(
                "Las figuras se generan directamente desde los marts procesados y coinciden con el panel operativo publicado.",
                S["caption"],
            ), ""]],
            colWidths=[COVER_META_WIDTH, CONTENT_W - COVER_META_WIDTH],
        )
    )
    return st


def toc_page() -> list:
    st = [Paragraph("Índice", S["h1_plain"]), hr()]
    toc = TableOfContents()
    toc.levelStyles = [S["toc1"]]
    st.append(toc)
    return st


# ──────────────────────────────────────────────────────────────────────────────
def _story_section_01() -> list:
    st: list = []
    st += h1("Resumen ejecutivo", "Sección 1")
    st.append(
        kpi_strip(
            [
                (f"{TOTAL / 1000:.1f}k", "ÓRDENES-VEHÍCULO ANALIZADAS"),
                (pct(SHARE_EV), "PESO EV EN EL FLUJO"),
                (pct(RATIO_LATE), "SALIDAS RETRASADAS"),
                (pct(READY / TOTAL), "LISTOS EN EXPEDICIÓN"),
            ]
        )
    )
    st.append(Spacer(1, 12))
    st.append(
        lead(
            "La dirección no debería acelerar la siguiente ola EV con el modelo de salida actual. La planta "
            "produce según plan, pero no expide limpiamente: la restricción se ha desplazado del volumen de "
            "producción a la preparación de salida, la espera preexpedición y la disciplina logística."
        )
    )
    st += data_table(
        ["Respuesta ejecutiva", "Lógica de decisión"],
        [
            [
                "Decisión",
                "Contener nuevas aceleraciones EV hasta controlar la puerta de salida; implantar el paquete correctivo combinado antes del siguiente escalón de rampa.",
            ],
            [
                "Por qué ahora",
                f"La planta completó {TOTAL:,} órdenes según plan, pero solo {ON_TIME_READY:,} salieron listas y a tiempo; quedan {CLEAN_EXIT_GAP:,} salidas no limpias.",
            ],
            [
                "Qué lo corrige",
                "Limitar la espera preexpedición, reservar carga EV en picos de turno y exigir una ventana de preparación antes de que los vehículos entren en esa zona.",
            ],
            [
                "Efecto esperado",
                f"Gemelo de escenarios: +{CORRECTIVE_THROUGHPUT_DELTA:.1f} vehículos/día, -{CORRECTIVE_LATE_PP_REDUCTION:.1f} pp de vehículos retrasados y +{DECISION_SCORE_UPLIFT:.1f} puntos de decisión frente a la base no gestionada.",
            ],
            [
                "Condición de gobernanza",
                "Tratar los impactos de escenario como hipótesis piloto hasta calibrar elasticidades de carga, espera preexpedición y expedición con datos reales de planta.",
            ],
        ],
        [CONTENT_W * 0.24, CONTENT_W * 0.76],
    )
    st.append(
        p(
            f"Durante toda la rampa la planta completó cada orden programada y sostuvo unos 160 vehículos diarios "
            "mientras la cuota eléctrica subió del 5% a casi el 80% de la producción semanal. Eso descarta el sospechoso "
            f"equivocado: el caudal productivo no es la restricción. La fiabilidad sí lo es: {pct(READY / TOTAL)} de los vehículos "
            f"terminados están listos en su ventana de expedición, {pct(RATIO_LATE)} de los listos salen con más de dos horas "
            f"de retraso y la tasa de salida limpia es solo {pct(CLEAN_EXIT_RATE)}."
        )
    )
    st.append(
        p(
            "El modo de fallo es lo bastante acotado para actuar. Las versiones eléctricas están listas "
            f"{pct(EV_READY_RATE)} del tiempo frente a {pct(ICE_READY_RATE)} en las versiones de combustión, y las cuatro "
            "versiones EV concentran el 78% de todos los minutos de retraso en expedición. El cuello físico también es estrecho: "
            f"la espera preexpedición alcanza {pre_salida['p95_dwell'] / 60:.0f} horas de permanencia p95 y bloqueo casi total, "
            "mientras las demás zonas de patio despejan mucho más rápido. El tiempo de ciclo interno no es el problema; EV e ICE "
            "se sitúan alrededor de 24 horas de extremo a extremo. La penalización aparece después del fin de línea, cuando carga, "
            "confirmación de SOC y ventanas de expedición tienen que alinearse."
        )
    )
    st.append(
        p(
            "La clasificación de prioridad resiste el contraste. En 300 remuestreos Monte Carlo de los pesos de puntuación, "
            f"Logística queda primera {pct(TOP1_LOGISTICS)} del tiempo y Patio toma el {pct(TOP1_YARD)} restante; ninguna otra área lidera. "
            "El gemelo de escenarios llega a la misma respuesta por otra vía: ninguna palanca aislada corrige la puerta de salida. "
            "El paquete combinado, disciplina de secuenciación más capacidad de carga más rediseño del pulmón de patio, es el único "
            "escenario que mejora caudal productivo, tiempo interno y salidas retrasadas a la vez."
        )
    )
    st.append(h3("Qué hacer, por orden"))
    st.append(
        p(
            "1. Tratar la zona de patio preexpedición como un recurso con capacidad limitada. Limitar ocupación, rediseñar "
            "el pulmón por ventana de destino y secuenciar vehículos hacia espera preexpedición solo cuando la ventana de expedición sea "
            f"real. Esto ataca una zona con {pre_salida['avg_dwell'] / 60:.1f} horas de permanencia media y "
            f"{pct(pre_salida['blocking_rate'])} de bloqueo, la expresión física del retraso de salida.<br/>"
            "2. Reservar puntos de carga para versiones EV y añadir capacidad en picos de turno. La capacidad de carga es la "
            f"palanca de mayor retorno en el modelo de escenarios, con un impacto esperado de {levers['impacto_esperado'].max():.2f}, "
            "y la preparación EV es la puerta que más mueve el paquete correctivo.<br/>"
            "3. Imponer una ventana de preparación en expedición. Dejar de liberar vehículos de combustión hacia una espera ya "
            "saturada por EVs esperando carga y confirmación de SOC permite recuperar la salida a tiempo sin añadir caudal productivo."
        )
    )
    st.append(
        p(
            "Las secciones siguientes muestran la cadena de evidencia, cuantifican el premio operativo y separan cambios de regla "
            "inmediatos de supuestos que requieren calibración antes de aprobar inversión."
        )
    )
    st.append(PageBreak())

    return st


def _story_section_02() -> list:
    st: list = []
    st += h1("Contexto y objetivos", "Sección 2")
    st.append(
        lead(
            "Una planta de vans en plena rampa eléctrica opera dos fábricas a la vez: la línea de combustión "
            "optimizada durante años y una línea eléctrica creciente que comparte el mismo patio, los mismos muelles "
            "de expedición y los mismos turnos, pero se comporta de forma distinta en cada paso. Este informe pregunta "
            "dónde duele esa colisión y qué impacto operativo genera."
        )
    )
    st.append(
        p(
            "Desde operaciones, un EV no es un vehículo de combustión con otro motor. Necesita carga antes de salir, "
            "lo que añade una dependencia que no existía. Tiene un objetivo de estado de carga que debe confirmarse en "
            "expedición. Compite por un conjunto finito de puntos de carga que la planta no necesitaba un año antes. Todo "
            "esto afecta a la segunda mitad del flujo, entre fin de línea y puerta de salida, donde la planta pierde fiabilidad."
        )
    )
    st.append(
        p(
            "El gemelo operativo integra cinco dominios que suelen mirarse por separado: secuenciación de producción, "
            "movimientos de patio, sesiones de carga, preparación para expedición y logística de salida. Verlos juntos es "
            "el punto central. Un vehículo rara vez llega tarde por una única razón. Llega tarde porque un punto de carga estaba "
            "ocupado, lo que lo retuvo en una zona de espera ya llena, lo que bloqueó al siguiente vehículo. Los paneles de un "
            "solo dominio pierden esa cadena; el gemelo está diseñado para seguirla."
        )
    )
    st.append(h2("Objetivos"))
    st.append(
        p(
            "El análisis tiene cuatro objetivos concretos y cada sección vuelve a uno de ellos.<br/>"
            "<b>Localizar el cuello de botella.</b> Identificar qué área y qué zona física limitan la salida fiable, "
            "con evidencia y no con intuición.<br/>"
            "<b>Cuantificar la penalización EV.</b> Separar lo que cuesta la cuota eléctrica del ruido operativo base "
            "de la planta.<br/>"
            "<b>Ordenar la respuesta.</b> Priorizar áreas y palancas de capacidad por retorno esperado, y comprobar "
            "si ese orden es estable o un artefacto de pesos elegidos.<br/>"
            "<b>Comparar futuros.</b> Poner la rampa no gestionada junto a intervenciones específicas y un paquete "
            "combinado, bajo una puntuación multicriterio consistente."
        )
    )
    st.append(h2("Cómo leer este informe"))
    st.append(
        p(
            "Los hallazgos se organizan por pregunta, no por fuente de datos. Cada hallazgo abre con la afirmación, muestra "
            "el gráfico que la sostiene y explica qué prueba y qué no prueba ese gráfico. Las puntuaciones de 0 a 100 son "
            "índices relativos de presión, no unidades físicas, y se definen en la metodología. Cuando una cifra es un supuesto "
            "de modelado en vez de una medición, el texto lo indica. Las recomendaciones finales se vinculan a los hallazgos "
            "específicos que las justifican."
        )
    )
    st.append(h2("Marco de decisión"))
    st.append(
        p(
            "El informe gira alrededor de una decisión de gestión: si la siguiente ola de rampa EV debe pasar por el modelo "
            "operativo actual, frenarse hasta recuperar fiabilidad de salida o apoyarse en cambios específicos de capacidad y "
            "secuenciación. La respuesta no se basa en una métrica aislada. Requiere una cadena de evidencia: la planta debe "
            "demostrar volumen, la puerta de salida debe fallar, la cuota EV debe explicar una parte material de ese fallo y las "
            "palancas recomendadas deben mejorar el modo de fallo sin crear un compensación mayor en otra parte."
        )
    )
    st += data_table(
        ["Prueba de decisión", "Evidencia usada", "Qué cambiaría la respuesta"],
        [
            [
                "¿Es el caudal productivo la restricción vinculante?",
                "Terminaciones diarias, brecha de caudal productivo y caudal productivo de escenarios",
                "Una brecha negativa material o una caída del ritmo devolvería la prioridad al flujo de producción.",
            ],
            [
                "¿La cuota EV crea una penalización distinta?",
                "Preparación por propulsión y versión; puntuaciones de presión EV vs ICE",
                "Si los datos reales mostraran preparación similar en EV e ICE, el remedio pasaría de puertas específicas EV a disciplina genérica de salida.",
            ],
            [
                "¿El cuello está lo bastante localizado?",
                "Permanencia por zona de patio, clasificación OPI, matriz de riesgo y correlaciones de factores",
                "Si la congestión estuviera repartida uniformemente, intervenir una sola zona de espera sería demasiado estrecho.",
            ],
            [
                "¿El paquete recomendado es estable?",
                "Comparación de escenarios, deltas base-vs-correctivo, Monte Carlo y sensibilidad",
                "Si Logística y Patio dejaran de liderar con pesos razonables, habría que rehacer el orden de prioridad antes de ejecutar.",
            ],
        ],
        [CONTENT_W * 0.25, CONTENT_W * 0.31, CONTENT_W * 0.44],
    )
    st.append(PageBreak())

    return st


def _story_section_03() -> list:
    st: list = []
    st += h1("Datos y metodología", "Sección 3")
    st.append(
        lead(
            "Cada figura de este informe es reproducible desde un único canalización determinista. Los mismos comandos que "
            "regeneran las tablas origen reconstruyen los marts, el diagnóstico y el panel, de modo que las cifras del informe, "
            "del panel y de los datos fuente no puedan divergir."
        )
    )
    st.append(h2("Base de datos"))
    st.append(
        p(
            "El análisis se apoya en 14 tablas sintéticas de origen que cubren órdenes, vehículos, versiones, movimientos "
            "de patio, sesiones de carga, puntos de carga, turnos, recursos operativos, eventos de cuello y logística de salida. "
            "El generador modela tres fases de la rampa, un periodo de pre-serie, la rampa y una cola de estabilización, por lo "
            f"que el peso EV sube en el tiempo como en un lanzamiento real y no queda en una media plana. Durante el periodo la "
            f"planta gestiona {TOTAL:,} órdenes-vehículo, de las cuales {pct(SHARE_EV)} son eléctricas, repartidas en tres familias "
            "de modelo y ocho versiones, con envío a siete mercados europeos."
        )
    )
    st.append(
        p(
            "Los datos son sintéticos y usan semilla fija. Es una elección deliberada para un análisis público y compartible: "
            "elimina restricciones de confidencialidad y hace auditable toda la cadena. También fija el límite de interpretación. "
            "Los patrones son realistas y consistentes internamente, pero las cifras absolutas no son mediciones de una planta real "
            "ni deben leerse como referencia."
        )
    )
    st.append(h2("Canalización"))
    st.append(
        p(
            "Las tablas origen se cargan en DuckDB, que construye una capa de preparación de datos, vistas integradas entre dominios, marts "
            "analíticos y KPIs gobernados. Python asume después el trabajo menos natural para SQL: ingeniería de variables, "
            "diagnóstico por área, gemelo de escenarios y puntuación. La puerta de publicación se ejecuta al final y comprueba "
            "integridad estructural, consistencia de métricas y contratos de datos del panel. La puerta no es cosmética: una "
            "única expedición real sin registro de preparación bloquea la publicación, porque implicaría que salió un vehículo "
            "que los datos dicen que nunca estuvo listo, y el hallazgo central depende de que esa señal sea fiable."
        )
    )
    st.append(h2("Calidad de datos"))
    st.append(
        p(
            f"En cada ejecución se corren {len(checks)} comprobaciones de integridad, cubriendo cardinalidad, integridad referencial, "
            "denominadores inválidos, órdenes y vehículos duplicados, secuenciación incoherente, sesiones de carga imposibles, "
            f"SOC fuera de rango y marcas temporales fuera de orden. Los {len(checks)} pasan con cero filas fallidas. Los hallazgos "
            "siguientes se apoyan en datos internamente limpios."
        )
    )
    st += data_table(
        ["Check de integridad", "Filas fallidas", "Estado"],
        [
            [
                label_from_code(c["check_name"], VALIDATION_CHECK_ES),
                int(c["failed_rows"]),
                STATUS_ES.get(c["status"], c["status"]),
            ]
            for _, c in checks.iterrows()
        ],
        [CONTENT_W * 0.62, CONTENT_W * 0.20, CONTENT_W * 0.18],
        aligns={1: "CENTER", 2: "CENTER"},
    )
    st.append(h2("Mapa de evidencia"))
    st.append(
        p(
            "La base de evidencia se organiza por propósito. Los marts SQL contienen métricas gobernadas; las tablas de variables "
            "exponen los mecanismos operativos; las salidas de diagnóstico convierten esos mecanismos en prioridades por área; "
            "las salidas de escenarios prueban decisiones de gestión. El informe no pide confiar en una caja negra: cada afirmación "
            "principal se puede trazar a la capa diseñada para esa función."
        )
    )
    st += data_table(
        ["Capa analítica", "Rol principal en el informe", "Uso para el lector"],
        [
            [
                "Mart de KPI gobernado",
                "Fuente única de verdad para órdenes totales, cuota EV, preparación, expedición tardía y permanencia.",
                "Resumen ejecutivo, interpretación del funnel y tabla KPI del apéndice.",
            ],
            [
                "Mart de preparación",
                "Cortes por versión, turno y propulsión para preparación de expedición y exposición a retraso.",
                "Penalización EV, concentración por versión y pruebas de descarte por turno.",
            ],
            [
                "Variables de patio y carga",
                "Mecanismo físico detrás de permanencia, bloqueo, colas y presión de puntos de carga.",
                "Localización del cuello, recomendación de espera preexpedición y lógica de palancas de carga.",
            ],
            [
                "Gemelo de escenarios",
                "Futuros operativos comparables bajo rampa no gestionada, palancas individuales y paquete combinado.",
                "Respuesta recomendada, compensacións y deltas base-vs-correctivo.",
            ],
            [
                "Gobernanza de puntuación",
                "Comprobaciones de dispersión, diversidad, separación de niveles y estabilidad de clasificación.",
                "Confianza para priorizar primero Logística y Patio.",
            ],
        ],
        [CONTENT_W * 0.24, CONTENT_W * 0.39, CONTENT_W * 0.37],
    )
    st.append(PageBreak())

    return st


def _story_section_04() -> list:
    st: list = []
    st += h1("Marco analítico", "Sección 4")
    st.append(
        lead(
            "Tres constructos sostienen el análisis: un conjunto de puntuaciones de presión operativa, un Índice de Prioridad "
            "Operativa (OPI) que ordena las áreas y una puntuación de decisión de escenarios. Se definen aquí para que los hallazgos puedan "
            "leerse sin reconstruir la matemática."
        )
    )
    st.append(h2("Puntuaciones de presión"))
    st.append(
        p(
            "Las métricas operativas de origen viven en escalas incompatibles. Una permanencia está en minutos, una ocupación es una fracción "
            "y un conteo de movimientos improductivos es un entero. Para comparar áreas y tipos de propulsión en un mismo eje, "
            "cada factor se normaliza a una puntuación de presión de 0 a 100, donde más alto significa más tensión operativa. Estos "
            "puntuaciones son explícitamente relativos. Una puntuación de congestión de patio de 74 no significa que el patio esté lleno al "
            "74%; significa que está alto en la distribución observada de presión de congestión. Las puntuaciones responden dónde se "
            "concentra la tensión, no qué mide en unidades físicas."
        )
    )
    st.append(h2("Índice de Prioridad Operativa (OPI)"))
    st.append(
        p(
            "El OPI combina cinco puntuaciones de presión en una clasificación única por área: pérdida de caudal productivo, riesgo de patio, "
            "riesgo de carga, riesgo de expedición y riesgo de transición de lanzamiento. Es la respuesta del modelo a una "
            "pregunta directa de gestión: si solo podemos estabilizar una o dos áreas la próxima semana, cuáles. El índice no "
            "es una estimación causal; es una priorización transparente y ponderada que puede recalcularse con otros pesos. La "
            "prueba de estabilidad de la clasificación lo hace explícitamente."
        )
    )
    st.append(h2("Puntuación de decisión"))
    st.append(
        p(
            "Los escenarios se puntúan con una función multicriterio que premia caudal productivo, preparación y estabilidad operativa, "
            "y penaliza salidas tardías y riesgo de congestión. La puntuación evita optimizar una sola métrica, porque el hallazgo "
            "central es que la planta puede cumplir el objetivo de volumen mientras falla el objetivo de fiabilidad. Una puntuación que "
            "solo mirase caudal productivo calificaría la rampa no gestionada como éxito."
        )
    )
    st.append(h2("Gobernanza"))
    st.append(
        p(
            "La priorización se somete a cinco comprobaciones de gobernanza antes de alimentar recomendaciones: el índice de prioridad "
            "debe mostrar dispersión real entre áreas, los factores de riesgo deben ser diversos y no colapsar en uno, los niveles "
            "de prioridad deben separarse y el área líder debe ser estable bajo remuestreo. Los cinco pasan. Destaca la dispersión: "
            "la desviación estándar del OPI entre áreas es 17,8 frente a un mínimo de 1,0, lo que significa que la clasificación separa "
            "áreas en vez de valorarlas todas igual."
        )
    )
    st += data_table(
        ["Check de gobernanza", "Valor", "Umbral", "Estado"],
        [
            [
                label_from_code(g["check_name"], GOVERNANCE_CHECK_ES),
                f"{float(g['value']):.2f}",
                f"{float(g['threshold']):.2f}",
                STATUS_ES.get(g["status"], g["status"]),
            ]
            for _, g in gov.iterrows()
        ],
        [CONTENT_W * 0.46, CONTENT_W * 0.18, CONTENT_W * 0.18, CONTENT_W * 0.18],
        aligns={1: "CENTER", 2: "CENTER", 3: "CENTER"},
    )
    st.append(h2("Reglas de interpretación"))
    st.append(
        p(
            "Tres reglas evitan sobreinterpretar el modelo. Primero, el OPI decide la secuencia de atención directiva, no la "
            "asignación presupuestaria. Una puntuación de 67 frente a 65 significa que Logística y Patio pertenecen a la misma ola de "
            "intervención, no que una merezca exactamente 3% más de financiación. Segundo, las puntuaciones de escenario ordenan "
            "configuraciones operativas bajo supuestos fijos; no pronostican valor financiero. Tercero, cualquier recomendación "
            "que dependa de elasticidades sintéticas se formula como prueba operativa escalonada antes de convertirse en caso de inversión."
        )
    )
    st += data_table(
        ["Salida del modelo", "Interpretación válida", "Interpretación inválida"],
        [
            [
                "Índice de Prioridad Operativa (OPI)",
                "Qué área debe estabilizarse primero bajo el perfil de riesgo observado.",
                "Valor monetario exacto, prueba de causa raíz o rating de productividad de personal.",
            ],
            [
                "Puntuación de decisión de escenario",
                "Atractivo operativo relativo entre futuros consistentes.",
                "Porcentaje garantizado de mejora o VAN de caso de negocio.",
            ],
            [
                "Puntuaciones de presión",
                "Tensión comparable entre dominios con unidades distintas.",
                "Porcentaje físico de utilización salvo que la métrica sea explícitamente una utilización.",
            ],
            [
                "Estabilidad Monte Carlo",
                "Si la prioridad principal sobrevive incertidumbre razonable de pesos.",
                "Prueba de que las elasticidades de escenario son causales.",
            ],
        ],
        [CONTENT_W * 0.24, CONTENT_W * 0.38, CONTENT_W * 0.38],
    )
    st.append(PageBreak())

    return st


def _story_section_05() -> list:
    st: list = []
    st += h1("Hallazgos: la planta produce volumen pero no lo expide limpio", "Sección 5")
    st.append(
        lead(
            "Empezamos por caudal productivo porque descarta el sospechoso equivocado. La planta no está limitada por capacidad "
            "de producción: fabrica lo que se le pide durante toda la rampa."
        )
    )
    st += fig(
        "01_throughput_daily.png",
        "Figura 1. Terminaciones diarias con media móvil de 7 días. La salida se mantiene cerca de 160 vehículos diarios durante todo el periodo; las ondas semanales son ritmo operativo, no deterioro.",
    )
    st.append(
        p(
            "Las terminaciones diarias se mantienen en una tendencia plana de unos 160 vehículos al día. La variación visible "
            "entre semanas es la respiración normal de una operación a tres turnos, no una deriva descendente. La brecha de caudal productivo "
            "frente al plan es cero: cada orden programada se completó. Si la única pregunta fuera si la planta puede fabricar los "
            "vehículos, la respuesta sería sí, y la rampa EV no la cambia."
        )
    )
    st += fig(
        "02_share_ev_weekly.png",
        "Figura 2. Cuota EV semanal del flujo completado. La cuota sube de cerca del 5% a casi el 80% durante la rampa, una transición real y no un piloto simbólico.",
    )
    st.append(
        p(
            "El segundo gráfico muestra que fue una transición real, no un piloto. La cuota eléctrica de la producción semanal "
            "sube desde cerca del 5% al inicio hasta casi el 80% al final. La planta absorbió ese cambio de composición sin perder volumen "
            "de fabricación. Eso separa la fortaleza de ejecución del fallo de salida. El problema no es fabricar EVs; es sacarlos "
            "limpiamente."
        )
    )
    st += fig(
        "09_dispatch_funnel.png",
        "Figura 3. De orden programada a salida limpia. La preparación resta 27 puntos al plan y la expedición tardía resta otros 31, dejando una tasa de salida limpia del 41%.",
    )
    st.append(
        p(
            f"El funnel de salida es donde termina la historia de volumen y empieza la de fiabilidad. Las {TOTAL:,} órdenes se "
            f"completan, pero solo {READY:,} están listas en su ventana de expedición, una pérdida de preparación de {pct(READY_LOSS_RATE)}. "
            f"De las que sí están listas, {pct(RATIO_LATE)} salen tarde. Sumadas ambas puertas, la tasa de salida limpia, a tiempo "
            f"y lista, cae a {pct(CLEAN_EXIT_RATE)}. La planta convierte una cartera totalmente fabricada en una minoría de salidas "
            "limpias. Las secciones siguientes explican ese colapso y dimensionan cuánto vale recuperarlo."
        )
    )
    st.append(PageBreak())

    return st


def _story_section_06() -> list:
    st: list = []
    st += h1("Hallazgos: la penalización EV es real, pero específica", "Sección 6")
    st.append(
        lead(
            "El diagnóstico tentador es culpar a la rampa completa. Los datos son más estrechos. La cuota eléctrica trae una "
            "penalización operativa clara, pero cae en tres lugares concretos, y un sospechoso evidente queda descartado."
        )
    )
    st += fig(
        "06_ev_vs_ice.png",
        "Figura 4. Puntuaciones de presión por tipo de propulsión. EV lidera en disrupción de secuencia, presión de carga y riesgo de retraso en expedición. La congestión de patio es la excepción: es estructural y algo mayor en ICE.",
    )
    st.append(
        p(
            "La comparación por factor es inequívoca en tres frentes. Los eléctricos puntúan más alto en disrupción de secuencia, "
            "en presión de carga, que los vehículos de combustión no generan, y con más fuerza en riesgo de retraso de expedición, "
            f"donde EV se sitúa cerca de {evice.set_index('tipo_propulsion').loc['EV', 'dispatch_delay_risk_score']:.0f} frente a "
            f"{evice.set_index('tipo_propulsion').loc['ICE', 'dispatch_delay_risk_score']:.0f} en combustión. Esos son los costes medidos de la transición y se concentran "
            "en la segunda mitad del flujo, alrededor de carga y salida."
        )
    )
    st.append(
        p(
            "La excepción importa tanto como la regla. La congestión de patio no es un problema EV. La puntuación de congestión es "
            "ligeramente mayor para combustión porque el patio es una restricción física compartida por todos los vehículos, sea "
            "cual sea su propulsión. El patio está congestionado, pero culpar a la cuota EV de esa congestión apuntaría la respuesta "
            "a la palanca equivocada."
        )
    )
    st += fig(
        "07_readiness_cohort.png",
        "Figura 5. Tasa de preparación por versión, ponderada por volumen. Todas las versiones EV quedan muy por debajo del objetivo del 95%; todas las versiones ICE se sitúan cómodamente por encima del 87%.",
    )
    st.append(
        p(
            "La preparación por versión ofrece la separación más limpia del informe. Ordenados por versión, eléctricos y combustión "
            f"se dividen en dos bandas con casi ningún solape. Las versiones EV están listas entre "
            f"{pct(version_readiness[version_readiness['tipo_propulsion'] == 'EV']['readiness'].min())} y "
            f"{pct(version_readiness[version_readiness['tipo_propulsion'] == 'EV']['readiness'].max())} del tiempo. "
            f"Las versiones de combustión se sitúan entre {pct(version_readiness[version_readiness['tipo_propulsion'] == 'ICE']['readiness'].min())} y "
            f"{pct(version_readiness[version_readiness['tipo_propulsion'] == 'ICE']['readiness'].max())}. La peor versión EV es "
            f"{worst_ev_version['version_id'].replace('_', ' ')}, con {pct(worst_ev_version['readiness'])}; la mejor ICE es "
            f"{best_ice_version['version_id'].replace('_', ' ')}, con {pct(best_ice_version['readiness'])}. Ninguna versión EV se acerca "
            "al objetivo del 95%. No es un problema de cola gestionable en el margen; es un déficit estructural de preparación en toda "
            "la gama eléctrica y, como la cuota EV sube hacia el 80% de la producción, crece cada semana de transición."
        )
    )
    st += fig(
        "11_delay_pareto.png",
        "Figura 6. Concentración de minutos totales de retraso en expedición por versión. Las cuatro versiones EV concentran el 78% del retraso; la curva acumulada confirma que el problema no está repartido por toda la gama.",
    )
    st.append(
        p(
            "Si la preparación es el mecanismo, los minutos de retraso en expedición son la consecuencia, y se concentran donde "
            "predice la brecha de preparación. Las cuatro versiones eléctricas soportan el 78% de todos los minutos tardíos. La curva "
            "de Pareto sube con fuerza y después se aplana, firma de un problema concentrado y no difuso. La planta no necesita "
            "mejorarlo todo a la vez; necesita mover la preparación de cuatro versiones."
        )
    )
    st += fig(
        "10_leadtime_distribution.png",
        "Figura 7. Las distribuciones de tiempo de paso interno se solapan casi por completo, con medianas de 24 y 25 horas. La penalización EV no está en el tiempo de ciclo.",
    )
    st.append(
        p(
            "El sospechoso descartado es el tiempo de ciclo. Sería razonable suponer que los eléctricos tardan más en moverse "
            "por la planta. No es así. Las distribuciones de tiempo de paso interno de EV e ICE están casi superpuestas, con medianas "
            "de 24 y 25 horas y percentiles 90 prácticamente idénticos. Los eléctricos se fabrican y se mueven al mismo ritmo que "
            "los de combustión. Lo que les ocurre después es distinto: llegan a fin de línea a tiempo y luego no salen, porque carga, "
            "confirmación de SOC y espacio de espera preexpedición no están disponibles cuando los necesitan. Por eso el informe separa desempeño "
            "de fabricación y desempeño de salida: son problemas distintos con dueños distintos."
        )
    )
    st += fig(
        "18_readiness_heatmap.png",
        "Figura 8. Preparación por versión y turno. Las bandas de color corren horizontalmente, no verticalmente: la preparación es propiedad de la versión, no del turno que la fabricó.",
    )
    st.append(
        p(
            "Otra explicación alternativa merece una prueba directa: quizá un turno concreto sea el eslabón débil. El heatmap lo "
            "descarta. El color varía hacia abajo, por versión, y apenas varía en horizontal, por turno. Los turnos A, B y C producen "
            "preparación casi idéntica para cada versión. El déficit viaja con el producto, no con el equipo; por tanto la corrección "
            "es de proceso y capacidad, no de formación o dotación dirigida a un turno."
        )
    )
    st.append(h2("Qué es la penalización EV y qué no es"))
    st.append(
        p(
            f"La brecha ponderada de preparación entre EV e ICE es de {READINESS_GAP_PP:.1f} puntos porcentuales sobre "
            f"{EV_ORDERS:,} órdenes eléctricas y {ICE_ORDERS:,} de combustión. Es suficiente para dominar el funnel de expedición, "
            "pero el diagnóstico circundante acota su significado. No es una penalización de ciclo de producción, porque las distribuciones "
            "de tiempo de paso se solapan. No es de turno, porque A, B y C muestran el mismo patrón por versión. No es de asignación de mercado, "
            "porque la preparación por destino es ampliamente plana. Es una penalización de preparación en salida causada por dependencia de "
            "carga, confirmación de SOC y un pulmón físico de espera preexpedición que absorbe vehículos aún no expedibles."
        )
    )
    st += data_table(
        ["Hipótesis probada", "Resultado", "Implicación de decisión"],
        [
            [
                "Los EV tardan más en fabricarse o moverse internamente",
                "Rechazada: las distribuciones de tiempo de paso EV e ICE se solapan alrededor de las mismas 24-25 horas.",
                "No atacar primero mediante proyectos de tiempo de ciclo de producción.",
            ],
            [
                "Un turno débil causa la baja preparación",
                "Rechazada: la preparación varía mucho más por versión que por turno.",
                "No localizar la corrección en un equipo; estandarizar el proceso de salida en todos los turnos.",
            ],
            [
                "Un mercado destino crea el retraso",
                "Rechazada: la preparación es materialmente uniforme entre mercados.",
                "No resolver con asignación de mercado antes de corregir la preparación interna de expedición.",
            ],
            [
                "La preparación EV está estructuralmente por debajo de ICE",
                f"Confirmada: la preparación EV es {pct(EV_READY_RATE)} frente a {pct(ICE_READY_RATE)} en ICE.",
                "Priorizar reserva de carga, confirmación de SOC y puertas de expedición específicas EV.",
            ],
        ],
        [CONTENT_W * 0.28, CONTENT_W * 0.35, CONTENT_W * 0.37],
    )
    st.append(PageBreak())

    return st


def _story_section_07() -> list:
    st: list = []
    st += h1("Hallazgos: dónde está físicamente el cuello", "Sección 7")
    st.append(
        lead(
            "La penalización EV explica quién llega tarde. No explica por sí sola dónde quedan atrapados los vehículos. Para eso "
            "hacen falta la vista física de patio y el diagnóstico por área."
        )
    )
    st += fig(
        "13_yard_zone_congestion.png",
        f"Figura 9. Permanencia p95 por zona de patio. La espera preexpedición domina con cerca de {pre_salida['p95_dwell'] / 60:.0f} "
        "horas y bloqueo casi total; la siguiente zona, pulmón de carga, despeja en unas cinco horas y las cuatro restantes quedan "
        "por debajo de tres horas y media.",
    )
    st.append(
        p(
            "El patio no está congestionado de forma uniforme. Una zona carga todo el problema. La espera preexpedición muestra una "
            f"permanencia media de {pre_salida['avg_dwell'] / 60:.1f} horas, una observación p95 por encima de {pre_salida['p95_dwell'] / 60:.1f} horas "
            f"y una tasa de bloqueo de {pct(pre_salida['blocking_rate'])}, mientras la siguiente zona, "
            f"{yard_zone_label(next_yard_zone['zona_patio'])}, promedia {next_yard_zone['avg_dwell'] / 60:.1f} horas. "
            "Los vehículos aún no listos se acumulan en espera preexpedición, la zona se satura y el bloqueo retrasa a los vehículos detrás, "
            f"incluidos los que sí están listos. La permanencia p95 de planta de {DWELL_P95_H:.0f} horas está impulsada por esta única zona."
        )
    )
    st += fig(
        "03_priority_ranking_opi.png",
        "Figura 10. Índice de Prioridad Operativa (OPI) por área. Logística y Patio forman un primer nivel claro; Carga, Producción, Expedición y Energía quedan bastante por debajo.",
    )
    st.append(
        p(
            "Llevado al nivel de área, el Índice de Prioridad Operativa (OPI) sitúa a Logística y Patio en un nivel propio, con 67 y 65, "
            "mientras el área siguiente queda a más de 25 puntos. Esto encaja con los datos de patio. Logística posee los fallos "
            "de preparación de expedición y ventana de salida; Patio posee la saturación física de la espera preexpedición. Son el mismo fallo visto "
            "desde dos ángulos, por eso se ordenan juntas y por eso las recomendaciones las tratan como una intervención coordinada y no "
            "como dos proyectos separados."
        )
    )
    st += data_table(
        ["Área", "OPI", "Factor principal de riesgo", "Acción recomendada", "Nivel"],
        [
            [
                r["area"],
                f"{r['operational_priority_index']:.1f}",
                label_from_code(r["main_risk_driver"], RISK_DRIVER_ES),
                r["recommended_action"].capitalize(),
                r["area_priority_tier"]
                .replace("estabilizar en la siguiente ola", "Estabilizar próxima ola")
                .replace("mantener bajo observación", "Vigilar")
                .replace("sin prioridad inmediata", "Sin prioridad inmediata"),
            ]
            for _, r in prio.sort_values("operational_priority_index", ascending=False).iterrows()
        ],
        [CONTENT_W * 0.16, CONTENT_W * 0.10, CONTENT_W * 0.22, CONTENT_W * 0.34, CONTENT_W * 0.18],
        aligns={1: "CENTER"},
    )
    st += fig(
        "04_risk_matrix.png",
        "Figura 11. Pérdida de caudal productivo frente a riesgo de expedición por área, con tamaño según estrés operativo. Logística y Patio caen en el cuadrante de intervención inmediata; Energía y Expedición quedan en sostenimiento.",
    )
    st.append(
        p(
            "La matriz de riesgo es la versión de una diapositiva de la historia de prioridad. Al cruzar pérdida de caudal productivo y riesgo "
            "de expedición, Logística cae en el cuadrante superior derecho de intervención inmediata, Patio queda alto en el mismo eje con "
            "estrés operativo elevado y el resto de áreas se desplaza hacia sostenimiento. La posición codifica riesgo en ambos ejes y el "
            "tamaño codifica estrés. Las dos áreas que requieren atención ejecutiva se ven sin leer la tabla."
        )
    )
    st += fig(
        "17_driver_correlation.png",
        "Figura 12. Correlación entre factores operativos a nivel área-turno. Congestión, espera y estrés se mueven juntos; la pérdida de caudal productivo es casi independiente de ellos.",
    )
    st.append(
        p(
            "La estructura de correlación explica por qué la lista de prioridades tiene dos cabezas y no una. Congestión, espera media, "
            "presión de cola y estrés operativo forman un clúster apretado: la familia física de riesgo de patio. La pérdida de caudal productivo "
            "queda casi ortogonal a ese clúster, lo que indica que la impulsa otra cosa: la familia preparación-expedición. Dos familias "
            "de riesgo independientes explican que Logística y Patio lleguen al primer nivel por factores distintos y que una palanca única "
            "no pueda resolver ambas."
        )
    )
    st.append(PageBreak())

    return st


def _story_section_08() -> list:
    st: list = []
    st += h1("Hallazgos: la presión sube con la cuota EV", "Sección 8")
    st.append(
        lead(
            "Un corte de un solo periodo puede ocultar si un problema empeora. La serie semanal de transición muestra que sí, "
            "y que el deterioro sigue de cerca la cuota EV."
        )
    )
    st += fig(
        "08_launch_transition_trend.png",
        "Figura 13. Cuota EV semanal frente a estrés de transición de patio y estabilidad de expedición. Al subir la cuota EV, el estrés de patio pasa de 25 a 53 y la estabilidad de expedición se erosiona en paralelo.",
    )
    st.append(
        p(
            f"La serie semanal añade la dimensión temporal. Al subir la cuota eléctrica {EV_SHARE_DELTA_PP:.1f} puntos porcentuales en "
            f"53 semanas, el índice de estrés de transición de patio sube {YARD_STRESS_DELTA:.1f} puntos y el índice de estabilidad de "
            f"expedición cae {abs(DISPATCH_STABILITY_DELTA):.1f} puntos. La presión al final de línea no es un coste fijo de operar EVs; "
            "escala con cuántos entran en el flujo. Sin gestión, la tendencia dice que el problema de salida empeora a medida que la planta "
            "cumple su objetivo de transición, la forma más peligrosa de un problema porque el éxito en una métrica fabrica fallo en otra."
        )
    )
    st.append(
        p(
            f"La brecha de capacidad de carga también se mueve en la dirección equivocada, ampliándose {CHARGE_GAP_DELTA:.1f} puntos desde "
            "la primera hasta la última semana operativa. Eso no significa que los cargadores estén saturados todo el año; la utilización "
            f"media reportada en el mart KPI gobernado es solo {pct(CHARGER_UTIL, 1)}. Significa que la restricción es concentración temporal: "
            "la demanda de carga llega en momentos desalineados con la disponibilidad de puntos de carga, y ese desajuste se agrava al subir la cuota "
            "eléctrico. Por tanto, el remedio debe incluir reserva y capacidad en picos de turno, no un objetivo de utilización media anual."
        )
    )
    st += fig(
        "12_market_geography.png",
        "Figura 14. Volumen y preparación por mercado destino. Iberia absorbe cerca de un tercio de la producción; la preparación es uniforme entre destinos.",
    )
    st.append(
        p(
            "La geografía es el segundo sospechoso descartado. La salida se concentra en Iberia, que absorbe alrededor de un tercio del volumen, "
            "seguida de Francia y Alemania, pero la preparación es esencialmente uniforme en los siete destinos. Ningún mercado está estructuralmente "
            "peor servido que otro. La corrección es interna y operativa, no una reasignación de volumen entre destinos ni una renegociación de ventanas "
            "de salida mercado por mercado. La palanca está dentro de la planta."
        )
    )
    st.append(PageBreak())

    return st


def _story_section_09() -> list:
    st: list = []
    st += h1("Hallazgos: qué respuesta funciona", "Sección 9")
    st.append(
        lead(
            "El diagnóstico dice qué está mal y dónde. El gemelo de escenarios pregunta qué hacer, simulando ocho futuros con "
            "una puntuación de decisión consistente y dejándolos competir."
        )
    )
    st += fig(
        "05_scenario_decision.png",
        "Figura 15. Puntuación de decisión en ocho escenarios. El paquete correctivo combinado lidera; acelerar la cuota EV sin soporte obtiene el peor resultado.",
    )
    st.append(
        p(
            f"El paquete correctivo combinado alcanza una puntuación de decisión de {best['decision_score']:.1f}. La rampa base no gestionada "
            f"obtiene {base['decision_score']:.1f}, y la peor opción, acelerar la cuota EV sin soporte operativo, cae a {worst['decision_score']:.1f}. "
            "El orden deja un mensaje claro: empujar más volumen eléctrico hacia la configuración existente es la peor opción disponible, porque carga "
            "exactamente la puerta que ya falla. Los escenarios de una sola palanca, más puntos de carga, mejor secuenciación o más espacio de patio, ayudan "
            "algo y se agrupan en el centro. Solo el paquete que combina las tres cosas se separa del resto."
        )
    )
    st += fig(
        "14_scenario_tradeoff.png",
        "Figura 16. Caudal productivo frente a vehículos retrasados por escenario, con eje x invertido para que mejor sea arriba a la izquierda. El paquete correctivo queda en la frontera eficiente.",
    )
    st.append(
        p(
            "La vista de compensación muestra por qué. Al cruzar caudal productivo con cuota de vehículos retrasados, los escenarios dibujan una frontera "
            "y el paquete correctivo se sitúa en su mejor esquina: más caudal productivo con menos vehículos tardíos al mismo tiempo. El escenario de cuota EV "
            "acelerado queda en la esquina opuesta, comprando caudal productivo nominal con un fuerte aumento de salidas tardías. Esto elimina la opción "
            "volumen-primero del conjunto de decisión. En estos datos, ir más rápido sin soporte mueve la planta en la dirección equivocada en la "
            "métrica que ya está rota."
        )
    )
    st += data_table(
        ["Escenario", "Caudal productivo", "Tiempo interno", "Veh. tarde", "Estabilidad", "Decisión"],
        [
            [
                label_from_code(s["escenario"], SCENARIO_NAME_ES),
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
        "Figura 17. Rampa base frente al paquete correctivo en cinco métricas. Caudal productivo, estabilidad, tiempo interno y vehículos tardíos mejoran; la espera de carga sube ligeramente como única compensación.",
    )
    st.append(
        p(
            f"Comparado directamente con la base no gestionada, el paquete correctivo mejora el caudal productivo en {scenario_delta_by_metric.loc['throughput', 'delta_pct'] * 100:.1f}%, eleva "
            f"la estabilidad operativa, reduce el tiempo interno en {abs(CORRECTIVE_INTERNAL_TIME_DELTA) * 100:.1f}% y baja los vehículos retrasados en {CORRECTIVE_LATE_PP_REDUCTION:.1f} puntos. "
            f"La única métrica que se mueve en contra es la espera media de carga, que sube {CORRECTIVE_CHARGE_WAIT_DELTA * 100:.1f}%. "
            "Esa compensación es material: hacer pasar más EVs por la preparación a tiempo genera más demanda simultánea sobre cargadores. "
            "También explica por qué la capacidad de carga es la primera palanca a probar."
        )
    )
    st += fig(
        "15_lever_ranking.png",
        "Figura 18. Palancas de capacidad por impacto esperado. La capacidad de carga lidera, con secuenciación y gestión de patio muy cerca.",
    )
    st.append(
        p(
            f"Ordenada individualmente, la capacidad de carga es la palanca de mayor retorno con impacto esperado de "
            f"{levers['impacto_esperado'].max():.2f}, seguida por secuenciación EV con {levers.set_index('palanca').loc['secuenciacion_ev', 'impacto_esperado']:.2f} "
            f"y gestión de patio con {levers.set_index('palanca').loc['gestion_patio', 'impacto_esperado']:.2f}. Las diferencias "
            "entre las tres primeras son pequeñas, razón cuantitativa por la que el paquete combinado supera a cualquier movimiento aislado. Ninguna palanca domina "
            "lo suficiente para sostenerse sola. Las cifras de impacto son supuestos paramétricos dentro del modelo de escenarios, no elasticidades medidas, y la sección "
            "de recomendaciones las trata como orden de prioridad, no como porcentajes prometidos."
        )
    )
    st.append(h2("Base frente a paquete correctivo"))
    st.append(
        p(
            "La comparación base-correctivo es la lectura de escenarios más útil para decidir porque aísla el paquete recomendado "
            "frente a la rampa no gestionada. El paquete mejora las métricas clave de la tesis operativa: fluyen más vehículos por el "
            "sistema, baja el tiempo interno, baja el riesgo de congestión y cae la tasa de vehículos retrasados. El aumento de espera "
            "media de carga es el único movimiento negativo, y solo es aceptable si el paquete se acompaña de reserva de carga en picos, "
            "no si se trata como un proyecto puro de patio o secuenciación."
        )
    )
    st += data_table(
        ["Métrica", "Base", "Paquete correctivo", "Movimiento"],
        [
            [
                "Caudal productivo",
                f"{scenario_delta_by_metric.loc['throughput', 'base']:.2f}",
                f"{scenario_delta_by_metric.loc['throughput', 'mejorado']:.2f}",
                f"+{scenario_delta_by_metric.loc['throughput', 'delta_pct'] * 100:.1f}%",
            ],
            [
                "Tiempo interno",
                f"{scenario_delta_by_metric.loc['tiempo_total_interno', 'base']:.0f}",
                f"{scenario_delta_by_metric.loc['tiempo_total_interno', 'mejorado']:.0f}",
                f"{scenario_delta_by_metric.loc['tiempo_total_interno', 'delta_pct'] * 100:.1f}%",
            ],
            [
                "Riesgo de congestión",
                f"{scenario_delta_by_metric.loc['riesgo_congestion', 'base']:.3f}",
                f"{scenario_delta_by_metric.loc['riesgo_congestion', 'mejorado']:.3f}",
                f"{scenario_delta_by_metric.loc['riesgo_congestion', 'delta_pct'] * 100:.1f}%",
            ],
            [
                "Vehículos retrasados",
                pct(scenario_delta_by_metric.loc["vehiculos_retrasados", "base"], 1),
                pct(scenario_delta_by_metric.loc["vehiculos_retrasados", "mejorado"], 1),
                f"{scenario_delta_by_metric.loc['vehiculos_retrasados', 'delta_pct'] * 100:.1f}%",
            ],
            [
                "Espera de carga",
                f"{scenario_delta_by_metric.loc['espera_carga', 'base']:.1f} min",
                f"{scenario_delta_by_metric.loc['espera_carga', 'mejorado']:.1f} min",
                f"+{scenario_delta_by_metric.loc['espera_carga', 'delta_pct'] * 100:.1f}%",
            ],
        ],
        [CONTENT_W * 0.31, CONTENT_W * 0.18, CONTENT_W * 0.27, CONTENT_W * 0.24],
        aligns={1: "CENTER", 2: "CENTER", 3: "CENTER"},
    )
    st.append(PageBreak())

    return st


def _story_section_10() -> list:
    st: list = []
    st += h1("Hallazgos: la clasificación es fiable", "Sección 10")
    st.append(
        lead(
            "Una priorización que cambia en cuanto alguien cuestiona los pesos no sirve para decidir. El marco de puntuación se somete "
            "a pruebas de estrés para defender la parte alta de la clasificación."
        )
    )
    st += fig(
        "16_montecarlo_robustness.png",
        "Figura 19. Área primer puesto en 300 remuestreos Monte Carlo de los pesos de puntuación. Logística lidera el 77% del tiempo y Patio toma el resto; ninguna otra área llega al primer puesto.",
    )
    st.append(
        p(
            "Los pesos del índice de prioridad son un juicio de gestión, por lo que el análisis los remuestrea 300 veces desde una distribución "
            "Dirichlet y recalcula la clasificación en cada caso. Logística queda primera en el 77% de las extracciones y Patio en el 23% restante. Ninguna "
            "tercera área toma el primer puesto. La conclusión principal, que Logística y Patio son las dos áreas a estabilizar, no depende de un conjunto "
            "particular de pesos. Se mantiene en todo el rango de ponderaciones razonables, y lo único que cambia es el orden de las dos primeras, ambas "
            "recomendadas para acción coordinada."
        )
    )
    st.append(
        p(
            "El análisis de sensibilidad lo refuerza por otra vía. Perturbar cada factor de riesgo individual arriba y abajo en 20% deja a Logística "
            "y Patio entre los dos primeros de la lista de tres primeras áreas en todos los casos. La puntuación media se desplaza solo en el segundo decimal. Una clasificación tan "
            "estable bajo remuestreo de pesos y perturbación de factores permite actuar sin tratar la recomendación como artefacto de una sola elección de modelado."
        )
    )
    st += data_table(
        ["Prueba de estrés", "Resultado observado", "Interpretación"],
        [
            [
                "Remuestreo Monte Carlo de pesos",
                f"Logística primer puesto en {pct(TOP1_LOGISTICS, 1)}; Patio primer puesto en {pct(TOP1_YARD, 1)}; ninguna otra área primera.",
                "La primera ola recomendada es estable aunque se discuta el orden exacto de Logística y Patio.",
            ],
            [
                "Perturbación de riesgo de patio",
                "El tres primeras sigue siendo Logística, Patio y Carga con factores -20% y +20%.",
                "La recomendación de patio no depende de sobredimensionar su propio puntuación de riesgo.",
            ],
            [
                "Perturbación de riesgo de carga",
                "Carga y Producción intercambian el tercer puesto; Logística y Patio siguen primeras.",
                "La recomendación de primera ola no cambia; carga sigue como palanca habilitadora, no como área principal.",
            ],
            [
                "Perturbación de riesgo de expedición",
                "Logística y Patio siguen primeras bajo ambas direcciones.",
                "El diagnóstico de preparación-expedición sobrevive a la incertidumbre del factor de expedición.",
            ],
        ],
        [CONTENT_W * 0.27, CONTENT_W * 0.38, CONTENT_W * 0.35],
    )
    st.append(h2("Incertidumbre residual"))
    st.append(
        p(
            "La incertidumbre restante no trata de qué dos áreas lideran. Trata del tamaño del beneficio después de intervenir. "
            "La distinción importa. El diagnóstico es suficientemente fuerte para empezar cambios operativos inmediatos, porque se ancla "
            "en mecánicas sintéticas observadas de flujo y clasificaciones estables. El caso de negocio para nuevos cargadores físicos o expansión de patio "
            "sigue requiriendo calibración con datos reales de planta, porque las elasticidades de escenario son supuestos. Empezar ahora con "
            "secuenciación, reserva y disciplina de espera preexpedición; usar el primer ciclo de implantación para estimar elasticidad real antes de comprometer capital irreversible."
        )
    )
    st.append(PageBreak())

    return st


def _story_section_11() -> list:
    st: list = []
    st += h1("Riesgos, límites y advertencias", "Sección 11")
    st.append(
        lead(
            "Un informe creíble explicita dónde se detiene. Los siguientes límites acotan hasta dónde deben llevarse estos hallazgos, "
            "y ninguno queda escondido en una nota al pie porque todos cambian cómo deben usarse los resultados."
        )
    )
    st.append(h3("Los datos son sintéticos"))
    st.append(
        p(
            "Es el primer caveat y el más importante. Los registros se generan, no se miden. La estructura es realista y las relaciones "
            "son consistentes internamente, suficiente para demostrar el método y mostrar qué emergería con datos reales. No es suficiente "
            "para tratar ninguna cifra absoluta como hecho de una planta física. La permanencia de espera preexpedición de "
            f"{pre_salida['p95_dwell'] / 60:.0f} horas y la tasa de salida limpia de {pct(CLEAN_EXIT_RATE)} son propiedades de este conjunto de datos, "
            "no referencias."
        )
    )
    st.append(h3("Los impactos de escenario son supuestos, no estimaciones"))
    st.append(
        p(
            "Los impactos de palanca y resultados de escenario proceden de elasticidades paramétricas elegidas por plausibilidad. No son "
            "estimaciones causales de un experimento o intervención natural. Los resultados deben leerse como una forma estructurada de comparar "
            "opciones bajo supuestos declarados, y la clasificación que producen es más fiable que cualquier porcentaje individual. Antes de cualquier "
            "compromiso de capital, estas elasticidades deberían calibrarse contra la respuesta histórica de la propia planta."
        )
    )
    st.append(h3("Las puntuaciones son relativos, no físicos"))
    st.append(
        p(
            "Cada puntuación de 0 a 100 en este informe es una posición en una distribución, no una medición en minutos, unidades o euros. "
            "Están construidos para comparar entre áreas y tipos de propulsión, tarea que cumplen bien. No están construidos para leerse "
            "como niveles absolutos ni deben citarse así fuera del contexto comparativo que les da sentido."
        )
    )
    st.append(h3("El proxy de pérdida de caudal productivo es atribución, no causalidad"))
    st.append(
        p(
            "La cifra de pérdida de caudal productivo por área atribuye flujo perdido a los eventos de cuello registrados contra un área. Es un proxy "
            "que reparte pérdida observada, no una estimación contrafactual de cuál habría sido la salida sin el cuello. Es fiable para ordenar "
            "dónde se concentra la pérdida y no debe sobreleerse como número preciso de output recuperable."
        )
    )
    st.append(h3("Pesos y umbrales requieren calibración local"))
    st.append(
        p(
            "El objetivo de preparación del 95%, el umbral de salida tardía de 120 minutos y los pesos de puntuación son defaults. Son razonables "
            "y las conclusiones se mantienen al moverlos dentro de rangos sensatos, como muestra la estabilidad de la clasificación, pero un despliegue "
            "real debería fijarlos según los estándares y acuerdos de nivel de servicio de la planta antes de que las cifras guíen inversión."
        )
    )
    st.append(h3("El panel necesita acceso de red"))
    st.append(
        p(
            "El panel complementario es un único HTML autocontenido con todos los datos embebidos, pero carga la librería de gráficos y fuentes web "
            "desde CDN. Renderiza completo online y degrada de forma razonable offline. Para un despliegue totalmente aislado habría que vendorizar "
            "esas dos dependencias dentro del fichero."
        )
    )
    st.append(PageBreak())

    return st


def _story_section_12() -> list:
    st: list = []
    st += h1("Recomendaciones y prioridades de acción", "Sección 12")
    st.append(
        lead(
            "La recomendación no es una transformación amplia. Es un paquete de control en cinco partes: tres cambios operativos que "
            "recuperan la puerta de salida, una decisión de gobernanza de transición y un paso de calibración antes de aprobar capital."
        )
    )
    st += data_table(
        ["#", "Acción", "Responsable", "Vinculado a"],
        [
            [
                "1",
                "Limitar y rediseñar la zona de espera preexpedición; secuenciar hacia espera preexpedición solo contra ventanas reales de expedición",
                "Patio / Logística",
                "Fig. 9, 11",
            ],
            [
                "2",
                "Reservar puntos de carga para versiones EV y añadir capacidad en picos de turno",
                "Energía / Carga",
                "Fig. 4, 5, 18",
            ],
            [
                "3",
                "Aplicar una ventana de preparación en expedición; mantener ICE fuera de una espera preexpedición saturada",
                "Logística",
                "Fig. 3, 5",
            ],
            [
                "4",
                "Detener la aceleración de la cuota EV hasta recuperar fiabilidad de salida",
                "Planta / Planificación",
                "Fig. 13, 15, 16",
            ],
            [
                "5",
                "Calibrar pesos, umbrales y elasticidades a estándares de planta antes de decisiones de capital",
                "Analítica de operaciones",
                "Sección 11",
            ],
        ],
        [CONTENT_W * 0.05, CONTENT_W * 0.55, CONTENT_W * 0.22, CONTENT_W * 0.18],
    )
    st.append(h2("Prioridad 1. Corregir el cuello de espera preexpedición"))
    st.append(
        p(
            f"La zona de espera preexpedición es el cuello físico, con {pre_salida['p95_dwell'] / 60:.0f} horas de permanencia p95 y "
            "bloqueo casi total frente a cinco horas o menos en el resto del patio. Tratarla como un recurso de capacidad limitada, no "
            "como suelo abierto. Fijar un límite duro de ocupación, segmentar el pulmón por ventana de expedición destino y llevar un vehículo "
            "a espera preexpedición solo cuando su ventana de salida esté confirmada. Es la recomendación de mayor confianza porque se apoya en datos de permanencia "
            "medidos, no en un supuesto de modelado, y porque alivia directamente el bloqueo que retrasa incluso vehículos listos."
        )
    )
    st.append(h2("Prioridad 2. Desbloquear preparación EV mediante carga"))
    st.append(
        p(
            "La preparación EV es la puerta, la carga es su entrada principal y la capacidad de carga es la palanca de mayor retorno del "
            "modelo de escenarios. Reservar puntos de carga para versiones eléctricas para que no compitan con demanda discrecional y añadir capacidad "
            "en los picos de turno donde se forma la cola. La utilización media de cargadores en el periodo es solo "
            f"{pct(CHARGER_UTIL)}, lo que indica que la restricción es momento y asignación en picos, no falta de energía total de carga. "
            f"La zona de carga con mayor presión es {highest_charge_zone['zona_carga'].title()}, con "
            f"{highest_charge_zone['sessions']:.0f} sesiones y una puntuación media de presión de {highest_charge_zone['pressure']:.1f}. "
            "La primera intervención puede ser una regla de asignación de puntos de carga antes de convertirse en programa de infraestructura."
        )
    )
    st.append(h2("Prioridad 3. Poner una puerta de preparación en expedición"))
    st.append(
        p(
            "Una parte grande del problema de salida es disciplina de secuenciación, no capacidad. Los vehículos de combustión, listos más del "
            "90% del tiempo, se liberan hacia una espera preexpedición ya saturada con EVs esperando carga y confirmación de SOC. Una ventana de preparación que "
            "mantenga vehículos fuera de espera preexpedición hasta que su ventana sea real descongestiona la zona sin añadir un metro cuadrado ni un cargador. Es "
            "el elemento de menor coste del paquete y sostiene los otros dos."
        )
    )
    st.append(h2("Prioridad 4. No acelerar la cuota EV hacia una puerta rota"))
    st.append(
        p(
            "La tendencia de transición y la clasificación de escenarios coinciden: empujar más volumen EV hacia la configuración actual es la peor opción disponible. "
            "Mantener estable la cuota planificada hasta que la puerta de salida se recupere. Es una decisión de secuenciación sobre la propia transición, y no cuesta "
            "más que paciencia, mientras protege a la planta de fabricar más salidas tardías en nombre de cumplir un hito de electrificación."
        )
    )
    st.append(h2("Prioridad 5. Calibrar antes de comprometer capital"))
    st.append(
        p(
            "Antes de que cualquiera de los puntos anteriores impulse una decisión de gasto, calibrar el modelo a la planta. Sustituir el objetivo default "
            "de preparación y el umbral de retraso por niveles de servicio propios de la planta, y ajustar elasticidades de escenario a la respuesta observada "
            "ante cambios operativos pasados. El diagnóstico y la clasificación son lo bastante sólidos para actuar ya. Las magnitudes de escenario no lo son y deben "
            "ganarse su lugar frente a historial real antes de dimensionar una inversión."
        )
    )
    st.append(h2("Tamaño del premio"))
    st.append(
        p(
            f"Para enmarcar el valor en una línea: la planta ya fabrica {TOTAL:,} vehículos según plan, pero solo unos "
            f"{ON_TIME_READY:,} salen hoy listos y a tiempo. El paquete correctivo no pide fabricar más; se centra en expedir limpiamente "
            "lo que la planta ya fabrica. Eso es mejora operativa de alto retorno, porque el volumen ya está pagado. Recuperar incluso la mitad "
            "de la brecha de salida limpia haría más por la fiabilidad de la planta que cualquier iniciativa de caudal productivo, a una fracción del coste."
        )
    )
    st.append(h2("Roadmap de implementación"))
    st.append(
        p(
            "Las recomendaciones no deben lanzarse como un gran programa único. La primera ola debe cambiar reglas y disciplina operativa, porque esas "
            "acciones son reversibles y generarán los datos de calibración necesarios para cualquier caso de capital posterior. La capacidad física debe ser "
            "segunda ola, no punto de partida, salvo que el piloto pruebe que la restricción de carga o espera preexpedición sigue siendo vinculante después de imponer "
            "secuenciación y reserva."
        )
    )
    st += data_table(
        ["Horizonte", "Acciones", "Criterio de salida"],
        [
            [
                "0-2 semanas",
                "Congelar aceleración de la cuota EV; definir ventana de preparación; introducir límite de ocupación en espera preexpedición; publicar métrica diaria de salida limpia.",
                "Tasa de salida limpia monitorizada a diario; ningún vehículo entra en espera preexpedición sin ventana de expedición confirmada.",
            ],
            [
                "2-6 semanas",
                "Reservar puntos de carga EV por versión y turno; rebalancear el pulmón preexpedición por ventana de salida; revisar sala de control por turno.",
                "La preparación EV mejora semana a semana; baja el bloqueo preexpedición; no se deteriora el caudal productivo total.",
            ],
            [
                "6-12 semanas",
                "Probar capacidad de carga en picos y cambios de dotación; comparar elasticidad observada contra supuestos de escenario.",
                "La respuesta medida respalda o rechaza el caso de inversión en expansión de cargadores y rediseño de patio.",
            ],
            [
                "Antes de capital",
                "Recalibrar modelo de escenarios con historial de planta; refrescar pesos OPI con liderazgo de operaciones; reejecutar puerta de publicación.",
                "El paquete de decisión separa ganancias operativas confirmadas de supuestos que requieren aprobación de inversión.",
            ],
        ],
        [CONTENT_W * 0.16, CONTENT_W * 0.46, CONTENT_W * 0.38],
    )
    st.append(h2("Controles de gestión"))
    st.append(
        p(
            "El set de control debe ser lo bastante pequeño para ejecutarse cada día. Seguir tasa de salida limpia, tasa de preparación EV, bloqueo "
            "de espera preexpedición, espera de carga en pico, minutos de retraso por versión y estabilidad de las dos primeras áreas del OPI. Las primeras cuatro métricas "
            "dicen si la intervención funciona; las dos últimas evitan declarar victoria porque mejora un síntoma mientras el cuello raíz se mueve a otro sitio."
        )
    )
    st += data_table(
        ["Control", "Responsable", "Disparador operativo"],
        [
            [
                "Tasa de salida limpia",
                "Director de planta",
                "La tasa diaria no mejora durante cinco días operativos tras activar reglas de espera preexpedición y preparación.",
            ],
            [
                "Tasa de preparación EV",
                "Logística / Carga",
                "Las versiones EV siguen por debajo del objetivo acordado tras introducir puntos de carga reservados.",
            ],
            [
                "Bloqueo preexpedición",
                "Patio",
                "El bloqueo no baja tras aplicar límites de ocupación y segmentación por ventana de destino.",
            ],
            [
                "Espera de carga en pico",
                "Energía / Carga",
                "La espera sube mientras mejora la preparación, señal de que la asignación de puntos de carga ya no basta.",
            ],
            [
                "Minutos tardíos por versión",
                "Analítica de operaciones",
                "La concentración de retraso se desplaza fuera de las cuatro versiones EV, indicando que el cuello se ha movido.",
            ],
            [
                "Estabilidad de las dos primeras áreas OPI",
                "Analítica de operaciones",
                "Logística y Patio dejan de estar entre las dos primeras con los últimos datos operativos.",
            ],
        ],
        [CONTENT_W * 0.26, CONTENT_W * 0.24, CONTENT_W * 0.50],
    )
    st.append(PageBreak())

    return st


def _story_section_13() -> list:
    st: list = []
    st += h1("Preguntas adicionales para despliegue real", "Sección 13")
    st.append(
        lead(
            "El informe está listo para decisión como gemelo operativo sintético y demostración metodológica. Un despliegue en planta real necesitaría "
            "cinco respuestas adicionales antes de apoyar aprobación de capital o compromisos contractuales."
        )
    )
    st += data_table(
        ["Pregunta", "Por qué importa", "Evidencia necesaria"],
        [
            [
                "¿Cuál es la definición real de nivel de servicio para una salida limpia?",
                "El informe usa preparación más umbral de dos horas; la planta puede gestionar ventanas cliente distintas.",
                "SLA de salida, reglas de ventanas de transportista, lógica de penalizaciones cliente y códigos de excepción aceptados.",
            ],
            [
                "¿Qué fallos de carga son de capacidad, programación o confirmación de SOC?",
                "La respuesta operativa cambia: añadir equipamiento, reservar puntos de carga o modificar inspección/firma.",
                "Marcas temporales de sesión, disponibilidad de cargadores, incumplimientos de SOC objetivo, causas de interrupción y registros de excepciones manuales.",
            ],
            [
                "¿Cuánta permanencia de espera preexpedición es evitable frente a inducido por política?",
                "Parte de la permanencia puede ser batching intencional para rutas de salida; solo la permanencia evitable debe impulsar un rediseño.",
                "Asignación de ventanas de expedición, agrupación por destino, llegadas de transportistas y códigos de intención de movimiento de patio.",
            ],
            [
                "¿Qué elasticidad muestra cada intervención en la práctica?",
                "Los impactos de escenario son supuestos hasta observarse en el proceso propio de la planta.",
                "Diseño piloto con ventanas antes/después, composición de demanda estable y respuesta medida en preparación, permanencia y salidas tardías.",
            ],
            [
                "¿Cómo debe ponderar la cuadro de mando coste, capex y penalizaciones de servicio?",
                "El modelo actual ordena resultados operativos, no retorno financiero.",
                "Coste de cargadores, cambios de patio, horas extra, penalizaciones de transportista, impacto en capital circulante y penalizaciones cliente.",
            ],
        ],
        [CONTENT_W * 0.29, CONTENT_W * 0.34, CONTENT_W * 0.37],
    )
    st.append(
        p(
            "Ninguna de estas preguntas bloquea las recomendaciones operativas. Sí bloquean un caso de negocio de capital. Ese es el límite correcto "
            "de gobernanza: usar el diagnóstico para corregir ahora fugas basadas en reglas y usar el periodo de implantación para convertir el gemelo "
            "sintético de escenarios en un modelo de inversión calibrado."
        )
    )
    st.append(PageBreak())

    return st


def _story_section_14() -> list:
    st: list = []
    st += h1("Apéndice", "Sección 14")
    bottleneck_cause_es = {"BLOQUEO_INTERNO_Y_REUBICACION": "Bloqueo interno y reubicación"}
    st += h2_table(
        "A. Resumen de KPI operativos",
        ["Métrica", "Valor"],
        [
            ["Órdenes-vehículo totales", f"{TOTAL:,}"],
            ["Brecha de caudal productivo frente al plan", f"{int(kpi['throughput_gap'])}"],
            ["Cuota EV del flujo", pct(SHARE_EV, 1)],
            ["Permanencia media en patio", f"{DWELL_MEAN_H:.1f} h"],
            ["Permanencia p95 en patio", f"{DWELL_P95_H:.1f} h"],
            ["Espera media de carga", f"{WAIT_CHARGE:.0f} min"],
            ["Utilización media de cargadores", pct(CHARGER_UTIL, 1)],
            ["Vehículos no listos en expedición", f"{NO_READY:,}"],
            ["Ratio de expedición tardía", pct(RATIO_LATE, 1)],
            ["Tasa de salida limpia (a tiempo y lista)", pct(CLEAN_EXIT_RATE, 1)],
            ["Puntuación global de preparación", f"{READINESS_GLOBAL:.1f}"],
            [
                "Causa principal de cuello",
                bottleneck_cause_es.get(
                    str(kpi["causa_principal_cuello"]), str(kpi["causa_principal_cuello"]).replace("_", " ").title()
                ),
            ],
            [
                "Área de mayor pérdida de caudal productivo",
                AREA_NAME_ES.get(
                    str(kpi["area_mayor_perdida_throughput"]), str(kpi["area_mayor_perdida_throughput"]).title()
                ),
            ],
        ],
        [CONTENT_W * 0.62, CONTENT_W * 0.38],
        aligns={1: "RIGHT"},
    )
    st += h2_table(
        "B. Puntuaciones de presión EV frente a ICE",
        ["Factor", "EV", "ICE"],
        [
            [
                "Disrupción de secuencia",
                f"{evice.set_index('tipo_propulsion').loc['EV', 'sequence_disruption_score']:.1f}",
                f"{evice.set_index('tipo_propulsion').loc['ICE', 'sequence_disruption_score']:.1f}",
            ],
            [
                "Congestión de patio",
                f"{evice.set_index('tipo_propulsion').loc['EV', 'yard_congestion_score']:.1f}",
                f"{evice.set_index('tipo_propulsion').loc['ICE', 'yard_congestion_score']:.1f}",
            ],
            [
                "Presión de carga",
                f"{evice.set_index('tipo_propulsion').loc['EV', 'charging_pressure_score']:.1f}",
                f"{evice.set_index('tipo_propulsion').loc['ICE', 'charging_pressure_score']:.1f}",
            ],
            [
                "Riesgo de retraso en expedición",
                f"{evice.set_index('tipo_propulsion').loc['EV', 'dispatch_delay_risk_score']:.1f}",
                f"{evice.set_index('tipo_propulsion').loc['ICE', 'dispatch_delay_risk_score']:.1f}",
            ],
            [
                "Estrés de transición de lanzamiento",
                f"{evice.set_index('tipo_propulsion').loc['EV', 'launch_transition_stress_score']:.1f}",
                f"{evice.set_index('tipo_propulsion').loc['ICE', 'launch_transition_stress_score']:.1f}",
            ],
        ],
        [CONTENT_W * 0.5, CONTENT_W * 0.25, CONTENT_W * 0.25],
        aligns={1: "CENTER", 2: "CENTER"},
    )
    st += h2_table(
        "C. Preparación por versión",
        ["Propulsión", "Versión", "Órdenes", "Preparación"],
        [
            [
                r["tipo_propulsion"],
                r["version_id"].replace("_", " "),
                f"{int(r['vehicles']):,}",
                pct(r["readiness"], 1),
            ]
            for _, r in version_readiness.sort_values(["tipo_propulsion", "readiness"]).iterrows()
        ],
        [CONTENT_W * 0.18, CONTENT_W * 0.38, CONTENT_W * 0.22, CONTENT_W * 0.22],
        aligns={2: "RIGHT", 3: "RIGHT"},
    )
    lever_name_es = {
        "capacidad_carga": "Capacidad de carga",
        "secuenciacion_ev": "Secuenciación EV",
        "gestion_patio": "Gestión de patio",
        "disciplina_expedicion": "Disciplina de expedición",
        "resiliencia_turno": "Resiliencia de turno",
    }
    st += h2_table(
        "D. Palancas de capacidad por impacto esperado",
        ["Palanca", "Impacto esperado"],
        [
            [
                lever_name_es.get(lv["palanca"], lv["palanca"].replace("_", " ").capitalize()),
                f"{lv['impacto_esperado']:.2f}",
            ]
            for _, lv in levers.sort_values("impacto_esperado", ascending=False).iterrows()
        ],
        [CONTENT_W * 0.62, CONTENT_W * 0.38],
        aligns={1: "RIGHT"},
    )
    # Etiqueta en español; algunas métricas se muestran como porcentaje de su valor.
    delta_metric_labels = {
        "throughput": ("Caudal productivo", False),
        "tiempo_total_interno": ("Tiempo interno", False),
        "ocupacion_media_patio": ("Ocupación media de patio", True),
        "ocupacion_pico_patio": ("Ocupación pico de patio", True),
        "espera_carga": ("Espera de carga (min)", False),
        "riesgo_salida_baja_readiness": ("Riesgo de salida con baja preparación", False),
        "riesgo_congestion": ("Riesgo de congestión", False),
        "vehiculos_retrasados": ("Vehículos retrasados", True),
        "estabilidad_operativa": ("Estabilidad operativa", False),
    }

    def _delta_value(x: float, as_pct: bool) -> str:
        if as_pct:
            return pct(x, 1)
        return f"{x:.3f}" if abs(x) < 10 else f"{x:.1f}"

    st += h2_table(
        "E. Resumen de deltas del paquete correctivo",
        ["Métrica", "Base", "Correctivo", "Delta"],
        [
            [
                delta_metric_labels.get(r["metrica"], (r["metrica"].replace("_", " ").capitalize(), False))[0],
                _delta_value(r["base"], delta_metric_labels.get(r["metrica"], (None, False))[1]),
                _delta_value(r["mejorado"], delta_metric_labels.get(r["metrica"], (None, False))[1]),
                f"{r['delta_pct'] * 100:+.1f}%",
            ]
            for _, r in scenario_delta.iterrows()
        ],
        [CONTENT_W * 0.42, CONTENT_W * 0.18, CONTENT_W * 0.20, CONTENT_W * 0.20],
        aligns={1: "RIGHT", 2: "RIGHT", 3: "RIGHT"},
    )
    st += h2_table(
        "F. Detalle de estabilidad de la clasificación",
        ["Prueba", "Resultado"],
        [
            ["Monte Carlo: primer puesto: Logística", pct(TOP1_LOGISTICS, 1)],
            ["Monte Carlo: primer puesto: Patio", pct(TOP1_YARD, 1)],
            ["Casos de sensibilidad probados", f"{len(sensitivity)}"],
            [
                "Patrón de las tres primeras áreas en sensibilidad",
                "; ".join(
                    sorted(
                        {
                            ", ".join(AREA_NAME_ES.get(a, a.title()) for a in areas.split(","))
                            for areas in sensitivity["top3_areas"]
                        }
                    )
                ),
            ],
        ],
        [CONTENT_W * 0.42, CONTENT_W * 0.58],
    )
    figs = [
        "Fig. 1 Caudal productivo diario",
        "Fig. 2 Cuota EV semanal",
        "Fig. 3 Funnel de expedición",
        "Fig. 4 Presión EV vs ICE",
        "Fig. 5 Cohorte de preparación",
        "Fig. 6 Concentración de retraso",
        "Fig. 7 Distribución de tiempo de paso",
        "Fig. 8 Heatmap de preparación",
        "Fig. 9 Congestión por zona de patio",
        "Fig. 10 Ranking OPI",
        "Fig. 11 Matriz de riesgo",
        "Fig. 12 Correlación de factores",
        "Fig. 13 Tendencia de transición",
        "Fig. 14 Geografía de mercado",
        "Fig. 15 Decisión de escenario",
        "Fig. 16 Compensación de escenarios",
        "Fig. 17 Antes y después",
        "Fig. 18 Ranking de palancas",
        "Fig. 19 Estabilidad Monte Carlo",
    ]
    rows = [[figs[i], figs[i + 1] if i + 1 < len(figs) else ""] for i in range(0, len(figs), 2)]
    st += h2_table(
        "G. Índice de figuras",
        ["Figura", "Figura"],
        rows,
        [CONTENT_W * 0.5, CONTENT_W * 0.5],
        highlight_first_col=False,
    )
    return st


def build_story() -> list:
    story: list = []
    story += cover()
    story.append(NextPageTemplate("main"))
    story.append(PageBreak())
    story += toc_page()
    story.append(PageBreak())
    for index, section in enumerate(
        (
            _story_section_01,
            _story_section_02,
            _story_section_03,
            _story_section_04,
            _story_section_05,
            _story_section_06,
            _story_section_07,
            _story_section_08,
            _story_section_09,
            _story_section_10,
            _story_section_11,
            _story_section_12,
            _story_section_13,
            _story_section_14,
        )
    ):
        story += compose_section(section(), is_first=index == 0)
    return story


def main() -> None:
    _load_report_data()
    OUT.mkdir(parents=True, exist_ok=True)
    doc = Report(
        str(PDF),
        title="Gemelo operativo para la transición a vans EV",
        author="Miguel Fidalgo Martins",
        subject="Diagnóstico operativo y escenarios para una transición industrial a vans eléctricas",
        keywords="gemelo operativo, vehículos eléctricos, operaciones, DuckDB, Python, simulación",
        creator="Python y ReportLab",
    )
    story = build_story()
    doc.multiBuild(story)
    print(f"OK - informe en {PDF.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
