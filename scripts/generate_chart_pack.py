"""Genera el pack de gráficos públicos (PNG) a partir de los datos procesados.

Salida: outputs/graphs/*.png

Pack ampliado de visuales seleccionados por su valor de decisión para revisión
ejecutiva y técnica. Cada gráfico responde a una sola pregunta analítica. Fondo
claro, tipografía consistente, anotación directa y un único color de acento.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd

# ── Rutas
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "processed" / "ev_factory"
OUT = ROOT / "outputs" / "graphs"
OUT.mkdir(parents=True, exist_ok=True)

# ── Paleta y tipografía
INK = "#1c1917"
INK_2 = "#44403c"
MUTED = "#78716c"
SUBTLE = "#a8a29e"
LINE = "#e7e5e4"
SURFACE = "#ffffff"

ACCENT = "#1d4ed8"
ACCENT_SOFT = "#bfdbfe"
DANGER = "#b91c1c"
WARN = "#a16207"
POSITIVE = "#15803d"
NEUTRAL_BAR = "#a8a29e"
EV_COLOR = "#1d4ed8"
ICE_COLOR = "#a8a29e"

TIER_COLOR = {
    "estabilizar en la siguiente ola": DANGER,
    "monitorizar muy de cerca": WARN,
    "mantener bajo observación": POSITIVE,
    "sin prioridad inmediata": POSITIVE,
}

plt.rcParams.update(
    {
        "font.family": ["Helvetica Neue", "Arial", "DejaVu Sans"],
        "font.size": 10,
        "axes.titlesize": 13,
        "axes.titleweight": "regular",
        "axes.labelsize": 10,
        "axes.labelcolor": MUTED,
        "axes.edgecolor": LINE,
        "axes.linewidth": 0.8,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.facecolor": SURFACE,
        "figure.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "grid.color": LINE,
        "grid.linewidth": 0.6,
        "grid.alpha": 1.0,
        "legend.frameon": False,
        "legend.fontsize": 10,
        "legend.handlelength": 1.2,
        "figure.dpi": 110,
    }
)


def title_block(ax, eyebrow: str, title: str, subtitle: str | None = None) -> None:
    fig = ax.figure
    fig.text(0.045, 0.955, eyebrow.upper(), color=SUBTLE, fontsize=9.5, weight="regular")
    fig.text(0.045, 0.905, title, color=INK, fontsize=17.5, weight="semibold")
    if subtitle:
        fig.text(0.045, 0.865, subtitle, color=MUTED, fontsize=11)


def footer(fig, source: str) -> None:
    fig.text(0.045, 0.022, source, color=SUBTLE, fontsize=8.5)
    fig.text(
        0.955,
        0.022,
        "Gemelo Operativo EV · Synthetic factory data",
        color=SUBTLE,
        fontsize=8.5,
        ha="right",
    )


def style_grid(ax, axis: str = "y") -> None:
    ax.set_axisbelow(True)
    ax.grid(True, axis=axis, color=LINE, linewidth=0.6)
    ax.tick_params(length=0)


def save(fig, name: str) -> Path:
    path = OUT / f"{name}.png"
    fig.savefig(path, dpi=200, bbox_inches="tight", pad_inches=0.4)
    plt.close(fig)
    print(f"  → {path.relative_to(ROOT)}")
    return path


# ──────────────────────────────────────────────────────────────────────────────
def chart_01_throughput() -> None:
    df = pd.read_csv(DATA / "vw_vehicle_flow_timeline.csv", parse_dates=["fecha_real"])
    daily = (
        df.groupby(df["fecha_real"].dt.date)
        .agg(real=("vehiculo_id", "nunique"))
        .reset_index()
        .rename(columns={"fecha_real": "fecha"})
    )
    daily["fecha"] = pd.to_datetime(daily["fecha"])
    daily = daily.sort_values("fecha").iloc[:-2]
    daily["real_7d"] = daily["real"].rolling(7, min_periods=1).mean()
    avg = daily["real"].mean()

    fig, ax = plt.subplots(figsize=(12, 5.6))
    plt.subplots_adjust(left=0.07, right=0.96, top=0.76, bottom=0.13)

    ax.scatter(daily["fecha"], daily["real"], s=8, color="#d6d3d1", alpha=0.9, zorder=1, label="Diario")
    ax.plot(daily["fecha"], daily["real_7d"], color=INK, linewidth=2.4, zorder=3, label="Media móvil 7 días")

    ax.axhline(avg, color=MUTED, linewidth=0.9, linestyle=(0, (3, 3)), zorder=2)
    ax.text(daily["fecha"].min(), avg, f"  media período: {avg:.0f}", va="bottom", ha="left", color=MUTED, fontsize=9.5)

    style_grid(ax, "y")
    ax.set_ylabel("Vehículos completados / día", color=MUTED)
    ax.set_ylim(daily["real"].min() * 0.92, daily["real"].max() * 1.06)
    ax.spines["bottom"].set_color(LINE)
    ax.spines["left"].set_visible(False)
    ax.legend(loc="lower right", fontsize=10)

    title_block(
        ax,
        "01 · Throughput operativo",
        "El sistema sostiene 160 vehículos/día durante el ramp-up EV",
        "Salida diaria del mart oficial; las olas semanales son ruido operativo, la media período es estable.",
    )
    footer(fig, "Fuente: vw_vehicle_flow_timeline · período 2025-01 a 2025-10")
    save(fig, "01_throughput_daily")


# ──────────────────────────────────────────────────────────────────────────────
def chart_02_share_ev_weekly() -> None:
    df = pd.read_csv(DATA / "vw_vehicle_flow_timeline.csv", parse_dates=["fecha_real"])
    df["week"] = df["fecha_real"].dt.to_period("W").dt.start_time
    weekly = (
        df.groupby("week")
        .agg(total=("vehiculo_id", "nunique"), ev=("tipo_propulsion", lambda x: (x == "EV").sum()))
        .reset_index()
    )
    weekly["share_ev"] = weekly["ev"] / weekly["total"].clip(lower=1)

    fig, ax = plt.subplots(figsize=(11, 5.0))
    plt.subplots_adjust(left=0.06, right=0.97, top=0.78, bottom=0.12)

    ax.fill_between(weekly["week"], 0, weekly["share_ev"], color=ACCENT, alpha=0.10, linewidth=0)
    ax.plot(weekly["week"], weekly["share_ev"], color=ACCENT, linewidth=2.2)

    first = weekly.iloc[0]
    last = weekly.iloc[-1]
    ax.scatter(
        [first["week"], last["week"]],
        [first["share_ev"], last["share_ev"]],
        color=ACCENT,
        s=42,
        zorder=4,
        edgecolors=SURFACE,
        linewidths=2,
    )
    ax.annotate(
        f"{first['share_ev'] * 100:.0f}%",
        (first["week"], first["share_ev"]),
        xytext=(10, -2),
        textcoords="offset points",
        color=ACCENT,
        fontsize=11,
        weight="medium",
        va="center",
    )
    ax.annotate(
        f"{last['share_ev'] * 100:.0f}%",
        (last["week"], last["share_ev"]),
        xytext=(-10, -2),
        textcoords="offset points",
        color=ACCENT,
        fontsize=11,
        weight="medium",
        va="center",
        ha="right",
    )

    style_grid(ax, "y")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    ax.set_ylim(0, max(0.6, weekly["share_ev"].max() * 1.15))
    ax.set_xlabel("")
    ax.set_ylabel("Share EV semanal")
    ax.spines["bottom"].set_color(LINE)
    ax.spines["left"].set_visible(False)

    title_block(
        ax,
        "02 · Mix EV",
        "El mix EV sube y arrastra presión hacia patio y carga",
        "Porcentaje semanal de vehículos EV sobre el total del flujo.",
    )
    footer(fig, "Fuente: vw_vehicle_flow_timeline")
    save(fig, "02_share_ev_weekly")


# ──────────────────────────────────────────────────────────────────────────────
def chart_03_priority_ranking() -> None:
    df = pd.read_csv(DATA / "operational_prioritization_table.csv")
    df = df.sort_values("operational_priority_index", ascending=True).reset_index(drop=True)
    colors = df["area_priority_tier"].map(TIER_COLOR).fillna(NEUTRAL_BAR).tolist()

    fig, ax = plt.subplots(figsize=(11, 5.6))
    plt.subplots_adjust(left=0.18, right=0.93, top=0.78, bottom=0.10)

    bars = ax.barh(df["area"], df["operational_priority_index"], color=colors, edgecolor="none", height=0.62)
    for rect, val in zip(bars, df["operational_priority_index"]):
        ax.text(
            val + 1.2,
            rect.get_y() + rect.get_height() / 2,
            f"{val:.1f}",
            va="center",
            ha="left",
            color=INK,
            fontsize=10.5,
            weight="medium",
        )

    style_grid(ax, "x")
    ax.set_xlim(0, df["operational_priority_index"].max() * 1.18)
    ax.set_xlabel("Operational Priority Index (OPI)")
    ax.set_ylabel("")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.tick_params(axis="y", labelcolor=INK, labelsize=10.5, pad=8)

    handles = [
        plt.Line2D([0], [0], marker="s", color="w", markerfacecolor=DANGER, markersize=10, label="Estabilizar (T0)"),
        plt.Line2D([0], [0], marker="s", color="w", markerfacecolor=WARN, markersize=10, label="Monitorizar (T1)"),
        plt.Line2D(
            [0], [0], marker="s", color="w", markerfacecolor=POSITIVE, markersize=10, label="Bajo observación (T2)"
        ),
    ]
    ax.legend(handles=handles, loc="lower right", fontsize=9.5)

    title_block(
        ax,
        "03 · Priorización operativa",
        "LOGÍSTICA y PATIO concentran la presión esta semana",
        "OPI por área: combina pérdida de throughput, riesgo de salida y stress operativo.",
    )
    footer(fig, "Fuente: operational_prioritization_table")
    save(fig, "03_priority_ranking_opi")


# ──────────────────────────────────────────────────────────────────────────────
def chart_04_risk_matrix() -> None:
    df = pd.read_csv(DATA / "operational_prioritization_table.csv")
    df = df[
        ["area", "throughput_loss_score", "dispatch_risk_score", "operational_stress_score", "area_priority_tier"]
    ].copy()
    colors = df["area_priority_tier"].map(TIER_COLOR).fillna(NEUTRAL_BAR).tolist()

    fig, ax = plt.subplots(figsize=(11, 6.4))
    plt.subplots_adjust(left=0.09, right=0.95, top=0.76, bottom=0.13)

    sizes = df["operational_stress_score"] / df["operational_stress_score"].max() * 1400 + 280
    ax.scatter(
        df["throughput_loss_score"],
        df["dispatch_risk_score"],
        s=sizes,
        c=colors,
        edgecolors=SURFACE,
        linewidths=3.0,
        alpha=0.92,
        zorder=3,
    )

    offsets = {
        "PATIO": (12, 8),
        "LOGISTICA": (12, -16),
        "EXPEDICION": (12, 8),
        "PRODUCCION": (-12, -18),
        "CARGA": (12, 8),
        "ENERGIA": (-12, -18),
    }
    for _, row in df.iterrows():
        dx, dy = offsets.get(row["area"], (10, 8))
        ha = "left" if dx > 0 else "right"
        ax.annotate(
            row["area"],
            (row["throughput_loss_score"], row["dispatch_risk_score"]),
            xytext=(dx, dy),
            textcoords="offset points",
            fontsize=11,
            color=INK,
            weight="medium",
            ha=ha,
        )

    ax.axvline(50, color=LINE, linewidth=0.8, zorder=1)
    ax.axhline(50, color=LINE, linewidth=0.8, zorder=1)

    ax.text(
        0.99,
        0.97,
        "Intervenir ahora",
        transform=ax.transAxes,
        color=DANGER,
        fontsize=10,
        weight="medium",
        ha="right",
        va="top",
        alpha=0.85,
    )
    ax.text(
        0.01, 0.97, "Riesgo de salida", transform=ax.transAxes, color=MUTED, fontsize=10, ha="left", va="top", alpha=0.7
    )
    ax.text(
        0.99,
        0.03,
        "Pérdida de flujo",
        transform=ax.transAxes,
        color=MUTED,
        fontsize=10,
        ha="right",
        va="bottom",
        alpha=0.7,
    )
    ax.text(
        0.01,
        0.03,
        "Sostener",
        transform=ax.transAxes,
        color=POSITIVE,
        fontsize=10,
        weight="medium",
        ha="left",
        va="bottom",
        alpha=0.85,
    )

    style_grid(ax, "both")
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.set_xlabel("Throughput loss score")
    ax.set_ylabel("Dispatch risk score")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)

    title_block(
        ax,
        "04 · Matriz de riesgo",
        "Pérdida de flujo × riesgo de salida — dónde actuar primero",
        "Tamaño del punto: operational stress. LOGÍSTICA y PATIO están en el cuadrante de intervención inmediata.",
    )
    footer(fig, "Fuente: operational_prioritization_table")
    save(fig, "04_risk_matrix")


# ──────────────────────────────────────────────────────────────────────────────
def chart_05_scenario_decision() -> None:
    df = pd.read_csv(DATA / "scenario_decision_comparison.csv")
    df = df.sort_values("decision_score", ascending=True).reset_index(drop=True)
    df["label"] = df["escenario"].str.replace(r"^\d+_", "", regex=True).str.replace("_", " ").str.capitalize()
    top_score = df["decision_score"].max()
    colors = [ACCENT if v == top_score else NEUTRAL_BAR for v in df["decision_score"]]

    fig, ax = plt.subplots(figsize=(11, 5.6))
    plt.subplots_adjust(left=0.34, right=0.96, top=0.78, bottom=0.10)

    bars = ax.barh(df["label"], df["decision_score"], color=colors, edgecolor="none", height=0.62)
    for rect, val in zip(bars, df["decision_score"]):
        ax.text(
            val + 0.4,
            rect.get_y() + rect.get_height() / 2,
            f"{val:.1f}",
            va="center",
            ha="left",
            color=INK,
            fontsize=10,
            weight="medium",
        )

    style_grid(ax, "x")
    ax.set_xlim(df["decision_score"].min() - 3, df["decision_score"].max() + 4)
    ax.set_xlabel("Decision score (multi-criterio)")
    ax.set_ylabel("")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.tick_params(axis="y", labelcolor=INK, labelsize=10.0, pad=8)

    title_block(
        ax,
        "05 · Comparador de escenarios",
        "La combinación de medidas correctivas gana al ramp-up puro",
        "Decision score = throughput + readiness + estabilidad − riesgo de salida.",
    )
    footer(fig, "Fuente: scenario_decision_comparison")
    save(fig, "05_scenario_decision")


# ──────────────────────────────────────────────────────────────────────────────
def chart_06_ev_vs_ice() -> None:
    df = pd.read_csv(DATA / "diagnostic_ev_vs_non_ev.csv")
    metrics = [
        ("sequence_disruption_score", "Disrupción de secuencia"),
        ("yard_congestion_score", "Congestión patio"),
        ("charging_pressure_score", "Presión de carga"),
        ("dispatch_delay_risk_score", "Riesgo de retraso"),
        ("launch_transition_stress_score", "Stress de transición"),
    ]
    ev = df[df["tipo_propulsion"] == "EV"].iloc[0]
    ice = df[df["tipo_propulsion"] == "ICE"].iloc[0]

    labels = [m[1] for m in metrics]
    ev_vals = [ev[m[0]] for m in metrics]
    ice_vals = [ice[m[0]] for m in metrics]

    y = np.arange(len(labels))
    h = 0.36

    fig, ax = plt.subplots(figsize=(11, 5.6))
    plt.subplots_adjust(left=0.24, right=0.96, top=0.78, bottom=0.14)

    ax.barh(y - h / 2, ice_vals, height=h, color=SUBTLE, label="ICE", edgecolor="none")
    ax.barh(y + h / 2, ev_vals, height=h, color=ACCENT, label="EV", edgecolor="none")

    for i, (e, ic) in enumerate(zip(ev_vals, ice_vals)):
        ax.text(e + 1.5, i + h / 2, f"{e:.0f}", va="center", color=INK, fontsize=9.5, weight="medium")
        ax.text(ic + 1.5, i - h / 2, f"{ic:.0f}", va="center", color=INK_2, fontsize=9.5)

    ax.set_yticks(y)
    ax.set_yticklabels(labels, color=INK, fontsize=10.5)
    ax.set_xlim(0, max(max(ev_vals), max(ice_vals)) * 1.18)
    style_grid(ax, "x")
    ax.set_xlabel("Score (0–100)")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.invert_yaxis()
    ax.legend(loc="lower right", fontsize=10)

    title_block(
        ax,
        "06 · EV vs ICE",
        "El mix EV añade carga en secuencia, expedición y energía",
        "Comparación por driver de presión operativa. La congestión de patio es estructural, no propia de la propulsión.",
    )
    footer(fig, "Fuente: diagnostic_ev_vs_non_ev")
    save(fig, "06_ev_vs_ice")


# ──────────────────────────────────────────────────────────────────────────────
def chart_07_readiness_cohort() -> None:
    """Readiness por versión (cohorte): el hueco EV salta a la vista."""
    df = pd.read_csv(DATA / "kpi_readiness_shift_version.csv")
    agg = (
        df.assign(ready=df["readiness_rate"] * df["total_vehiculos"])
        .groupby(["version_id", "tipo_propulsion"], as_index=False)
        .agg(ready=("ready", "sum"), total=("total_vehiculos", "sum"))
    )
    agg["readiness_rate"] = agg["ready"] / agg["total"]
    agg = agg.sort_values("readiness_rate", ascending=True).reset_index(drop=True)
    colors = [EV_COLOR if t == "EV" else ICE_COLOR for t in agg["tipo_propulsion"]]
    labels = agg["version_id"].str.replace("VAN_", "").str.replace("_", " ")

    fig, ax = plt.subplots(figsize=(11, 6.0))
    plt.subplots_adjust(left=0.26, right=0.95, top=0.78, bottom=0.11)

    bars = ax.barh(labels, agg["readiness_rate"], color=colors, edgecolor="none", height=0.66)
    for rect, val in zip(bars, agg["readiness_rate"]):
        ax.text(
            val + 0.012,
            rect.get_y() + rect.get_height() / 2,
            f"{val * 100:.0f}%",
            va="center",
            ha="left",
            color=INK,
            fontsize=10,
            weight="medium",
        )

    ax.axvline(0.95, color=DANGER, linewidth=1.0, linestyle=(0, (3, 3)), zorder=1)
    ax.text(0.95, len(agg) - 0.4, " objetivo 95%", color=DANGER, fontsize=9, va="center")

    style_grid(ax, "x")
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    ax.set_xlim(0, 1.08)
    ax.set_xlabel("Readiness rate a la salida")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.tick_params(axis="y", labelcolor=INK, labelsize=10)

    handles = [
        plt.Line2D([0], [0], marker="s", color="w", markerfacecolor=EV_COLOR, markersize=10, label="EV"),
        plt.Line2D([0], [0], marker="s", color="w", markerfacecolor=ICE_COLOR, markersize=10, label="ICE"),
    ]
    ax.legend(handles=handles, loc="lower right", fontsize=10)

    title_block(
        ax,
        "07 · Cohorte de readiness",
        "Las versiones EV salen listas el 28–59% de las veces; las ICE, por encima del 87%",
        "Readiness rate ponderado por volumen y versión. Ninguna versión EV se acerca al objetivo del 95%.",
    )
    footer(fig, "Fuente: kpi_readiness_shift_version")
    save(fig, "07_readiness_cohort")


# ──────────────────────────────────────────────────────────────────────────────
def chart_08_launch_trend() -> None:
    """Tendencia semanal: a más mix EV, más stress de transición de patio."""
    df = pd.read_csv(DATA / "launch_transition_features.csv", parse_dates=["week"]).sort_values("week")

    fig, ax = plt.subplots(figsize=(12, 5.8))
    plt.subplots_adjust(left=0.07, right=0.92, top=0.76, bottom=0.13)

    ax.fill_between(df["week"], 0, df["share_ev"], color=ACCENT, alpha=0.10, linewidth=0)
    ax.plot(df["week"], df["share_ev"], color=ACCENT, linewidth=2.2, label="Share EV (izq.)")
    ax.set_ylim(0, 1.0)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    ax.set_ylabel("Share EV", color=ACCENT)
    ax.tick_params(axis="y", colors=ACCENT)
    style_grid(ax, "y")

    ax2 = ax.twinx()
    ax2.plot(
        df["week"],
        df["yard_transition_stress_index"],
        color=DANGER,
        linewidth=2.0,
        label="Yard transition stress (der.)",
    )
    ax2.plot(
        df["week"], df["dispatch_stability_index"], color=POSITIVE, linewidth=2.0, label="Dispatch stability (der.)"
    )
    ax2.set_ylim(0, 100)
    ax2.set_ylabel("Índice (0–100)", color=MUTED)
    ax2.spines["top"].set_visible(False)
    ax2.grid(False)
    ax2.tick_params(length=0)

    lines = ax.get_lines() + ax2.get_lines()
    ax.legend(lines, [ln.get_label() for ln in lines], loc="upper left", fontsize=9.5, ncol=1)

    title_block(
        ax,
        "08 · Transición de lanzamiento",
        "El stress de patio sube con el mix EV; la estabilidad de salida cae en paralelo",
        "Series semanales del ramp-up (53 semanas). El stress de transición pasa de 25 a 53 cuando el EV domina el flujo.",
    )
    footer(fig, "Fuente: launch_transition_features")
    save(fig, "08_launch_transition_trend")


# ──────────────────────────────────────────────────────────────────────────────
def chart_09_dispatch_funnel() -> None:
    """Funnel: de orden programada a salida puntual y lista."""
    kpi = pd.read_csv(DATA / "kpi_operativos.csv").iloc[0]
    total = int(kpi["total_ordenes"])
    no_ready = int(kpi["vehiculos_no_ready"])
    ready = total - no_ready
    ratio_late = float(kpi["ratio_salida_retrasada"])
    on_time_ready = int(round(ready * (1 - ratio_late)))

    stages = ["Órdenes\nprogramadas", "Vehículos\ncompletados", "Listos a la\nsalida", "Salida puntual\ny lista"]
    vals = [total, total, ready, on_time_ready]
    colors = [SUBTLE, SUBTLE, WARN, DANGER]

    fig, ax = plt.subplots(figsize=(11, 6.0))
    plt.subplots_adjust(left=0.06, right=0.94, top=0.76, bottom=0.10)

    y = np.arange(len(stages))[::-1]
    maxv = max(vals)
    for yi, v, c, label in zip(y, vals, colors, stages):
        left = (maxv - v) / 2
        ax.barh(yi, v, left=left, color=c, height=0.62, edgecolor="none")
        ax.text(
            maxv / 2,
            yi,
            f"{v:,}".replace(",", " "),
            va="center",
            ha="center",
            color=SURFACE,
            fontsize=12,
            weight="semibold",
        )
        pct = v / total * 100
        ax.text(maxv + maxv * 0.02, yi, f"{pct:.0f}% del plan", va="center", ha="left", color=MUTED, fontsize=10)

    ax.set_yticks(y)
    ax.set_yticklabels(stages, color=INK, fontsize=10.5)
    ax.set_xlim(0, maxv * 1.18)
    ax.set_xticks([])
    for sp in ax.spines.values():
        sp.set_visible(False)
    ax.tick_params(length=0)

    drop_ready = (1 - ready / total) * 100
    drop_late = (ready - on_time_ready) / total * 100
    title_block(
        ax,
        "09 · Funnel de expedición",
        f"Solo el {on_time_ready / total * 100:.0f}% de las órdenes sale puntual y lista",
        f"La caída de readiness drena {drop_ready:.0f} pp del plan; el atraso de salida, otros {drop_late:.0f} pp.",
    )
    footer(fig, "Fuente: kpi_operativos · readiness y ratio de salida retrasada")
    save(fig, "09_dispatch_funnel")


# ──────────────────────────────────────────────────────────────────────────────
def chart_10_leadtime_distribution() -> None:
    """Distribución del lead time interno, EV vs ICE."""
    df = pd.read_csv(
        DATA / "mart_vehicle_day.csv", usecols=["tipo_propulsion", "total_internal_lead_time_min"]
    ).dropna()
    df = df[df["total_internal_lead_time_min"] > 0]
    ev = df.loc[df["tipo_propulsion"] == "EV", "total_internal_lead_time_min"] / 60
    ice = df.loc[df["tipo_propulsion"] == "ICE", "total_internal_lead_time_min"] / 60

    bins = np.linspace(0, 70, 36)
    fig, ax = plt.subplots(figsize=(11, 5.6))
    plt.subplots_adjust(left=0.07, right=0.96, top=0.76, bottom=0.13)

    ax.hist(ice, bins=bins, color=ICE_COLOR, alpha=0.55, label=f"ICE (mediana {ice.median():.0f} h)", density=True)
    ax.hist(ev, bins=bins, color=ACCENT, alpha=0.55, label=f"EV (mediana {ev.median():.0f} h)", density=True)
    ax.axvline(ice.median(), color=INK_2, linewidth=1.2, linestyle=(0, (3, 3)))
    ax.axvline(ev.median(), color=ACCENT, linewidth=1.4, linestyle=(0, (3, 3)))

    style_grid(ax, "y")
    ax.set_xlabel("Lead time interno total (horas)")
    ax.set_ylabel("Densidad")
    ax.set_yticklabels([])
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.legend(loc="upper right", fontsize=10)

    title_block(
        ax,
        "10 · Distribución de lead time",
        f"EV e ICE comparten perfil de lead time interno ({ev.median():.0f} h vs {ice.median():.0f} h)",
        "Tiempo interno de extremo a extremo. El coste EV no está en el tiempo de ciclo, sino en readiness y salida.",
    )
    footer(fig, "Fuente: mart_vehicle_day")
    save(fig, "10_leadtime_distribution")


# ──────────────────────────────────────────────────────────────────────────────
def chart_11_delay_pareto() -> None:
    """Concentración: qué versiones acumulan el retraso de salida."""
    df = pd.read_csv(
        DATA / "mart_vehicle_day.csv", usecols=["version_id", "tipo_propulsion", "dispatch_delay_min"]
    ).dropna()
    g = (
        df.groupby(["version_id", "tipo_propulsion"], as_index=False)["dispatch_delay_min"]
        .sum()
        .sort_values("dispatch_delay_min", ascending=False)
        .reset_index(drop=True)
    )
    total = g["dispatch_delay_min"].sum()
    g["cum_share"] = g["dispatch_delay_min"].cumsum() / total
    g["share"] = g["dispatch_delay_min"] / total
    colors = [EV_COLOR if t == "EV" else ICE_COLOR for t in g["tipo_propulsion"]]
    labels = g["version_id"].str.replace("VAN_", "").str.replace("_", " ")

    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    plt.subplots_adjust(left=0.08, right=0.92, top=0.76, bottom=0.16)

    x = np.arange(len(g))
    ax.bar(x, g["share"], color=colors, edgecolor="none", width=0.66)
    for xi, s in zip(x, g["share"]):
        ax.text(xi, s + 0.008, f"{s * 100:.0f}%", ha="center", va="bottom", color=INK, fontsize=9.5)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=9, color=INK)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    ax.set_ylabel("Cuota del retraso total")
    ax.set_ylim(0, max(g["share"]) * 1.25)
    style_grid(ax, "y")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)

    ax2 = ax.twinx()
    ax2.plot(x, g["cum_share"], color=INK, linewidth=2.0, marker="o", markersize=4)
    ax2.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    ax2.set_ylim(0, 1.05)
    ax2.set_ylabel("Acumulado", color=MUTED)
    ax2.grid(False)
    ax2.spines["top"].set_visible(False)
    ax2.tick_params(length=0)
    ax2.axhline(0.8, color=MUTED, linewidth=0.8, linestyle=(0, (3, 3)))

    ev_share = g.loc[g["tipo_propulsion"] == "EV", "share"].sum()
    title_block(
        ax,
        "11 · Concentración del retraso",
        f"Las cuatro versiones EV acumulan el {ev_share * 100:.0f}% del retraso total de salida",
        "Pareto por versión. La curva acumulada confirma que el retraso no está repartido: es un problema EV.",
    )
    footer(fig, "Fuente: mart_vehicle_day · suma de dispatch_delay_min")
    save(fig, "11_delay_pareto")


# ──────────────────────────────────────────────────────────────────────────────
def chart_12_market_geography() -> None:
    """Geografía: readiness y volumen por mercado de destino."""
    df = pd.read_csv(
        DATA / "mart_vehicle_day.csv", usecols=["mercado_destino", "readiness_final_flag", "tipo_propulsion"]
    )
    df["ready"] = df["readiness_final_flag"].astype(str).str.lower().isin(["true", "1"])
    g = df.groupby("mercado_destino", as_index=False).agg(vol=("ready", "size"), ready=("ready", "mean"))
    g = g.sort_values("vol", ascending=True).reset_index(drop=True)

    fig, ax = plt.subplots(figsize=(11, 5.8))
    plt.subplots_adjust(left=0.16, right=0.95, top=0.78, bottom=0.12)

    norm = (g["ready"] - g["ready"].min()) / (g["ready"].max() - g["ready"].min() + 1e-9)
    colors = [plt.cm.RdYlGn(0.15 + 0.7 * n) for n in norm]
    bars = ax.barh(g["mercado_destino"], g["vol"], color=colors, edgecolor="none", height=0.66)
    for rect, vol, r in zip(bars, g["vol"], g["ready"]):
        ax.text(
            vol + max(g["vol"]) * 0.01,
            rect.get_y() + rect.get_height() / 2,
            f"{vol:,} und · {r * 100:.0f}% listo".replace(",", " "),
            va="center",
            ha="left",
            color=INK,
            fontsize=9.5,
        )

    style_grid(ax, "x")
    ax.set_xlim(0, max(g["vol"]) * 1.30)
    ax.set_xlabel("Volumen despachado (vehículos)")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.tick_params(axis="y", labelcolor=INK, labelsize=10.5)

    title_block(
        ax,
        "12 · Mercados de destino",
        "Iberia concentra un tercio del volumen; el readiness es uniforme entre destinos",
        "Volumen por mercado y readiness final (color verde = más listo). El riesgo es operativo, no geográfico.",
    )
    footer(fig, "Fuente: mart_vehicle_day")
    save(fig, "12_market_geography")


# ──────────────────────────────────────────────────────────────────────────────
def chart_13_yard_zones() -> None:
    """Geografía interna: dónde se acumula el dwell en el patio."""
    df = pd.read_csv(DATA / "vw_yard_congestion.csv", usecols=["zona_patio", "p95_dwell_time_min", "blocking_rate"])
    g = df.groupby("zona_patio", as_index=False).agg(
        p95=("p95_dwell_time_min", "mean"), block=("blocking_rate", "mean")
    )
    g["p95_h"] = g["p95"] / 60
    g = g.sort_values("p95_h", ascending=True).reset_index(drop=True)
    colors = [DANGER if z == "PRE_SALIDA" else (WARN if z == "BUFFER_CARGA" else NEUTRAL_BAR) for z in g["zona_patio"]]

    fig, ax = plt.subplots(figsize=(11, 5.6))
    plt.subplots_adjust(left=0.18, right=0.95, top=0.78, bottom=0.12)

    bars = ax.barh(g["zona_patio"], g["p95_h"], color=colors, edgecolor="none", height=0.62)
    for rect, h, b in zip(bars, g["p95_h"], g["block"]):
        ax.text(
            h + max(g["p95_h"]) * 0.01,
            rect.get_y() + rect.get_height() / 2,
            f"{h:.1f} h · bloqueo {b * 100:.0f}%",
            va="center",
            ha="left",
            color=INK,
            fontsize=9.5,
        )

    style_grid(ax, "x")
    ax.set_xlim(0, max(g["p95_h"]) * 1.28)
    ax.set_xlabel("Dwell p95 por zona de patio (horas)")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.tick_params(axis="y", labelcolor=INK, labelsize=10.5)

    title_block(
        ax,
        "13 · Congestión por zona de patio",
        "PRE_SALIDA es el verdadero cuello: 43 h de dwell p95 y bloqueo casi total",
        "Tiempo de permanencia p95 por zona. La zona de pre-salida domina; el resto del patio fluye.",
    )
    footer(fig, "Fuente: vw_yard_congestion")
    save(fig, "13_yard_zone_congestion")


# ──────────────────────────────────────────────────────────────────────────────
def chart_14_scenario_tradeoff() -> None:
    """Trade-off: throughput vs vehículos retrasados, por escenario."""
    df = pd.read_csv(DATA / "scenario_decision_comparison.csv").reset_index(drop=True)
    df["label"] = df["escenario"].str.replace(r"^\d+_", "", regex=True).str.replace("_", " ")
    best = df["decision_score"].idxmax()
    base_mask = df["escenario"].str.startswith("1_")

    fig, ax = plt.subplots(figsize=(11, 6.2))
    plt.subplots_adjust(left=0.09, right=0.95, top=0.76, bottom=0.13)

    x = df["vehiculos_retrasados"] * 100
    yv = df["throughput"]
    colors = []
    for i in df.index:
        if i == best:
            colors.append(ACCENT)
        elif base_mask[i]:
            colors.append(WARN)
        else:
            colors.append(NEUTRAL_BAR)
    sizes = (df["decision_score"] - df["decision_score"].min() + 1) * 60
    ax.scatter(x, yv, s=sizes, c=colors, edgecolors=SURFACE, linewidths=2.5, alpha=0.92, zorder=3)

    for i, row in df.iterrows():
        lab = row["label"]
        if i == best:
            lab = "★ " + lab
        ax.annotate(
            lab.capitalize(),
            (x[i], yv[i]),
            xytext=(7, 6),
            textcoords="offset points",
            fontsize=8.6,
            color=INK if i == best else MUTED,
        )

    style_grid(ax, "both")
    ax.set_xlabel("Vehículos retrasados (% del flujo)")
    ax.set_ylabel("Throughput (índice de escenario)")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.invert_xaxis()

    title_block(
        ax,
        "14 · Trade-off de escenarios",
        "La combinación correctiva es la frontera: más throughput con menos retraso",
        "Cada punto es un escenario. Arriba-izquierda es mejor (eje X invertido). El tamaño es el decision score.",
    )
    footer(fig, "Fuente: scenario_decision_comparison")
    save(fig, "14_scenario_tradeoff")


# ──────────────────────────────────────────────────────────────────────────────
def chart_15_lever_ranking() -> None:
    """Ranking de palancas por impacto esperado."""
    df = pd.read_csv(DATA / "scenario_lever_ranking.csv").sort_values("impacto_esperado", ascending=True)
    labels = df["palanca"].str.replace("_", " ").str.capitalize()
    top = df["impacto_esperado"].max()
    colors = [ACCENT if v == top else NEUTRAL_BAR for v in df["impacto_esperado"]]

    fig, ax = plt.subplots(figsize=(11, 5.0))
    plt.subplots_adjust(left=0.26, right=0.94, top=0.78, bottom=0.12)

    bars = ax.barh(labels, df["impacto_esperado"], color=colors, edgecolor="none", height=0.6)
    for rect, v in zip(bars, df["impacto_esperado"]):
        ax.text(
            v + 0.005,
            rect.get_y() + rect.get_height() / 2,
            f"{v:.2f}",
            va="center",
            ha="left",
            color=INK,
            fontsize=10.5,
            weight="medium",
        )

    style_grid(ax, "x")
    ax.set_xlim(0, df["impacto_esperado"].max() * 1.18)
    ax.set_xlabel("Impacto esperado (proxy paramétrico)")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.tick_params(axis="y", labelcolor=INK, labelsize=10.5)

    title_block(
        ax,
        "15 · Ranking de palancas",
        "La capacidad de carga es la palanca de mayor retorno esperado",
        "Impacto relativo de cada palanca operativa. Secuenciación y gestión de patio la siguen de cerca.",
    )
    footer(fig, "Fuente: scenario_lever_ranking")
    save(fig, "15_lever_ranking")


# ──────────────────────────────────────────────────────────────────────────────
def chart_16_montecarlo_robustness() -> None:
    """Robustez: estabilidad del área top-1 bajo remuestreo de pesos."""
    draws = pd.read_csv(DATA / "scoring_montecarlo_draws.csv")
    top1 = draws["top3_areas"].str.split(",").str[0].str.strip()
    counts = top1.value_counts(normalize=True).sort_values(ascending=True)
    colors = [ACCENT if i == counts.index[-1] else NEUTRAL_BAR for i in counts.index]

    fig, ax = plt.subplots(figsize=(11, 4.8))
    plt.subplots_adjust(left=0.20, right=0.94, top=0.78, bottom=0.14)

    bars = ax.barh(counts.index, counts.values, color=colors, edgecolor="none", height=0.5)
    for rect, v in zip(bars, counts.values):
        ax.text(
            v + 0.01,
            rect.get_y() + rect.get_height() / 2,
            f"{v * 100:.0f}%",
            va="center",
            ha="left",
            color=INK,
            fontsize=10.5,
            weight="medium",
        )

    style_grid(ax, "x")
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    ax.set_xlim(0, 1.0)
    ax.set_xlabel("Frecuencia como área #1 en 300 sorteos de pesos")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.tick_params(axis="y", labelcolor=INK, labelsize=11)

    title_block(
        ax,
        "16 · Robustez del ranking",
        "LOGÍSTICA encabeza el ranking en el 77% de los remuestreos Monte Carlo",
        "300 sorteos Dirichlet sobre los pesos del score. La conclusión no depende de una única ponderación.",
    )
    footer(fig, "Fuente: scoring_montecarlo_draws (n=300)")
    save(fig, "16_montecarlo_robustness")


# ──────────────────────────────────────────────────────────────────────────────
def chart_17_driver_correlation() -> None:
    """Correlación entre drivers operativos por área-turno."""
    df = pd.read_csv(DATA / "mart_area_shift.csv")
    cols = {
        "congestion_index": "Congestión",
        "avg_wait_time": "Espera media",
        "queue_pressure_score": "Presión de cola",
        "bottleneck_density": "Densidad cuello",
        "operational_stress_score": "Stress operativo",
        "area_throughput_loss_proxy": "Pérdida throughput",
    }
    sub = df[list(cols)].rename(columns=cols)
    corr = sub.corr(numeric_only=True)

    fig, ax = plt.subplots(figsize=(8.8, 7.6))
    plt.subplots_adjust(left=0.26, right=0.98, top=0.74, bottom=0.20)

    im = ax.imshow(corr.values, cmap="RdBu_r", vmin=-1, vmax=1)
    ax.set_xticks(range(len(corr)))
    ax.set_yticks(range(len(corr)))
    ax.set_xticklabels(corr.columns, rotation=35, ha="right", fontsize=9, color=INK)
    ax.set_yticklabels(corr.index, fontsize=9, color=INK)
    for i in range(len(corr)):
        for j in range(len(corr)):
            v = corr.values[i, j]
            ax.text(j, i, f"{v:.2f}", ha="center", va="center", color=SURFACE if abs(v) > 0.55 else INK, fontsize=8.6)
    ax.tick_params(length=0)
    for sp in ax.spines.values():
        sp.set_visible(False)
    cb = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cb.outline.set_visible(False)
    cb.ax.tick_params(length=0, labelsize=8)

    title_block(
        ax,
        "17 · Correlación de drivers",
        "Congestión, espera y stress se mueven juntos; la pérdida de throughput es casi independiente",
        "Correlación de Pearson entre drivers a nivel área-turno. Confirma dos familias de riesgo distintas.",
    )
    footer(fig, "Fuente: mart_area_shift")
    save(fig, "17_driver_correlation")


# ──────────────────────────────────────────────────────────────────────────────
def chart_18_readiness_heatmap() -> None:
    """Varianza: readiness por turno × versión."""
    df = pd.read_csv(DATA / "kpi_readiness_shift_version.csv")
    piv = df.pivot_table(index="version_id", columns="turno", values="readiness_rate", aggfunc="mean")
    order = df.groupby("version_id")["readiness_rate"].mean().sort_values(ascending=False).index
    piv = piv.loc[order]
    labels = [v.replace("VAN_", "").replace("_", " ") for v in piv.index]

    fig, ax = plt.subplots(figsize=(8.6, 7.2))
    plt.subplots_adjust(left=0.28, right=0.98, top=0.74, bottom=0.12)

    ax.imshow(piv.values, cmap="RdYlGn", vmin=0.25, vmax=1.0, aspect="auto")
    ax.set_xticks(range(len(piv.columns)))
    ax.set_xticklabels([f"Turno {c}" for c in piv.columns], fontsize=10, color=INK)
    ax.set_yticks(range(len(piv.index)))
    ax.set_yticklabels(labels, fontsize=9.5, color=INK)
    for i in range(piv.shape[0]):
        for j in range(piv.shape[1]):
            v = piv.values[i, j]
            ax.text(j, i, f"{v * 100:.0f}%", ha="center", va="center", color=INK, fontsize=9)
    ax.tick_params(length=0)
    for sp in ax.spines.values():
        sp.set_visible(False)

    title_block(
        ax,
        "18 · Readiness por turno × versión",
        "El hueco de readiness es de versión, no de turno: los tres turnos se comportan igual",
        "Readiness rate por versión y turno. La variación entre turnos es marginal frente al salto EV→ICE.",
    )
    footer(fig, "Fuente: kpi_readiness_shift_version")
    save(fig, "18_readiness_heatmap")


# ──────────────────────────────────────────────────────────────────────────────
def chart_19_before_after() -> None:
    """Antes vs después: base ramp-up vs combinación correctiva."""
    df = pd.read_csv(DATA / "scenario_decision_comparison.csv").set_index("escenario")
    base = df.loc["1_ramp_up_ev_base"]
    best = df.loc["8_combinacion_medidas_correctivas"]

    metrics = [
        ("throughput", "Throughput", False),
        ("estabilidad_operativa", "Estabilidad operativa", False),
        ("tiempo_total_interno", "Tiempo interno total", True),
        ("espera_carga", "Espera de carga (min)", True),
        ("vehiculos_retrasados", "Vehículos retrasados (%)", True),
    ]

    fig, ax = plt.subplots(figsize=(11, 6.0))
    plt.subplots_adjust(left=0.26, right=0.95, top=0.76, bottom=0.10)

    y = np.arange(len(metrics))[::-1]
    for yi, (col, label, lower_better) in zip(y, metrics):
        b = base[col]
        m = best[col]
        improved = (m < b) if lower_better else (m > b)
        col_pt = POSITIVE if improved else DANGER
        ax.plot([0, 1], [yi, yi], color=LINE, linewidth=2.0, zorder=1)
        ax.scatter([0], [yi], s=120, color=NEUTRAL_BAR, zorder=3, edgecolors=SURFACE, linewidths=2)
        ax.scatter([1], [yi], s=160, color=col_pt, zorder=3, edgecolors=SURFACE, linewidths=2)
        if col == "vehiculos_retrasados":
            bt, mt = f"{b * 100:.0f}%", f"{m * 100:.0f}%"
        else:
            bt, mt = f"{b:.0f}", f"{m:.0f}"
        ax.text(-0.03, yi, bt, ha="right", va="center", color=MUTED, fontsize=10)
        ax.text(1.03, yi, mt, ha="left", va="center", color=col_pt, fontsize=10.5, weight="medium")
        delta = (m - b) / b * 100
        ax.text(0.5, yi + 0.18, f"{delta:+.0f}%", ha="center", va="bottom", color=col_pt, fontsize=9)

    ax.set_yticks(y)
    ax.set_yticklabels([m[1] for m in metrics], fontsize=10.5, color=INK)
    ax.set_xlim(-0.2, 1.2)
    ax.set_xticks([0, 1])
    ax.set_xticklabels(["Ramp-up base", "Combinación correctiva"], fontsize=10, color=INK)
    ax.tick_params(length=0)
    for sp in ax.spines.values():
        sp.set_visible(False)
    ax.grid(False)

    title_block(
        ax,
        "19 · Antes vs después",
        "El paquete correctivo mejora throughput, estabilidad y tiempo interno a la vez",
        "Escenario base de ramp-up frente a la combinación de medidas. Verde = mejora, rojo = deterioro.",
    )
    footer(fig, "Fuente: scenario_decision_comparison")
    save(fig, "19_before_after")


# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    print("Generando chart pack…")
    chart_01_throughput()
    chart_02_share_ev_weekly()
    chart_03_priority_ranking()
    chart_04_risk_matrix()
    chart_05_scenario_decision()
    chart_06_ev_vs_ice()
    chart_07_readiness_cohort()
    chart_08_launch_trend()
    chart_09_dispatch_funnel()
    chart_10_leadtime_distribution()
    chart_11_delay_pareto()
    chart_12_market_geography()
    chart_13_yard_zones()
    chart_14_scenario_tradeoff()
    chart_15_lever_ranking()
    chart_16_montecarlo_robustness()
    chart_17_driver_correlation()
    chart_18_readiness_heatmap()
    chart_19_before_after()
    print(f"OK — 19 PNGs en {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
