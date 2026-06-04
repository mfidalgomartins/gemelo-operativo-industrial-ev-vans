"""Genera el pack de gráficos de portfolio (PNG) a partir de los datos procesados.

Salida: outputs/graphs/*.png

Filosofía: 6 gráficos seleccionados por su valor analítico para review ejecutivo
y para README/LinkedIn. Estilo sobrio inspirado en Linear/Stripe — fondo claro,
una sola tipografía sans, paleta neutra con un acento azul y un rojo semántico
solo cuando el dato lo exige.
"""
from __future__ import annotations

from pathlib import Path

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
DANGER = "#b91c1c"
WARN = "#a16207"
POSITIVE = "#15803d"
NEUTRAL_BAR = "#a8a29e"

TIER_COLOR = {
    "estabilizar en la siguiente ola": DANGER,
    "monitorizar muy de cerca": WARN,
    "mantener bajo observación": POSITIVE,
}

plt.rcParams.update({
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
})


def title_block(ax, eyebrow: str, title: str, subtitle: str | None = None) -> None:
    """Bloque tipográfico Linear-style anclado al figure."""
    fig = ax.figure
    fig.text(0.045, 0.955, eyebrow.upper(),
             color=SUBTLE, fontsize=9.5, weight="regular")
    fig.text(0.045, 0.905, title, color=INK, fontsize=17.5, weight="semibold")
    if subtitle:
        fig.text(0.045, 0.865, subtitle, color=MUTED, fontsize=11)


def footer(fig, source: str) -> None:
    fig.text(0.045, 0.022, source, color=SUBTLE, fontsize=8.5)
    fig.text(0.955, 0.022, "Gemelo Operativo EV · Synthetic factory data", color=SUBTLE, fontsize=8.5, ha="right")


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
    """Throughput diario: la línea ejecutiva número uno."""
    df = pd.read_csv(DATA / "vw_vehicle_flow_timeline.csv", parse_dates=["fecha_real"])
    daily = (
        df.groupby(df["fecha_real"].dt.date)
        .agg(real=("vehiculo_id", "nunique"))
        .reset_index()
        .rename(columns={"fecha_real": "fecha"})
    )
    daily["fecha"] = pd.to_datetime(daily["fecha"])
    daily = daily.sort_values("fecha").iloc[:-2]  # drop incomplete tail
    daily["real_7d"] = daily["real"].rolling(7, min_periods=1).mean()
    avg = daily["real"].mean()

    fig, ax = plt.subplots(figsize=(12, 5.6))
    plt.subplots_adjust(left=0.07, right=0.96, top=0.76, bottom=0.13)

    ax.scatter(daily["fecha"], daily["real"], s=8, color="#d6d3d1", alpha=0.9, zorder=1, label="Diario")
    ax.plot(daily["fecha"], daily["real_7d"], color=INK, linewidth=2.4, zorder=3, label="Media móvil 7 días")

    ax.axhline(avg, color=MUTED, linewidth=0.9, linestyle=(0, (3, 3)), zorder=2)
    ax.text(daily["fecha"].min(), avg, f"  media período: {avg:.0f}",
            va="bottom", ha="left", color=MUTED, fontsize=9.5)

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
        .agg(total=("vehiculo_id", "nunique"),
             ev=("tipo_propulsion", lambda x: (x == "EV").sum()))
        .reset_index()
    )
    weekly["share_ev"] = weekly["ev"] / weekly["total"].clip(lower=1)

    fig, ax = plt.subplots(figsize=(11, 5.0))
    plt.subplots_adjust(left=0.06, right=0.97, top=0.78, bottom=0.12)

    ax.fill_between(weekly["week"], 0, weekly["share_ev"], color=ACCENT, alpha=0.10, linewidth=0)
    ax.plot(weekly["week"], weekly["share_ev"], color=ACCENT, linewidth=2.2)

    # Highlight first and last value
    first = weekly.iloc[0]
    last = weekly.iloc[-1]
    ax.scatter([first["week"], last["week"]], [first["share_ev"], last["share_ev"]],
               color=ACCENT, s=42, zorder=4, edgecolors=SURFACE, linewidths=2)
    ax.annotate(f"{first['share_ev']*100:.0f}%", (first["week"], first["share_ev"]),
                xytext=(10, -2), textcoords="offset points",
                color=ACCENT, fontsize=11, weight="medium", va="center")
    ax.annotate(f"{last['share_ev']*100:.0f}%", (last["week"], last["share_ev"]),
                xytext=(-10, -2), textcoords="offset points",
                color=ACCENT, fontsize=11, weight="medium", va="center", ha="right")

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

    bars = ax.barh(df["area"], df["operational_priority_index"],
                   color=colors, edgecolor="none", height=0.62)
    for rect, val in zip(bars, df["operational_priority_index"]):
        ax.text(val + 1.2, rect.get_y() + rect.get_height() / 2, f"{val:.1f}",
                va="center", ha="left", color=INK, fontsize=10.5, weight="medium")

    style_grid(ax, "x")
    ax.set_xlim(0, df["operational_priority_index"].max() * 1.18)
    ax.set_xlabel("Operational Priority Index (OPI)")
    ax.set_ylabel("")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.tick_params(axis="y", labelcolor=INK, labelsize=10.5, pad=8)

    # Legend chips
    handles = [
        plt.Line2D([0], [0], marker="s", color="w", markerfacecolor=DANGER, markersize=10,
                   label="Estabilizar (T0)"),
        plt.Line2D([0], [0], marker="s", color="w", markerfacecolor=WARN, markersize=10,
                   label="Monitorizar (T1)"),
        plt.Line2D([0], [0], marker="s", color="w", markerfacecolor=POSITIVE, markersize=10,
                   label="Bajo observación (T2)"),
    ]
    ax.legend(handles=handles, loc="lower right", fontsize=9.5)

    title_block(
        ax,
        "03 · Priorización operativa",
        "PATIO y LOGÍSTICA concentran la presión esta semana",
        "OPI por área: combina pérdida de throughput, riesgo de salida y stress operativo.",
    )
    footer(fig, "Fuente: operational_prioritization_table")
    save(fig, "03_priority_ranking_opi")


# ──────────────────────────────────────────────────────────────────────────────
def chart_04_risk_matrix() -> None:
    """Matriz 2x2: throughput loss × dispatch risk, por área.

    Usa los scores precomputados en operational_prioritization_table porque
    tienen variancia entre áreas (los promedios brutos de mart_area_shift
    son casi planos al normalizar por shift).
    """
    df = pd.read_csv(DATA / "operational_prioritization_table.csv")
    df = df[["area", "throughput_loss_score", "dispatch_risk_score",
             "operational_stress_score", "area_priority_tier"]].copy()
    colors = df["area_priority_tier"].map(TIER_COLOR).fillna(NEUTRAL_BAR).tolist()

    fig, ax = plt.subplots(figsize=(11, 6.4))
    plt.subplots_adjust(left=0.09, right=0.95, top=0.76, bottom=0.13)

    sizes = (df["operational_stress_score"] / df["operational_stress_score"].max() * 1400 + 280)
    ax.scatter(df["throughput_loss_score"], df["dispatch_risk_score"], s=sizes,
               c=colors, edgecolors=SURFACE, linewidths=3.0, alpha=0.92, zorder=3)

    # Smart label placement: offset based on local density
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
        ax.annotate(row["area"], (row["throughput_loss_score"], row["dispatch_risk_score"]),
                    xytext=(dx, dy), textcoords="offset points",
                    fontsize=11, color=INK, weight="medium", ha=ha)

    # Quadrant lines at 50 (the score midpoint)
    ax.axvline(50, color=LINE, linewidth=0.8, zorder=1)
    ax.axhline(50, color=LINE, linewidth=0.8, zorder=1)

    # Quadrant labels (corners)
    ax.text(0.99, 0.97, "Intervenir ahora", transform=ax.transAxes,
            color=DANGER, fontsize=10, weight="medium", ha="right", va="top", alpha=0.85)
    ax.text(0.01, 0.97, "Riesgo de salida", transform=ax.transAxes,
            color=MUTED, fontsize=10, ha="left", va="top", alpha=0.7)
    ax.text(0.99, 0.03, "Pérdida de flujo", transform=ax.transAxes,
            color=MUTED, fontsize=10, ha="right", va="bottom", alpha=0.7)
    ax.text(0.01, 0.03, "Sostener", transform=ax.transAxes,
            color=POSITIVE, fontsize=10, weight="medium", ha="left", va="bottom", alpha=0.85)

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
        "Tamaño del punto: operational stress. PATIO y LOGÍSTICA están en el cuadrante de intervención inmediata.",
    )
    footer(fig, "Fuente: operational_prioritization_table")
    save(fig, "04_risk_matrix")


# ──────────────────────────────────────────────────────────────────────────────
def chart_05_scenario_decision() -> None:
    df = pd.read_csv(DATA / "scenario_decision_comparison.csv")
    df = df.sort_values("decision_score", ascending=True).reset_index(drop=True)

    # Clean scenario labels
    df["label"] = (
        df["escenario"]
        .str.replace(r"^\d+_", "", regex=True)
        .str.replace("_", " ")
        .str.capitalize()
    )
    top_score = df["decision_score"].max()
    colors = [ACCENT if v == top_score else NEUTRAL_BAR for v in df["decision_score"]]

    fig, ax = plt.subplots(figsize=(11, 5.6))
    plt.subplots_adjust(left=0.34, right=0.96, top=0.78, bottom=0.10)

    bars = ax.barh(df["label"], df["decision_score"], color=colors, edgecolor="none", height=0.62)
    for rect, val in zip(bars, df["decision_score"]):
        ax.text(val + 0.4, rect.get_y() + rect.get_height() / 2,
                f"{val:.1f}", va="center", ha="left",
                color=INK, fontsize=10, weight="medium")

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

    ax.barh(y - h/2, ice_vals, height=h, color=SUBTLE, label="ICE", edgecolor="none")
    ax.barh(y + h/2, ev_vals,  height=h, color=ACCENT, label="EV",  edgecolor="none")

    for i, (e, ic) in enumerate(zip(ev_vals, ice_vals)):
        ax.text(e + 1.5, i + h/2, f"{e:.0f}", va="center", color=INK, fontsize=9.5, weight="medium")
        ax.text(ic + 1.5, i - h/2, f"{ic:.0f}", va="center", color=INK_2, fontsize=9.5)

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
        "El mix EV añade carga en secuencia, expedición y carga energética",
        "Comparación por driver de presión operativa. Congestión de patio es propia de la operación, no de la propulsión.",
    )
    footer(fig, "Fuente: diagnostic_ev_vs_non_ev")
    save(fig, "06_ev_vs_ice")


# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    print("Generando chart pack…")
    chart_01_throughput()
    chart_02_share_ev_weekly()
    chart_03_priority_ranking()
    chart_04_risk_matrix()
    chart_05_scenario_decision()
    chart_06_ev_vs_ice()
    print(f"OK — 6 PNGs en {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
