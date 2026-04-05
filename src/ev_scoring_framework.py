from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT


EV_DIR = DATA_PROCESSED_DIR / "ev_factory"


@dataclass
class ScoringResult:
    areas: int
    top_area: str


def _read(name: str) -> pd.DataFrame:
    path = EV_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"No existe tabla para scoring: {path}")
    return pd.read_csv(path)


def _normalize_100(series: pd.Series, upper: float) -> pd.Series:
    return np.clip(series / upper, 0, 1) * 100


def _normalize_percentile(series: pd.Series, q_hi: float = 0.95) -> pd.Series:
    upper = float(series.quantile(q_hi)) if len(series) else 0.0
    if upper <= 0:
        upper = 1.0
    return np.clip(series / upper, 0, 1) * 100


def _map_action(driver: str) -> str:
    mapping = {
        "charging_risk_score": "ampliar infraestructura de carga",
        "yard_risk_score": "revisar política de buffer en patio",
        "dispatch_risk_score": "priorizar expedición selectiva",
        "throughput_loss_score": "ajustar turnos o recursos",
        "readiness_score": "cambiar lógica de secuenciación",
        "launch_transition_risk_score": "redistribuir capacidad entre zonas",
    }
    return mapping.get(driver, "mantener disciplina operativa")


def _map_tier(score: float) -> str:
    if score >= 80:
        return "intervenir ahora"
    if score >= 65:
        return "estabilizar en la siguiente ola"
    if score >= 50:
        return "monitorizar muy de cerca"
    if score >= 35:
        return "mantener bajo observación"
    return "sin prioridad inmediata"


def run_ev_scoring_framework() -> ScoringResult:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    area = _read("area_shift_features")
    diagnostic = _read("diagnostic_area_scores")
    launch = _read("launch_transition_features")

    base = area.groupby("area", as_index=False).agg(
        throughput_gap=("throughput_gap", "mean"),
        congestion_index=("congestion_index", "mean"),
        avg_wait_time=("avg_wait_time", "mean"),
        queue_pressure_score=("queue_pressure_score", "mean"),
        slot_utilization=("slot_utilization", "mean"),
        bottleneck_density=("bottleneck_density", "mean"),
        dispatch_risk_density=("dispatch_risk_density", "mean"),
        operational_stress_score=("operational_stress_score", "mean"),
    )

    # Normalizaciones robustas a escala del dataset para evitar colapso de scores.
    n_wait = _normalize_percentile(base["avg_wait_time"].clip(lower=0), 0.95)
    n_congestion = _normalize_percentile(base["congestion_index"].clip(lower=0), 0.95)
    n_slot = _normalize_percentile(base["slot_utilization"].clip(lower=0), 0.95)
    n_dispatch = _normalize_percentile(base["dispatch_risk_density"].clip(lower=0), 0.95)
    n_throughput_loss = _normalize_percentile(base["throughput_gap"].abs(), 0.95)
    n_bneck = _normalize_percentile(base["bottleneck_density"].clip(lower=0), 0.95)
    n_stress = _normalize_percentile(base["operational_stress_score"].clip(lower=0), 0.95)
    n_queue = _normalize_percentile(base["queue_pressure_score"].clip(lower=0), 0.95)

    # Scoring por área
    base["readiness_score"] = 100 - np.clip(
        0.35 * n_wait
        + 0.35 * n_dispatch
        + 0.20 * n_queue
        + 0.10 * n_bneck,
        0,
        100,
    )
    base["yard_risk_score"] = np.clip(
        0.45 * n_congestion
        + 0.30 * n_wait
        + 0.15 * n_stress
        + 0.10 * n_bneck,
        0,
        100,
    )
    base["charging_risk_score"] = np.clip(
        0.40 * n_slot
        + 0.30 * n_wait
        + 0.20 * n_queue
        + 0.10 * n_stress,
        0,
        100,
    )
    base["dispatch_risk_score"] = np.clip(
        0.45 * n_dispatch
        + 0.25 * n_wait
        + 0.20 * n_bneck
        + 0.10 * n_stress,
        0,
        100,
    )
    base["throughput_loss_score"] = np.clip(
        0.75 * n_throughput_loss + 0.25 * n_stress,
        0,
        100,
    )

    launch_risk = np.clip(
        0.50 * (launch["share_ev"].mean() * 100)
        + 0.25 * launch["charging_capacity_gap"].mean()
        + 0.25 * launch["yard_transition_stress_index"].mean(),
        0,
        100,
    )
    base["launch_transition_risk_score"] = np.clip(
        launch_risk + 0.20 * n_stress,
        0,
        100,
    )

    # Índice compuesto de prioridad
    weights = {
        "yard_risk_score": 0.20,
        "charging_risk_score": 0.20,
        "dispatch_risk_score": 0.18,
        "throughput_loss_score": 0.18,
        "launch_transition_risk_score": 0.16,
        "readiness_score": 0.08,  # invertido en cálculo
    }

    base["operational_priority_index"] = (
        weights["yard_risk_score"] * base["yard_risk_score"]
        + weights["charging_risk_score"] * base["charging_risk_score"]
        + weights["dispatch_risk_score"] * base["dispatch_risk_score"]
        + weights["throughput_loss_score"] * base["throughput_loss_score"]
        + weights["launch_transition_risk_score"] * base["launch_transition_risk_score"]
        + weights["readiness_score"] * (100 - base["readiness_score"])
    )

    def _weighted_score(df: pd.DataFrame, w: Dict[str, float]) -> pd.Series:
        return (
            w["yard_risk_score"] * df["yard_risk_score"]
            + w["charging_risk_score"] * df["charging_risk_score"]
            + w["dispatch_risk_score"] * df["dispatch_risk_score"]
            + w["throughput_loss_score"] * df["throughput_loss_score"]
            + w["launch_transition_risk_score"] * df["launch_transition_risk_score"]
            + w["readiness_score"] * (100 - df["readiness_score"])
        )

    risk_columns = [
        "yard_risk_score",
        "charging_risk_score",
        "dispatch_risk_score",
        "throughput_loss_score",
        "launch_transition_risk_score",
        "readiness_score",
    ]

    base["main_risk_driver"] = base[risk_columns].idxmax(axis=1)
    base["recommended_action"] = base["main_risk_driver"].map(_map_action)
    base["area_priority_tier"] = base["operational_priority_index"].apply(_map_tier)

    # Añade lectura diagnóstica (modo de bottleneck driver)
    diag_driver = (
        diagnostic.groupby("area", as_index=False)
        .agg(main_bottleneck_driver=("main_bottleneck_driver", lambda s: s.mode().iat[0] if not s.mode().empty else "N/A"))
    )
    out = base.merge(diag_driver, on="area", how="left")
    out = out.sort_values("operational_priority_index", ascending=False)

    out.to_csv(EV_DIR / "operational_prioritization_table.csv", index=False)

    # Sensibilidad: variación +/-20% de pesos de cada driver
    sensitivity_rows: List[Dict[str, object]] = []
    for col in [
        "yard_risk_score",
        "charging_risk_score",
        "dispatch_risk_score",
        "throughput_loss_score",
        "launch_transition_risk_score",
    ]:
        for shift in [0.8, 1.2]:
            w = weights.copy()
            w[col] = w[col] * shift
            norm = sum(w.values())
            w = {k: v / norm for k, v in w.items()}

            score = _weighted_score(out, w)
            ranked = out.assign(score_tmp=score).sort_values("score_tmp", ascending=False).reset_index(drop=True)
            top3 = ",".join(ranked.head(3)["area"].tolist())
            sensitivity_rows.append(
                {
                    "driver_perturbed": col,
                    "factor": shift,
                    "top3_areas": top3,
                    "score_medio": float(score.mean()),
                    "desviacion_score": float(score.std()),
                }
            )

    sensitivity_df = pd.DataFrame(sensitivity_rows)
    sensitivity_df.to_csv(EV_DIR / "scoring_sensitivity_analysis.csv", index=False)

    # Robustez del ranking con perturbación estocástica de pesos (Monte Carlo reproducible).
    rng = np.random.default_rng(20260402)
    weight_keys = [
        "yard_risk_score",
        "charging_risk_score",
        "dispatch_risk_score",
        "throughput_loss_score",
        "launch_transition_risk_score",
        "readiness_score",
    ]
    base_weight_vec = np.array([weights[k] for k in weight_keys], dtype=float)
    draws = 300
    top1_rows: List[Dict[str, object]] = []
    top3_rows: List[Dict[str, object]] = []
    for draw_id in range(draws):
        noise = rng.lognormal(mean=0.0, sigma=0.22, size=len(weight_keys))
        w_vec = base_weight_vec * noise
        w_vec = w_vec / w_vec.sum()
        w_draw = {k: float(w_vec[i]) for i, k in enumerate(weight_keys)}

        score = _weighted_score(out, w_draw)
        ranked = out.assign(score_tmp=score).sort_values("score_tmp", ascending=False).reset_index(drop=True)
        top1_rows.append({"draw_id": draw_id, "top1_area": str(ranked.loc[0, "area"])})
        top3_rows.append(
            {
                "draw_id": draw_id,
                "top3_areas": ",".join(ranked.head(3)["area"].tolist()),
                "w_yard": w_draw["yard_risk_score"],
                "w_charging": w_draw["charging_risk_score"],
                "w_dispatch": w_draw["dispatch_risk_score"],
                "w_throughput": w_draw["throughput_loss_score"],
                "w_launch": w_draw["launch_transition_risk_score"],
                "w_readiness": w_draw["readiness_score"],
            }
        )

    top1_df = pd.DataFrame(top1_rows)
    rank_stability = (
        top1_df.groupby("top1_area", as_index=False)
        .agg(freq=("draw_id", "count"))
        .sort_values("freq", ascending=False)
    )
    rank_stability["freq_share"] = rank_stability["freq"] / draws
    rank_stability.to_csv(EV_DIR / "scoring_rank_stability.csv", index=False)
    pd.DataFrame(top3_rows).to_csv(EV_DIR / "scoring_montecarlo_draws.csv", index=False)

    governance_checks = [
        {
            "check_name": "opi_diversity",
            "metric": "operational_priority_index_nunique",
            "value": float(out["operational_priority_index"].nunique()),
            "threshold": 3.0,
            "status": "PASS" if out["operational_priority_index"].nunique() >= 3 else "WARN",
        },
        {
            "check_name": "risk_driver_diversity",
            "metric": "main_risk_driver_nunique",
            "value": float(out["main_risk_driver"].nunique()),
            "threshold": 2.0,
            "status": "PASS" if out["main_risk_driver"].nunique() >= 2 else "WARN",
        },
        {
            "check_name": "tier_diversity",
            "metric": "area_priority_tier_nunique",
            "value": float(out["area_priority_tier"].nunique()),
            "threshold": 2.0,
            "status": "PASS" if out["area_priority_tier"].nunique() >= 2 else "WARN",
        },
        {
            "check_name": "opi_dispersion",
            "metric": "operational_priority_index_std",
            "value": float(out["operational_priority_index"].std(ddof=0)),
            "threshold": 1.0,
            "status": "PASS" if float(out["operational_priority_index"].std(ddof=0)) >= 1.0 else "WARN",
        },
        {
            "check_name": "rank_stability_top1_share",
            "metric": "max_top1_share_montecarlo",
            "value": float(rank_stability["freq_share"].max()) if not rank_stability.empty else 0.0,
            "threshold": 0.45,
            "status": "PASS" if (not rank_stability.empty and float(rank_stability["freq_share"].max()) >= 0.45) else "WARN",
        },
    ]
    governance_df = pd.DataFrame(governance_checks)
    governance_df.to_csv(EV_DIR / "scoring_governance_checks.csv", index=False)

    top_areas = out.head(10)
    top_actions = (
        out.groupby("recommended_action", as_index=False)
        .agg(
            areas_afectadas=("area", "count"),
            prioridad_media=("operational_priority_index", "mean"),
        )
        .sort_values("prioridad_media", ascending=False)
    )
    top_areas.to_csv(OUTPUT_REPORTS_DIR / "top_areas_criticas.csv", index=False)
    top_actions.to_csv(OUTPUT_REPORTS_DIR / "top_acciones_recomendadas.csv", index=False)

    # Documento framework
    doc = PROJECT_ROOT / "docs" / "scoring_framework.md"
    doc.write_text(
        """# Scoring Framework - Priorización Operativa EV

## Objetivo
Priorizar acciones de secuenciación, patio, carga, expedición y capacidad para sostener el ramp-up EV.

## Scores mínimos
- `readiness_score`
- `yard_risk_score`
- `charging_risk_score`
- `dispatch_risk_score`
- `throughput_loss_score`
- `launch_transition_risk_score`
- `operational_priority_index`
- `area_priority_tier`
- `main_risk_driver`
- `recommended_action`

## Regla de tier
- >=80: intervenir ahora
- 65-79: estabilizar en la siguiente ola
- 50-64: monitorizar muy de cerca
- 35-49: mantener bajo observación
- <35: sin prioridad inmediata

## Lógica de decisión
El `operational_priority_index` combina riesgo de patio, carga, expedición, pérdida de throughput y tensión de transición EV.

## Sensibilidad
Se aplica perturbación de pesos (+/-20%) para verificar estabilidad del ranking de áreas críticas.
Se añade test Monte Carlo de estabilidad de top-1 bajo ruido de pesos.
""",
        encoding="utf-8",
    )

    summary_lines = [
        "# Scoring y Priorización - Resumen",
        "",
        "## Top áreas críticas",
    ]
    for row in top_areas.itertuples(index=False):
        summary_lines.append(
            f"- {row.area}: OPI={row.operational_priority_index:.1f}, tier={row.area_priority_tier}, driver={row.main_risk_driver}, acción={row.recommended_action}"
        )
    summary_lines.extend(["", "## Top acciones"]) 
    for row in top_actions.head(8).itertuples(index=False):
        summary_lines.append(
            f"- {row.recommended_action}: prioridad_media={row.prioridad_media:.1f}, áreas_afectadas={int(row.areas_afectadas)}"
        )
    summary_lines.extend(
        [
            "",
            "## Governance checks",
        ]
    )
    for row in governance_df.itertuples(index=False):
        summary_lines.append(f"- {row.check_name}: {row.status} (valor={row.value:.2f}, umbral={row.threshold:.2f})")
    if not rank_stability.empty:
        summary_lines.extend(
            [
                "",
                "## Estabilidad Monte Carlo (top-1)",
                f"- Área dominante: {rank_stability.iloc[0]['top1_area']}",
                f"- Frecuencia top-1: {rank_stability.iloc[0]['freq_share']:.2%}",
            ]
        )

    (OUTPUT_REPORTS_DIR / "scoring_summary.md").write_text("\n".join(summary_lines), encoding="utf-8")

    return ScoringResult(areas=int(out.shape[0]), top_area=str(out.iloc[0]["area"]))


if __name__ == "__main__":
    res = run_ev_scoring_framework()
    print("Scoring framework EV completado")
    print(f"- áreas evaluadas: {res.areas}")
    print(f"- área top: {res.top_area}")
