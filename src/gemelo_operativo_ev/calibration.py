from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from statistics import NormalDist

import numpy as np
import pandas as pd

from .ev_scenario_twin import CALIBRATABLE_SCENARIO_METRICS, SCENARIO_PARAM_KEYS

CALIBRATABLE_METRICS = CALIBRATABLE_SCENARIO_METRICS

CALIBRATION_COLUMNS = (
    "observation_id",
    "unit_id",
    "period",
    "metric",
    "baseline_value",
    "observed_value",
    *SCENARIO_PARAM_KEYS,
)

CALIBRATION_RESULT_COLUMNS = (
    "metric",
    "lever",
    "estimate",
    "std_error",
    "ci_low",
    "ci_high",
    "p_value",
    "n_obs",
    "clusters",
    "r_squared",
    "condition_number",
    "method",
    "calibration_status",
)


@dataclass(frozen=True)
class CalibrationConfig:
    min_observations: int = 60
    min_clusters: int = 5
    confidence_level: float = 0.95
    max_condition_number: float = 1e8
    include_unit_effects: bool = True
    include_period_effects: bool = True

    def validate(self) -> None:
        if self.min_observations <= 0 or self.min_clusters < 2:
            raise ValueError("Los mínimos de observaciones y clusters no son válidos")
        if not 0 < self.confidence_level < 1:
            raise ValueError("confidence_level debe estar entre 0 y 1")
        if self.max_condition_number <= 1:
            raise ValueError("max_condition_number debe ser mayor que 1")


@dataclass(frozen=True)
class CalibrationResult:
    coefficients: pd.DataFrame
    metrics_estimated: int
    observations: int


def validate_calibration_frame(frame: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in CALIBRATION_COLUMNS if column not in frame.columns]
    unexpected = [column for column in frame.columns if column not in CALIBRATION_COLUMNS]
    if missing or unexpected:
        raise ValueError(f"Contrato de calibración inválido; faltan={missing}, sobran={unexpected}")
    if frame.empty:
        raise ValueError("El dataset de calibración no puede estar vacío")
    if frame.duplicated(["observation_id", "metric"]).any():
        raise ValueError("observation_id + metric debe ser único")
    if frame[["observation_id", "unit_id", "period", "metric"]].isna().any().any():
        raise ValueError("Identificadores, período y métrica no pueden ser nulos")

    unknown_metrics = sorted(set(frame["metric"].astype(str)) - set(CALIBRATABLE_METRICS))
    if unknown_metrics:
        raise ValueError(f"Métricas no calibrables: {unknown_metrics}")

    numeric_columns = ["baseline_value", "observed_value", *SCENARIO_PARAM_KEYS]
    validated = frame.copy()
    for column in numeric_columns:
        validated[column] = pd.to_numeric(validated[column], errors="coerce")
    if validated[numeric_columns].isna().any().any():
        raise ValueError("La calibración contiene valores numéricos ausentes o inválidos")
    if not np.isfinite(validated[numeric_columns].to_numpy(dtype=float)).all():
        raise ValueError("La calibración contiene valores no finitos")
    if (validated[["baseline_value", "observed_value"]] <= 0).any().any():
        raise ValueError("baseline_value y observed_value deben ser estrictamente positivos")
    lever_values = validated[list(SCENARIO_PARAM_KEYS)]
    if ((lever_values < 0) | (lever_values > 1)).any().any():
        raise ValueError("Las intensidades de palanca deben permanecer en [0, 1]")

    validated["period"] = pd.to_datetime(validated["period"], errors="raise", utc=True)
    return validated.loc[:, list(CALIBRATION_COLUMNS)]


def _design_matrix(frame: pd.DataFrame, config: CalibrationConfig) -> tuple[np.ndarray, list[str]]:
    components = [pd.Series(1.0, index=frame.index, name="intercept"), frame[list(SCENARIO_PARAM_KEYS)]]
    if config.include_unit_effects:
        components.append(pd.get_dummies(frame["unit_id"].astype(str), prefix="unit", drop_first=True, dtype=float))
    if config.include_period_effects:
        period_key = frame["period"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        components.append(pd.get_dummies(period_key, prefix="period", drop_first=True, dtype=float))
    design = pd.concat(components, axis=1).astype(float)
    return design.to_numpy(), list(design.columns)


def _cluster_covariance(x: np.ndarray, residuals: np.ndarray, clusters: pd.Series) -> np.ndarray:
    n_obs, n_parameters = x.shape
    unique_clusters = pd.unique(clusters.astype(str))
    cluster_count = len(unique_clusters)
    if cluster_count < 2 or n_obs <= n_parameters:
        raise ValueError("No hay grados de libertad suficientes para covarianza agrupada")

    bread = np.linalg.inv(x.T @ x)
    meat = np.zeros((n_parameters, n_parameters), dtype=float)
    cluster_values = clusters.astype(str).to_numpy()
    for cluster in unique_clusters:
        mask = cluster_values == cluster
        score = x[mask].T @ residuals[mask]
        meat += np.outer(score, score)
    correction = (cluster_count / (cluster_count - 1)) * ((n_obs - 1) / (n_obs - n_parameters))
    return correction * bread @ meat @ bread


def calibrate_scenario_coefficients(
    frame: pd.DataFrame,
    *,
    config: CalibrationConfig | None = None,
) -> CalibrationResult:
    config = config or CalibrationConfig()
    config.validate()
    validated = validate_calibration_frame(frame)
    critical_value = NormalDist().inv_cdf((1 + config.confidence_level) / 2)
    rows: list[dict[str, object]] = []

    for metric in CALIBRATABLE_METRICS:
        metric_frame = validated.loc[validated["metric"] == metric].reset_index(drop=True)
        if len(metric_frame) < config.min_observations:
            raise ValueError(f"{metric}: requiere al menos {config.min_observations} observaciones")
        cluster_count = metric_frame["unit_id"].nunique()
        if cluster_count < config.min_clusters:
            raise ValueError(f"{metric}: requiere al menos {config.min_clusters} unidades independientes")
        no_support = [lever for lever in SCENARIO_PARAM_KEYS if metric_frame[lever].nunique() < 2]
        if no_support:
            raise ValueError(f"{metric}: sin variación identificadora en {no_support}")

        x, names = _design_matrix(metric_frame, config)
        if x.shape[0] <= x.shape[1]:
            raise ValueError(f"{metric}: más parámetros que grados de libertad")
        rank = int(np.linalg.matrix_rank(x))
        if rank != x.shape[1]:
            raise ValueError(f"{metric}: matriz de diseño no identificada")
        condition_number = float(np.linalg.cond(x))
        if not np.isfinite(condition_number) or condition_number > config.max_condition_number:
            raise ValueError(f"{metric}: condición numérica inaceptable ({condition_number:.3g})")

        y = np.log(metric_frame["observed_value"].to_numpy() / metric_frame["baseline_value"].to_numpy())
        coefficients, _, _, _ = np.linalg.lstsq(x, y, rcond=None)
        residuals = y - x @ coefficients
        covariance = _cluster_covariance(x, residuals, metric_frame["unit_id"])
        standard_errors = np.sqrt(np.maximum(np.diag(covariance), 0))
        total_variation = float(np.sum((y - y.mean()) ** 2))
        r_squared = 1 - float(residuals @ residuals) / total_variation if total_variation > 0 else 0.0

        for lever in SCENARIO_PARAM_KEYS:
            index = names.index(lever)
            estimate = float(coefficients[index])
            std_error = float(standard_errors[index])
            statistic = abs(estimate / std_error) if std_error > 0 else float("inf")
            p_value = 2 * (1 - NormalDist().cdf(statistic))
            rows.append(
                {
                    "metric": metric,
                    "lever": lever,
                    "estimate": estimate,
                    "std_error": std_error,
                    "ci_low": estimate - critical_value * std_error,
                    "ci_high": estimate + critical_value * std_error,
                    "p_value": p_value,
                    "n_obs": len(metric_frame),
                    "clusters": cluster_count,
                    "r_squared": r_squared,
                    "condition_number": condition_number,
                    "method": "log_ratio_ols_unit_period_fe_cluster_unit",
                    "calibration_status": "approved",
                }
            )

    coefficients_frame = pd.DataFrame(rows, columns=list(CALIBRATION_RESULT_COLUMNS))
    coefficients_frame = coefficients_frame.sort_values(["metric", "lever"]).reset_index(drop=True)
    return CalibrationResult(
        coefficients=coefficients_frame,
        metrics_estimated=coefficients_frame["metric"].nunique(),
        observations=len(validated),
    )


def load_approved_calibration(path: Path) -> Mapping[str, Mapping[str, float]]:
    frame = pd.read_csv(path)
    missing = [column for column in CALIBRATION_RESULT_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Fichero de calibración incompleto: {missing}")
    if frame.duplicated(["metric", "lever"]).any():
        raise ValueError("La calibración contiene pares metric + lever duplicados")
    if set(frame["metric"]) != set(CALIBRATABLE_METRICS):
        raise ValueError("La calibración no cubre todas las métricas requeridas")
    if set(frame["lever"]) != set(SCENARIO_PARAM_KEYS):
        raise ValueError("La calibración no cubre todas las palancas requeridas")
    expected_pairs = len(CALIBRATABLE_METRICS) * len(SCENARIO_PARAM_KEYS)
    if len(frame) != expected_pairs or not frame["calibration_status"].eq("approved").all():
        raise ValueError("La calibración no está aprobada o no es completa")
    estimates = pd.to_numeric(frame["estimate"], errors="coerce")
    if estimates.isna().any() or not np.isfinite(estimates.to_numpy()).all():
        raise ValueError("La calibración contiene coeficientes inválidos")

    return {
        metric: {
            lever: float(frame.loc[(frame["metric"] == metric) & (frame["lever"] == lever), "estimate"].iloc[0])
            for lever in SCENARIO_PARAM_KEYS
        }
        for metric in CALIBRATABLE_METRICS
    }


def configured_calibration_path() -> Path | None:
    raw_path = os.getenv("EV_TWIN_CALIBRATION_FILE")
    return Path(raw_path).expanduser().resolve() if raw_path else None
