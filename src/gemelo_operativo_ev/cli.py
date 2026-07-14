from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from .api.repository import read_status
from .calibration import CalibrationConfig, calibrate_scenario_coefficients
from .config import EV_DATA_RAW_DIR, OUTPUT_REPORTS_DIR, RUNTIME_STATE_DIR
from .ev_release_gate import run_release_gate
from .ingestion.factory import build_connector_registry
from .ingestion.service import IngestionMode, run_ingestion
from .run_pipeline import run_pipeline
from .synthetic_data_gen import SyntheticGenerationConfig, generate_synthetic_factory_data
from .utils import atomic_write_dataframe_csv, write_json_utf8


def _add_generation_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--seed", type=int, default=20260328, help="Semilla de reproducibilidad")
    parser.add_argument("--start-date", default="2025-01-01", help="Fecha inicial YYYY-MM-DD")
    parser.add_argument("--months", type=int, default=12, help="Horizonte en meses (9-15)")
    parser.add_argument("--output-raw", type=Path, default=EV_DATA_RAW_DIR)
    parser.add_argument("--output-reports", type=Path, default=OUTPUT_REPORTS_DIR)


def _run_generate_data(args: argparse.Namespace) -> int:
    config = SyntheticGenerationConfig(
        seed=args.seed,
        start_date=args.start_date,
        months=args.months,
        output_raw_dir=args.output_raw,
        output_report_dir=args.output_reports,
    )
    summary = generate_synthetic_factory_data(config)
    write_json_utf8(config.output_report_dir / "synthetic_generation_run.json", summary, default=str)
    print(
        json.dumps(
            {
                "status": summary["validation"]["status_global"],
                "tables": len(summary["cardinalidades"]),
                "output_raw": str(config.output_raw_dir),
            },
            ensure_ascii=False,
        )
    )
    return 0


def generate_data_main() -> None:
    parser = argparse.ArgumentParser(description="Generador sintético industrial para la transición EV")
    _add_generation_arguments(parser)
    raise SystemExit(_run_generate_data(parser.parse_args()))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ev-twin", description="Control operativo del gemelo EV")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate-data", help="Generar el dataset sintético canónico")
    _add_generation_arguments(generate_parser)

    run_parser = subparsers.add_parser("run", help="Ejecutar la canalización completa")
    run_parser.add_argument("--generate-data", action="store_true")
    run_parser.add_argument("--seed", type=int, default=20260328)
    run_parser.add_argument("--months", type=int, default=12)

    ingest_parser = subparsers.add_parser("ingest", help="Ingestar fuentes configuradas")
    ingest_parser.add_argument("--config", type=Path, required=True)
    ingest_parser.add_argument("--mode", choices=[mode.value for mode in IngestionMode], default="incremental")
    ingest_parser.add_argument("--table", action="append", dest="tables")
    ingest_parser.add_argument(
        "--allow-http-development",
        action="store_true",
        help="Permitir HTTP sin TLS solo para entornos locales controlados",
    )

    calibrate_parser = subparsers.add_parser("calibrate", help="Estimar coeficientes del gemelo")
    calibrate_parser.add_argument("--input", type=Path, required=True)
    calibrate_parser.add_argument(
        "--output",
        type=Path,
        default=RUNTIME_STATE_DIR / "calibration" / "scenario_coefficients.csv",
    )
    calibrate_parser.add_argument("--min-observations", type=int, default=60)
    calibrate_parser.add_argument("--min-clusters", type=int, default=5)

    subparsers.add_parser("release-check", help="Ejecutar la puerta de publicación")
    subparsers.add_parser("charts", help="Construir el paquete de 19 gráficos")
    subparsers.add_parser("report", help="Construir el informe PDF")
    subparsers.add_parser("artifacts", help="Construir gráficos e informe PDF")
    subparsers.add_parser("status", help="Mostrar estado técnico, SLA y publicación")
    return parser


def _execute(args: argparse.Namespace) -> int:
    if args.command == "generate-data":
        return _run_generate_data(args)
    if args.command == "run":
        result = run_pipeline(generate_data=args.generate_data, seed=args.seed, months=args.months)
        print(json.dumps(asdict(result), ensure_ascii=False))
        return 0 if result.release_approved else 1
    if args.command == "ingest":
        registry = build_connector_registry(args.config.resolve(), production=not args.allow_http_development)
        result = run_ingestion(
            registry,
            target_dir=EV_DATA_RAW_DIR,
            state_dir=RUNTIME_STATE_DIR / "state",
            lineage_path=RUNTIME_STATE_DIR / "lineage" / "latest_ingestion.json",
            mode=IngestionMode(args.mode),
            tables=tuple(args.tables) if args.tables else None,
        )
        print(json.dumps(asdict(result), ensure_ascii=False))
        return 0
    if args.command == "calibrate":
        frame = pd.read_csv(args.input)
        result = calibrate_scenario_coefficients(
            frame,
            config=CalibrationConfig(
                min_observations=args.min_observations,
                min_clusters=args.min_clusters,
            ),
        )
        atomic_write_dataframe_csv(result.coefficients, args.output)
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "metrics_estimated": result.metrics_estimated,
                    "observations": result.observations,
                    "output": str(args.output),
                },
                ensure_ascii=False,
            )
        )
        return 0
    if args.command == "release-check":
        result = run_release_gate()
        print(json.dumps(asdict(result), ensure_ascii=False))
        return 0 if result.approved else 1
    if args.command in {"charts", "artifacts"}:
        from .reporting.chart_pack import main as build_charts

        build_charts()
        if args.command == "charts":
            return 0
    if args.command in {"report", "artifacts"}:
        from .reporting.report import main as build_report

        build_report()
        return 0
    if args.command == "status":
        payload = read_status(reports_dir=OUTPUT_REPORTS_DIR, runtime_state_dir=RUNTIME_STATE_DIR)
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    raise RuntimeError(f"Comando no implementado: {args.command}")


def main(argv: Sequence[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    raise SystemExit(_execute(args))


if __name__ == "__main__":
    main()
