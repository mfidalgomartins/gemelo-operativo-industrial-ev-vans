from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.synthetic_data_gen import SyntheticGenerationConfig, generate_synthetic_factory_data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generador sintético industrial EV para gemelo operativo."
    )
    parser.add_argument("--seed", type=int, default=20260328, help="Seed de reproducibilidad")
    parser.add_argument("--start-date", type=str, default="2025-01-01", help="Fecha de inicio (YYYY-MM-DD)")
    parser.add_argument("--months", type=int, default=12, help="Horizonte en meses (9-15)")
    parser.add_argument(
        "--output-raw",
        type=str,
        default="data/raw/ev_factory",
        help="Directorio de salida raw EV",
    )
    parser.add_argument(
        "--output-reports",
        type=str,
        default="outputs/reports",
        help="Directorio de reportes",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = SyntheticGenerationConfig(
        seed=args.seed,
        start_date=args.start_date,
        months=args.months,
        output_raw_dir=Path(args.output_raw),
        output_report_dir=Path(args.output_reports),
    )
    summary = generate_synthetic_factory_data(cfg)

    run_path = cfg.output_report_dir / "synthetic_generation_run.json"
    run_path.parent.mkdir(parents=True, exist_ok=True)
    run_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    print("Generación EV completada")
    print(f"- output raw: {cfg.output_raw_dir}")
    print(f"- output reports: {cfg.output_report_dir}")
    print(f"- tablas: {len(summary['cardinalidades'])}")
    print(f"- validación: {summary['validation']['status_global']}")


if __name__ == "__main__":
    main()
