from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_ev_csv(
    name: str,
    ev_dir: Path,
    parse_dates: list[str] | None = None,
    context: str = "tabla",
) -> pd.DataFrame:
    path = ev_dir / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"No existe {context}: {path}")
    return pd.read_csv(path, parse_dates=parse_dates)


def to_markdown_safe(df: pd.DataFrame) -> str:
    """Render a DataFrame as markdown without requiring tabulate."""
    try:
        return df.to_markdown(index=False)
    except Exception:
        if df.empty:
            return "_(sin filas)_"
        cols = [str(c) for c in df.columns]
        header = "| " + " | ".join(cols) + " |"
        sep = "| " + " | ".join(["---"] * len(cols)) + " |"
        rows = []
        for _, row in df.iterrows():
            vals = [str(row[c]).replace("\n", " ") for c in df.columns]
            rows.append("| " + " | ".join(vals) + " |")
        return "\n".join([header, sep] + rows)
