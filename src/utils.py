from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pandas as pd


def write_text_utf8(path: Path, text: str) -> None:
    """Write UTF-8 text with a final newline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text if text.endswith("\n") else f"{text}\n", encoding="utf-8")


def write_json_utf8(
    path: Path,
    payload: Any,
    *,
    default: Callable[[object], object] | None = None,
) -> None:
    """Write stable, readable UTF-8 JSON with a final newline."""
    write_text_utf8(path, json.dumps(payload, indent=2, ensure_ascii=False, default=default))


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


def require_columns(df: pd.DataFrame, required: Sequence[str], context: str) -> None:
    """Validate required DataFrame columns before analytical calculations."""
    missing = [col for col in required if col not in df.columns]
    if missing:
        missing_cols = ", ".join(missing)
        raise ValueError(f"{context}: faltan columnas requeridas: {missing_cols}")


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
        rows = [
            "| " + " | ".join(str(value).replace("\n", " ") for value in values) + " |"
            for values in df.itertuples(index=False, name=None)
        ]
        return "\n".join([header, sep] + rows)
