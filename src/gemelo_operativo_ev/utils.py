from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable, Sequence
from hashlib import sha256
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


def atomic_write_text_utf8(path: Path, text: str) -> None:
    """Publica texto UTF-8 mediante reemplazo atómico y sin residuos."""
    path.parent.mkdir(parents=True, exist_ok=True)
    final_text = text if text.endswith("\n") else f"{text}\n"
    file_descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(final_text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def atomic_write_json_utf8(
    path: Path,
    payload: Any,
    *,
    default: Callable[[object], object] | None = None,
) -> None:
    atomic_write_text_utf8(path, json.dumps(payload, indent=2, ensure_ascii=False, default=default))


def sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_dataframe_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(file_descriptor)
    temporary_path = Path(temporary_name)
    try:
        frame.to_csv(temporary_path, index=False, lineterminator="\n")
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


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
    except ImportError:
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
