from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from gemelo_operativo_ev.utils import read_ev_csv, to_markdown_safe, write_json_utf8, write_text_utf8


def test_read_ev_csv_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="No existe"):
        read_ev_csv("nonexistent_table", tmp_path)


def test_read_ev_csv_reads_csv(tmp_path: Path) -> None:
    csv = tmp_path / "test_table.csv"
    csv.write_text("a,b\n1,2\n3,4", encoding="utf-8")
    df = read_ev_csv("test_table", tmp_path)
    assert list(df.columns) == ["a", "b"]
    assert len(df) == 2


def test_read_ev_csv_custom_context_in_error(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="tabla de prueba"):
        read_ev_csv("missing", tmp_path, context="tabla de prueba")


def test_to_markdown_safe_returns_string() -> None:
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    result = to_markdown_safe(df)
    assert isinstance(result, str)
    assert "col1" in result
    assert "col2" in result


def test_to_markdown_safe_empty_dataframe() -> None:
    df = pd.DataFrame()
    result = to_markdown_safe(df)
    assert isinstance(result, str)


def test_to_markdown_safe_newlines_in_values() -> None:
    df = pd.DataFrame({"text": ["line1\nline2"]})
    result = to_markdown_safe(df)
    assert "\n\n" not in result.split("|")[1]


def test_to_markdown_safe_does_not_hide_rendering_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_rendering_error(self: pd.DataFrame, index: bool = False) -> str:
        raise ValueError("rendering failed")

    monkeypatch.setattr(pd.DataFrame, "to_markdown", _raise_rendering_error)

    with pytest.raises(ValueError, match="rendering failed"):
        to_markdown_safe(pd.DataFrame({"value": [1]}))


def test_write_text_utf8_adds_final_newline(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "report.md"
    write_text_utf8(output, "contenido")
    assert output.read_bytes() == b"contenido\n"


def test_write_json_utf8_is_readable_and_ends_with_newline(tmp_path: Path) -> None:
    output = tmp_path / "report.json"
    write_json_utf8(output, {"estado": "PASS"})
    assert output.read_text(encoding="utf-8").endswith("\n")
    assert '"estado": "PASS"' in output.read_text(encoding="utf-8")
