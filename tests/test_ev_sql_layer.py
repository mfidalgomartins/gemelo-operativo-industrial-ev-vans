from __future__ import annotations

from pathlib import Path

import pytest

from src import ev_sql_layer as sql_layer


class _FakeResult:
    def __init__(self, *, rows: list[tuple[object, ...]] | None = None, one: tuple[object, ...] | None = None) -> None:
        self._rows = rows or []
        self._one = one

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows

    def fetchone(self) -> tuple[object, ...] | None:
        return self._one


class _FakeConnection:
    def __init__(self, columns_by_object: dict[str, list[str]], counts_by_object: dict[str, int]) -> None:
        self.columns_by_object = columns_by_object
        self.counts_by_object = counts_by_object
        self.statements: list[str] = []
        self.params: list[list[object] | None] = []

    def execute(self, statement: str, params: list[object] | None = None) -> _FakeResult:
        self.statements.append(statement)
        self.params.append(params)
        if "information_schema.columns" in statement:
            obj = (
                str(params[0])
                if params
                else next(name for name in self.columns_by_object if f"table_name = '{name}'" in statement)
            )
            return _FakeResult(rows=[(column,) for column in self.columns_by_object[obj]])
        if statement.startswith("SELECT COUNT(*) FROM"):
            obj = statement.removeprefix("SELECT COUNT(*) FROM").strip().strip('"')
            return _FakeResult(one=(self.counts_by_object[obj],))
        return _FakeResult()


def test_resolve_raw_csv_raises_with_official_ev_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sql_layer, "EV_DATA_RAW_DIR", tmp_path / "raw_ev")

    with pytest.raises(FileNotFoundError, match="Falta tabla raw requerida"):
        sql_layer._resolve_raw_csv("ordenes")


@pytest.mark.parametrize("identifier", ["ordenes", "_tmp_1"])
def test_quote_identifier_accepts_safe_sql_identifiers(identifier: str) -> None:
    assert sql_layer._quote_identifier(identifier) == f'"{identifier}"'


@pytest.mark.parametrize("identifier", ["bad-name", "ordenes;DROP TABLE ordenes", "1tabla"])
def test_quote_identifier_rejects_unsafe_sql_identifiers(identifier: str) -> None:
    with pytest.raises(ValueError, match="Identificador SQL inválido"):
        sql_layer._quote_identifier(identifier)


def test_load_raw_tables_loads_each_required_csv_in_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    for table in ["ordenes", "vehiculos"]:
        (raw_dir / f"{table}.csv").write_text("id\n1\n", encoding="utf-8")

    con = _FakeConnection(columns_by_object={}, counts_by_object={})
    monkeypatch.setattr(sql_layer, "EV_DATA_RAW_DIR", raw_dir)
    monkeypatch.setattr(sql_layer, "RAW_TABLES", ["ordenes", "vehiculos"])

    sql_layer._load_raw_tables(con)  # type: ignore[arg-type]

    assert 'CREATE OR REPLACE TABLE "ordenes" AS' in con.statements[0]
    assert 'CREATE OR REPLACE TABLE "vehiculos" AS' in con.statements[1]
    assert con.params == [[(raw_dir / "ordenes.csv").as_posix()], [(raw_dir / "vehiculos.csv").as_posix()]]


def test_run_sql_files_rejects_missing_sql_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sql_layer, "SQL_LAYER_DIR", tmp_path / "missing_sql")

    with pytest.raises(FileNotFoundError, match="No existe directorio SQL requerido"):
        sql_layer._run_sql_files(_FakeConnection(columns_by_object={}, counts_by_object={}))  # type: ignore[arg-type]


def test_run_sql_files_executes_configured_files_in_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for name, body in [("01.sql", "SELECT 1;"), ("02.sql", "SELECT 2;")]:
        (tmp_path / name).write_text(body, encoding="utf-8")
    con = _FakeConnection(columns_by_object={}, counts_by_object={})

    monkeypatch.setattr(sql_layer, "SQL_LAYER_DIR", tmp_path)
    monkeypatch.setattr(sql_layer, "SQL_FILES_IN_ORDER", ["01.sql", "02.sql"])

    executed = sql_layer._run_sql_files(con)  # type: ignore[arg-type]

    assert executed == ["01.sql", "02.sql"]
    assert con.statements == ["SELECT 1;", "SELECT 2;"]


def test_export_objects_orders_by_all_columns_and_returns_row_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    con = _FakeConnection(columns_by_object={"vw_test": ["fecha", "turno"]}, counts_by_object={"vw_test": 3})

    monkeypatch.setattr(sql_layer, "DATA_PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(sql_layer, "EXPORT_OBJECTS", ["vw_test"])

    exported = sql_layer._export_objects(con)  # type: ignore[arg-type]

    assert exported == {"vw_test": 3}
    copy_statement = next(statement for statement in con.statements if "COPY (" in statement)
    assert 'ORDER BY "fecha", "turno"' in copy_statement
    assert con.params[-2] == [(tmp_path / "processed" / "ev_factory" / "vw_test.csv").as_posix()]
    assert (tmp_path / "processed" / "ev_factory").exists()


def test_export_objects_rejects_missing_or_columnless_object(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    con = _FakeConnection(columns_by_object={"vw_missing": []}, counts_by_object={})

    monkeypatch.setattr(sql_layer, "DATA_PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(sql_layer, "EXPORT_OBJECTS", ["vw_missing"])

    with pytest.raises(ValueError, match="objeto sin columnas o inexistente"):
        sql_layer._export_objects(con)  # type: ignore[arg-type]
