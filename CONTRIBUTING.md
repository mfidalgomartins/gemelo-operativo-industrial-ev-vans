# Contributing

Thanks for your interest in improving the EV operational twin. This project is a
reproducible analytics pipeline, so the bar for contributions is: **the pipeline
stays deterministic, the tests stay green, and the published outputs remain
regenerable from source.**

## Development setup

```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install -e ".[dev]"      # add ".[security]" to run the scanners
```

Supported Python: **3.10–3.12** (CI matrix runs 3.10 and 3.12).

Optionally install the git hooks so the same checks CI runs fire on every commit:

```bash
pre-commit install
```

## The pipeline

```bash
generate-data --seed 20260328 --start-date 2025-01-01 --months 12  # canonical snapshot
python -m src.run_pipeline           # features, diagnostics, scenarios, scoring
python scripts/generate_chart_pack.py
python scripts/generate_report.py
python -m src.ev_release_gate        # PASS/FAIL governance gate
```

The DuckDB file under `data/processed/` is a rebuildable intermediate and is **not**
tracked in git. The CSV marts beside it are the canonical, committed outputs.

## Checks to run before opening a PR

```bash
ruff check .
ruff format --check .
pytest                                # fast unit suite (no side effects)
pytest -m integration                 # full pipeline; writes to data/ (run last)
```

- **Unit tests** must not write outside `tmp_path`. If a test needs to run the
  full pipeline or touch the data directory, mark it `@pytest.mark.integration`.
- Keep combined coverage at or above the CI gate (**85%**). New analytical
  helpers should ship with unit tests for their pure logic.
- Use Python 3.10+ built-in generics (`list[str]`, `dict[str, int]`), not
  `typing.List`/`Dict`.

## Commit and PR conventions

- Use clear, imperative commit subjects (`fix(report): ...`, `chore(ci): ...`).
- Keep generated artifacts (charts, report, marts) in sync with the code that
  produces them when a change affects them.
- Fill in the pull-request template; describe what you changed and how you
  verified it.

## Determinism

This is a synthetic-data project: every figure must be reproducible from the
canonical seed above. Do not introduce wall-clock time, unseeded randomness, or
network dependencies into the pipeline.
