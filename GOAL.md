# GOAL — Bring the repository to 10/10 across all levels

**Set:** 2026-06-26 · **Status target:** publication-grade open-source repository.

This file is the working goal for the current pass. It records where the repo
already stands, what "10/10" means level by level, and the concrete actions
taken to close the remaining gaps.

## Baseline (measured, not assumed)

| Signal | State at start |
|---|---|
| Lint (`ruff check .`) | clean |
| Format (`ruff format --check .`) | clean |
| Unit tests (`pytest`) | 107 passed |
| Combined coverage (unit + integration) | **90%** |
| CI | `quality` (3.10/3.12 matrix), `package`, `integration`, `security` jobs |
| Security | bandit + pip-audit in CI |
| Packaging | `pyproject.toml`, build job produces sdist + wheel |
| Docs | 11 topic docs + governance, bilingual README, 34-page PDF report |
| Git working tree | clean |

The analytical core is solid. The remaining work is repo *health and hygiene*,
not correctness.

## What 10/10 means, level by level

1. **Code quality** — already there: centralised utils, modern typing, ruff
   clean. No action needed.
2. **Testing** — 90% combined coverage is strong; the gap was that nothing
   *enforced* it. → add a coverage gate so regressions fail CI.
3. **CI/CD** — comprehensive already; gate is the one missing guardrail.
4. **Repo hygiene** — a 34 MB non-deterministic DuckDB binary was tracked and
   re-committed on every pipeline run (13 times), dirtying the tree and bloating
   history. → untrack it; CI/users rebuild it from the committed CSVs.
5. **Community health** — missing the GitHub "community standards" set:
   CONTRIBUTING, SECURITY, CODE_OF_CONDUCT, issue/PR templates, CHANGELOG,
   CITATION. → add them.
6. **Discoverability** — README had no status badges. → add CI, license,
   Python, coverage badges.

## Actions

- [x] Untrack `data/processed/gemelo_operativo_ev.duckdb` (keep local, no
      history rewrite); it is rebuildable from the committed CSV marts.
- [x] Add `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`,
      `CHANGELOG.md`, `CITATION.cff`.
- [x] Add `.github/ISSUE_TEMPLATE/` (bug + feature) and
      `.github/PULL_REQUEST_TEMPLATE.md`.
- [x] Enforce coverage in CI: the `integration` job runs the full suite with
      `--cov-fail-under=85` (margin under the measured 90%).
- [x] Add README badges (CI, license, Python, coverage).
- [x] Fix pre-existing `ruff format` drift in `scripts/generate_report.py`
      (would have failed the CI format-check step; pure line-wrapping, no logic
      change).
- [x] Add `.pre-commit-config.yaml` (ruff check + format mirroring CI, hygiene
      hooks, and a 25 MB large-file guard that blocks re-adding the DuckDB while
      allowing the ~20 MB committed marts). Wired into `dev` extras + CONTRIBUTING.
- [x] Add `.github/dependabot.yml` for weekly `pip` + `github-actions` updates.

### Deliberately not done

- **mypy / strict type-checking gate.** A lenient run surfaces ~45 errors across
  7 files, mostly broad `object` typing and dict-invariance in stable,
  90%-covered pipeline code. Forcing it green would mean invasive churn in a
  deterministic pipeline for little real safety — against this project's
  "clean and direct, no over-engineering" stance. Revisit only if the type
  surface is tightened deliberately.

## Design-level pass (2026-06-26)

Audited the three visual deliverables for design quality, not just correctness.

- **Chart pack (19 PNGs)** — found and fixed two genuine defects:
  - *Chart 12 (market geography):* readiness spans only 72–73%, yet min/max
    colour normalisation stretched that 1-point gap across the full red→green
    spectrum, so 72% read as alarming-red and 73% as deep-green — directly
    contradicting the chart's own "readiness es uniforme" message. Now uses a
    fixed 0.25–1.0 readiness domain (same scale as chart 18); uniform readiness
    now reads as uniform colour.
  - *Chart 04 (risk matrix):* bubbles at the score extremes were clipped by the
    hardcoded 0–100 axis limits. Padded to −8..108 (centre stays 50) with
    explicit 0..100 ticks so every marker sits fully inside the frame.
  - Regenerated PNGs + the embedding report PDF; verified visually. The other 17
    charts were audited (scatter, funnel, heatmap, correlation, dumbbell,
    coloured bars) and are clean and consistent.
- **Report PDF (40 pp)** — rebuilt to pick up the corrected charts; builds clean.
- **Dashboard (flagship HTML)** — rendered headless and inspected: Geist
  typography, dark-mode tokens, blue `#1d4ed8` accent consistent with the chart
  pack, clean KPI strip and filter bar, charts render correctly. No defects.
  (`Gap vs plan = 0` is a legitimate on-plan value, not a bug.)

## Definition of done

- `ruff check .` and `ruff format --check .` clean.
- Full suite green with the coverage gate active.
- Git working tree clean after an integration run (no binary churn).
- GitHub community-standards checklist complete.
