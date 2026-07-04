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

## Report-quality pass (2026-06-26): Big Four consulting standard

Goal: bring `outputs/reports/ev_transition_operating_twin_report.pdf` (built by
`scripts/generate_report.py`) to executive-consulting quality — eliminate AI
writing patterns, and strengthen analytical rigor, structure and logic.

**Baseline assessment.** Read every section's prose in full. Found zero AI
writing tells (no "leverage/delve/moreover/robust solution/seamless" etc., zero
em-dashes, no generic hedging or filler transitions) and a strong existing
structure: hypothesis-elimination findings (systematically clearing shift
effects, cycle-time, and yard-as-EV-cause before landing on the real driver),
a decision-frame section with falsifiable tests, honest calibration caveats,
and a phased roadmap with exit criteria. The prose did not need a rewrite; it
needed a rigor audit.

**Found and fixed one real analytical bug**, not a style issue:
- `generate_report.py`'s `yard_zone` aggregation used `p95_dwell=(...,"max")`
  — the single worst hourly p95 reading across the entire 12-month window
  (65.7 h) — while `generate_chart_pack.py` correctly used `.mean()` on the
  same underlying metric (43.0 h). Two pipelines silently diverged on "the"
  number for the same concept. The report's own hand-typed captions (Figure 9,
  Priority 1) had been kept in sync with the *correct* 43 h chart-pack figure,
  while live f-string citations elsewhere in the same report (Section 7, and
  a "49-hour" figure in the executive summary and risk section that actually
  conflated the *plant-level* p95 with the *zone-level* p95) quietly drifted.
  Fixed the aggregation to `.mean()` (verified it now reconciles exactly with
  the chart pack: 43.04 h both ways) and converted every hardcoded literal to
  read the live variable, so this class of drift can't recur silently.
- Corrected a categorical overclaim ("every other zone clears in under three
  hours") that was false for the buffer-charging zone (5.2 h p95); now states
  the two-tier reality precisely.
- Fixed a citation gap: the recommendations table tied "stop accelerating the
  EV mix" to Fig 13/16, but the chart whose caption states the exact claim
  ("accelerating the mix... scores worst") is Fig 15. Added it.

**Verified, did not change:** figure-to-citation mapping for all other
recommendations, cross-section numerical consistency (78% late-dispatch share,
41% clean-exit rate, 24h lead time, decision scores, Monte Carlo 77/23 split
— all consistent everywhere cited), dynamic TOC page numbers (real
`TableOfContents`, not hardcoded), page-break integrity (no orphaned headers,
no empty pages) across a full-document sample render.

**Deliberately not done:** re-localising the 19 embedded chart images from
Spanish to English to match the English report prose. That's a chart-pack
scope change (the charts are shared with the Spanish-facing dashboard), not a
report-prose fix, and was out of scope for this pass.

## Visual-design pass (2026-07-04): Big Four presentation standard

Goal: redesign `outputs/reports/ev_transition_operating_twin_report.pdf` so every
page looks flawlessly laid out by a human consultant — formatting, hierarchy,
typography, spacing, alignment, tables — not just correct prose.

**Found and fixed four genuine layout/presentation defects:**

1. **Stranded table rows.** `data_table()`'s underlying `Table(...,
   repeatRows=1)` let a 15-row table split mid-body across pages 8-9, leaving
   two rows stranded above a repeated header. Fixed by wrapping every table in
   `KeepTogether`. While fixing this, found the prose itself was wrong — it
   claimed "ten integrity checks" when the CSV has 15 — corrected to a live
   `len(checks)` count.
2. **Orphaned appendix headings.** Once tables were wrapped in `KeepTogether`,
   a heading with no prose before its table (all 7 appendix subsections, A-G)
   could still be stranded alone at the bottom of a page while its
   now-monolithic table got pushed whole to the next, leaving ~40% blank space.
   Added an `h2_table()` helper that binds heading + table into one
   `KeepTogether` unit. First attempt nested two `KeepTogether`s (h2_table
   wrapping data_table's own wrapped output), which confused ReportLab's
   space calculation and forced every subsection onto its own page (41→46
   pages). Fixed by extracting `_table_flowables()` (unwrapped) so each
   helper wraps in exactly one `KeepTogether`, never nested.
3. **Untranslated Spanish leaking into an English report.** Three appendix
   tables rendered raw source-data codes instead of the English names used
   everywhere else in the document:
   - Appendix A: `causa_principal_cuello` ("BLOQUEO_INTERNO_Y_REUBICACION")
     and `area_mayor_perdida_throughput` ("LOGISTICA") printed as raw/badly
     title-cased Spanish.
   - Appendix D: capacity levers (`capacidad_carga`, `secuenciacion_ev`,
     `gestion_patio`, `disciplina_expedicion`, `resiliencia_turno`) printed as
     "Capacidad carga", "Secuenciacion ev", etc.
   - Appendix E: corrective-package metrics (`tiempo_total_interno`,
     `ocupacion_media_patio`, `riesgo_salida_baja_readiness`, ...) printed as
     "Tiempo total interno", "Ocupacion media patio", etc., with no
     percentage-vs-raw-decimal distinction.
   - Appendix F: `top3_areas` sensitivity results printed as raw comma-joined
     codes like "LOGISTICA,PATIO,CARGA".
   Fixed with a module-level `AREA_NAME_EN` map plus scoped `lever_name_en`,
   `delta_metric_labels` and `bottleneck_cause_en` dicts, all cross-checked
   against the English names the body prose already uses for the same
   concepts (e.g. Figure 18's "sequencing and yard management", Figure 10's
   "Charging, Production, Dispatch and Energy").

**Verified via full-document visual sweep:** rendered all 41 pages at 100 dpi
and reviewed as tiled contact sheets, plus targeted 150 dpi renders of the
appendix. No orphaned headers, no split tables, no remaining non-English
strings, consistent spacing/typography/alignment throughout. Page count holds
at 41 after all fixes. `ruff check .`, `ruff format --check .`, and the full
107-test unit suite all pass.

## Definition of done

- `ruff check .` and `ruff format --check .` clean.
- Full suite green with the coverage gate active.
- Git working tree clean after an integration run (no binary churn).
- GitHub community-standards checklist complete.
