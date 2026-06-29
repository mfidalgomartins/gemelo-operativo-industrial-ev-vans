---
name: Bug report
about: Report a defect in the pipeline, analysis, or outputs
title: "[bug] "
labels: bug
---

## Summary

A clear, concise description of the bug.

## Reproduction

Steps to reproduce, ideally from a clean checkout:

```bash
generate-data --seed 20260328 --start-date 2025-01-01 --months 12
python -m src.run_pipeline
# ...
```

## Expected vs actual

- **Expected:** what you expected to happen.
- **Actual:** what happened instead (include error output / tracebacks).

## Environment

- OS:
- Python version:
- Package version / commit:

## Additional context

Anything else that helps — affected file, chart, KPI, or report page.
