## What

Brief description of the change and the decision question or issue it addresses.

## Why

Context / motivation. Link any related issue (`Closes #...`).

## How verified

- [ ] `ruff check .` clean
- [ ] `ruff format --check .` clean
- [ ] `pytest` (unit suite) green
- [ ] `pytest -m integration` green (if the pipeline or outputs changed)
- [ ] Coverage stays at or above the CI gate (85%)
- [ ] Generated artifacts (charts / report / marts) regenerated if affected

## Notes

Anything reviewers should know — trade-offs, follow-ups, or out-of-scope items.
