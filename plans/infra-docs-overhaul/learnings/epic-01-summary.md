# Epic-01 Learnings: Packaging & CI Modernization

## Codebase Facts

- **92 Python source files** in `./app`, **222 tests** in `./tests`
- Polars DataFrames used extensively; `inewave` library provides untyped data (no stubs)
- `mypy strict` requires `ignore_missing_imports = true` due to `inewave`, `cfinterface` lacking type stubs
- Many `# type: ignore[union-attr]` needed for inewave attribute access patterns (returns `Optional` types)
- `types-python-dateutil` added as dev dependency for dateutil stubs
- sphinx-gallery examples live in `examples/` directory (3 scripts)
- CI matrix covers Python 3.10-3.14
- Build backend: hatchling via uv

## Key Decisions

- Kept `sphinx-rtd-theme` alongside `furo` in dev deps (transition in epic-02)
- `id-token: write` scoped at job level in release.yml, workflow level in docs.yml
- mypy hook runs `./app` as a whole (`pass_filenames: false`) — individual file checking causes false positives
- ruff-pre-commit uses official astral-sh repo (faster than local hook)
- ruff version pinned at v0.15.5 to match lockfile

## Scope Surprises

- Ticket-005 (pre-commit) expanded from 2 files to ~65 files due to pre-existing mypy/ruff errors
- Fixing mypy strict across ./app was the largest effort — mostly adding type annotations and `# type: ignore` for inewave interop
- Some examples/ and tests/ files had ruff E402/F841 violations requiring fixes or noqa directives

## Patterns for Future Epics

- When touching docs/source/conf.py (epic-02), the Sphinx config already has sphinx-gallery configured
- README currently minimal — badges and quickstart to be added in epic-04
- CHANGELOG.md exists but is simple format — needs Keep a Changelog conversion in epic-04
- CONTRIBUTING.md does not exist at repo root — only in docs/source/
- The `app/` package has `py.typed` marker now — downstream consumers can use strict typing
