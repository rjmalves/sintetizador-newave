# ticket-018 Add Type Annotations and Remove Dead Code

## Context

### Background

After the structural decomposition in tickets 015-017, the codebase has cleaner module boundaries but still carries technical debt: 25 `# type: ignore` comments across `app/`, unused imports accumulated during migrations, the unused `pd_to_pl_lazy` utility identified in epic-04 learnings, and retained pandas fallback functions (`_calc_quantiles`, `_calc_mean_std`) that are now redundant. This ticket performs a systematic cleanup pass to bring the decomposed codebase to a consistent quality standard.

### Relation to Epic

This is the final ticket in Epic 05 and the final ticket of the entire performance overhaul plan. It depends on all three decomposition tickets (015-017) being complete so that the cleanup pass covers the final module structure.

### Current State

Known technical debt from learnings and codebase inspection:

1. **`# type: ignore` comments**: 25 occurrences across `app/`. Most are on `import pandas as pd  # type: ignore` and `import numpy as np  # type: ignore` lines. Some are on inewave return values that return `Any`.
2. **Unused utility**: `pd_to_pl_lazy` exported from `app/utils/dataframe.py` but never imported anywhere in the codebase (confirmed in epic-04 learnings).
3. **Retained pandas fallbacks**: `_calc_quantiles` and `_calc_mean_std` in `app/utils/operations.py` were retained as pandas fallback paths. The Polars `_calc_statistics_polars` function has been stable through epics 02-04 with no fallback triggered.
4. **ruff configuration**: `pyproject.toml` has `[tool.ruff]` with `line-length = 80` only -- no rule selection, no mypy configuration.
5. **Missing type annotations**: Many public methods in the decomposed modules lack return type annotations or parameter type annotations beyond the basic ones inherited from the original monolithic files.

## Specification

### Requirements

1. **Remove dead code**:
   - Delete `pd_to_pl_lazy` from `app/utils/dataframe.py` and remove it from `__all__` if present.
   - Delete `_calc_quantiles` and `_calc_mean_std` from `app/utils/operations.py`. Update any `__all__` or imports accordingly.
   - Run `ruff check --select F401` across `app/` to identify unused imports; remove them.
   - Search for and remove any commented-out code blocks (lines starting with `# ` that contain Python syntax like `# def `, `# import `, `# cls.`, `# if `).

2. **Add type annotations to public functions**:
   - Add return type annotations to all public methods (not prefixed with `_`) in the decomposed modules created in tickets 015-017.
   - Add parameter type annotations where the type is unambiguous from the existing code (e.g., `uow: AbstractUnitOfWork`, `df: pd.DataFrame`, `s: OperationSynthesis`).
   - Do NOT annotate parameters that receive inewave return values typed as `Any` -- leave these with `# type: ignore` comments, as the upstream library lacks type stubs.

3. **Clean up `# type: ignore` comments**:
   - Remove `# type: ignore` from `import pandas as pd` and `import numpy as np` lines -- these are unnecessary with modern pandas/numpy type stubs.
   - Keep `# type: ignore` on inewave imports and return values where the library genuinely lacks type information.
   - Target: reduce from 25 to at most 10 `# type: ignore` comments across `app/`.

4. **Update ruff configuration** in `pyproject.toml`:
   - Add `[tool.ruff.lint]` section with `select = ["E", "F", "W", "I"]` (pycodestyle errors, pyflakes, warnings, isort).
   - Add `[tool.ruff.lint.isort]` section with `known-first-party = ["app"]`.

5. **Verify `ruff check` passes clean** across the entire `app/` directory with the updated configuration.

### Inputs/Props

- All files under `app/` as they exist after tickets 015-017.
- `pyproject.toml` for ruff configuration.

### Outputs/Behavior

- Cleaner codebase with fewer `# type: ignore` comments (at most 10).
- All public functions in decomposed modules have type annotations.
- `ruff check app/` passes with zero errors.
- No behavioral changes.

### Error Handling

No changes to error handling logic. Only type annotations and dead code removal.

## Acceptance Criteria

- [ ] Given the file `app/utils/dataframe.py`, when searching for `pd_to_pl_lazy`, then the function definition is not found
- [ ] Given the file `app/utils/operations.py`, when searching for `_calc_quantiles` or `_calc_mean_std`, then neither function definition is found
- [ ] Given the command `ruff check app/`, when executed with the updated `pyproject.toml` configuration, then the exit code is 0 and no errors are printed
- [ ] Given all files under `app/`, when counting `# type: ignore` comments with `grep -r "# type: ignore" app/ --include="*.py" | grep -v __pycache__ | wc -l`, then the count is at most 10
- [ ] Given the full test suite, when running `python -m pytest tests/ -x`, then all tests pass with exit code 0

## Implementation Guide

### Suggested Approach

1. Start with dead code removal -- this is the safest and most mechanical step:
   - Delete `pd_to_pl_lazy` from `app/utils/dataframe.py`.
   - Delete `_calc_quantiles` and `_calc_mean_std` from `app/utils/operations.py`.
   - Run tests to verify nothing depended on them.

2. Run `ruff check --select F401 app/` to find unused imports. Remove each unused import and run tests.

3. Update `pyproject.toml` with the ruff lint configuration. Run `ruff check app/` and fix any errors.

4. Address `# type: ignore` comments:
   - Remove from `import pandas as pd  # type: ignore` and `import numpy as np  # type: ignore` lines globally.
   - For each remaining `# type: ignore`, determine if it is genuinely needed (inewave untyped API) or can be replaced with a proper type annotation.

5. Add type annotations to public functions in the decomposed modules:
   - Start with `app/services/synthesis/operation/` modules.
   - Then `app/services/deck/` domain modules.
   - Then `app/adapters/repository/mappings/` modules.

6. Run the full test suite as a final verification.

### Key Files to Modify

- `app/utils/dataframe.py` (remove `pd_to_pl_lazy`)
- `app/utils/operations.py` (remove `_calc_quantiles`, `_calc_mean_std`)
- `pyproject.toml` (update ruff configuration)
- All files under `app/services/synthesis/operation/` (type annotations, import cleanup)
- All files under `app/services/deck/` (type annotations, import cleanup)
- All files under `app/adapters/repository/` (type annotations, import cleanup)

### Patterns to Follow

- Use `Optional[X]` for parameters that can be `None`, following the existing codebase convention (not `X | None` syntax, as the codebase uses `from typing import Optional`).
- Use `Dict`, `List`, `Tuple` from `typing` module (existing convention) rather than lowercase `dict`, `list`, `tuple`.
- For functions that return inewave objects, use the concrete type (e.g., `-> Dger`, `-> Pmo`) where available.

### Pitfalls to Avoid

- Do NOT add mypy to CI or enforce `mypy --strict` -- the inewave library lacks type stubs and would generate hundreds of errors. The goal is incremental improvement, not full strict typing.
- Do NOT remove `_calc_quantiles` and `_calc_mean_std` if any test file imports them -- check tests first.
- Do NOT remove `# type: ignore` on inewave imports that genuinely lack type stubs (e.g., `from dateutil.relativedelta import relativedelta  # type: ignore`).
- Do NOT change function signatures or behavior while adding type annotations -- this ticket is annotation-only, not refactoring.

## Testing Requirements

### Unit Tests

No new unit tests required. All existing tests must pass unchanged after dead code removal and annotation additions.

### Integration Tests

Run `python -m pytest tests/ -x` to verify no import breakage.

### E2E Tests

Not applicable -- this is a cleanup ticket with no behavioral changes.

## Dependencies

- **Blocked By**: ticket-017-decompose-files.md
- **Blocks**: None

## Effort Estimate

**Points**: 3
**Confidence**: High
