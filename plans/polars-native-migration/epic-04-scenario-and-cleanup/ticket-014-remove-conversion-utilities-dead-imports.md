# ticket-014 Remove conversion utilities and dead pandas imports

## Context

### Background

After ticket-013, all synthesis modules (`scenario.py`, `execution.py`, `system.py`, `operation/*`) use polars natively. The `pd_to_pl()` and `pl_to_pd()` utility functions in `app/utils/dataframe.py` are no longer called by any production code. The `__init__.py` re-export in the operation package is also dead. Several modules still have `import pandas as pd` that is no longer used (only needed for metadata export or boundary code). Additionally, there are remaining `# SHIM` annotations in `deck.py`, `readers.py`, and `bounds.py` that should be evaluated.

### Relation to Epic

This is the second and final ticket of Epic 4 and the entire plan. After this ticket, the polars-native migration is complete.

### Current State

**`pd_to_pl`/`pl_to_pd` callers** (after ticket-013 removes scenario.py usage):

- `app/services/synthesis/operation/__init__.py` line 5: re-export (`# noqa: F401`)
- `tests/app/utils/test_polars_concat_sort.py`: 12 occurrences in test utilities

**`import pandas as pd` in app/ modules** (after ticket-013):

- `app/utils/dataframe.py` — defines `pd_to_pl`/`pl_to_pd` (will be deleted)
- `app/utils/operations.py` — `fast_group_df()` uses pandas groupby (check if still used)
- `app/utils/tz.py` — timezone utility using pandas
- `app/adapters/repository/files.py` — NWLISTOP reader, permanent boundary
- `app/adapters/repository/export.py` — export adapter, permanent boundary
- `app/services/deck/accessors.py` — inewave accessor, permanent boundary
- `app/services/deck/readers.py` — inewave reader, permanent boundary
- `app/services/deck/deck.py` — Deck facade (check if still needed after shim removal)
- `app/services/deck/bounds.py` — has 1 SHIM at line 1525
- `app/services/deck/hydro.py` — TYPE_CHECKING guard only
- `app/services/deck/temporal.py` — TYPE_CHECKING guard only
- `app/services/synthesis/operation/orchestrator.py` — check if still needed
- `app/services/synthesis/operation/export.py` — metadata export uses pandas
- `app/services/synthesis/operation/pipeline.py` — NWLISTOP boundary conversion
- `app/services/synthesis/policy.py` — check if still needed

**Remaining `# SHIM` annotations** (after ticket-013):

- `app/services/deck/deck.py` line 301: bounds.py pandas shim
- `app/services/deck/readers.py` line 418: temporal.py pandas shim
- `app/services/deck/bounds.py` line 1525: `.to_pandas()` shim

## Specification

### Requirements

1. **Delete `pd_to_pl()` and `pl_to_pd()`** from `app/utils/dataframe.py`. If this file has no other functions, delete the file entirely.
2. **Remove re-export** from `app/services/synthesis/operation/__init__.py` line 5.
3. **Delete or port `test_polars_concat_sort.py`**: These tests validate `pd_to_pl`/`pl_to_pd` behavior. Either delete the test file (if the utilities are removed) or port to use `pl.from_pandas()`/`.to_pandas()` directly.
4. **Remove dead `import pandas as pd`** from modules where pandas is no longer used at runtime. Keep it in:
   - Boundary code: `files.py`, `export.py` (adapters), `accessors.py`, `readers.py`
   - `pipeline.py` (NWLISTOP boundary `pl.from_pandas()`)
   - `operation/export.py` (metadata export)
   - TYPE_CHECKING guards (hydro.py, temporal.py)
5. **Evaluate remaining `# SHIM` annotations** in `deck.py`, `readers.py`, `bounds.py`:
   - `deck.py:301` — bounds.py still uses pandas for some Deck accessors. Check if this is still true after epic-03.
   - `readers.py:418` — temporal.py accepts `pd.DataFrame` for 3 functions. Check if these are still called with pandas.
   - `bounds.py:1525` — `.to_pandas()` shim. Check if this can be removed.
   - Remove SHIM annotations that are no longer relevant. Keep those that are still active boundary code.
6. **Check `fast_group_df()` in `operations.py`**: If it uses pandas and is still called, evaluate whether to port or remove. If unused, delete it.
7. **Final verification**: Grep the entire `app/` directory for `pd_to_pl`, `pl_to_pd`, and `# SHIM`. Only permanent boundary annotations should remain.

### Outputs/Behavior

- `app/utils/dataframe.py` deleted or stripped of `pd_to_pl`/`pl_to_pd`
- No `pd_to_pl`/`pl_to_pd` calls remain in production code
- No dead `import pandas as pd` in migrated modules
- All `# SHIM` annotations in synthesis modules removed; only permanent boundary annotations remain in deck modules

## Acceptance Criteria

- [ ] Given a search for `pd_to_pl` or `pl_to_pd` in `app/`, when the search completes, then zero matches are found
- [ ] Given a search for `# SHIM` in `app/services/synthesis/`, when the search completes, then zero matches are found
- [ ] Given a search for `import pandas as pd` in `app/services/synthesis/operation/`, when the search completes, then matches are found ONLY in files that have legitimate pandas usage (export.py metadata, pipeline.py NWLISTOP boundary, orchestrator.py if still needed)
- [ ] Given all existing tests are executed via `pytest`, when the test suite completes, then all tests pass with zero failures

## Implementation Guide

### Suggested Approach

1. **Delete `pd_to_pl`/`pl_to_pd`**: Check `app/utils/dataframe.py` for other functions. If only these two exist, delete the file. Otherwise remove just these two functions.
2. **Remove `__init__.py` re-export**: Delete line 5 from `app/services/synthesis/operation/__init__.py`.
3. **Handle test file**: Delete `tests/app/utils/test_polars_concat_sort.py` since it only tests the deleted utilities.
4. **Sweep dead imports**: For each module with `import pandas as pd`, check if any pandas operation remains. Remove the import if not needed.
5. **Evaluate SHIM annotations**: Read each annotated line, determine if the shim is still active, remove annotation or the shim itself.
6. **Run full test suite** to verify.

### Key Files to Modify

- `app/utils/dataframe.py` — delete or strip pd_to_pl/pl_to_pd
- `app/services/synthesis/operation/__init__.py` — remove re-export
- `tests/app/utils/test_polars_concat_sort.py` — delete
- `app/services/synthesis/operation/orchestrator.py` — remove dead import if applicable
- `app/services/deck/deck.py` — evaluate SHIM at line 301
- `app/services/deck/bounds.py` — evaluate SHIM at line 1525
- `app/services/deck/readers.py` — evaluate SHIM at line 418
- Various modules — remove dead `import pandas as pd`

### Pitfalls to Avoid

- **Do not remove pandas from boundary code**: `files.py`, `export.py` (adapters), `accessors.py`, `readers.py` legitimately use pandas for inewave interop.
- **`operations.py` `fast_group_df()`**: Check callers before removing. If it's called from scenario.py (now polars), it may need porting rather than deletion.
- **`deck/bounds.py` SHIM**: This may be a permanent boundary shim if bounds.py still calls pandas-only Deck facade methods. Evaluate carefully.
- **TYPE_CHECKING guards**: `hydro.py` and `temporal.py` use `if TYPE_CHECKING: import pandas as pd` for type annotations on functions that accept pandas from readers. These are correct and should stay.

## Testing Requirements

- All existing tests must pass via `pytest` with zero failures
- Verify no import errors after removing modules/functions

## Dependencies

- **Blocked By**: ticket-013-port-scenario-polars.md
- **Blocks**: None (final ticket)

## Effort Estimate

**Points**: 2
**Confidence**: High (mostly deletions and import cleanup)
