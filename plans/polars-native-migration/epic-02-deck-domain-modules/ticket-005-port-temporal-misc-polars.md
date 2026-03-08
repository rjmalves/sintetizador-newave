# ticket-005 Port temporal.py and misc.py to polars

## Context

### Background

`app/services/deck/temporal.py` manages time period calculations and `app/services/deck/misc.py` provides block lengths, costs, and other utility data. Both modules use pandas for their DataFrame operations. Most of temporal.py returns scalar values (int, datetime, list) rather than DataFrames, making it a light migration. misc.py has 3 pandas operations, primarily in `block_lengths()` which uses `groupby().sum()` and `pd.concat()`.

### Relation to Epic

This is the second ticket of Epic 2. temporal.py and misc.py are dependencies for hydro.py, thermal.py, energy.py, exchange.py, and the synthesis pipeline. Porting them early removes shims from multiple downstream consumers.

### Current State

**temporal.py** (~500 lines):

- Most functions return scalars (int, datetime, List[datetime]) by reading dger/pmo fields -- no DataFrame operations needed
- `configurations_pmo()`: returns `pd.DataFrame` from pmo.configuracoes_gerais_estudo
- `configurations_dger()`: returns `pd.DataFrame` constructed manually
- `configurations()`: calls one of the above, returns `pd.DataFrame`
- `consider_post_study_years()`: modifies a `pd.DataFrame` in-place (filters + concat for post-study period extension)
- `stages_starting_dates_final_simulation()`: returns `List[datetime]`

**misc.py** (145 lines):

- `costs()`: returns `pd.DataFrame` from pmo (simple accessor, cached)
- `block_lengths()`: uses `groupby(START_DATE_COL, as_index=False).sum()`, `pd.concat()`, `sort_values()`, returns `pd.DataFrame`
- `runtimes()`: returns `pd.DataFrame` from newavetim (simple accessor)
- Scalar functions (num_blocks, num_iterations, etc.): no DataFrame operations

## Specification

### Requirements

1. All DataFrame-returning functions in temporal.py must return `pl.DataFrame`
2. All DataFrame-returning functions in misc.py must return `pl.DataFrame`
3. `consider_post_study_years()` must accept and return `pl.DataFrame` (it is called by energy.py, thermal.py, exchange.py, entities.py with their DataFrames)
4. Remove `import pandas as pd` from both files (replace with `import polars as pl`)
5. Remove `.to_pandas()` shims added in ticket-003 for these files
6. Remove `.copy()` calls on returns
7. Scalar-returning functions (int, datetime, list, bool) remain unchanged

### Inputs/Props

- Polars DataFrames from accessors
- Scalar configuration values from dger, pmo

### Outputs/Behavior

- `configurations()` returns `pl.DataFrame` with columns `[START_DATE_COL, VALUE_COL]`
- `block_lengths()` returns `pl.DataFrame` with columns `[START_DATE_COL, BLOCK_COL, VALUE_COL]`
- `consider_post_study_years()` accepts and returns `pl.DataFrame`
- `costs()` and `runtimes()` return `pl.DataFrame`

### Error Handling

- Same as current -- validation happens in readers.validate_data before conversion

## Acceptance Criteria

- [ ] Given `temporal.py`, when searching for `import pandas`, then zero matches are found
- [ ] Given `misc.block_lengths()`, when called, then it returns a `pl.DataFrame` with the pat0 row (BLOCK_COL=0) computed as the sum of block durations per date
- [ ] Given `temporal.consider_post_study_years()`, when called with a `pl.DataFrame`, then it returns a `pl.DataFrame` with post-study period rows appended
- [ ] Given the full test suite, when run after this ticket, then all tests pass (with shim updates for downstream consumers that call these functions)

## Implementation Guide

### Suggested Approach

**temporal.py:**

1. Replace `import pandas as pd` with `import polars as pl`
2. `configurations_pmo()`: Convert pmo result with `pl.from_pandas()` at cache insertion
3. `configurations_dger()`: Build polars DataFrame directly: `pl.DataFrame({"col": values})`
4. `configurations()`: No changes needed beyond what the above provide
5. `consider_post_study_years()`: Convert pandas in-place mutation to polars functional style:
   - `df.loc[condition]` becomes `df.filter(condition)`
   - `pd.concat([df, extension])` becomes `pl.concat([df, extension])`
   - The function signature changes from `pd.DataFrame` to `pl.DataFrame`
6. `stages_starting_dates_final_simulation()` and other list-returning functions: No changes needed

**misc.py:**

1. Replace `import pandas as pd` with `import polars as pl`
2. `costs()`: Convert pmo result with `pl.from_pandas()`, remove `.copy()`
3. `block_lengths()`: Rewrite `__eval_pat0`:
   - `df.groupby(START_DATE_COL, as_index=False).sum()` becomes `df.group_by(START_DATE_COL).agg(pl.col(VALUE_COL).sum())`
   - `pd.concat([df, df_pat_0])` becomes `pl.concat([df, df_pat_0])`
   - `df.sort_values(...)` becomes `df.sort(...)`
4. `runtimes()`: Convert newavetim result, remove `.copy()`

### Key Files to Modify

- `app/services/deck/temporal.py` -- port DataFrame functions to polars
- `app/services/deck/misc.py` -- port DataFrame functions to polars
- Downstream consumers with shims for temporal/misc calls -- update shims

### Patterns to Follow

- `df.group_by(col).agg(pl.col(val).sum())` for groupby+sum
- `pl.concat([df1, df2])` for concatenation
- `df.sort(cols)` for sorting
- `df.filter(pl.col(col) condition)` for filtering

### Pitfalls to Avoid

- `consider_post_study_years()` is called by many modules (entities.py, thermal.py, exchange.py) -- its signature change must be coordinated with the callers' migration status. Callers not yet migrated need a `.to_pandas()` shim after calling it
- `block_lengths()` groupby: pandas `as_index=False` has no polars equivalent -- polars `group_by` always returns all grouping columns as regular columns
- The `sort_values(inplace=True)` pattern in misc.py must become a reassignment: `df = df.sort(...)`

## Testing Requirements

### Unit Tests

- Run full test suite to verify no regressions
- Verify `block_lengths()` returns correct pat0 aggregation

### Integration Tests

- Synthesis pipeline uses block_lengths via DeckContext -- verify via existing tests

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-004-port-entities-polars.md
- **Blocks**: ticket-006-port-exchange-energy-polars.md, ticket-007-port-hydro-polars.md, ticket-008-port-storage-thermal-polars.md

## Effort Estimate

**Points**: 2
**Confidence**: High

## Out of Scope

- Porting energy.py, exchange.py, hydro.py, thermal.py (separate tickets)
- Porting the synthesis pipeline (Epic 3)
