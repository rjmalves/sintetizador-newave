# ticket-006 Port exchange.py and energy.py to polars

## Context

### Background

`app/services/deck/exchange.py` (144 lines) computes exchange bounds between submarkets. `app/services/deck/energy.py` (333 lines) computes stored energy bounds. Both use pandas operations including `apply(lambda)`, `groupby`, `join`, and `concat`. These are medium-complexity modules that depend on entities.py and temporal.py (both already ported in tickets 004-005).

### Relation to Epic

This is the third ticket of Epic 2. Exchange and energy are grouped together because they are both medium-complexity, have no mutual dependencies, and together represent a manageable scope. After this ticket, the simpler deck domain modules are done, leaving only hydro.py, storage.py, thermal.py, and policy.py.

### Current State

**exchange.py** (4 pandas ops):

- `_drops_exchange_direction_flag()`: Swaps source/target codes using `.loc[]` assignment
- `_cast_exchange_bounds_to_MWmes()`: Uses `.apply(lambda, axis=1)` for MWmes conversion, `np.tile()` for block length multiplication
- `exchange_block_limits()`: `pd.concat()` + `sort_values(inplace=True)` for pat0 evaluation
- `exchange_bounds()`: Orchestrates the above, uses `.reset_index(drop=True)`

**energy.py** (16 pandas ops):

- `convergence()`: Simple accessor, returns pmo convergence table
- `stored_energy_upper_bounds_inputs()`: Complex function with nested helpers using `iterrows()`, `groupby().sum()`, `pd.concat()`, `join()`, `apply(lambda)`
- `stored_energy_upper_bounds_pmo()`: Uses `pd.concat()`, `np.repeat()`, `join()`, `apply(lambda)`
- `eer_stored_energy_lower_bounds()`: Uses `groupby()`, `pd.concat()`, `sort_values()`, `join()`

## Specification

### Requirements

1. All functions in exchange.py must use polars operations
2. All functions in energy.py must use polars operations
3. Remove `import pandas as pd` from both files
4. Remove `.to_pandas()` shims from ticket-003
5. Remove `.copy()` calls on cached returns
6. The `apply(lambda)` pattern in `_cast_exchange_bounds_to_MWmes()` must be vectorized as a polars join + arithmetic expression
7. The `iterrows()` loop in `stored_energy_upper_bounds_inputs()` must be converted to polars operations

### Inputs/Props

- `pl.DataFrame` from accessors, entities, temporal, misc
- Scalar values from temporal functions

### Outputs/Behavior

- `exchange_bounds()` returns `pl.DataFrame`
- `exchange_block_limits()` returns `pl.DataFrame`
- `stored_energy_upper_bounds()` returns `pl.DataFrame`
- `eer_stored_energy_lower_bounds()` returns `pl.DataFrame`
- `convergence()` returns `pl.DataFrame`

### Error Handling

- Same as current (readers.validate_data for initial validation)
- `stored_energy_upper_bounds_pmo()` returns `None` if pmo data is missing -- this stays unchanged

## Acceptance Criteria

- [ ] Given `exchange.py`, when searching for `import pandas`, then zero matches are found
- [ ] Given `energy.py`, when searching for `import pandas`, then zero matches are found
- [ ] Given `exchange.exchange_bounds()`, when called, then it returns a `pl.DataFrame` with exchange source/target codes and MWmes values
- [ ] Given `energy.stored_energy_upper_bounds()`, when called, then it returns a `pl.DataFrame` with EER energy bounds per stage
- [ ] Given the full test suite, when run after this ticket, then all tests pass

## Implementation Guide

### Suggested Approach

**exchange.py:**

1. `_drops_exchange_direction_flag()`: Replace `.loc[]` conditional swap with polars `when/then/otherwise`:

   ```python
   df = df.with_columns([
       pl.when(pl.col("sentido") == 1)
         .then(pl.col(EXCHANGE_TARGET_CODE_COL))
         .otherwise(pl.col(EXCHANGE_SOURCE_CODE_COL))
         .alias(EXCHANGE_SOURCE_CODE_COL),
       pl.when(pl.col("sentido") == 1)
         .then(pl.col(EXCHANGE_SOURCE_CODE_COL))
         .otherwise(pl.col(EXCHANGE_TARGET_CODE_COL))
         .alias(EXCHANGE_TARGET_CODE_COL),
   ]).drop("sentido")
   ```

   Note: This requires reading source and target before overwriting. Store originals first or use a single `with_columns` that reads both before writing.

2. `_cast_exchange_bounds_to_MWmes()`: Replace `apply(lambda)` with a join:

   ```python
   block_bounds = block_bounds.join(
       average_bounds.select([SOURCE, TARGET, START_DATE, VALUE_COL]),
       on=[SOURCE, TARGET, START_DATE],
       suffix="_avg"
   ).with_columns(
       (pl.col(VALUE_COL) * pl.col(VALUE_COL + "_avg")).alias(VALUE_COL)
   ).drop(VALUE_COL + "_avg")
   ```

   Then the `np.tile()` block length multiplication becomes a join on `[START_DATE, BLOCK]`.

3. `exchange_block_limits()` pat0: Use polars `group_by` + `pl.concat` + `sort()`

**energy.py:**

1. `convergence()`: Simple `pl.from_pandas()` at cache, remove `.copy()`
2. `stored_energy_upper_bounds_inputs()`: The `iterrows()` loop over configuration dates must become a cross-join or loop that builds polars DataFrames per stage, then `pl.concat()`. The nested `_join_*` helpers become polars `.join()` calls. The `_volume_to_energy()` helper uses `.loc[]` conditional assignment, which becomes `with_columns(when/then)`.
3. `stored_energy_upper_bounds_pmo()`: Replace `apply(lambda)` with join + select. Replace `np.repeat()` with polars cross-join or `pl.DataFrame.join()`.
4. `eer_stored_energy_lower_bounds()`: Replace groupby, concat, sort with polars equivalents.

### Key Files to Modify

- `app/services/deck/exchange.py` -- full rewrite to polars
- `app/services/deck/energy.py` -- full rewrite to polars

### Patterns to Follow

- `pl.when(condition).then(value).otherwise(other)` for conditional column assignment
- `df.join(other, on=cols)` for lookup-style operations replacing `apply(lambda)`
- `df.group_by(cols).agg(...)` for aggregation
- `pl.concat([dfs])` for concatenation

### Pitfalls to Avoid

- The column swap in `_drops_exchange_direction_flag()` must read both original values before overwriting -- use a single `with_columns` with temporary aliases or `pl.struct`
- `stored_energy_upper_bounds_inputs()` has an `iterrows()` loop that is genuinely iterative (each iteration processes one configuration date). This can stay as a Python loop over dates, building polars DataFrames per iteration, then `pl.concat()` at the end
- `energy.py` imports from `storage` and `hydro` lazily to avoid circular imports -- these lazy imports stay unchanged
- `stored_energy_upper_bounds_pmo()` may return `None` -- callers handle this, do not change the None return

## Testing Requirements

### Unit Tests

- Run full test suite to verify no regressions
- Verify exchange bounds contain correct source/target/value columns
- Verify energy bounds contain correct EER/stage/value columns

### Integration Tests

- The test suite exercises these through the synthesis pipeline

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-004-port-entities-polars.md, ticket-005-port-temporal-misc-polars.md
- **Blocks**: ticket-007-port-hydro-polars.md (energy.py calls hydro functions; hydro.py calls energy functions via lazy import)

## Effort Estimate

**Points**: 3
**Confidence**: Medium (the `apply(lambda)` to join vectorization requires careful column handling)

## Out of Scope

- Porting hydro.py, storage.py, thermal.py (separate tickets)
- Porting policy.py (separate ticket)
- Optimizing the iterrows loop in energy.py beyond converting to polars
