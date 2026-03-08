# ticket-008 Port storage.py and thermal.py to polars

## Context

### Background

`app/services/deck/storage.py` (371 lines) computes initial stored energy and volume using hydro productivity calculations with `apply(lambda)` for polynomial evaluation and graph BFS traversal. `app/services/deck/thermal.py` (346 lines) computes thermal generation bounds using `resample().ffill()` for maintenance application and `iterrows()` for change application. Both modules depend on entities, temporal, hydro (all ported in previous tickets).

### Relation to Epic

This is the fifth ticket of Epic 2. Storage and thermal are the last complex domain modules before policy.py. Storage depends heavily on hydro.py (ticket-007) for volume bounds and drops. Thermal depends on entities and temporal.

### Current State

**storage.py** (18 pandas ops):

- `evaluate_productivity()`: Uses `df.apply(lambda, axis=1)` for polynomial evaluation (height-area-volume curves), `np.polyval()`, conditional logic per row
- `accumulate_productivity()`: Graph BFS traversal with `.at[idx, col]` mutations
- `_hydro_accumulated_productivity_at_volume()`: Multiple `.join()` calls with indexed DataFrames
- `_initial_stored_energy_from_pmo()`: `.apply(lambda)`, `.set_index()`, `.sort_index()`
- `_initial_stored_energy_from_confhd_hidr()`: Complex with nested helpers, `.join()`, `.groupby().sum()`, `pd.concat()`
- `initial_stored_volume()`: `.apply(lambda)` for percentage-to-hm3 conversion

**thermal.py** (7 pandas ops):

- `_apply_thermal_single_change()`: `.loc[]` conditional assignment
- `_apply_thermal_bounds_maintenance_and_changes()`: `resample("D").ffill()` + `resample("MS").mean()` for daily interpolation and back to monthly, `iterrows()` for maintenance changes
- `_thermal_generation_bounds_term_manutt_expt()`: Stage expansion with `pd.concat`, `iterrows` for changes
- `_thermal_generation_bounds_pmo()`: Simple accessor with filter
- `thermal_costs()`: `apply(lambda)` for date construction, `iterrows` for cost changes

## Specification

### Requirements

1. All functions in storage.py must use polars operations
2. All functions in thermal.py must use polars operations
3. Remove `import pandas as pd` from both files
4. The `evaluate_productivity()` function must be rewritten with polars expressions for polynomial evaluation
5. The `accumulate_productivity()` graph traversal can keep its Python loop but must use polars operations for cell access
6. The `resample().ffill()` pattern in thermal.py must be replaced with polars `upsample()` + `forward_fill()` or an equivalent date-range expansion approach
7. `iterrows()` loops for applying thermal changes must be converted to vectorized operations or kept as Python loops with polars mutations

### Inputs/Props

- `pl.DataFrame` from accessors, entities, hydro, temporal, misc
- Graph structure from hydro cascade topology

### Outputs/Behavior

- `initial_stored_energy()` returns `pl.DataFrame`
- `initial_stored_volume()` returns `pl.DataFrame`
- `thermal_generation_bounds()` returns `pl.DataFrame`
- `thermal_costs()` returns `pl.DataFrame`

### Error Handling

- Same as current -- `_initial_stored_energy_from_pmo()` returns None if pmo data missing
- `_thermal_generation_bounds_pmo()` returns None if pmo data missing

## Acceptance Criteria

- [ ] Given `storage.py`, when searching for `import pandas`, then zero matches are found
- [ ] Given `thermal.py`, when searching for `import pandas`, then zero matches are found
- [ ] Given `storage.initial_stored_energy()`, when called, then it returns a `pl.DataFrame` with EER energy data
- [ ] Given `thermal.thermal_generation_bounds()`, when called, then it returns a `pl.DataFrame` with thermal code, stage, lower bound, and upper bound columns
- [ ] Given the full test suite, when run after this ticket, then all tests pass

## Implementation Guide

### Suggested Approach

**storage.py:**

1. `evaluate_productivity()`: The polynomial evaluation uses `np.polyval()` which works on scalars. Options:
   a. Use `map_elements()` for the polynomial evaluation (slower but correct)
   b. Vectorize by computing the polynomial terms as polars expressions: `sum(coef_i * vol^i for i in range(5))`
   c. Hybrid: use numpy on the column arrays directly (`.to_numpy()`) then assign back

   Recommended: Option (c) -- extract columns as numpy arrays, compute vectorized, assign back:

   ```python
   coefs = [df[c].to_numpy() for c in HEIGHT_POLY_COLS]
   volumes = df[volume_col].to_numpy()
   result = np.polyval(coefs_as_matrix, volumes)
   df = df.with_columns(pl.Series(UPPER_DROP_COL, result))
   ```

2. `accumulate_productivity()`: The graph BFS loop with `.at[idx, col]` mutations. Convert to:

   ```python
   prod = df[PRODUCTIVITY_TMP_COL].to_list()
   following = df[FOLLOWING_HYDRO_COL].to_list()
   codes = df[HYDRO_CODE_COL].to_list()
   code_to_idx = {c: i for i, c in enumerate(codes)}
   for hydro_code in bfs:
       downstream = following[code_to_idx[hydro_code]]
       if downstream == 0:
           continue
       prod[code_to_idx[hydro_code]] += prod[code_to_idx[downstream]]
   df = df.with_columns(pl.Series(PRODUCTIVITY_TMP_COL, prod))
   ```

3. `_initial_stored_energy_from_confhd_hidr()`: Replace `.join()` calls with polars `.join(on=...)`, replace `.groupby().sum()` with `.group_by().agg()`, replace `pd.concat()` with `pl.concat()`.

**thermal.py:**

1. `_apply_thermal_single_change()`: Replace `.loc[]` with polars `when/then`:

   ```python
   df = df.with_columns(
       pl.when(
           (pl.col(THERMAL_CODE_COL) == thermal_code)
           & (pl.col(START_DATE_COL) >= start_date)
           & (pl.col(START_DATE_COL) <= end_date)
       ).then(pl.lit(value))
       .otherwise(pl.col(col))
       .alias(col)
   )
   ```

2. `resample().ffill()` pattern (thermal maintenance): This is a pandas-specific operation for upsampling monthly data to daily, applying maintenance changes, then downsampling back to monthly. In polars:
   a. Create a daily date range with `pl.date_range()`
   b. Join the monthly data to the daily range
   c. Use `forward_fill()` to fill gaps
   d. Apply maintenance subtractions
   e. Group by month and take mean

3. `_thermal_generation_bounds_term_manutt_expt()`: Replace `pd.concat` stage expansion with cross-join. Replace `iterrows` change application with vectorized `when/then`.

### Key Files to Modify

- `app/services/deck/storage.py` -- full rewrite to polars
- `app/services/deck/thermal.py` -- full rewrite to polars

### Patterns to Follow

- Extract to numpy, compute, assign back for complex mathematical operations
- `pl.when().then().otherwise()` for conditional mutations
- `pl.date_range()` for date sequence generation
- `.forward_fill()` for ffill equivalent
- Python loops with list extraction for graph traversal mutations

### Pitfalls to Avoid

- `evaluate_productivity()` has different formulas for regulated ("M") vs run-of-river plants -- the conditional logic must be preserved
- `accumulate_productivity()` depends on BFS ordering -- the Graph class returns nodes in the correct order
- The thermal maintenance `resample("D").ffill()` + `resample("MS").mean()` pattern is tricky in polars. Consider using `upsample()` or building the daily range manually
- `thermal_costs()` uses `apply(lambda line: datetime(...))` to construct dates from year+month -- replace with `pl.date(year_expr, month_expr, 1)` or equivalent
- storage.py has circular import avoidance (lazy imports of hydro) -- preserve this pattern

## Testing Requirements

### Unit Tests

- Run full test suite to verify no regressions
- Verify initial stored energy values are numerically consistent with current output
- Verify thermal bounds are correct for a case with maintenance and expansions

### Integration Tests

- The synthesis pipeline exercises both through bounds resolution

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-007-port-hydro-polars.md, ticket-004-port-entities-polars.md, ticket-005-port-temporal-misc-polars.md
- **Blocks**: ticket-009-port-policy-polars.md

## Effort Estimate

**Points**: 5
**Confidence**: Medium (the productivity polynomial evaluation and resample/ffill patterns are the most challenging conversions)

## Out of Scope

- Porting policy.py (separate ticket)
- Optimizing the graph BFS traversal
- Porting the synthesis pipeline (Epic 3)
