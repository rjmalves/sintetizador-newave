# ticket-007 Port hydro.py to polars

## Context

### Background

`app/services/deck/hydro.py` (480 lines) computes hydro volume bounds, turbined flow bounds, outflow bounds, and hydro drops for all hydro plants. It is the most complex deck domain module, with 19 pandas operations including `apply()` with row-level calculations, `set_index()`/`.at[]` patterns, `pd.concat()` for stage expansion, `join()` with indexed DataFrames, and interactions with `readers.apply_modif_changes_to_hydros()` and `readers.apply_modif_changes_to_hydros_in_stages()`.

### Relation to Epic

This is the fourth ticket of Epic 2. hydro.py depends on entities.py (ticket-004) and temporal.py (ticket-005). It is a dependency for storage.py (ticket-008) which uses hydro volume bounds and drops in its productivity calculations.

### Current State

`app/services/deck/hydro.py` key functions and their pandas patterns:

1. `hydro_volume_bounds()`: `hidr.reset_index()`, `.loc[].set_index()`, `.rename()`, `.join()`
2. `hydro_volume_bounds_with_changes()`: `.copy()`, `.apply(lambda, axis=1)` for percentage-to-hm3 conversion, `.loc[]` assignment
3. `_hydro_volume_bounds_in_stages()`: `pd.concat([base] * N)`, `np.repeat()`, `.sort_values()`, `.apply(lambda)`
4. `hydro_turbined_flow_bounds()`: `.apply(_calc_turbined_flow, axis=1)` (row-level numpy computation), `.set_index()`, `.join()`
5. `_apply_turbined_flow_changes()`: `modif.modificacoes_usina(idx)` loop with `.at[]` mutations
6. `hydro_outflow_bounds()`: Similar to volume bounds pattern
7. `hydro_outflow_bounds_in_stages()`: Stage expansion with `pd.concat`, block expansion
8. `hydro_drops()`: `.loc[].set_index()`, `.join()`
9. `hydro_drops_in_stages()`: Stage expansion + modif changes
10. `_expand_hydro_to_stages()`: Private helper using `pd.concat([df] * N)`, `np.repeat()`
11. `_expand_to_blocks()`: Private helper using `pd.concat([df] * N)`, `np.tile()`

Critical challenge: `readers.apply_modif_changes_to_hydros()` and `readers.apply_modif_changes_to_hydros_in_stages()` in `readers.py` currently accept and mutate `pd.DataFrame` in-place. Since readers.py stays pandas (it's the inewave boundary), hydro.py must convert to pandas before calling these functions, then convert back.

## Specification

### Requirements

1. All functions in hydro.py must use polars for internal operations
2. Remove `import pandas as pd` and add `import polars as pl`
3. Remove all `.copy()` calls on returns
4. The `_calc_turbined_flow()` function (line 41) processes a row as a `pd.Series` -- it must be rewritten as a polars expression or `map_elements` function
5. For `readers.apply_modif_changes_to_hydros()` and `readers.apply_modif_changes_to_hydros_in_stages()` calls: convert to pandas before calling, convert back after. These are boundary calls to readers.py which stays pandas.
6. Stage expansion helpers must use polars equivalents of `pd.concat([df] * N)` + `np.repeat()`
7. The percentage-to-hm3 conversion in `hydro_volume_bounds_with_changes()` must be vectorized

### Inputs/Props

- `pl.DataFrame` from accessors (hidr, confhd, modif)
- `pl.DataFrame` from entities (hydro_eer_submarket_map, hydro_code_order)
- `pl.DataFrame` from temporal (stages_starting_dates)
- Scalar values from misc (num_blocks)

### Outputs/Behavior

- All hydro functions return `pl.DataFrame`
- Stage-expanded DataFrames use polars construction instead of `pd.concat([df] * N)`
- Bounds computations are vectorized using polars expressions

### Error Handling

- Same as current (validation happens in accessors/readers before hydro.py)
- The `_apply_turbined_flow_changes()` function iterates over hydro codes from modif -- this loop stays as a Python loop but operates on polars DataFrames

## Acceptance Criteria

- [ ] Given `hydro.py`, when searching for `import pandas`, then zero matches are found (except potentially for the readers.py boundary conversion)
- [ ] Given `hydro.hydro_volume_bounds()`, when called, then it returns a `pl.DataFrame` with columns `[HYDRO_CODE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL, ...]`
- [ ] Given `hydro.hydro_volume_bounds_in_stages()`, when called, then it returns a `pl.DataFrame` with bounds expanded to all study period stages
- [ ] Given `hydro._calc_turbined_flow()` equivalent, when applied to a hydro with 3 conjuntos, then it correctly sums `num_machines * flow_per_machine` across all conjuntos
- [ ] Given the full test suite, when run after this ticket, then all tests pass

## Implementation Guide

### Suggested Approach

**Stage expansion pattern** (used in 3+ functions):
Replace `pd.concat([df] * N)` + `np.repeat(dates, num)` with:

```python
dates_df = pl.DataFrame({START_DATE_COL: dates})
expanded = df.join(dates_df, how="cross")
expanded = expanded.sort([HYDRO_CODE_COL, START_DATE_COL])
```

**`_calc_turbined_flow()` rewrite**:
The current function accesses `line["numero_conjuntos_maquinas"]`, then dynamically accesses columns `f"maquinas_conjunto_{i}"` and `f"vazao_nominal_conjunto_{i}"`. In polars, rewrite as:

```python
def _calc_turbined_flow_expr(max_conjuntos: int) -> pl.Expr:
    terms = []
    for i in range(1, max_conjuntos + 1):
        terms.append(
            pl.col(f"maquinas_conjunto_{i}").fill_null(0)
            * pl.col(f"vazao_nominal_conjunto_{i}").fill_null(0)
        )
    return sum(terms)  # polars Expr addition
```

Where `max_conjuntos` can be determined from the DataFrame columns.

**Percentage-to-hm3 conversion** in `hydro_volume_bounds_with_changes()`:
Replace `.apply(lambda)` with polars expressions:

```python
df = df.with_columns(
    pl.when(pl.col(unit_col) == Unit.perc_modif.value)
    .then(
        pl.col(col) * (hm3_upper - hm3_lower) / 100.0 + hm3_lower
    )
    .otherwise(pl.col(col))
    .alias(col)
)
```

Where `hm3_upper` and `hm3_lower` come from a join with the base bounds DataFrame.

**readers.py boundary calls**:

```python
# Convert to pandas for readers.apply_modif_changes_to_hydros
df_pd = df.to_pandas()
df_pd = readers.apply_modif_changes_to_hydros(deck_cls, cache, df_pd, ...)
df = pl.from_pandas(df_pd)
```

### Key Files to Modify

- `app/services/deck/hydro.py` -- full rewrite to polars

### Patterns to Follow

- Cross-join for stage expansion: `df.join(dates_df, how="cross")`
- `pl.when().then().otherwise()` for conditional value assignment
- Vectorized arithmetic expressions instead of `apply(lambda)`
- `.to_pandas()` / `pl.from_pandas()` only at readers.py boundary

### Pitfalls to Avoid

- `_apply_turbined_flow_changes()` mutates individual cells via `.at[idx, col]` -- in polars, mutations require rebuilding the DataFrame. Consider converting to pandas just for this function, then back to polars
- The `_hydro_volume_bounds_in_stages()` function has a complex percentage-to-hm3 conversion that uses `hm3_expanded.loc[line.name, ...]` referencing another DataFrame by positional index. This must be replaced with a join on `[HYDRO_CODE_COL, START_DATE_COL]`
- `hydro_drops_in_stages()` calls `readers.apply_modif_changes_to_hydros_in_stages()` which takes `pd.DataFrame` -- boundary conversion needed
- The `set_index(HYDRO_CODE_COL)` pattern is used to enable `.join()` by index -- in polars, use explicit `on=HYDRO_CODE_COL` in joins

## Testing Requirements

### Unit Tests

- Run full test suite to verify no regressions
- Verify hydro bounds DataFrames have correct shape (num_hydros \* num_stages rows for stage-expanded variants)

### Integration Tests

- The synthesis pipeline exercises hydro bounds through bounds resolution

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-004-port-entities-polars.md, ticket-005-port-temporal-misc-polars.md
- **Blocks**: ticket-008-port-storage-thermal-polars.md

## Effort Estimate

**Points**: 5
**Confidence**: Medium (the modif changes boundary and \_calc_turbined_flow rewrite add complexity)

## Out of Scope

- Modifying readers.py or its apply_modif_changes functions (stays pandas)
- Porting storage.py or thermal.py (separate ticket)
- Optimizing the modif change application loop
