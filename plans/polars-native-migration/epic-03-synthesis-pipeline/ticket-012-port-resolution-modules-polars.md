# ticket-012 Port resolution modules and spatial dispatch to polars

## Context

### Background

After tickets 010 and 011, the pipeline layer (`pipeline.py`, `bounds.py`, `cache.py`, `export.py`, `spatial.py`) operates entirely in polars. `post_resolve()` returns `pl.DataFrame`, `get_from_cache()` returns `pl.DataFrame`, `resolve_bounds()` accepts/returns `pl.DataFrame`, and `calc_statistics()` accepts/returns `pl.DataFrame`. The resolution modules (`resolution_sbm.py`, `resolution_ree.py`, `resolution_uhe.py`, `resolution_ute.py`, `resolution_sbp.py`, `resolution_sin.py`, `resolution_pee.py`) and stubs modules (`stubs.py`, `_stubs_helpers.py`, `_stubs_market.py`) still contain `.to_pandas()` SHIM calls for Deck entity lookups, pandas-based filtering/sorting, and `pd_to_pl()` conversion at resolution boundaries. This ticket removes all remaining pandas shims and conversions in the resolution and stubs modules.

### Relation to Epic

This is the third and final ticket in epic-03. After this ticket completes, the entire synthesis operation pipeline (from NWLISTOP file read through resolution, pipeline, bounds, cache, export, and statistics) operates in native polars end-to-end. The only remaining pandas usage will be in `scenario.py`, `execution.py`, `system.py` (epic-04), the NWLISTOP reader boundary (`uow.files.get_nwlistop()` which returns `pd.DataFrame`), and `export_metadata()` (tiny metadata builder).

### Current State

**Resolution modules with `.to_pandas()` SHIM calls:**

- `resolution_sbm.py` line 77: `Deck.submarkets(uow).to_pandas().reset_index(drop=True)` -- filters real submarkets using `submarkets.loc[submarkets["ficticio"] == 0, :]` and `.sort_values()`, extracts names with `.loc[condition, col].iloc[0]`
- `resolution_ree.py` lines 53-59, 87-91: `deck_context.eer_submarket_map.to_pandas().set_index(EER_CODE_COL)` for per-entity submarket lookup via `.at[]`; `Deck.eers(uow).to_pandas().reset_index(drop=True).sort_values()` for iteration
- `resolution_uhe.py` lines 184-192, 227-231: `deck_context.hydro_eer_submarket_map.to_pandas().set_index(HYDRO_CODE_COL)` for per-entity EER/submarket lookup via `.at[]`; `Deck.hydros(uow).to_pandas().reset_index(drop=True).sort_values()` for iteration; `_limit_stages_with_hydro()` (line 130-143) accepts `pd.DataFrame` and uses `.loc[]` filter
- `resolution_ute.py` lines 79-84: Same submarket lookup pattern as `resolution_sbm.py`; `resolve_GTER_UTE()` has `pd_to_pl()` boundary conversion at lines 121-124 (converting `pd.DataFrame` from `post_resolve_GTER_UTE_entity` to polars for `post_resolve`)
- `resolution_sbp.py` lines 85-87: `Deck.submarkets(uow).to_pandas().reset_index(drop=True)` for submarket pairs

**Stubs modules with `.to_pandas()` SHIM calls:**

- `_stubs_helpers.py` line 110: `Deck.hydros(uow).to_pandas().reset_index(drop=True)` in `calc_accumulated_productivity()`; `fill_initial_storage_df()` and `build_initial_stage_indices()` accept pandas; `two_cache_op()` uses pandas `.to_numpy()` and `.assign()`
- `_stubs_market.py` lines 75-76, 145, 161: `Deck.submarkets(uow).to_pandas()` for MER/MERL resolution; `Deck.non_simulated_generation(uow).to_pandas()` for GUNS
- `stubs.py` lines 296, 335: `Deck.hidr(uow).to_pandas().set_index("codigo_usina")` in `stub_resolve_initial_stored_volumes()` and `stub_EARM_UHE()`; multiple functions use pandas operations (`.assign()`, `.loc[]`, `.set_index()`, `.sort_values()`, `.copy()`) on cache results that are now `pl.DataFrame`

**Hook functions in resolution modules that receive `pl.DataFrame` after ticket-010:**

- `_limit_stages_with_hydro()` in `resolution_uhe.py` (line 130-143): Accepts `pd.DataFrame`, uses `.loc[]` filter. Must change to polars `filter()`.
- `_sort_thermals()` in `resolution_ute.py` (line 71-77): Accepts `pd.DataFrame`, uses `.sort_values()`. Must change to polars `sort()`.

**Return types:**

- `resolve_SBM()`, `resolve_REE()`, `resolve_UHE()`, `resolve_UTE()`, `resolve_SBP()`, `resolve_SIN()`, `resolve_PEE()` currently annotated as returning `Optional[pd.DataFrame]` -- after ticket-011, `post_resolve()` returns `pl.DataFrame`, so these should return `Optional[pl.DataFrame]`.
- `resolve_GTER_UTE_entity()` returns `Optional[pd.DataFrame]` -- after ticket-010, `post_resolve_GTER_UTE_entity()` returns `Optional[pl.DataFrame]`, so this should return `Optional[pl.DataFrame]`.

## Specification

### Requirements

1. **Port entity lookup patterns in resolution modules**: Replace `.to_pandas().reset_index(drop=True).sort_values(COL)` with polars `.sort(COL)` for all Deck entity calls (`Deck.submarkets()`, `Deck.eers()`, `Deck.hydros()`). Replace `.loc[condition, col]` filtering with polars `.filter()` and `.select()`. Replace `deck_context.eer_submarket_map.to_pandas().set_index(KEY).at[idx, COL]` with polars `.filter(pl.col(KEY) == idx).item(0, COL)` or a join-based lookup.
2. **Port `_limit_stages_with_hydro()` hook in `resolution_uhe.py`**: Accept `pl.DataFrame`, use polars `filter()` instead of `.loc[]`.
3. **Port `_sort_thermals()` hook in `resolution_ute.py`**: Accept `pl.DataFrame`, use polars `sort()` instead of `.sort_values()`.
4. **Remove `pd_to_pl()` boundary conversion in `resolution_ute.py`**: After ticket-010, `post_resolve_GTER_UTE_entity()` returns `pl.DataFrame`, so the `pd_to_pl()` conversion in `resolve_GTER_UTE()` (lines 121-124) is no longer needed.
5. **Port `_stubs_helpers.py`**: Port `fill_initial_storage_df()`, `build_initial_stage_indices()`, `two_cache_op()`, and `calc_accumulated_productivity()` to accept/return `pl.DataFrame`. Replace pandas `.copy()`, `.assign()`, `.loc[]`, `.to_numpy()`, `.isin()`, `.iloc[]` with polars equivalents.
6. **Port `_stubs_market.py`**: Remove `.to_pandas()` shims for `Deck.submarkets()` and `Deck.non_simulated_generation()`. Port `generate_scenarios()` calls (now expects `pl.DataFrame` input after ticket-010). Port `_resolve_SBM_MER_MERL()`, `stub_MER_MERL()`, `stub_GUNS()` to polars.
7. **Port `stubs.py`**: Port all stub functions that use pandas operations on cached DataFrames. After ticket-011, `get_from_cache()` returns `pl.DataFrame`. Port `stub_resolve_initial_stored_energy()`, `stub_resolve_initial_stored_volumes()`, `stub_EARM_UHE()`, `convert_volume_to_flow()`, `convert_flow_to_volume()`, `resolve_stub()`, and all simple stubs (`stub_QDEF`, `stub_VDEF`, etc.) to work with polars DataFrames. Remove `from app.utils.dataframe import pd_to_pl` import.
8. **Update return type annotations**: Change all `resolve_*()` functions from `Optional[pd.DataFrame]` to `Optional[pl.DataFrame]`. Update `resolve_stub()` return type from `Tuple[pd.DataFrame, bool]` to `Tuple[pl.DataFrame, bool]`.
9. **Remove unused pandas imports**: Remove `import pandas as pd` from resolution modules where no longer used. Remove `from app.utils.dataframe import pd_to_pl, pl_to_pd` from modules where no longer used.

### Inputs/Props

- All resolution `resolve_*_entity()` functions: unchanged inputs (cls, uow, synthesis, index/name, deck_context)
- All resolution `resolve_*()` functions: unchanged inputs (cls, synthesis, uow, deck_context, executor)
- Hook functions: `df` parameter changes from `pd.DataFrame` to `pl.DataFrame`
- `fill_initial_storage_df()`: `df` changes from `pd.DataFrame` to `pl.DataFrame`
- `two_cache_op()`: no input changes, but internal `cls._get_from_cache()` now returns `pl.DataFrame`
- `calc_accumulated_productivity()`: `df` changes from `pd.DataFrame` to `pl.DataFrame`

### Outputs/Behavior

- All `resolve_*()` and `resolve_*_entity()` functions return `Optional[pl.DataFrame]` (was `Optional[pd.DataFrame]`)
- `resolve_stub()` returns `Tuple[pl.DataFrame, bool]` (was `Tuple[pd.DataFrame, bool]`)
- `fill_initial_storage_df()` returns `pl.DataFrame` (was `pd.DataFrame`)
- `two_cache_op()` returns `pl.DataFrame` (was `pd.DataFrame`)
- `calc_accumulated_productivity()` returns `pl.DataFrame` (was `pd.DataFrame`)
- All stub functions return `pl.DataFrame` (was `pd.DataFrame`)

### Error Handling

- `resolve_PEE()` still raises `NotImplementedError()` (unchanged).
- No fallback paths -- errors propagate naturally.

### Out of Scope

- `scenario.py` -- epic-04 (ticket-013)
- `execution.py`, `system.py` -- epic-04 (ticket-014)
- The Deck facade `.to_pandas()` shims in `deck.py` for bounds-related methods -- future cleanup
- `uow.files.get_nwlistop()` pandas boundary -- permanent external dependency
- `app/utils/dataframe.py` utility removal -- ticket-014

## Acceptance Criteria

- [ ] Given a recursive search for `# SHIM` in `app/services/synthesis/operation/`, when the search completes, then zero matches are found (all SHIM annotations removed from operation synthesis modules)
- [ ] Given a search for `.to_pandas()` in all files under `app/services/synthesis/operation/`, when the search completes, then zero matches are found
- [ ] Given a search for `pd_to_pl` or `pl_to_pd` in all files under `app/services/synthesis/operation/`, when the search completes, then zero matches are found (except in `__init__.py` which re-exports for the package namespace)
- [ ] Given a search for `import pandas as pd` in resolution module files (`resolution_*.py`, `spatial.py`, `stubs.py`, `_stubs_*.py`), when the search completes, then zero matches are found
- [ ] Given all 349+ existing tests are executed via `pytest`, when the test suite completes, then all tests pass with zero failures

## Implementation Guide

### Suggested Approach

**Phase A: Resolution modules (entity lookup pattern)**

For each resolution module, apply the same pattern:

1. **Submarket lookup** (used in `resolution_sbm.py`, `resolution_ute.py`, `resolution_sbp.py`, `_stubs_market.py`):
   - Before: `submarkets = Deck.submarkets(uow).to_pandas().reset_index(drop=True)`; `real_submarkets = submarkets.loc[submarkets["ficticio"] == 0, :].sort_values(COL)`
   - After: `submarkets = Deck.submarkets(uow).sort(COL)`; `real_submarkets = submarkets.filter(pl.col("ficticio") == 0)`
   - Name extraction: Before: `real_submarkets.loc[real_submarkets[CODE] == s, NAME].iloc[0]`; After: `real_submarkets.filter(pl.col(CODE) == s).item(0, NAME)` or build a lookup dict upfront: `name_map = dict(zip(real_submarkets[CODE].to_list(), real_submarkets[NAME].to_list()))`; then `name_map[s]`

2. **EER/submarket map lookup** (used in `resolution_ree.py`, `resolution_uhe.py`):
   - Before: `aux_df = deck_context.eer_submarket_map.to_pandas().set_index(KEY)`; `aux_df.at[idx, COL]`
   - After: `aux_df = deck_context.eer_submarket_map`; `aux_df.filter(pl.col(KEY) == idx).item(0, COL)`
   - For efficiency when called in a loop per entity, build a dict upfront: `lookup = dict(zip(aux_df[KEY].to_list(), aux_df[COL].to_list()))`; then `lookup[idx]`

3. **EER/hydro iteration** (used in `resolution_ree.py`, `resolution_uhe.py`):
   - Before: `eers = Deck.eers(uow).to_pandas().reset_index(drop=True).sort_values(COL)`; `eers_idx = eers[COL]`
   - After: `eers = Deck.eers(uow).sort(COL)`; `eers_idx = eers[COL].to_list()`; `eers_name = eers[NAME_COL].to_list()`

**Phase B: Hook functions**

4. **`_limit_stages_with_hydro()` in `resolution_uhe.py`**: Change `df: pd.DataFrame` to `df: pl.DataFrame`. Replace `df.loc[df[START_DATE_COL] < ending_date,].reset_index(drop=True)` with `df.filter(pl.col(START_DATE_COL) < ending_date)`. Return `pl.DataFrame`.

5. **`_sort_thermals()` in `resolution_ute.py`**: Change `df: pd.DataFrame` to `df: pl.DataFrame`. Replace `df.sort_values(cols).reset_index(drop=True)` with `df.sort(cols)`. Return `pl.DataFrame`.

**Phase C: Stubs modules**

6. **`_stubs_helpers.py`**:
   - `fill_initial_storage_df()`: Accept `pl.DataFrame`. Replace `df.copy()` with `df.clone()`. Replace `result[VALUE_COL].to_numpy().copy()` with `result[VALUE_COL].to_numpy().copy()` (polars `.to_numpy()` works the same). Replace `result[VALUE_COL] = arr` with `result = result.with_columns(pl.Series(VALUE_COL, arr))`. Replace `result[VALUE_COL].fillna(0.0)` with `.fill_null(0.0)`.
   - `two_cache_op()`: `cls._get_from_cache()` now returns `pl.DataFrame`. Replace `a[VALUE_COL].to_numpy()` (works same in polars). Replace `a.assign(**{VALUE_COL: result})` with `a.with_columns(pl.Series(VALUE_COL, result))`.
   - `calc_accumulated_productivity()`: Replace `Deck.hydros(uow).to_pandas().reset_index(drop=True)` with `Deck.hydros(uow)`. Port pandas `.loc[]` indexing to polars `.filter()` and `.item()`. Port `hydro_df.loc[condition, cols].to_numpy()` to `hydro_df.filter(condition).select(cols).to_numpy()`. The BFS loop mutates `df` in-place via `hp += dp` -- since polars is immutable, use `.to_numpy()` arrays for the mutation, then reconstruct the column at the end.

7. **`_stubs_market.py`**:
   - `_resolve_SBM_MER_MERL()`: Same submarket lookup pattern as phase A.
   - `stub_GUNS._resolve_SIN()` and `._resolve_SBM()`: Replace `Deck.non_simulated_generation(uow).to_pandas()` with `Deck.non_simulated_generation(uow)` (already polars from entities.py). Port `.groupby().sum().reset_index().drop(columns=[COL])` to `group_by().agg(pl.sum()).drop(COL)`. Port `.sort_values()`, `.loc[]` filter, `.unique().tolist()` to polars equivalents. `generate_scenarios()` now accepts polars (ticket-010).

8. **`stubs.py`**:
   - `stub_resolve_initial_stored_energy()`: `initial_stored_energy_df()` now returns `pl.DataFrame` (ticket-010). Port `.set_index()` / `.loc[]` indexing to polars filter/select. Port `cls._get_from_cache()` result usage (now polars).
   - `stub_resolve_initial_stored_volumes()`: Replace `Deck.hidr(uow).to_pandas().set_index("codigo_usina")` with `Deck.hidr(uow)` and polars filter. Port `initial_data.loc[].to_numpy()` to polars `.filter().select().to_numpy()`.
   - `stub_EARM_UHE()`: Replace `Deck.hidr(uow).to_pandas().set_index("codigo_usina")` with polars. Port extensive pandas operations (`.copy()`, `.loc[]`, `.sort_values()`, `.to_numpy()`, in-place `[]` assignment) to polars equivalents. This is the most complex stub -- use extract-to-numpy for arithmetic, then reconstruct with `with_columns`.
   - `convert_volume_to_flow()` and `convert_flow_to_volume()`: Replace `df.assign(**{VALUE_COL: expr})` with `df.with_columns((expr).alias(VALUE_COL))`.
   - `resolve_stub()`: Change empty fallback from `pd.DataFrame()` to `pl.DataFrame()`. Remove `pd_to_pl()` boundary conversion at line 421 (cache result is already polars). Remove `from app.utils.dataframe import pd_to_pl`.

**Phase D: Cleanup**

9. Remove `import pandas as pd` from all ported files. Remove `from app.utils.dataframe import pd_to_pl, pl_to_pd` where no longer needed. Update return type annotations to `pl.DataFrame`.

### Key Files to Modify

- `app/services/synthesis/operation/resolution_sbm.py` -- remove .to_pandas() shim, port submarket filtering
- `app/services/synthesis/operation/resolution_ree.py` -- remove .to_pandas() shims, port eer lookup
- `app/services/synthesis/operation/resolution_uhe.py` -- remove .to_pandas() shims, port hydro lookup, port \_limit_stages_with_hydro hook
- `app/services/synthesis/operation/resolution_ute.py` -- remove .to_pandas() shim, port \_sort_thermals hook, remove pd_to_pl boundary
- `app/services/synthesis/operation/resolution_sbp.py` -- remove .to_pandas() shim, port submarket lookup
- `app/services/synthesis/operation/resolution_sin.py` -- update return type annotation (no shims to remove)
- `app/services/synthesis/operation/resolution_pee.py` -- update return type annotation (no shims to remove)
- `app/services/synthesis/operation/stubs.py` -- port all stub functions to polars, remove pd_to_pl
- `app/services/synthesis/operation/_stubs_helpers.py` -- port fill_initial_storage_df, two_cache_op, calc_accumulated_productivity to polars
- `app/services/synthesis/operation/_stubs_market.py` -- remove .to_pandas() shims, port GUNS/MER_MERL to polars

### Patterns to Follow

- **Dict-based lookups for per-entity resolution**: Build `dict(zip(df[KEY].to_list(), df[VAL].to_list()))` before the executor loop, then use `lookup[key]` inside the entity function. This avoids repeated `.filter().item()` calls.
- **Extract-to-numpy for in-place mutation**: When stub logic requires in-place array mutation (like `calc_accumulated_productivity` BFS), extract to numpy, mutate, then reconstruct with `df.with_columns(pl.Series(name, arr))`. Established in `app/services/deck/storage.py` lines 44-103.
- **`pl.col(COL).is_in(values)` replaces `df[COL].isin(values)`**: Same semantics, used in filter expressions.
- **`df.item(row, col)` for scalar extraction**: Replaces `df.at[key, col]` / `df.iloc[0]` after filtering to a single row.

### Pitfalls to Avoid

- **`calc_accumulated_productivity` BFS mutation**: The BFS loop mutates `df.loc[condition, PRODUCTIVITY_TMP_COL]` in-place via `hp += dp`. Polars DataFrames are immutable. Extract the productivity column to a numpy array indexed by hydro code, mutate in numpy, then write back once at the end with `with_columns()`.
- **`stub_EARM_UHE` complexity**: This is the most complex function to port. It uses `.copy()`, `.loc[]` with multiple conditions, `.sort_values()`, and in-place `[]` assignment extensively. Port carefully: extract to numpy for the arithmetic, then reconstruct. Test thoroughly.
- **`Deck.hidr(uow)` returns polars with accessor shim**: After epic-02, `Deck.hidr(uow)` returns the inewave object, not a DataFrame. `accessors.hidr()` uses `include_index=True` to get `codigo_usina` as a column. Use `Deck.hidr(uow).cadastro` if needed, and convert from the accessor. Check the actual return path.
- **`resolve_GTER_UTE_entity` returns type**: After ticket-010, `post_resolve_GTER_UTE_entity()` returns `Optional[pl.DataFrame]`. The `resolve_GTER_UTE()` function in `resolution_ute.py` adds `SUBMARKET_CODE_COL` to the raw pandas df from NWLISTOP using `df[SUBMARKET_CODE_COL] = sbm_index`. This is pandas mutation. Since the df comes from NWLISTOP (pandas), either convert to polars first and use `with_columns`, or keep the pandas mutation before passing to `post_resolve_GTER_UTE_entity()` which converts internally.
- **`__init__.py` re-exports**: The `__init__.py` re-exports `pd_to_pl` and `pl_to_pd` from `app.utils.dataframe`. Keep these re-exports since other modules outside the operation package may still use them. Only remove from individual module imports.

## Testing Requirements

### Unit Tests

- Existing tests in `test_entity_pipeline.py` and `test_temporal_resolution.py` validate the pipeline layer.
- Existing `test_operation.py` exercises the full synthesis path including resolution modules and stubs.

### Integration Tests

- All existing 349+ tests must pass via `pytest` with zero failures.
- The full integration test suite in `test_operation.py` exercises every resolution module and most stubs through the `synthetize()` entry point.

## Dependencies

- **Blocked By**: ticket-011-port-synthesis-bounds-cache-export-polars.md
- **Blocks**: ticket-013-port-scenario-polars.md

## Effort Estimate

**Points**: 5
**Confidence**: Medium (the stubs module is complex with many interacting functions; `stub_EARM_UHE` and `calc_accumulated_productivity` require careful porting of in-place mutation patterns)
