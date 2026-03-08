# ticket-010 Port pipeline.py to native polars

## Context

### Background

After epics 01 and 02, all Deck domain modules (`entities.py`, `temporal.py`, `misc.py`, `exchange.py`, `energy.py`, `hydro.py`, `storage.py`, `thermal.py`, `policy.py`) return `pl.DataFrame` natively. The Deck facade (`deck.py`) now returns `pl.DataFrame` for `block_lengths()`, `eers()`, `submarkets()`, `hydros()`, and all entity lookup methods. The synthesis pipeline module (`pipeline.py`) is the first module downstream of the Deck layer that still contains pandas fallback paths, dual implementations, and `pd_to_pl()`/`pl_to_pd()` conversion shims. This ticket ports `pipeline.py` to work natively with polars, removing all pandas fallback code and the dead `resolve_starting_stage` function. It also ports `calc_statistics()` in `app/utils/operations.py` to accept `pl.DataFrame` directly, eliminating the intermediate `pd_to_pl()` conversion at the boundary.

### Relation to Epic

This is the first ticket in epic-03 (Synthesis Pipeline). It establishes the polars-native contract for the central pipeline functions (`resolve_temporal_resolution`, `resolve_starting_stage_polars`, `post_resolve`, `post_resolve_entity`, `post_resolve_GTER_UTE_entity`, `initial_stored_energy_df`, `generate_scenarios`, `resolve_temporal_resolution_GTER_UTE`) that all resolution modules (ticket-012) and the bounds/cache/export layer (ticket-011) depend on. By making `post_resolve` return `pl.DataFrame` instead of `pd.DataFrame`, this ticket sets the contract for the entire downstream chain.

### Current State

- `pipeline.py` imports `from app.utils.dataframe import pl_to_pd` (line 29) and uses `pl_to_pd()` inside `post_resolve()` (lines 470, 488) to convert the concatenated polars result back to pandas for hooks and `get_unique_column_values_in_order()`.
- `resolve_temporal_resolution()` (line 158) accepts `Optional[pd.DataFrame]` and has a `try/except` structure with a Polars primary path and a pandas fallback path (lines 236-264). The pandas fallback calls `_replace_scenario_info()`, `_add_stage_info()`, `_add_block_duration_info()` -- three dead helper functions (lines 43-108) that are only used by fallback paths.
- `resolve_starting_stage()` (line 267) is a pure-pandas function that is no longer called anywhere since `resolve_starting_stage_polars` (line 283) has a self-contained fallback. The only caller in `post_resolve_GTER_UTE_entity()` (line 422) still calls the pandas version.
- `resolve_starting_stage_polars()` (line 283) has its own try/except fallback to pandas via `pl_to_pd(df)` then back to `pd_to_pl(pd_df)`.
- `initial_stored_energy_df()` (line 314) returns `pd.DataFrame` and uses `.to_pandas()` SHIM at line 331 for `eer_submarket_map`. It also uses pandas-only `.apply()`, `.set_index()`, `.groupby().sum()`, `.dropna()` operations.
- `generate_scenarios()` (line 346) accepts/returns `pd.DataFrame` and uses `pd.concat` and numpy.
- `resolve_temporal_resolution_GTER_UTE()` (line 359) accepts/returns `pd.DataFrame` and uses the dead pandas helpers. It has a `.to_pandas()` SHIM at line 406 for `block_lengths`.
- `post_resolve_GTER_UTE_entity()` (line 412) calls `resolve_starting_stage` (pandas variant) and returns `pd.DataFrame`.
- `post_resolve()` (line 454) returns `pd.DataFrame` via `pl_to_pd()` conversion. It calls `get_unique_column_values_in_order()` which expects `pd.DataFrame` with `.unique().tolist()`.
- `get_unique_column_values_in_order()` (line 133) accepts `pd.DataFrame` and uses `df[col].unique().tolist()`.
- `app/utils/operations.py`: `calc_statistics()` (line 118) accepts `pd.DataFrame`, calls `_calc_statistics_polars()` which does `pd_to_pl(df)` internally (line 72), then `pl_to_pd(result)` at the end (line 115).
- A SHIM comment at line 215 notes the old `pd_to_pl` shim for `block_lengths` was already removed.

## Specification

### Requirements

1. **Remove pandas fallback path from `resolve_temporal_resolution()`**: Delete the entire `except` block (lines 226-264) and the `try` wrapper, keeping only the current polars primary path. Change the input type from `Optional[pd.DataFrame]` to `Optional[pd.DataFrame]` (unchanged -- input still comes from `uow.files.get_nwlistop` which returns pandas).
2. **Remove dead pandas helper functions**: Delete `_replace_scenario_info()`, `_add_stage_info()`, `_add_block_duration_info()` (lines 43-108). They are only used by the fallback paths being deleted.
3. **Remove `resolve_starting_stage()` (pandas variant)**: Delete the function entirely (lines 267-280). Replace the call in `post_resolve_GTER_UTE_entity()` with `resolve_starting_stage_polars()`.
4. **Remove fallback path from `resolve_starting_stage_polars()`**: Delete the `try/except` wrapper, keeping only the polars implementation. Remove the `pl_to_pd` import usage for this function.
5. **Port `post_resolve()` to return `pl.DataFrame`**: Replace the `pl_to_pd()` conversion at lines 470-475 with native polars `pl.concat().sort()`. Port early_hooks and late_hooks to receive `pl.DataFrame`. Port `get_unique_column_values_in_order()` to accept `pl.DataFrame`. The return type changes from `Optional[pd.DataFrame]` to `Optional[pl.DataFrame]`.
6. **Port `get_unique_column_values_in_order()` to polars**: Replace `df[col].unique().tolist()` with `df[col].unique(maintain_order=True).to_list()`.
7. **Port `initial_stored_energy_df()` to polars**: Remove the `.to_pandas()` SHIM for `eer_submarket_map`. Convert pandas `.apply()`, `.set_index()`, `.groupby().sum()`, `.dropna()` to polars equivalents. Return `pl.DataFrame` instead of `pd.DataFrame`.
8. **Port `generate_scenarios()` to polars**: Replace `pd.concat([df] * num_scenarios)` with polars cross-join or equivalent. Accept and return `pl.DataFrame`. Input comes from stubs which will need to convert their `pd.DataFrame` input to polars before calling this function.
9. **Port `resolve_temporal_resolution_GTER_UTE()` to polars**: Rewrite using polars joins and expressions instead of the dead pandas helpers. Remove the `.to_pandas()` SHIM at line 406 for `block_lengths`. Accept `Optional[pd.DataFrame]` input (from NWLISTOP) and return `Optional[pl.DataFrame]`.
10. **Port `post_resolve_GTER_UTE_entity()` to polars**: Replace `resolve_starting_stage()` call with `resolve_starting_stage_polars()`. Return `Optional[pl.DataFrame]`.
11. **Port `calc_statistics()` in `app/utils/operations.py`**: Accept `pl.DataFrame` directly, removing the `pd_to_pl(df)` conversion at line 72 and the `pl_to_pd(result)` at line 115. Return `pl.DataFrame`. Remove `fast_group_df()` if it becomes dead code (it may still be used elsewhere).
12. **Remove unused imports**: Remove `from app.utils.dataframe import pl_to_pd` from `pipeline.py` if no longer needed. Remove `import pandas as pd` if no longer needed. Clean up `app/utils/operations.py` imports.

### Inputs/Props

- `resolve_temporal_resolution()` input: `Optional[pd.DataFrame]` from NWLISTOP (unchanged), `uow`, `deck_context`
- `post_resolve()` input: `Dict[str, Optional[pl.DataFrame]]` (unchanged), early/late hooks now receive `pl.DataFrame`
- `calc_statistics()` input: changes from `pd.DataFrame` to `pl.DataFrame`
- `initial_stored_energy_df()` input: `cls`, `s: OperationSynthesis`, `uow: AbstractUnitOfWork`
- `generate_scenarios()` input: changes from `pd.DataFrame` to `pl.DataFrame`

### Outputs/Behavior

- `post_resolve()` returns `Optional[pl.DataFrame]` (was `Optional[pd.DataFrame]`)
- `resolve_temporal_resolution()` returns `Optional[pl.DataFrame]` (unchanged)
- `resolve_starting_stage_polars()` returns `pl.DataFrame` (unchanged type, but no fallback)
- `initial_stored_energy_df()` returns `pl.DataFrame` (was `pd.DataFrame`)
- `generate_scenarios()` returns `pl.DataFrame` (was `pd.DataFrame`)
- `resolve_temporal_resolution_GTER_UTE()` returns `Optional[pl.DataFrame]` (was `Optional[pd.DataFrame]`)
- `post_resolve_GTER_UTE_entity()` returns `Optional[pl.DataFrame]` (was `Optional[pd.DataFrame]`)
- `calc_statistics()` returns `pl.DataFrame` (was `pd.DataFrame`)

### Error Handling

- Remove all `try/except` fallback-to-pandas blocks in `resolve_temporal_resolution()` and `resolve_starting_stage_polars()`. Any polars errors should propagate naturally.
- `resolve_temporal_resolution()` still returns `None` when input is `None`.

### Out of Scope

- Resolution modules (`resolution_sbm.py`, `resolution_ree.py`, etc.) -- those are ticket-012
- `bounds.py`, `cache.py`, `export.py` -- those are ticket-011
- `stubs.py`, `_stubs_helpers.py`, `_stubs_market.py` -- those are ticket-012
- `scenario.py`, `execution.py`, `system.py` -- those are epic-04
- `orchestrator.py` type annotation updates -- those are ticket-011 since they depend on knowing the final return types of cache/export
- The early_hooks/late_hooks callers in resolution modules (e.g., `_limit_stages_with_hydro` in `resolution_uhe.py`, `_sort_thermals` in `resolution_ute.py`) must be updated to accept `pl.DataFrame` -- but since those are in resolution modules, they belong to ticket-012. For this ticket, the `post_resolve()` hooks signature changes to `Callable[[OperationSynthesis, pl.DataFrame, AbstractUnitOfWork], pl.DataFrame]` and callers in resolution modules will need to adapt.

## Acceptance Criteria

- [ ] Given `pipeline.py` is open, when searching for `def _replace_scenario_info`, `def _add_stage_info`, or `def _add_block_duration_info`, then zero matches are found (dead helper functions removed)
- [ ] Given `pipeline.py` is open, when searching for `def resolve_starting_stage(` (pandas variant without `_polars` suffix), then zero matches are found (dead function removed)
- [ ] Given `pipeline.py` is open, when searching for `except Exception` or `falling back to pandas`, then zero matches are found in `resolve_temporal_resolution` and `resolve_starting_stage_polars` (fallback paths removed)
- [ ] Given `pipeline.py` is open, when searching for `pl_to_pd` or `from app.utils.dataframe import.*pl_to_pd`, then zero matches are found (no pandas conversion in pipeline)
- [ ] Given `post_resolve()` is called with a dict of `pl.DataFrame` values, when it completes, then it returns an instance of `pl.DataFrame` (not `pd.DataFrame`)

## Implementation Guide

### Suggested Approach

1. **Delete dead helpers** (lines 43-108): `_replace_scenario_info`, `_add_stage_info`, `_add_block_duration_info`. These are only used by fallback paths.
2. **Simplify `resolve_temporal_resolution()`**: Remove the `try/except` wrapper. Keep only the polars primary path (lines 171-225). Remove the SHIM comment at line 215. The function already works correctly in the polars path.
3. **Simplify `resolve_starting_stage_polars()`**: Remove the `try/except`. Keep only lines 295-297. Rename to `resolve_starting_stage()` if desired, but keeping the `_polars` suffix is acceptable for now to minimize rename churn across files.
4. **Delete `resolve_starting_stage()` (pandas)**: Remove lines 267-280.
5. **Port `post_resolve_GTER_UTE_entity()`**: Replace `resolve_starting_stage(df, uow, deck_context)` with `resolve_starting_stage_polars(df, deck_context, uow)` (note argument order difference). This requires the input `df` to already be `pl.DataFrame`, which it will be after step 8.
6. **Port `resolve_temporal_resolution_GTER_UTE()`**: Rewrite using polars expressions. Convert the input pandas df to polars with `pd_to_pl()` at the start (NWLISTOP still returns pandas). Use `pl.Series()` for column construction as in the existing polars path of `resolve_temporal_resolution`. Use the native polars `block_lengths` directly (no `.to_pandas()` SHIM). Return `pl.DataFrame`.
7. **Port `initial_stored_energy_df()`**: Use polars join instead of `.apply()` for eer-to-submarket mapping. Use `group_by().agg(pl.sum())` instead of `groupby().sum()`. Use `drop_nulls()` instead of `.dropna()`. Return `pl.DataFrame`.
8. **Port `generate_scenarios()`**: Accept `pl.DataFrame`. Use polars operations to expand rows across scenarios. Pattern: create scenario column with `np.repeat`, then use polars `with_columns` and `pl.concat` or cross-join.
9. **Port `get_unique_column_values_in_order()`**: Change parameter type to `pl.DataFrame`. Use `df[col].unique(maintain_order=True).to_list()`. Note: polars `unique()` does not guarantee order by default -- `maintain_order=True` is required.
10. **Port `post_resolve()`**: Remove `pl_to_pd()` on the concatenated result. The `pl.concat(valid_dfs).sort(...)` is already polars. Remove the `.reset_index(drop=True)` (polars has no index). Hooks now receive `pl.DataFrame`. `get_unique_column_values_in_order()` now accepts `pl.DataFrame`. Remove the `pl_to_pd(valid_dfs[0])` call at line 488 -- pass `valid_dfs[0]` directly.
11. **Port `calc_statistics()`**: Change input type to `pl.DataFrame`. In `_calc_statistics_polars`, remove `pd_to_pl(df)` -- accept `pl.DataFrame` directly. Remove `pl_to_pd(result)` -- return `pl.DataFrame` directly. Update `calc_statistics` empty-check: `if df.is_empty():` instead of `if df.empty:`. Return type changes to `pl.DataFrame`. Check if `fast_group_df` is used elsewhere; if not, it can be deleted or left for ticket-014.
12. **Clean up imports**: Remove `from app.utils.dataframe import pl_to_pd` from `pipeline.py`. Remove `import pandas as pd` from `pipeline.py` if no pandas usage remains (note: `initial_stored_energy_df` may still need it depending on the approach). Remove unused imports from `operations.py`.

### Key Files to Modify

- `app/services/synthesis/operation/pipeline.py` -- primary target: remove fallback paths, dead functions, port all functions to polars
- `app/utils/operations.py` -- port `calc_statistics` and `_calc_statistics_polars` to accept/return `pl.DataFrame`
- `tests/app/services/synthesis/test_temporal_resolution.py` -- update tests: remove fallback-path test (`test_polars_path_matches_pandas_fallback`, `test_polars_error_falls_back_to_pandas_with_warning`), update assertions for post_resolve return type
- `tests/app/services/synthesis/test_entity_pipeline.py` -- update `TestPostResolveNoPdToPl` to expect `pl.DataFrame` from `post_resolve()` instead of `pd.DataFrame`

### Patterns to Follow

- **Convert-at-boundary**: NWLISTOP still returns `pd.DataFrame`. Use `pd_to_pl(df.rename(columns={...}))` at the entry point of `resolve_temporal_resolution()` as currently done. The conversion happens once at the pipeline boundary.
- **Polars join for block durations**: Already established in the polars path of `resolve_temporal_resolution()` at lines 216-224. Reuse the same pattern for `resolve_temporal_resolution_GTER_UTE()`.
- **`maintain_order=True`**: Always use with `unique()` and `group_by()` to preserve deterministic ordering, as established in epic-02.
- **`pl.from_pandas()` at NWLISTOP boundary**: Use `pd_to_pl()` (which calls `pl.from_pandas()`) for the NWLISTOP boundary, as done throughout the codebase.

### Pitfalls to Avoid

- **`get_unique_column_values_in_order` ordering**: Polars `unique()` without `maintain_order=True` does NOT guarantee order. Always pass `maintain_order=True`.
- **Hook signature change**: `post_resolve()` early/late hooks currently expect `pd.DataFrame`. After this ticket, they will receive `pl.DataFrame`. The hooks in resolution modules (`_limit_stages_with_hydro`, `_sort_thermals`) must be updated in ticket-012. The hooks within `pipeline.py` itself (none exist currently) are fine.
- **`generate_scenarios` callers**: `_stubs_market.py` calls `generate_scenarios(cls, df, uow)` where `df` comes from `uow.files.get_nwlistop()` (pandas). After this ticket, callers must convert to polars before calling. This is a ticket-012 responsibility, but be aware of the contract change.
- **`initial_stored_energy_df` return type change**: `stubs.py` calls `initial_stored_energy_df()` and uses the result with `.set_index()` and `.loc[]` (pandas operations). Since stubs.py is ticket-012 scope, this function can return `pl.DataFrame` and stubs.py will be updated later. However, ensure the polars DataFrame has the same columns and data.
- **`calc_statistics` callers**: `export.py` calls `calc_statistics(scenarios_df)` where `scenarios_df` is currently `pd.DataFrame`. After this ticket, `calc_statistics` expects `pl.DataFrame`. The `export.py` changes are ticket-011 scope, so be aware of this contract change.

## Testing Requirements

### Unit Tests

- Update `tests/app/services/synthesis/test_temporal_resolution.py`:
  - Remove `test_polars_path_matches_pandas_fallback` (no more fallback)
  - Remove `test_polars_error_falls_back_to_pandas_with_warning` (no more fallback)
  - Existing tests (`test_output_shape`, `test_output_columns`, `test_scenario_col_values`, `test_stage_col_values`, `test_block_duration_col_values`, `test_deck_context_prevents_deck_calls`, `test_none_input_returns_none`) should pass unchanged
- Update `tests/app/services/synthesis/test_entity_pipeline.py`:
  - `TestPostResolveNoPdToPl.test_pd_to_pl_not_called_per_entity`: Update assertion to expect `pl.DataFrame` from `_post_resolve` (line 573: `assert isinstance(result, pl.DataFrame)`)

### Integration Tests

- All existing 349+ tests must pass via `pytest` with no regressions.

## Dependencies

- **Blocked By**: ticket-009-port-policy-polars.md
- **Blocks**: ticket-011-port-synthesis-bounds-cache-export-polars.md

## Effort Estimate

**Points**: 5
**Confidence**: High
