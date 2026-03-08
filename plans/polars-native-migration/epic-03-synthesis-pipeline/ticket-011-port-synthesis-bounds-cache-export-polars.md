# ticket-011 Port synthesis bounds, cache, and export to polars

## Context

### Background

After ticket-010, the pipeline functions `post_resolve()`, `initial_stored_energy_df()`, `generate_scenarios()`, and `calc_statistics()` all accept and return `pl.DataFrame`. The `post_resolve()` return type change from `pd.DataFrame` to `pl.DataFrame` ripples into three downstream modules: `bounds.py` (which wraps `OperationVariableBounds.resolve_bounds`), `cache.py` (which stores synthesis results), and `export.py` (which exports scenarios and statistics). Currently these three modules operate entirely in pandas with `pd_to_pl()`/`pl_to_pd()` conversion pairs at their boundaries. This ticket eliminates those conversion pairs by porting all three modules to accept `pl.DataFrame` natively. It also updates `orchestrator.py` type annotations and the class-level type hints for `CACHED_SYNTHESIS` and `SYNTHESIS_STATS`.

### Relation to Epic

This is the second ticket in epic-03. It bridges the pipeline layer (ticket-010) and the resolution/stubs layer (ticket-012). Once bounds, cache, and export accept `pl.DataFrame`, the resolution modules can pass polars DataFrames end-to-end without any pandas conversion in the core synthesis path.

### Current State

- **`bounds.py`** (line 1-48): `resolve_bounds()` accepts `pd.DataFrame`, converts to polars with `pd_to_pl(df)` (line 39), calls `OperationVariableBounds.resolve_bounds()` (which already accepts and returns `pl.DataFrame` from the `app/services/deck/bounds.py` module ported in epic-02), then converts back with `pl_to_pd(df_pl)` (line 46). Returns `pd.DataFrame`.
- **`cache.py`** (line 1-61): `get_from_cache()` and `get_from_cache_if_exists()` return `pd.DataFrame`. `store_in_cache_if_needed()` stores `pd.DataFrame`. The `CACHED_SYNTHESIS` dict on `orchestrator.py` line 60 is typed `Dict[OperationSynthesis, pd.DataFrame]`.
- **`export.py`** (line 1-158): `export_scenario_synthesis()` accepts `pd.DataFrame`, converts to polars with `pd_to_pl()` for sorting (line 108), converts back with `pl_to_pd()` (line 112) for `calc_statistics()` and `store_in_cache_if_needed()`. `add_synthesis_stats()` stores `pd.DataFrame` in `SYNTHESIS_STATS`. `export_stats()` uses `pd.concat()` on the stats list and `pd_to_pl()` at the end (line 156) for parquet export. `export_metadata()` builds a small metadata DataFrame with `pd.DataFrame` -- this stays pandas since it uses `pd.concat` for metadata and writes via `uow.export.synthetize_df()`.
- **`orchestrator.py`** (line 60-62): `CACHED_SYNTHESIS: Dict[OperationSynthesis, pd.DataFrame]`, `ORDERED_SYNTHESIS_ENTITIES: Dict[...]`, `SYNTHESIS_STATS: Dict[SpatialResolution, List[pd.DataFrame]]`. Method `_resolve_bounds` (line 202) returns `pd.DataFrame`. `_synthetize_single_variable` (line 367) uses `df.empty` check (pandas attribute) on the result of `get_from_cache_if_exists`.
- **`spatial.py`** (line 1-72): `resolve_spatial_resolution()` and `resolve_synthesis()` return `pd.DataFrame`. `resolve_synthesis()` calls `resolve_bounds()` passing `pd.DataFrame`.

## Specification

### Requirements

1. **Port `bounds.py`**: Remove `pd_to_pl(df)` and `pl_to_pd(df_pl)` conversion pair. Accept `pl.DataFrame` directly and return `pl.DataFrame`. Remove `from app.utils.dataframe import pd_to_pl, pl_to_pd`.
2. **Port `cache.py`**: Change all type annotations from `pd.DataFrame` to `pl.DataFrame`. `get_from_cache()` returns `pl.DataFrame`. `get_from_cache_if_exists()` returns `pl.DataFrame` (empty polars DataFrame when cache miss: `pl.DataFrame()`). `store_in_cache_if_needed()` accepts `pl.DataFrame`. Remove `import pandas as pd`.
3. **Port `export.py`**:
   - `export_scenario_synthesis()`: Accept `pl.DataFrame`. Remove `pd_to_pl(df.astype(...))` -- cast SCENARIO_COL to int using polars `cast()`. Pass `pl.DataFrame` directly to `calc_statistics()` (which now accepts polars after ticket-010). Pass `pl.DataFrame` to `store_in_cache_if_needed()`. Remove `pl_to_pd()` calls.
   - `add_synthesis_stats()`: Accept `pl.DataFrame`. Store `pl.DataFrame` in `SYNTHESIS_STATS`.
   - `export_stats()`: Replace `pd.concat(dfs)` with `pl.concat(dfs)`. Replace pandas sort/filter/astype with polars equivalents. Remove `pd_to_pl(df)` at line 156 -- already polars. Remove `from app.utils.dataframe import pd_to_pl, pl_to_pd`.
   - `export_metadata()`: Keep as pandas. This builds a small metadata DataFrame and uses `uow.export.synthetize_df()` which expects pandas. This is a tiny non-performance-critical path.
4. **Port `spatial.py`**: Change return types of `resolve_spatial_resolution()` and `resolve_synthesis()` from `pd.DataFrame` to `pl.DataFrame`. In `resolve_spatial_resolution()`, the empty fallback changes from `pd.DataFrame()` to `pl.DataFrame()`. Remove `import pandas as pd`.
5. **Update `orchestrator.py` type annotations**: Change `CACHED_SYNTHESIS` from `Dict[OperationSynthesis, pd.DataFrame]` to `Dict[OperationSynthesis, pl.DataFrame]`. Change `SYNTHESIS_STATS` from `Dict[SpatialResolution, List[pd.DataFrame]]` to `Dict[SpatialResolution, List[pl.DataFrame]]`. Update `_resolve_bounds` return type to `pl.DataFrame`. Update `_synthetize_single_variable` to use `df.is_empty()` (polars) instead of `df.empty` (pandas). Add `import polars as pl` if not present.
6. **Remove unused imports**: Remove `import pandas as pd` from `bounds.py`, `cache.py`, `spatial.py` where no longer needed. Remove `from app.utils.dataframe import pd_to_pl, pl_to_pd` from `bounds.py`, `export.py`.

### Inputs/Props

- `resolve_bounds()`: `cls`, `s: OperationSynthesis`, `df: pl.DataFrame`, `uow: AbstractUnitOfWork` -- input changes from `pd.DataFrame` to `pl.DataFrame`
- `store_in_cache_if_needed()`: `cls`, `s: OperationSynthesis`, `df: pl.DataFrame` -- input changes from `pd.DataFrame` to `pl.DataFrame`
- `export_scenario_synthesis()`: `cls`, `s: OperationSynthesis`, `df: pl.DataFrame`, `uow: AbstractUnitOfWork` -- input changes from `pd.DataFrame` to `pl.DataFrame`
- `add_synthesis_stats()`: `cls`, `s: OperationSynthesis`, `df: pl.DataFrame` -- input changes from `pd.DataFrame` to `pl.DataFrame`

### Outputs/Behavior

- `resolve_bounds()` returns `pl.DataFrame` (was `pd.DataFrame`)
- `get_from_cache()` returns `pl.DataFrame` (was `pd.DataFrame`)
- `get_from_cache_if_exists()` returns `pl.DataFrame` (was `pd.DataFrame`)
- `resolve_spatial_resolution()` returns `pl.DataFrame` (was `pd.DataFrame`)
- `resolve_synthesis()` returns `pl.DataFrame` (was `pd.DataFrame`)
- `export_scenario_synthesis()` now writes polars directly without intermediate pandas conversion
- `export_stats()` uses `pl.concat()` instead of `pd.concat()`

### Error Handling

- `get_from_cache()`: Still raises `RuntimeError()` when key is missing or value is `None`.
- `get_from_cache_if_exists()`: Returns `pl.DataFrame()` (empty polars DataFrame) instead of `pd.DataFrame()` on cache miss.

### Out of Scope

- `export_metadata()` stays pandas -- it builds a tiny metadata DataFrame and uses `uow.export.synthetize_df()` which expects pandas
- Resolution modules (`resolution_*.py`) -- ticket-012
- Stubs modules (`stubs.py`, `_stubs_helpers.py`, `_stubs_market.py`) -- ticket-012
- `scenario.py`, `execution.py`, `system.py` -- epic-04
- The Deck facade `.to_pandas()` shims in `deck.py` for bounds methods (e.g., `hydro_volume_bounds_in_stages`, `thermal_generation_bounds`) -- those are consumed by `app/services/deck/bounds.py` (deck-layer, not synthesis-layer) and will remain until bounds.py in the deck layer is updated in a future cleanup

## Acceptance Criteria

- [ ] Given `bounds.py` in `app/services/synthesis/operation/` is open, when searching for `pd_to_pl` or `pl_to_pd`, then zero matches are found
- [ ] Given `cache.py` in `app/services/synthesis/operation/` is open, when searching for `pd.DataFrame`, then zero matches are found in function signatures and return type annotations
- [ ] Given `export.py` in `app/services/synthesis/operation/` is open, when searching for `pl_to_pd` or `pd_to_pl`, then zero matches are found (except in `export_metadata` which stays pandas)
- [ ] Given the `_synthetize_single_variable` method in `orchestrator.py` is inspected, when checking the cache-miss empty check, then it uses `df.is_empty()` (polars) not `df.empty` (pandas)
- [ ] Given all 349+ existing tests are executed via `pytest`, when the test suite completes, then all tests pass with zero failures

## Implementation Guide

### Suggested Approach

1. **Port `bounds.py`** first (smallest, simplest):
   - Remove `from app.utils.dataframe import pd_to_pl, pl_to_pd` import
   - Remove `import pandas as pd`
   - Add `import polars as pl` if not present
   - Change `df: pd.DataFrame` to `df: pl.DataFrame` in function signature and return type
   - Remove `df_pl = pd_to_pl(df)` -- pass `df` directly to `OperationVariableBounds.resolve_bounds()`
   - Remove `df = pl_to_pd(df_pl)` -- return `df_pl` directly
   - Simplify: the function body becomes just the `resolve_bounds()` call wrapped in `time_and_log`

2. **Port `cache.py`**:
   - Replace `import pandas as pd` with `import polars as pl`
   - Change all `pd.DataFrame` annotations to `pl.DataFrame`
   - In `get_from_cache_if_exists()`, change `return pd.DataFrame()` to `return pl.DataFrame()`
   - The rest is just type annotation changes; the actual storage/retrieval logic is generic

3. **Port `export.py`**:
   - In `export_scenario_synthesis()`:
     - Accept `df: pl.DataFrame` instead of `pd.DataFrame`
     - Replace `pd_to_pl(df.astype({SCENARIO_COL: int}))` with `df.cast({SCENARIO_COL: pl.Int64})` or `df.with_columns(pl.col(SCENARIO_COL).cast(pl.Int64))`
     - Remove `scenarios_df = pl_to_pd(scenarios_pl).reset_index(drop=True)` -- pass `scenarios_pl` directly to `calc_statistics()` (which now accepts polars)
     - Pass polars DataFrame to `store_in_cache_if_needed()`
   - In `add_synthesis_stats()`:
     - Accept `df: pl.DataFrame`
     - Replace `df[VARIABLE_COL] = s.variable.value` with `df = df.with_columns(pl.lit(s.variable.value).alias(VARIABLE_COL))`
   - In `export_stats()`:
     - Replace `pd.concat(dfs, ignore_index=True)` with `pl.concat(dfs)`
     - Replace pandas column selection `df = df[[cols]]` with `df = df.select(cols)`
     - Replace `df.astype({VARIABLE_COL: STRING_DF_TYPE})` with `df.cast({VARIABLE_COL: pl.Utf8})`
     - Replace `df.sort_values(cols).reset_index(drop=True)` with `df.sort(cols)`
     - Replace `df.drop_duplicates()` with `df.unique()`
     - Handle `existing_df = uow.export.read_df(stats_filename)` -- this returns pandas or None. If not None, convert with `pl.from_pandas(existing_df)` before `pl.concat`.
     - Remove the final `df_pl = pd_to_pl(df)` -- already polars
   - Keep `export_metadata()` as pandas
   - Remove `from app.utils.dataframe import pd_to_pl, pl_to_pd`

4. **Port `spatial.py`**:
   - Replace `import pandas as pd` with `import polars as pl`
   - Change return types to `pl.DataFrame`
   - Change empty fallback from `pd.DataFrame()` to `pl.DataFrame()`

5. **Update `orchestrator.py`**:
   - Change `CACHED_SYNTHESIS: Dict[OperationSynthesis, pd.DataFrame]` to `Dict[OperationSynthesis, pl.DataFrame]`
   - Change `SYNTHESIS_STATS: Dict[SpatialResolution, List[pd.DataFrame]]` to `Dict[SpatialResolution, List[pl.DataFrame]]`
   - Add `import polars as pl` (keep `import pandas as pd` since `_synthetize_single_variable` may still need it for interop)
   - In `_synthetize_single_variable()`: change `df.empty` to `df.is_empty()` for the polars empty check

### Key Files to Modify

- `app/services/synthesis/operation/bounds.py` -- remove pd/pl conversion pair, accept/return pl.DataFrame
- `app/services/synthesis/operation/cache.py` -- change all pd.DataFrame to pl.DataFrame
- `app/services/synthesis/operation/export.py` -- remove pd_to_pl/pl_to_pd, port sort/concat/stats to polars
- `app/services/synthesis/operation/spatial.py` -- change return types to pl.DataFrame
- `app/services/synthesis/operation/orchestrator.py` -- update type annotations, .empty -> .is_empty()

### Patterns to Follow

- **Polars `cast` for type conversion**: `df.with_columns(pl.col(COL).cast(pl.Int64))` replaces `df.astype({COL: int})`
- **Polars `with_columns` for mutation**: `df.with_columns(pl.lit(val).alias(COL))` replaces `df[COL] = val`
- **Polars `unique()` for dedup**: `df.unique()` replaces `df.drop_duplicates()`
- **Polars `sort()` replaces `sort_values()`**: `df.sort(cols)` replaces `df.sort_values(cols).reset_index(drop=True)`
- **Polars `select()` for column selection**: `df.select(cols)` replaces `df[cols]`

### Pitfalls to Avoid

- **`export_stats` reads existing pandas**: `uow.export.read_df(stats_filename)` returns `pd.DataFrame` or `None`. Must convert to polars with `pl.from_pandas()` before `pl.concat()`. Do not forget this boundary.
- **`export_metadata` stays pandas**: Do not try to port this -- it uses `uow.export.synthetize_df()` which expects pandas. The metadata is a tiny DataFrame and the pandas dependency here is acceptable.
- **`df.empty` vs `df.is_empty()`**: pandas uses `df.empty` (property), polars uses `df.is_empty()` (method). Missing the parentheses will silently evaluate to a truthy method object.
- **`CACHED_SYNTHESIS` consumers in `stubs.py`**: After this ticket, `get_from_cache()` returns `pl.DataFrame`. Stubs like `two_cache_op()` in `_stubs_helpers.py` use pandas operations (`.to_numpy()`, `.assign()`) on the cached value. These stubs will break until ticket-012 ports them. This is expected and acceptable because ticket-012 is blocked by this ticket.
- **`add_synthesis_stats` polars mutation**: Polars DataFrames are immutable. `df[VARIABLE_COL] = val` does not work. Must use `df = df.with_columns(...)` and return the new DataFrame or store it correctly.

## Testing Requirements

### Unit Tests

- No new dedicated unit tests required for bounds/cache/export since they are thin wrappers.
- The existing integration tests exercise the full pipeline including bounds, cache, and export.

### Integration Tests

- All existing 349+ tests must pass via `pytest` with zero failures.
- The existing `test_operation.py` exercises the full `synthetize()` path which calls `_synthetize_single_variable` -> `resolve_synthesis` -> `resolve_bounds` -> `export_scenario_synthesis` -> `store_in_cache_if_needed` -> `export_stats`. This end-to-end path validates the conversion.

## Dependencies

- **Blocked By**: ticket-010-port-pipeline-native-polars.md
- **Blocks**: ticket-012-port-resolution-modules-polars.md

## Effort Estimate

**Points**: 3
**Confidence**: High
