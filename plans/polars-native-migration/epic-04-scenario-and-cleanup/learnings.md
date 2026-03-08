# Epic 04 Learnings: Scenario Synthesis and Final Cleanup

**Epic**: epic-04-scenario-and-cleanup
**Tickets**: ticket-013 (scenario.py, execution.py, system.py), ticket-014 (remove utilities and dead imports)
**Date**: 2026-03-07

---

## Patterns Established

### 1. Integer column name resolution: `str(station_code)` key access

`Deck.vazoes()` returns a wide-format `pl.DataFrame` where the inewave flow station codes are string column names (because `pl.from_pandas()` converts integer column names to strings). The correct access pattern is `vazoes[str(inflow_station)].to_numpy()`, not `vazoes[inflow_station]` (integer key fails) and not re-casting columns to int. This pattern is in `_generate_hydro_incremental_inflow_dataframe()` at `app/services/synthesis/scenario.py` lines 135 and 157.

### 2. `.item(0)` as the polars replacement for pandas `.iloc[0]` / `.loc[mask].iloc[0]`

The pandas idiom `df.loc[df[col] == val, other_col].iloc[0]` for single-row scalar extraction is replaced by `df.filter(pl.col(col) == val)[other_col].item(0)`. This pattern is used throughout `_generate_hydro_incremental_inflow_dataframe()` for extracting `posto`, `ano_inicio_historico`, and `ano_fim_historico` scalars at `app/services/synthesis/scenario.py` lines 133-168.

### 3. `.group_by().agg(pl.col(COL).mean()).sort()` replaces pandas `.groupby().mean()`

The LTA (long-term average inflow) computation `df.groupby([month_col]).mean()` is replaced by the polars chain `df.with_columns(pl.col(DATE_COL).dt.month().alias(MONTH_COL)).group_by(MONTH_COL).agg(pl.col(VALUE_COL).mean()).sort(MONTH_COL)`. Note that polars `.group_by()` does not preserve order -- the explicit `.sort()` is required. Canonical example: `_eval_monthly_lta()` at `app/services/synthesis/scenario.py` lines 182-189.

### 4. `pl.col(DATE_COL).dt.month()` replaces `.apply(lambda x: x.month)`

The pandas idiom `df[DATE_COL].apply(lambda x: x.month)` for extracting the month integer from a date column is replaced by the polars expression `pl.col(DATE_COL).dt.month()` inside a `.with_columns()` call. Same site as pattern 3 above.

### 5. `df.with_columns(pl.col(VALUE_COL) * SCALAR)` replaces in-place `df[col] *= scalar`

The pandas in-place multiply idiom `df[VALUE_COL] *= STAGE_DURATION_HOURS` is replaced by `df.with_columns(pl.col(VALUE_COL) * STAGE_DURATION_HOURS)` in polars. The return value is a new DataFrame -- the result must be assigned back. Applied in `system.py` `__resolve_PAT()` at `app/services/synthesis/system.py` line 126.

### 6. `.select([COL_A, COL_B])` replaces pandas `df[[COL_A, COL_B]]` column selection

Column projection in polars is `df.select([list_of_cols])`, not `df[list_of_cols]`. Applied for cost column selection in `execution.py` `_resolve_cost()` at `app/services/synthesis/execution.py` line 125, and for submarket columns in `system.py` `__resolve_SBM()` at line 130-132.

### 7. `df.filter(pl.col(etapa) != "Tempo Total")` replaces `.loc[]` row filtering

Boolean row filtering in polars uses `.filter(polars_expr)` instead of `.loc[condition]`. Applied in `execution.py` `_resolve_runtime()` at `app/services/synthesis/execution.py` line 131.

### 8. `df.with_columns(pl.col("tempo").dt.total_seconds().alias("tempo"))` replaces pandas duration `.dt.total_seconds()`

Pandas `.dt.total_seconds()` on a timedelta column mutated in place is replaced by a polars `.with_columns()` expression that aliases the result back to the same column name. The Duration dtype in polars supports `.dt.total_seconds()` identically to pandas timedelta. Applied in `execution.py` at `app/services/synthesis/execution.py` line 130.

### 9. `ProcessPoolExecutor` worker functions must serialize to `pd.DataFrame` across process boundaries

`scenario.py` uses `ProcessPoolExecutor` to parallelise per-iteration resolution functions (`_resolve_forward_energy_iteration`, `_resolve_backward_energy_iteration`, `_resolve_forward_inflow_iteration`, `_resolve_backward_inflow_iteration`). Each worker function computes a `pl.DataFrame` internally, then calls `.to_pandas()` before returning. The coordinator function `_post_resolve()` converts received `pd.DataFrame` objects back with `pl.from_pandas()`. This round-trip is architecturally necessary because polars DataFrames are not guaranteed to pickle cleanly across subprocess boundaries in all polars versions. The pattern lives at `app/services/synthesis/scenario.py` lines 831, 903, 960, 1008 (worker returns) and lines 843-849 (`_post_resolve` conversion).

### 10. Export boundary: `.to_pandas()` before `synthetize_df()`, or use `synthetize_pl()`

`uow.export.synthetize_df()` always expects a pandas DataFrame. For polars-native synthesis data, the call is `scenarios_pl.to_pandas()` at the final export point (`app/services/synthesis/scenario.py` line 1370 and 1406). The alternative `synthetize_pl()` exists in the export adapter and internally calls `synthetize_df(df.to_pandas(), ...)`. Both approaches are equivalent -- `synthetize_df(df.to_pandas(), ...)` is explicit and is the pattern used by scenario.py.

---

## Architectural Decisions

### Decision: Keep `pd.date_range()` for date-range generation in scenario.py

- **Decision**: `scenario.py` retains three `pd.date_range(start, end, freq="MS")` calls for generating monthly date sequences (lines 169, 298, 718). The resulting `DatetimeIndex` is used either via `.to_pydatetime().tolist()` (passing to a `pl.DataFrame` constructor) or via `np.array([d.month for d in dates])` (extracting month numbers as a numpy array).
- **Rejected alternatives**: (a) `pl.date_range(start, end, interval="1mo", eager=True)` -- exists in polars but the calling code passes the result into contexts that mix numpy operations with list iteration; the refactor cost exceeded the benefit; (b) `pd.date_range` replaced by pure `relativedelta` loop -- more verbose and no performance advantage.
- **Rationale**: `pd.date_range` for range generation is not pandas in the "DF-as-memory" sense; it is a date utility. The result is consumed as Python objects, not held in a pandas DataFrame. The `import pandas as pd` in `scenario.py` is retained specifically for this usage.
- **File**: `app/services/synthesis/scenario.py` lines 9, 169, 298, 718.

### Decision: Mixed `pl.DataFrame | pd.DataFrame` return type for `_resolve()` dispatch methods in execution.py and system.py

- **Decision**: `_resolve()` in both `ExecutionSynthetizer` and `SystemSynthetizer` has return type `pl.DataFrame | pd.DataFrame`. Variables that have been ported (`_resolve_cost`, `_resolve_runtime`, `__resolve_PAT`, `__resolve_SBM`, etc.) return `pl.DataFrame`. Legacy variables (`_resolve_program`, `_resolve_version`, `_resolve_title`, `_resolve_convergence`, `__resolve_EST`, `__resolve_CVU`) still return `pd.DataFrame`.
- **Rejected alternatives**: (a) Port all variables in ticket-013 -- the CVU and convergence variables involve inewave raw data structures not yet in polars form; (b) Introduce a new dispatch method splitting polars vs pandas resolvers -- over-engineering for what is a temporary state.
- **Rationale**: The `_synthetize_single_variable()` method in both classes now handles both types via `isinstance(df, pl.DataFrame)` before calling `synthetize_df()`. This is the correct transition-state pattern -- the union type documents the partial migration explicitly.
- **Files**: `app/services/synthesis/execution.py` lines 84, 179-183; `app/services/synthesis/system.py` lines 99, 194-198.

### Decision: Delete `app/utils/dataframe.py` rather than retain it empty

- **Decision**: `app/utils/dataframe.py` (which contained only `pd_to_pl()` and `pl_to_pd()`) was deleted entirely rather than being kept as an empty module or with a deprecation stub.
- **Rejected alternatives**: Keep it with deprecated wrappers -- unnecessary, since all callers were updated in the same commit.
- **Rationale**: The file had exactly two functions, both now dead. Deleting is cleaner than retaining a hollow module. The re-export from `operation/__init__.py` was also removed.
- **Files**: `app/utils/dataframe.py` (deleted), `app/services/synthesis/operation/__init__.py` (re-export line removed).

### Decision: Retain `# SHIM` annotations in deck.py, readers.py, and bounds.py

- **Decision**: Three `# SHIM` annotations remain in deck-layer files: `app/services/deck/deck.py` line 301 (bounds.py pandas shim), `app/services/deck/readers.py` line 418 (temporal.py pandas shim), and `app/services/deck/bounds.py` line 1525 (`.to_pandas()` call). These were NOT removed in ticket-014.
- **Rejected alternatives**: Remove all SHIM annotations as part of cleanup -- rejected because these annotations track genuine remaining pandas dependencies in modules not yet in scope.
- **Rationale**: `bounds.py` and the two `readers.py`/`deck.py` shims are permanent until those modules are ported. Removing the annotation without porting the code would make the technical debt invisible.
- **Files**: `app/services/deck/deck.py` line 301, `app/services/deck/readers.py` line 418, `app/services/deck/bounds.py` line 1525.

---

## Files and Structures Created / Deleted

- `app/services/synthesis/scenario.py` -- 1610 lines reduced to ~1450; all 17 Deck accessor SHIM calls removed; `pd_to_pl`/`pl_to_pd` removed; `CACHED_SYNTHESIS`, `CACHED_MLT_VALUES`, `SYNTHESIS_STATS` class variables retyped to `pl.DataFrame`; `_eval_monthly_lta()` is the canonical polars LTA computation pattern; `_post_resolve()` handles the polars/pandas union from process pool workers.
- `app/services/synthesis/execution.py` -- 2 SHIM calls removed (`_resolve_cost`, `_resolve_runtime`); `pl.DataFrame` return type added; `_synthetize_single_variable()` gained `isinstance(df, pl.DataFrame)` dispatch for export.
- `app/services/synthesis/system.py` -- 5 SHIM calls removed (`__resolve_PAT`, `__resolve_SBM`, `__resolve_REE`, `__resolve_UTE`, `__resolve_UHE`); `pl.DataFrame` return type added for those methods; same export dispatch pattern as execution.py.
- `app/utils/dataframe.py` -- deleted; contained `pd_to_pl()` and `pl_to_pd()` only.
- `app/services/synthesis/operation/__init__.py` -- dead `pd_to_pl, pl_to_pd` re-export removed.
- `app/services/synthesis/operation/orchestrator.py` -- dead `import pandas as pd` removed.
- `tests/app/utils/test_polars_concat_sort.py` -- deleted; tested `pd_to_pl`/`pl_to_pd` which no longer exist.
- `tests/app/utils/test_dataframe.py` -- deleted; tested the same deleted utilities.
- `tests/app/services/synthesis/test_entity_pipeline.py` -- `TestPostResolveNoPdToPl` class (82 lines) removed; the test patched `app.services.synthesis.operation.pd_to_pl` which no longer exists.

---

## Conventions Adopted

### `str(station_code)` for wide-format vazoes column access

When accessing a column in the `Deck.vazoes()` wide DataFrame by station code (an integer), always call `str(station_code)` to produce the string key. The column names in the polars DataFrame are always strings after `pl.from_pandas()`. This convention is established in `app/services/synthesis/scenario.py` lines 135 and 157.

### Export boundary: explicit `.to_pandas()` before `synthetize_df()`

All polars DataFrames exported via `uow.export.synthetize_df()` are converted explicitly at the call site: `uow.export.synthetize_df(df.to_pandas(), filename)`. This makes the pandas boundary visible and avoids hidden conversions inside the export adapter. Consistent across `scenario.py` lines 1370/1406, `system.py` line 199, and `execution.py` line 183.

### `isinstance(df, pl.DataFrame)` dispatch for mixed-type resolver returns

When a `_resolve()` dispatch method can return either `pl.DataFrame` or `pd.DataFrame`, the export step uses `export_df = df.to_pandas() if isinstance(df, pl.DataFrame) else df`. This is the standard transition pattern in `execution.py` lines 179-183 and `system.py` lines 194-198. It will be removed when remaining pandas resolvers are ported.

### Process pool workers return `pd.DataFrame` (not `pl.DataFrame`)

Functions submitted to `ProcessPoolExecutor` that return DataFrames serialize the result as `pd.DataFrame` (call `.to_pandas()` before returning). The coordinator `_post_resolve()` converts them back with `pl.from_pandas()`. This is a subprocess serialization constraint, not a pandas preference. Convention established in `app/services/synthesis/scenario.py` lines 831, 903, 960, 1008.

---

## Surprises and Deviations

### 1. `pd.date_range()` was retained in scenario.py (not replaced)

- **Expected**: All pandas usage in scenario.py would be eliminated; the ticket noted that `pd.DataFrame()` construction and `.groupby()` were the main pandas calls.
- **What happened**: Three `pd.date_range()` calls remained after migration. They generate `DatetimeIndex` objects that are converted to Python lists or numpy arrays before being passed to polars constructors. The calls are utility uses of pandas, not DataFrame-as-memory pandas. The `import pandas as pd` at the top of `scenario.py` is retained for these three calls.
- **Where**: `app/services/synthesis/scenario.py` lines 169, 298, 718.
- **Impact**: `scenario.py` is not fully pandas-free. The dependency is minimal (3 calls), isolated to date-range generation, and does not involve any pandas DataFrame in memory. Acceptable as a permanent or near-permanent detail.

### 2. `ProcessPoolExecutor` worker functions kept `pd.DataFrame` return type instead of switching to `pl.DataFrame`

- **Expected**: Ticket-013 planned to remove all `.to_pandas()` calls, including those at the end of worker functions used in `ProcessPoolExecutor`.
- **What happened**: The four worker functions (`_resolve_forward_energy_iteration`, `_resolve_backward_energy_iteration`, `_resolve_forward_inflow_iteration`, `_resolve_backward_inflow_iteration`) each call `.to_pandas()` before returning. The `_post_resolve()` coordinator explicitly handles `pd.DataFrame` inputs with `pl.from_pandas()`. This was a deliberate choice to preserve subprocess serialization safety.
- **Where**: `app/services/synthesis/scenario.py` lines 811-831, 886-960, 989-1008.
- **Impact**: Two round-trip conversions per iteration-file (polars -> pandas -> polars). This is a non-trivial cost for large cases (many iterations, many stages). A future improvement would be to verify polars pickle compatibility and eliminate the round-trip.

### 3. Stats export retains `synthetize_df(df.to_pandas(), ...)` rather than switching to `synthetize_pl()`

- **Expected**: Ticket-013 specified using `synthetize_pl()` for all polars data export.
- **What happened**: The export calls use `uow.export.synthetize_df(scenarios_pl.to_pandas(), str(s))` and `uow.export.synthetize_df(df.to_pandas(), filename)` directly. The `synthetize_pl()` adapter method was not used.
- **Where**: `app/services/synthesis/scenario.py` lines 1370 and 1406.
- **Impact**: Functionally identical -- `synthetize_pl()` internally calls `synthetize_df(df.to_pandas(), ...)`. The explicit pattern is more transparent about where the conversion occurs, which is actually preferable for readability. No functional deviation.

### 4. `test_entity_pipeline.py` `TestPostResolveNoPdToPl` class removed (not ported)

- **Expected**: The test class that verified `pd_to_pl` was not called per-entity would be ported or replaced with a test verifying the polars-native path.
- **What happened**: The entire `TestPostResolveNoPdToPl` class (82 lines) was deleted without a replacement. The test patched `app.services.synthesis.operation.pd_to_pl` which no longer exists after the deletion of `dataframe.py`.
- **Where**: `tests/app/services/synthesis/test_entity_pipeline.py`.
- **Impact**: One area of test coverage was reduced. The absence of `pd_to_pl` in the codebase is the structural guarantee that the old pattern cannot recur; the patch-based test is no longer meaningful. A replacement test verifying that `_post_resolve()` handles `pl.DataFrame` directly (without `isinstance` branching) would be the correct replacement.

---

## Recommendations for Future Epics

- If a future epic ports `bounds.py` and the two remaining `readers.py`/`temporal.py` shims, use the three `# SHIM` annotations in `app/services/deck/deck.py` line 301, `app/services/deck/readers.py` line 418, and `app/services/deck/bounds.py` line 1525 as the removal checklist.
- The `ProcessPoolExecutor` worker functions in `app/services/synthesis/scenario.py` (lines 811-831, 886-960, 989-1008) perform a polars-to-pandas-to-polars round-trip. Verify polars pickle/multiprocessing compatibility, then eliminate the round-trip by having workers return `pl.DataFrame` directly.
- `execution.py` and `system.py` still have pandas-returning resolver methods (`_resolve_program`, `_resolve_version`, `_resolve_title`, `_resolve_convergence`, `__resolve_EST`, `__resolve_CVU`). Once those are ported, the `isinstance(df, pl.DataFrame)` dispatch in `_synthetize_single_variable()` and the `import pandas as pd` in both files can be removed.
- The `pd.date_range()` calls in `scenario.py` (lines 169, 298, 718) can be replaced with `pl.date_range(..., eager=True)` and `.to_list()` to fully eliminate the pandas import from `scenario.py`. Each call is isolated to a helper function, making the replacement straightforward.
- `TestPostResolveNoPdToPl` in `test_entity_pipeline.py` was deleted without a replacement. Consider adding a test that verifies `_post_resolve()` correctly concatenates a dict of `pl.DataFrame` objects without triggering any pandas conversion path.
