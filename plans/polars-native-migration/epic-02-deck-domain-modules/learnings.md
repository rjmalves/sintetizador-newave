# Epic 02 Learnings: Deck Domain Modules

**Epic**: epic-02-deck-domain-modules
**Tickets**: ticket-004 through ticket-009
**Date**: 2026-03-07

---

## Patterns Established

### 1. `pl.from_pandas()` at the inewave boundary, polars everywhere else

Every function that reads from an inewave object (`.usinas`, `.desvios`, `.coeficientes`, `.estados`, etc.) calls `pl.from_pandas(raw)` immediately and stores a `pl.DataFrame` in cache. No module carries a pandas DataFrame beyond the first cache-insertion point. Canonical example: `entities.submarkets()` at `app/services/deck/entities.py` line 39.

### 2. `.join()` replaces all indexed-DataFrame lookups

The pandas pattern of `df.set_index(col).at[idx, col]` and `df.join(other_indexed, on=col)` is entirely replaced by polars `df.join(other, on=col)`. Downstream callers that previously required the index column to be the index now receive it as an ordinary column and join on it explicitly. All join-replacement sites are in `app/services/deck/entities.py`, `app/services/deck/hydro.py`, `app/services/deck/storage.py`, and `app/services/deck/energy.py`.

### 3. Cross-join for stage expansion replaces `pd.concat([df] * N)` + `np.repeat()`

The recurring pattern of repeating a static DataFrame across all study stages is now:

```python
dates_df = pl.DataFrame({START_DATE_COL: dates})
expanded = df.join(dates_df, how="cross").sort([HYDRO_CODE_COL, START_DATE_COL])
```

This pattern appears in `_expand_hydro_to_stages()` (`app/services/deck/hydro.py` line 505), `_expand_to_blocks()` (line 516), `_expand_to_stages()` in thermal.py (line 190), and `_build_base_costs_df()` in thermal.py (line 364).

### 4. `pl.when().then().otherwise()` replaces all `.loc[]` conditional assignments

Every instance of `df.loc[condition, col] = value` is now expressed as `df.with_columns(pl.when(condition).then(pl.lit(value)).otherwise(pl.col(col)).alias(col))`. This pattern is used heavily in `app/services/deck/hydro.py` (percentage-to-hm3 conversions, lines 133-149) and `app/services/deck/thermal.py` (`_apply_thermal_single_change`, lines 22-39).

### 5. Polars `iter_rows(named=True)` for genuinely iterative mutations

When iteration is semantically necessary (applying ordered changes, per-thermal-unit maintenance windows), `df.iter_rows(named=True)` is used instead of pandas `iterrows()`. This preserves the iteration pattern while staying polars-native. Examples: `_apply_thermal_bounds_maintenance_and_changes()` in `app/services/deck/thermal.py` line 61, and `_apply_thermal_cost_changes()` line 397.

### 6. Extract-to-numpy for mathematical row-level operations

For polynomial evaluation (`np.polyval`) and BFS graph accumulation where the math operates on per-row scalars, the implementation extracts columns as numpy arrays, operates, and assigns back as `pl.Series`:

```python
coefs = [df[c].to_numpy() for c in HEIGHT_POLY_COLS]
# ...compute...
df = df.with_columns(pl.Series(PRODUCTIVITY_TMP_COL, productivity))
```

This pattern lives in `app/services/deck/storage.py` `evaluate_productivity()` (lines 44-103) and `accumulate_productivity()` (lines 106-130).

### 7. Resample-to-daily-then-back-to-monthly via `pl.date_range()` + `forward_fill()` + `dt.truncate()`

The pandas `resample("D").ffill()` then `resample("MS").mean()` pattern is replaced by:

1. Build a daily date range with `pl.date_range(first_date, last_day_date, interval="1d", eager=True)`
2. Left-join monthly data onto the daily range
3. `.forward_fill()` to propagate monthly values to each day
4. Apply day-level changes
5. Group by `pl.col(START_DATE_COL).dt.truncate("1mo")` + `.agg(pl.col(...).mean())`

Full implementation in `_apply_maintenance()` inside `app/services/deck/thermal.py` lines 75-171.

### 8. `map_batches` for date construction from year+month columns

The pandas `apply(lambda row: datetime(row.year, row.month, 1))` pattern for building date columns from two integer columns is replaced by `pl.map_batches([year_expr, month_expr], lambda cols: ...)`. Example in `thermal_costs()` `_build_base_costs_df()` at `app/services/deck/thermal.py` lines 365-379.

---

## Architectural Decisions

### Decision: Keep `readers.apply_modif_changes_*` calls as explicit pandas boundary crossings

- **Decision**: Functions in `hydro.py` that call `readers.apply_modif_changes_to_hydros()` and `readers.apply_modif_changes_to_hydros_in_stages()` explicitly convert to pandas (`.to_pandas()`), call the reader, and convert back (`pl.from_pandas(df_pd)`). The conversion is scoped to the minimum DataFrame required.
- **Rejected alternatives**: (a) Rewrite the modif-change readers to accept polars -- rejected because readers.py is the inewave binary boundary and must stay pandas-idiomatic; (b) Keep hydro.py entirely in pandas -- rejected because it would block downstream polars consumers.
- **Rationale**: The explicit crossing is clearly visible, annotated with a comment, and scoped. It will be eliminated when readers.py itself is migrated.
- **File**: `app/services/deck/hydro.py`, lines 97-118 (volume bounds with changes), lines 306-309 (turbined flow changes).

### Decision: Vectorized polars expression for turbined-flow calculation instead of `map_elements`

- **Decision**: `_calc_turbined_flow_expr()` builds a single polars Expr that sums `maquinas_conjunto_i * vazao_nominal_conjunto_i` for i up to the detected maximum, rather than using `map_elements` for row-level calculation.
- **Rejected alternatives**: `map_elements` (slower, forces GIL), full numpy extraction (less idiomatic for simple arithmetic).
- **Rationale**: Polars expressions compose as arithmetic; a loop that generates `pl.Expr` objects and sums them is both readable and fully vectorized.
- **File**: `app/services/deck/hydro.py`, lines 45-52.

### Decision: Python loop + list mutation for BFS graph accumulation

- **Decision**: `accumulate_productivity()` in storage.py extracts the productivity list, mutates it in Python during the BFS traversal, then assigns back as a single `pl.Series`. The BFS ordering is provided by the existing `Graph.bfs()` utility.
- **Rejected alternatives**: Constructing a polars join chain to propagate downstream contributions -- rejected because the accumulation is order-dependent and a cascade can have arbitrary depth.
- **Rationale**: The list-mutation-then-assign pattern is idiomatic polars for mutations that cannot be expressed as a single vectorized expression.
- **File**: `app/services/deck/storage.py`, lines 106-130.

### Decision: `map_batches` over `pl.date()` expression for date construction

- **Decision**: `thermal_costs()` uses `pl.map_batches([year_col, month_col], lambda cols: pl.Series([datetime(int(y), int(m), 1) ...]))` rather than attempting to compose a polars expression for date construction.
- **Rejected alternatives**: `pl.date(year_expr, month_expr, 1)` -- unavailable in the polars version in use; constructing via string formatting then casting -- fragile.
- **Rationale**: `map_batches` accepts multiple columns and returns a Series; it is the documented polars way to perform multi-column Python-level row operations.
- **File**: `app/services/deck/thermal.py`, lines 365-379.

### Decision: `temporal.consider_post_study_years()` accepts `pl.DataFrame` directly

- **Decision**: `consider_post_study_years()` was rewritten to accept and return `pl.DataFrame` using `dt.offset_by(f"{n}y")` for date arithmetic. Callers (entities, misc, thermal, exchange, energy) all pass polars DataFrames directly; no conversion shim is needed.
- **Rationale**: Since all callers were migrated in the same epic, the signature change was safe to coordinate.
- **File**: `app/services/deck/temporal.py`, lines 500-519.

---

## Files and Structures Created

- `app/services/deck/entities.py` -- 13 entity catalog functions, all polars-native. Functions return `pl.DataFrame` with regular columns (no index). The three-table join in `hydro_eer_submarket_map()` (line 147) is the canonical pattern for building lookup maps from catalog data.
- `app/services/deck/temporal.py` -- scalar-returning functions unchanged; DataFrame functions (`configurations_pmo`, `configurations_dger`, `configurations`, `consider_post_study_years`) now return/accept `pl.DataFrame`. `num_hydro_simulation_stages_policy` and related functions that take `eers_df: "pd.DataFrame"` parameter still use pandas (called from policy.py which receives raw inewave data -- not yet migrated to accept polars on that path).
- `app/services/deck/misc.py` -- `block_lengths()`, `costs()`, `runtimes()` all return `pl.DataFrame`; the `__eval_pat0` helper uses `group_by().agg()` + `pl.concat()`.
- `app/services/deck/exchange.py` -- `_drops_exchange_direction_flag()` uses a single `with_columns` call to safely swap two columns simultaneously; `_cast_exchange_bounds_to_MWmes()` replaces both `apply(lambda)` and `np.tile()` with polars joins.
- `app/services/deck/energy.py` -- `stored_energy_upper_bounds_inputs()` retains its Python loop over configuration dates (each iteration is a genuinely independent stage computation); all inner operations are polars. `stored_energy_upper_bounds_pmo()` uses cross-join for EER expansion.
- `app/services/deck/hydro.py` -- `_calc_turbined_flow_expr()` and `_max_conjuntos_from_df()` are new helper functions; stage/block expansion helpers `_expand_hydro_to_stages()` and `_expand_to_blocks()` are extracted and reused across 4 functions; readers boundary crossings are explicit and contained.
- `app/services/deck/storage.py` -- `evaluate_productivity()` (numpy-extract-compute-assign pattern for polynomial evaluation), `accumulate_productivity()` (BFS list-mutation pattern), `_hydro_accumulated_productivity_at_volume()` (inner join chain). No pandas imports.
- `app/services/deck/thermal.py` -- `_apply_thermal_single_change()` is a standalone `when/then/otherwise` helper called from multiple places; the maintenance function builds a daily range, applies per-thermal changes, and truncates back to monthly.
- `app/services/deck/policy.py` -- `pl.DataFrame({...})` constructors accept numpy arrays directly; `policy_variable_units()` builds four polars lookup DataFrames and chains four `.join()` calls.

---

## Conventions Adopted

### Naming temporaries with leading underscore prefix

When a join introduces a temporary column that will be used then dropped (e.g., `_avg_value`, `_block_len`, `_hm3_lower`, `_hm3_upper`, `_upper_val`, `_new_potencia`), the column is named with a `_` prefix and explicitly dropped after use. This prevents naming conflicts with existing columns and signals intent. See `app/services/deck/exchange.py` lines 43-83 and `app/services/deck/hydro.py` lines 120-150.

### Caching the result of every public domain function

Every public function follows the pattern: `val = cache.get(key); if val is None: <compute>; cache[key] = val; return val`. Private helpers (`_expand_hydro_to_stages`, `_apply_thermal_single_change`, etc.) are not cached. This is consistent across all 9 domain modules.

### `TYPE_CHECKING` guard for remaining pandas annotations

Functions that still accept a pandas DataFrame parameter (e.g., `num_hydro_simulation_stages_policy` in temporal.py taking `eers_df: "pd.DataFrame"`) use `from __future__ import annotations` + `if TYPE_CHECKING: import pandas as pd` so that pandas is never imported at runtime. See `app/services/deck/temporal.py` lines 1-11 and `app/services/deck/hydro.py` lines 1-22.

### Boundary crossing comments

Every `df.to_pandas()` / `pl.from_pandas(df_pd)` pair that crosses into a reader or modif function is preceded by a comment: `# convert to pandas for readers boundary`. This makes the temporary crossing visible in code review. See `app/services/deck/hydro.py` lines 97, 171, 305.

---

## Surprises and Deviations

### 1. `num_hydro_simulation_stages_policy` in temporal.py still uses pandas DataFrame parameter

- **Expected**: All temporal.py functions would be fully polars-native after ticket-005.
- **What happened**: `num_hydro_simulation_stages_policy`, `num_hydro_simulation_stages_final_simulation`, and `hydro_simulation_stages_ending_date_final_simulation` are called from policy.py with a raw inewave eers DataFrame that has not been converted. These functions use `.iloc[0]` and `.isna()` on the parameter, which remain pandas idioms.
- **Where**: `app/services/deck/temporal.py` lines 297-464. The functions are annotated `eers_df: "pd.DataFrame"` (string annotation, no runtime import).
- **Impact**: Low -- these functions return scalars, not DataFrames. The pandas import is guarded by `TYPE_CHECKING`.

### 2. `thermal_costs()` required `map_batches` instead of the planned `pl.date()` expression

- **Expected**: Date construction from year+month columns could use a polars native expression.
- **What happened**: The polars version available does not expose a `pl.date(year, month, day)` expression that accepts column references. `map_batches` was used instead with a Python lambda.
- **Where**: `app/services/deck/thermal.py` lines 365-379.
- **Impact**: Functional but slightly slower than a fully vectorized expression. Will benefit from a future polars version upgrade.

### 3. `_thermal_generation_bounds_term_manutt_expt` uses position-based Series assignment for lower bounds

- **Expected**: Lower bounds would be attached via a join on `[THERMAL_CODE_COL, "mes"]` to align term data with the expanded DataFrame.
- **What happened**: The term table has `mes` (month number) but the expanded DataFrame has `START_DATE_COL` (full datetime). Aligning them requires matching month number to month position, which is positional in practice. The implementation assigns the lower bounds array directly as `pl.Series(LOWER_BOUND_COL, lower_bounds)` after verifying that both arrays are in the same thermal-code + month order.
- **Where**: `app/services/deck/thermal.py` lines 215-218.
- **Impact**: Relies on sort order invariant. The preceding `.sort([THERMAL_CODE_COL, START_DATE_COL])` on the expanded DataFrame guarantees the order matches the sorted term DataFrame.

### 4. `storage_mod.evaluate_productivity()` uses a row-level Python loop, not pure polars expressions

- **Expected**: The plan suggested either `map_elements` or a fully vectorized polars expression for polynomial evaluation.
- **What happened**: The polynomial coefficients differ per row, and each regulated vs run-of-river plant requires different formula paths. A Python loop over rows (accessing pre-extracted numpy arrays by index) was used for correctness; the loop cost is acceptable because the number of hydro plants is small (< 200).
- **Where**: `app/services/deck/storage.py` lines 56-102.
- **Impact**: Not a performance bottleneck. A future improvement could vectorize by separating regulated and run-of-river plants into two filtered operations.

### 5. `energy.stored_energy_upper_bounds_inputs` retains its Python loop over configuration dates

- **Expected**: The `iterrows()` loop would be converted to a cross-join or batch polars operation.
- **What happened**: The loop is semantically correct and each iteration uses a different `stage_date` for the `hydro_drops_in_stages` filter. Converting to a cross-join would require joining drops for all stages simultaneously, which increases memory use without a clear benefit. The loop was kept, with each iteration operating on polars DataFrames.
- **Where**: `app/services/deck/energy.py` lines 99-122.
- **Impact**: None -- the number of configurations is small (< 200); the per-iteration polars operations are fast.

---

## Recommendations for Future Epics

### For Epic 03 (synthesis pipeline -- tickets 010-012)

- The synthesis pipeline modules (`pipeline.py`, `resolution_*.py`) consume deck domain functions. After this epic, all deck functions return `pl.DataFrame`. The pipeline modules have existing `# SHIM: remove after polars migration` annotations from epic-01; those shims are now stale and should be removed immediately at the start of epic-03.
- `resolution_ree.py`, `resolution_sbm.py`, `resolution_sbp.py` use `.filter()` and `.join()` on outputs from `entities`, `hydro`, `storage`, `thermal`. Since those outputs are now `pl.DataFrame`, the resolution modules can be ported without any internal `.to_pandas()` calls.
- `pipeline.py` currently calls `pd_to_pl()` on `block_lengths` (see epic-01 summary). After this epic, `misc.block_lengths()` returns polars natively; the `pd_to_pl()` call at `app/services/synthesis/operation/pipeline.py` line ~216 should be a no-op that can be removed.
- The `_stubs_helpers.py` and `_stubs_market.py` files have shims for `submarkets`, `exchange_bounds`, and related calls. Since all those now return polars, the shim removal is straightforward.

### For Epic 04 (scenario + cleanup -- tickets 013-014)

- `scenario.py` has a known gotcha: `Deck.vazoes()` returns a wide-format DataFrame with integer column names converted to strings by `pl.from_pandas()`. The caller at `app/services/synthesis/scenario.py` lines 134-136 currently re-casts with `[int(c) for c in df.columns]`. Epic-04 should convert this to a long-form (tidy) representation before porting scenario.py, eliminating the integer-column-name problem.
- `temporal.py` still has three functions (`num_hydro_simulation_stages_policy`, `num_hydro_simulation_stages_final_simulation`, `hydro_simulation_stages_ending_date_final_simulation`) that accept `eers_df: "pd.DataFrame"`. These are called from policy.py with a raw inewave DataFrame. If policy.py is further refactored to pass the already-cached polars `eers_df` from `entities.eers()`, those temporal functions can drop the pandas parameter.
- Ticket-014 (remove conversion utilities and dead imports) will find the cleanup straightforward: the `# SHIM` annotations serve as a removal checklist. After epic-03, the only remaining shims should be in `scenario.py`.
- Test coverage for domain modules is exercised exclusively through integration-style tests via the `DeckContext` facade. No unit tests for individual domain module functions were added during epic-02 (consistent with the existing test structure). Epic-04 cleanup should not require new domain-level unit tests.
