# Epic 02 — Statistics & Data Pipeline Optimization: Learnings

## Patterns Established

1. **pandas-boundary pattern** — Polars is used only inside a function; the function signature remains `pd.DataFrame -> pd.DataFrame`. Conversion is performed at entry (`pd_to_pl`) and exit (`pl_to_pd`). Callers never see Polars types. Established in `app/utils/operations.py` (`calc_statistics`) and `app/services/synthesis/operation.py` (`_post_resolve`, `_export_scenario_synthesis`).

2. **Single-pass group_by with unpivot** — Instead of calling `group_by` once per statistic (23 separate aggregations), all 23 statistics are computed in a single `group_by(...).agg([...])` call and then reshaped from wide to long format via `unpivot()`. This is the canonical Polars pattern for multi-statistic aggregations. See `_calc_statistics_polars` in `app/utils/operations.py`.

3. **Inline Polars sort chain** — The concat+sort combination is expressed as a one-liner: `pl.concat([pd_to_pl(df) for df in valid_dfs]).sort(cols, maintain_order=True)` wrapped in `pl_to_pd(...)`. This avoids holding an intermediate Polars variable and reads as a clear data pipeline. Used in `app/services/synthesis/operation.py` line 283-288 and `app/services/synthesis/scenario.py` line 998.

4. **`.assign()` for cache-safe column mutation** — Stub methods that derive a new variable from a cached DataFrame use `df.assign(**{VALUE_COL: new_values})` instead of in-place mutation. This creates a new DataFrame sharing all columns except the modified one and is explicit about which data is new. All stub methods (`__stub_QDEF`, `__stub_VDEF`, `__stub_VEVAP`, `__stub_CTO`, `__stub_EVER`, `_convert_volume_to_flow`, `_convert_flow_to_volume`) follow this pattern in `app/services/synthesis/operation.py`.

5. **Polars fallback to pandas** — The Polars hot path is wrapped in try/except. On any Polars exception, `calc_statistics` logs a warning and falls back to the original pandas implementation. This ensures production resilience while the Polars path matures. See `calc_statistics` in `app/utils/operations.py` lines 185-194.

## Architectural Decisions

1. **Centralized conversion utilities in `app/utils/dataframe.py`** — A dedicated module provides `pd_to_pl`, `pl_to_pd`, `pd_to_pl_lazy` as the single import location for all conversion code. Rejected: scattering `pl.from_pandas()` calls inline throughout service files. Rationale: a single module makes it trivial to swap the conversion strategy (e.g., Arrow-based zero-copy) in one place.

2. **No Polars in entity resolution subprocesses** — `_post_resolve_entity` and all `__resolve_*` methods remain pure pandas. Polars is only applied in the main-process aggregation step (`_post_resolve`). Rejected: converting inside each subprocess. Rationale: each subprocess resolves a single entity and the benefit of Polars for one entity is small; the conversion overhead would dominate. The payoff comes in the main-process concat of many entity DataFrames.

3. **Cache stores pandas DataFrames without copy** — `__store_in_cache_if_needed` stores `df` directly (no `.copy()`); `_get_from_cache` returns the reference directly. The responsibility for not mutating cached data is pushed to callers via the `.assign()` pattern. Rejected: storing Polars DataFrames in the cache (which are immutable by nature). Rationale: stubs feed into bounds resolution and export which both expect pandas; converting at every cache read would add overhead on hot paths.

4. **`maintain_order=True` in all group_by and sort calls** — All Polars operations that produce ordered results use `maintain_order=True`. This ensures deterministic output regardless of thread scheduling. Rejected: relying on Polars' default non-stable behaviour. Rationale: the synthesis pipeline's test suite compares row-by-row; non-stable ordering would produce spurious test failures.

5. **Single-pass aggregation over loop of group_by calls** — ticket-005 suggested two approaches: (a) 23 separate `group_by` calls with `pl.concat`, or (b) one `group_by` with 23 agg expressions plus `unpivot`. The implementation chose (b). This avoids scanning the full DataFrame 23 times and keeps the result in Polars memory until the final `pl_to_pd` conversion. The implementation is in `_calc_statistics_polars` in `app/utils/operations.py`.

## Files & Structures Created

- `app/utils/dataframe.py` — New module. Three thin wrappers: `pd_to_pl`, `pl_to_pd`, `pd_to_pl_lazy`. This is the sole import point for pandas/Polars boundary crossings.
- `tests/app/utils/test_dataframe.py` — New test module. Covers type fidelity, round-trip numeric accuracy, lazy frame creation, and empty DataFrame edge cases.
- `app/utils/operations.py` — Added `_calc_statistics_polars` (the Polars implementation) and modified `calc_statistics` to call it with fallback. The older `_calc_quantiles` and `_calc_mean_std` functions are retained as fallback implementations.
- `app/services/synthesis/operation.py` — Modified `_post_resolve` to use `pl.concat + sort`, modified `_export_scenario_synthesis` to use Polars sort, removed `.copy()` from cache store/retrieve, rewrote all stub methods to use `.assign()`.
- `app/services/synthesis/scenario.py` — Modified `_post_resolve` to use `pl.concat`, modified `_export_scenario_synthesis` equivalent to use Polars sort.
- `pyproject.toml` — Added `polars>=1.0.0` to `[project] dependencies`.

## Conventions Adopted

1. **Import only from `app.utils.dataframe`** — Any file that needs pandas/Polars conversion imports `pd_to_pl` or `pl_to_pd` from `app.utils.dataframe`, never calls `pl.from_pandas()` directly. Currently applied in `app/utils/operations.py`, `app/services/synthesis/operation.py`, and `app/services/synthesis/scenario.py`.

2. **Polars-internal function naming** — When a public function delegates to a Polars implementation, the Polars implementation is named with a `_polars` suffix (e.g., `_calc_statistics_polars`). This makes it easy to locate the Polars-specific code and allows the original pandas implementation to remain intact as fallback.

3. **`polars` imported at module level, not lazily** — Both `app/utils/operations.py` and `app/services/synthesis/operation.py` import `polars as pl` at the top of the file, not inside the function. This is consistent with the project's existing import style and avoids per-call import overhead.

4. **No Polars in subprocess scope** — Per the architecture decision above, no `import polars` appears in any function that executes inside a `multiprocessing.Pool` worker. This avoids Polars' thread pool initialization competing with Python's process pool.

5. **`.assign()` for any cache-derived mutation** — Any method that reads from `CACHED_SYNTHESIS` and needs to modify `VALUE_COL` must use `df.assign(**{VALUE_COL: ...})` rather than in-place assignment. All stubs in `operation.py` (lines 880, 908, 933, 957, 981, 1005, 1029, 1735) follow this pattern.

## Surprises & Deviations

1. **`unpivot` instead of `melt`** — The ticket's suggested implementation used `unpivot` (which is the Polars 1.x API name for what older Polars called `melt`). The actual implementation in `_calc_statistics_polars` uses `unpivot` correctly, which is the right API for Polars >= 1.0. A developer coming from Polars < 1.0 would need to be aware of this rename. See `app/utils/operations.py` line 153.

2. **scenario.py `_post_resolve` does not sort** — ticket-006 planned to apply Polars sort in `ScenarioSynthetizer._post_resolve()` equivalent. The actual implementation at `app/services/synthesis/scenario.py` line 998 uses `pl.concat` without a subsequent sort. The sort is applied in the export step (`_export_scenario_synthesis`) at line 1520-1524 instead. This is a valid deviation because scenario synthesis does not have the same spatial-entity sort requirement as operation synthesis.

3. **Epic-02 changes are uncommitted** — All four tickets' changes are in the working tree only (untracked `app/utils/dataframe.py` and `tests/app/utils/test_dataframe.py`; modified tracked files). The git baseline commit for this epic is `7eb13c6`. A commit should be made before moving to epic-03.

4. **Old pandas fallback functions retained** — `_calc_quantiles` and `_calc_mean_std` in `app/utils/operations.py` were not removed. They serve as the fallback path inside `calc_statistics`. The ticket did not require their deletion, and retaining them increases resilience. Future epics should evaluate whether to remove them once the Polars path has been stable in production.

5. **Cache still holds pandas DataFrames** — The ticket offered Polars immutable DataFrames as an alternative cache storage strategy but the implementation kept pandas in the cache. The `.assign()` pattern is simpler and avoids per-read conversion costs. The ticket explicitly listed "Changing the cache data structure" as out of scope, so this matches the plan.

## Recommendations for Future Epics

- **Do not `import polars` inside subprocess-dispatched functions** — Keep the subprocess boundary at `app/services/synthesis/operation.py`'s entity resolution methods (`__resolve_SIN`, `__resolve_UHE`, etc.). When epic-03 migrates temporal resolution and entity post-processing, the Polars operations should be applied after the `_post_resolve` aggregation in the main process, not inside the per-entity functions.

- **The `pd_to_pl_lazy` function is unused** — `app/utils/dataframe.py` exports `pd_to_pl_lazy` but no production code calls it. Epic-03 should either use it for lazy evaluation chains or remove it to avoid dead code.

- **Benchmark before and after each migration** — The epic-02 tickets referenced 5x and 50% speedup targets, but no benchmark results were recorded in the learnings. Epic-03 should capture a timing table (variable, rows, pandas time, polars time) to justify the migration and catch regressions.

- **Test coverage gap: `calc_statistics` numerical parity** — The tests in `tests/app/utils/test_dataframe.py` cover the conversion utilities but there is no test that calls `calc_statistics` and compares its output to the pandas fallback on the same input. This is the highest-risk correctness gap from this epic. Add such a test before or during epic-03.

- **Commit all epic-02 changes** — `app/utils/dataframe.py` and `tests/app/utils/` are untracked. Make a single commit for the full epic before starting epic-03 refinement.
