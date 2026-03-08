# Accumulated Learnings — Epic 03 (covers Epics 01 + 02 + 03)

## Architecture & Boundaries

- `FSUnitOfWork` uses absolute paths internally; `chdir()` was safely removed with no downstream breakage (`app/utils/fs.py`)
- `DeckContext` dataclass pre-computes deck data in the main process and passes it via pickle to subprocesses; keep it small (~KB) and use `Optional[DeckContext] = None` for gradual adoption (`app/services/deck/context.py`)
- Polars is used inside individual methods and pipeline steps; outer public API stays pandas until each boundary is deliberately pushed
- Polars boundary has now been pushed to: temporal resolution (`_resolve_temporal_resolution`), entity post-processing (`_post_resolve_entity`), bounds computation (`resolve_bounds`), and Parquet export (`synthetize_pl`)
- `_post_resolve` still converts to pandas at exit; `early_hooks` and `late_hooks` inside it expect pandas — migrating those hooks is the remaining blocker for a fully Polars main pipeline
- No `import polars` inside subprocess-dispatched entity resolution methods; Polars thread pool must not compete with Python process pool

## Polars Integration Patterns

- Single import point for all conversions: `from app.utils.dataframe import pd_to_pl, pl_to_pd` — never call `pl.from_pandas()` directly in service files (exception: fallback paths that must remain independent of `pd_to_pl` patches)
- Single-pass `group_by(...).agg([N exprs]).unpivot()` for multi-statistic aggregations; avoids N sequential DataFrame scans (`app/utils/operations.py` — `_calc_statistics_polars`)
- Always `maintain_order=True` in Polars `group_by` and `sort` — default is non-stable, which breaks row-by-row test comparisons
- Polars internal implementations named with `_polars` suffix and nested inside the public method when keeping the fallback collocated is important (e.g., `_add_temporal_info_polars`, `_resolve_starting_stage_polars`)
- Wrap Polars hot paths in try/except with logger warning and pandas fallback at every new Polars conversion point
- `unpivot` is the Polars >= 1.0 API (previously `melt`); use `unpivot` throughout
- Partial numpy retention inside Polars functions is acceptable: when result shape is known upfront, constructing `pl.Series` from `np.tile`/`np.repeat` arrays is idiomatic and fast
- Join on `(START_DATE_COL, BLOCK_COL)` replaces Python for-loops over block-length lookups (`app/services/synthesis/operation.py` lines 522-535)
- `_calc_block_0_weighted_mean` uses two `group_by` passes (one for weighted sum, one for `pl.first()` representative rows) joined together; single-pass was impossible without including all columns as group keys

## Conversion Boundary Placement

- Additive method pattern for export: add `synthetize_pl` to the abstract interface with a default fallback; override only in `ParquetExportRepository`; CSV and test repos inherit the default (`app/adapters/repository/export.py`)
- Wrap-and-delegate for complex subsystems: `resolve_bounds` accepts/returns `pl.DataFrame` but converts to pandas internally for all ~10 helpers; the conversion lives at the method boundary, not scattered inside helpers (`app/services/deck/bounds.py`)
- `isinstance` guard before conversion for code paths that may receive either type: `pd_to_pl(df) if isinstance(df, pd.DataFrame) else df` — used where cache-sourced DataFrames feed into Polars pipelines (`app/services/synthesis/operation.py` lines 2583-2584)
- Fallback path in `_resolve_temporal_resolution` uses `pl.from_pandas` directly (not `pd_to_pl`) so that tests can patch `pd_to_pl` to trigger the fallback without side effects on the fallback's own conversion (`app/services/synthesis/operation.py` line 552)

## Parquet Export

- Polars `write_parquet(use_pyarrow=True)` does not embed pandas Parquet metadata; `pd.read_parquet` therefore does not restore UTC datetime dtype on read-back
- Workaround: convert `pl.DataFrame → Arrow → pandas → PyArrow table` before calling `pq.write_table`; this embeds the pandas metadata with correct UTC annotations
- `isinstance(dtype, pl.Datetime)` is required (not `dtype == pl.Datetime`) because Polars datetime types carry time_unit and time_zone parameters; equality check fails for parameterized types

## Cache Safety

- `CACHED_SYNTHESIS` stores pandas DataFrames with no `.copy()` on store or retrieve (`operation.py` lines ~2601, 2636)
- Any method reading from cache that modifies `VALUE_COL` must use `df.assign(**{VALUE_COL: ...})` — never mutate in-place; all stubs follow this pattern

## Data Pipeline Milestones

- Statistics moved from per-entity to post-concatenation (ticket-003); called once per variable instead of ~200 times for UHE
- Per-entity `pd_to_pl` conversion in `_post_resolve` eliminated (ticket-009); `pl.concat` now receives native `pl.DataFrame` objects from entity resolvers
- `_resolve_bounds` boundary added one `pd_to_pl` / `pl_to_pd` pair but eliminated the previous pattern of bounds helpers receiving raw pandas from outside the function
- Parquet export now bypasses direct pandas-to-PyArrow path for the scenario DataFrame (ticket-011)

## Testing Conventions

- Test stubs replicate closure logic directly when the closure is defined inside an entity resolution method and cannot be imported directly; see `TestCalcBlock0WeightedMean._invoke_stub` in `tests/app/services/synthesis/test_entity_pipeline.py`
- New Polars test modules follow naming pattern: `test_<subsystem>_polars.py` (e.g., `test_bounds_polars.py`, `test_export_polars.py`)
- Polars/pandas parity tests use `np.testing.assert_allclose` with `atol=1e-6` for float columns and `np.testing.assert_array_equal` for integer/datetime columns
- `caplog` + `OperationSynthetizer.logger = test_logger` pattern required for capturing `_log` warnings in unit tests (the class uses its own logger field, not the module logger)

## Dead Code & Technical Debt

- `pd_to_pl_lazy` is exported from `app/utils/dataframe.py` but unused in production code across all three epics — remove or adopt in epic-04 lazy chains
- `_calc_quantiles` and `_calc_mean_std` retained in `app/utils/operations.py` as pandas fallback paths — remove only after Polars `calc_statistics` is proven stable in production
- Closure-style internal stubs (`_calc_block_0_weighted_mean`, etc.) cannot be imported directly for testing; consider extracting as `@staticmethod` to enable direct test import without reconstruction
