# Accumulated Learnings — Epic 04 (covers Epics 01 + 02 + 03 + 04)

## Architecture & Boundaries

- `FSUnitOfWork` uses absolute paths internally; `chdir()` was safely removed with no downstream breakage (`app/utils/fs.py`)
- `DeckContext` dataclass pre-computes deck data in the main process and passes it via pickle to subprocesses; keep it small (~KB) and use `Optional[DeckContext] = None` for gradual adoption (`app/services/deck/context.py`)
- Polars boundary has been pushed to: temporal resolution (`_resolve_temporal_resolution`), entity post-processing (`_post_resolve_entity`), bounds computation (`resolve_bounds`), and Parquet export (`synthetize_pl`); `_post_resolve` still converts to pandas at exit (early/late hooks expect pandas)
- No `import polars` inside subprocess-dispatched entity resolution methods; Polars thread pool must not compete with Python process pool
- `ProcessPoolExecutor` (stdlib `concurrent.futures`) replaces `multiprocessing.Pool` in both `operation.py` and `scenario.py`; `uow.queue` (`multiprocessing.Queue`) is kept for subprocess logging and is independent of the pool API

## Parallelism Patterns

- One `ProcessPoolExecutor` is allocated per spatial-resolution group in `synthetize()`, tracked by `current_resolution` / `current_executor` sentinels; shut down at each resolution boundary and after the loop (`app/services/synthesis/operation.py` around line 2884)
- The `executor: Optional[ProcessPoolExecutor] = None` parameter is threaded through five levels: `synthetize()` -> `_synthetize_single_variable()` -> `_resolve_synthesis()` -> `_resolve_spatial_resolution()` -> each `__resolve_*()` method; follows the same pattern as `Optional[DeckContext] = None`
- `__resolve_*` methods assert `executor is not None` at the pool call site; no silent local fallback; the assertion message names the correct entry point
- `_resolve_spatial_resolution()` special-cases `SpatialResolution.SISTEMA_INTERLIGADO` to skip passing the executor (SIN reads a single file, no pool needed)
- `_resolve_SBM_MER_MERL` (nested inside `__resolve_UTE`) creates its own local executor independently — architectural anomaly to resolve in epic-05 decomposition
- `ScenarioSynthetizer` parallelism model unchanged: still `ProcessPoolExecutor` per-iteration; variable-group grouping does not apply because iterations are independent and do not share entity lists
- Thread-based I/O parallelism was evaluated (ticket-014) but not adopted: GIL status of `cfinterface` parsers is unmeasured, `Log.configure_process_logger` uses `multiprocessing.Queue` (not thread-safe with `ThreadPoolExecutor`), and no empirical data met the 10% speedup threshold; findings in `plans/performance-overhaul/epic-04-parallelism-overhaul/BENCHMARKS.md`

## Polars Integration Patterns

- Single import point for all conversions: `from app.utils.dataframe import pd_to_pl, pl_to_pd`; never call `pl.from_pandas()` directly in service files
- Single-pass `group_by(...).agg([N exprs]).unpivot()` for multi-statistic aggregations (`app/utils/operations.py` — `_calc_statistics_polars`)
- Always `maintain_order=True` in Polars `group_by` and `sort` — non-stable default breaks row-by-row test comparisons
- Polars internal implementations named with `_polars` suffix; wrap in try/except with pandas fallback at every new conversion point
- `unpivot` is the Polars >= 1.0 API (previously `melt`)
- `isinstance(dtype, pl.Datetime)` required (not `==`) because parameterized Polars types carry time_unit and time_zone
- Polars `write_parquet(use_pyarrow=True)` does not embed pandas metadata; workaround: convert `pl.DataFrame -> Arrow -> pandas -> PyArrow table` before `pq.write_table`

## Conversion Boundary Placement

- Additive method pattern for export: `synthetize_pl` added to abstract interface with default fallback; overridden only in `ParquetExportRepository` (`app/adapters/repository/export.py`)
- Wrap-and-delegate for complex subsystems: `resolve_bounds` accepts/returns `pl.DataFrame` but converts to pandas internally for all helpers (`app/services/deck/bounds.py`)
- `isinstance` guard before conversion where cache-sourced DataFrames feed into Polars pipelines: `pd_to_pl(df) if isinstance(df, pd.DataFrame) else df`

## Cache Safety

- `CACHED_SYNTHESIS` stores pandas DataFrames with no `.copy()` on store or retrieve (`operation.py`)
- Any method reading from cache that modifies `VALUE_COL` must use `df.assign(**{VALUE_COL: ...})` — never mutate in-place; all stubs follow this pattern

## Testing Conventions

- `_TrackedExecutor` stub + `patch` pattern for counting executor lifecycles: patch `app.services.synthesis.operation.ProcessPoolExecutor` with a minimal class that records `__init__` calls; simultaneously patch `_synthetize_single_variable` to a no-op to avoid I/O (`tests/app/services/synthesis/test_operation.py` — `test_executor_criado_uma_vez_por_grupo_resolucao_uhe`)
- Test stubs replicate closure logic directly when closures cannot be imported; see `TestCalcBlock0WeightedMean._invoke_stub` in `tests/app/services/synthesis/test_entity_pipeline.py`
- Polars/pandas parity tests use `np.testing.assert_allclose(atol=1e-6)` for float columns and `np.testing.assert_array_equal` for integer/datetime columns
- `caplog` + `OperationSynthetizer.logger = test_logger` required for capturing `_log` warnings (class uses its own logger field)

## Dead Code & Technical Debt

- `pd_to_pl_lazy` exported from `app/utils/dataframe.py` but unused across all four epics — remove or adopt in epic-05 lazy chains
- `_calc_quantiles` and `_calc_mean_std` retained in `app/utils/operations.py` as pandas fallback paths — remove only after Polars `calc_statistics` is proven stable in production
- `_resolve_SBM_MER_MERL` nested function inside `__resolve_UTE` should be extracted as `@classmethod` in epic-05 and receive the group executor via injection
- Thread executor adoption (ticket-014) is deferred, not cancelled; re-run `benchmarks/bench_executor.py` when NEWAVE output data is available before adopting `ThreadPoolExecutor`
