# Accumulated Learnings — Epic 05 (covers Epics 01 + 02 + 03 + 04 + 05)

## Architecture & Boundaries

- `FSUnitOfWork` uses absolute paths internally; `chdir()` was safely removed with no downstream breakage (`app/utils/fs.py`)
- `DeckContext` dataclass pre-computes deck data in the main process and passes it via pickle to subprocesses; keep it small (~KB) and use `Optional[DeckContext] = None` for gradual adoption (`app/services/deck/context.py`)
- Polars boundary: temporal resolution (`_resolve_temporal_resolution`), entity post-processing (`_post_resolve_entity`), bounds computation (`resolve_bounds`), Parquet export (`synthetize_pl`); `_post_resolve` still converts to pandas at exit (early/late hooks expect pandas)
- No `import polars` inside subprocess-dispatched entity resolution methods; Polars thread pool must not compete with Python process pool
- `ProcessPoolExecutor` (stdlib `concurrent.futures`) is the pool API for both `operation/` package and `scenario.py`; `uow.queue` (`multiprocessing.Queue`) is kept for subprocess logging
- Module decomposition strategy: **file -> package** (for `operation.py`) vs **flat siblings** (for `deck.py`): use package conversion when the module name would conflict with the new directory name; use flat siblings when sibling files already exist in the parent directory and the class import path stays the same

## Decomposition Patterns

- **Package conversion**: `operation.py` -> `app/services/synthesis/operation/` package with `__init__.py` re-exporting `OperationSynthetizer`; 17 files, all under 500 lines (`app/services/synthesis/operation/`)
- **Facade with flat domain modules**: `deck.py` (4283 lines) -> thin facade (373 lines) + 11 domain modules in `app/services/deck/`; `Deck` class retains thin `@classmethod` wrappers delegating to free functions
- **Registry dict extraction**: `RawFilesRepository.__regras` (790 lines) -> `app/adapters/repository/mappings/` with one `get_rules(repo)` function per variable category; `build_regras(repo)` in `mappings/__init__.py` merges all
- **Free functions receive `cls` as first parameter**: Extracted methods become module-level free functions taking `cls: Type[OperationSynthetizer]`; thin `@classmethod` wrappers remain in the orchestrator class
- **Sub-helper split for 500-line limit**: `stubs.py` split into `stubs.py` + `_stubs_helpers.py` + `_stubs_market.py`; public API re-exported via `stubs.py`'s `__all__`; `_` prefix signals internal helper

## Parallelism Patterns

- One `ProcessPoolExecutor` per spatial-resolution group in `orchestrator.py`; `current_resolution` / `current_executor` sentinels track active group; executor shut down at each resolution boundary and after the loop
- `executor: Optional[ProcessPoolExecutor] = None` threaded through 5 call levels; same pattern as `Optional[DeckContext] = None`
- `assert executor is not None` at pool call site in each `resolve_*` function; no silent local fallback; assertion message names the correct entry point
- `_resolve_spatial_resolution()` special-cases `SpatialResolution.SISTEMA_INTERLIGADO` to skip executor — single-file read, no pool needed
- `resolve_SBM_entity_MER_MERL` (in `_stubs_market.py`) creates its own `ProcessPoolExecutor` locally — group executor injection remains deferred

## mock.patch Compatibility After Package Conversion

- `unittest.mock.patch("module.X")` resolves `X` as an attribute of the module object; for a package, the module object is `__init__.py`
- After converting `operation.py` to a package, add `# noqa: F401` imports to `__init__.py` for every name that tests patch: `Deck`, `pd_to_pl`, `pl_to_pd`, `ProcessPoolExecutor`
- Pattern lives at `app/services/synthesis/operation/__init__.py`; apply this audit to any future module-to-package conversion

## Polars Integration Patterns

- Single import point: `from app.utils.dataframe import pd_to_pl, pl_to_pd`; never `pl.from_pandas()` directly in service files
- Single-pass `group_by(...).agg([N exprs]).unpivot()` for multi-statistic aggregations (`app/utils/operations.py` — `_calc_statistics_polars`)
- Always `maintain_order=True` in Polars `group_by` and `sort`
- Polars implementations named with `_polars` suffix; wrap in try/except with pandas fallback at every new conversion point
- `unpivot` is Polars >= 1.0 API; `isinstance(dtype, pl.Datetime)` required for parameterized types
- Polars `write_parquet(use_pyarrow=True)` does not embed pandas metadata; convert `pl.DataFrame -> Arrow -> pandas -> PyArrow table` before `pq.write_table`

## Cache Safety

- `CACHED_SYNTHESIS` stores pandas DataFrames with no `.copy()` on store or retrieve (`operation/cache.py`)
- Any method reading from cache that modifies `VALUE_COL` must use `df.assign(**{VALUE_COL: ...})` — never mutate in-place

## Lint & Type Conventions

- `ruff check app/` is the authoritative lint gate: `select = ["E", "F", "W", "I"]`, `ignore = ["E501"]`, `known-first-party = ["app"]` (`pyproject.toml`)
- Adding the `I` (isort) rule generates a large mechanical diff; run `ruff --fix` in a dedicated commit before functional changes to keep history clean
- `# type: ignore` target is <= 10 in `app/`; remove from `import pandas` / `import numpy` (stubs available); retain on inewave imports and returns
- Type hints use `Optional[X]`, `Dict`, `List`, `Tuple` from `typing` (not `X | None` or lowercase generics) for Python 3.9 compatibility

## Testing Conventions

- `_TrackedExecutor` stub + `patch` pattern for executor lifecycle counting: patch `app.services.synthesis.operation.ProcessPoolExecutor` with a minimal recording class; simultaneously patch `_synthetize_single_variable` to no-op (`tests/app/services/synthesis/test_operation.py`)
- Polars/pandas parity tests: `np.testing.assert_allclose(atol=1e-6)` for floats, `np.testing.assert_array_equal` for integers/datetimes
- `caplog` + `OperationSynthetizer.logger = test_logger` to capture `_log` warnings
- Pre-existing test failures (`test_sintese_merl_sbm`, QINC tests) are `Settings().installdir = None` environment issues — exclude them from DoD verification rather than treating as regressions

## Dead Code Removed

- `pd_to_pl_lazy` removed from `app/utils/dataframe.py` (flagged in epics 02-04, finally removed in ticket-018)
- `_calc_quantiles` and `_calc_mean_std` removed from `app/utils/operations.py` (pandas fallback paths superseded by stable Polars `_calc_statistics_polars`)
- Thread executor adoption (ticket-014) is deferred, not cancelled; re-run `benchmarks/bench_executor.py` when NEWAVE output data is available

## mappings/ Extension Guide

- Add a new variable category: create `mappings/newcategory.py` with `get_rules(repo)`, add `rules.update(newcategory.get_rules(repo))` in `mappings/__init__.py`; no other files need changes
- Each category imports only the inewave reader classes it uses (not all 95)
- Use `from __future__ import annotations` + `TYPE_CHECKING` guard in all mapping modules to avoid circular imports
