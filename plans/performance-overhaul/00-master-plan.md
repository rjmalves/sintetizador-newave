# Master Plan: sintetizador-newave Performance and Architecture Overhaul

## Executive Summary

The sintetizador-newave application post-processes NEWAVE HPC output files (positional text) into Parquet files for analytics. With tens of thousands of output files (e.g., ~80 variables x ~200 UHEs), the current runtime is approximately 40 minutes even with 64+ threads. This plan targets an 8x improvement (under 5 minutes) through a systematic overhaul of the data processing pipeline, parallelism model, and code architecture. The approach is incremental: each epic delivers measurable speedups while keeping the test suite green and the CLI/Parquet schema unchanged.

## Goals & Non-Goals

### Goals

- Reduce end-to-end runtime from ~40 minutes to under 5 minutes for a typical large case
- Eliminate the `chdir()` anti-pattern that makes multiprocessing unsafe
- Replace per-entity statistics computation with a single post-concatenation pass
- Migrate from pandas to Polars for all groupby, quantile, and aggregation operations
- Restructure parallelism to avoid redundant deck re-reads in subprocesses
- Decompose the four monolithic files (operation.py 2538 lines, deck.py 4283 lines, files.py 1912 lines, scenario.py 1663 lines)
- Maintain 100% backward compatibility for CLI interface and Parquet output schema

### Non-Goals

- Changing the inewave library API (it is an external dependency)
- Modifying the NEWAVE output file format
- Adding new synthesis variables or spatial resolutions
- Changing the Click CLI framework
- Supporting real-time or streaming output
- Rewriting in a different language

## Architecture Overview

### Current State

```
CLI (Click)
  |
  v
handlers.py -- dispatches to Synthetizer classes
  |
  v
OperationSynthetizer (2538 lines, class-level state)
  |-- for each variable (sequential):
  |     |-- resolve spatial resolution
  |     |     |-- multiprocessing.Pool per entity (SBM/REE/UHE)
  |     |     |     |-- each subprocess: chdir(), re-create UoW, re-read deck files, read ONE nwlistop file
  |     |     |     |-- calc_statistics() per entity BEFORE concatenation
  |     |     |-- pd.concat all entities
  |     |-- export to Parquet
  |
  v
Deck (4283 lines, class-level DECK_DATA_CACHING dict)
  |-- re-read in every subprocess (cache not shared)
  |
  v
RawFilesRepository (1912 lines)
  |-- __regras dict: (Variable, SpatialResolution) -> lambda reader
  |-- __read_nwlistop_setting_version: BlockFile.read(path).valores per file
```

**Key problems:**

1. `os.chdir()` in `FSUnitOfWork.__enter__` -- global process state, not safe for concurrent access
2. `Deck.DECK_DATA_CACHING` is a class-level dict -- not shared across multiprocessing subprocesses, causing redundant re-reads
3. `calc_statistics()` computes 21 quantiles + mean + std per entity, then entities are concatenated -- statistics should be computed once after concatenation
4. Variables are processed sequentially in a for loop -- no batching by spatial resolution
5. Each nwlistop file is read individually via `BlockFile.read(path).valores` -- no batch I/O
6. Extensive use of `pd.concat()` in loops creating intermediate DataFrames
7. `df.copy()` in cache storage and retrieval doubles memory usage

### Target State

```
CLI (Click) -- unchanged interface
  |
  v
handlers.py -- dispatches to refactored Synthetizer classes
  |
  v
OperationSynthetizer (decomposed into modules)
  |-- pre-read all deck data in main process (shared via fork or serialization)
  |-- group variables by spatial resolution
  |-- for each resolution group:
  |     |-- concurrent.futures.ProcessPoolExecutor for CPU work
  |     |-- each subprocess receives pre-computed deck data (no re-read)
  |     |-- no chdir() -- paths passed explicitly
  |     |-- Polars LazyFrame for groupby/quantile/aggregation
  |     |-- statistics computed ONCE after full concatenation
  |     |-- batch Parquet export
```

### Key Design Decisions

1. **Polars over pandas for hot-path operations**: Polars provides native multi-threaded execution for groupby and quantile operations, lazy evaluation to avoid materializing intermediates, and is 10-50x faster for these workloads. pandas will be retained only at the boundary with inewave (which returns pandas DataFrames) and for the final Parquet export schema enforcement.

2. **Eliminate chdir() entirely**: The `FSUnitOfWork` will store an absolute path and pass it to all file operations. No global process state mutation.

3. **Pre-compute and serialize deck data**: All deck file reads happen once in the main process. The resulting data is passed to subprocesses either via fork (Linux COW semantics) or explicit serialization, eliminating redundant re-reads.

4. **concurrent.futures over multiprocessing.Pool**: More modern API, better error handling, context manager support. ProcessPoolExecutor for CPU-bound work, ThreadPoolExecutor for I/O-bound work.

5. **Statistics after concatenation, not before**: The `_post_resolve_entity` method currently calls `calc_statistics()` per entity. This will be moved to after all entities are concatenated, computing statistics once over the full DataFrame.

## Technical Approach

### Tech Stack

- **Python >= 3.10** (unchanged)
- **Polars**: New dependency for data manipulation (replaces pandas in hot paths)
- **pandas**: Retained at inewave boundary and for backward-compatible DataFrame handling
- **pyarrow >= 18**: Unchanged, used for Parquet writing
- **click >= 8.1.7**: Unchanged CLI framework
- **inewave >= 1.9.2**: External dependency, API stable
- **concurrent.futures**: Stdlib replacement for multiprocessing.Pool
- **numba >= 0.60**: Will be evaluated -- may be removable if Polars handles all hot paths

### Component/Module Breakdown

| Component    | Current                                       | Target                                                             |
| ------------ | --------------------------------------------- | ------------------------------------------------------------------ |
| UnitOfWork   | `chdir()` in `__enter__`                      | Absolute path stored, no `chdir()`                                 |
| Deck caching | Class-level dict, not shared                  | Pre-computed in main process, serialized to workers                |
| Statistics   | Per-entity, pandas groupby                    | Post-concatenation, Polars native                                  |
| File reading | Individual `BlockFile.read()` per entity      | Same API but no redundant deck reads                               |
| Parallelism  | `multiprocessing.Pool.apply_async` per entity | `concurrent.futures.ProcessPoolExecutor` with pre-computed context |
| Data flow    | pandas throughout                             | inewave->pandas->Polars (hot path)->pandas (export boundary)       |
| operation.py | 2538 lines monolith                           | Split into resolution modules                                      |
| deck.py      | 4283 lines monolith                           | Split into logical groups                                          |

### Data Flow

```
inewave reader -> pd.DataFrame (boundary)
    -> pl.from_pandas() at pipeline entry
    -> Polars LazyFrame operations (temporal resolution, entity enrichment)
    -> pl.concat() all entities
    -> calc_statistics() via Polars (single pass)
    -> .to_pandas() at export boundary
    -> pyarrow Parquet write
```

### Testing Strategy

- All existing tests must pass at every commit
- Each epic adds targeted benchmarks for the changed components
- Integration tests verify Parquet output schema unchanged
- Performance regression tests compare before/after for key operations

## Phases & Milestones

| Epic | Name                                    | Duration  | Key Deliverable                                      | Expected Impact          |
| ---- | --------------------------------------- | --------- | ---------------------------------------------------- | ------------------------ |
| 1    | Foundation Fixes                        | 1-2 weeks | Eliminate chdir(), fix deck caching, move statistics | ~2-3x speedup            |
| 2    | Statistics & Data Pipeline Optimization | 1-2 weeks | Polars for statistics, optimized concat patterns     | ~2-3x additional speedup |
| 3    | Polars Migration                        | 2-3 weeks | Full hot-path migration to Polars LazyFrames         | ~2-4x additional speedup |
| 4    | Parallelism Overhaul                    | 2-3 weeks | New parallelism model with pre-computed context      | Eliminate redundant I/O  |
| 5    | Code Decomposition & Cleanup            | 2-3 weeks | Split monoliths, type safety, remove dead code       | Maintainability          |

## Risk Analysis

| Risk                                           | Probability | Impact | Mitigation                                                          |
| ---------------------------------------------- | ----------- | ------ | ------------------------------------------------------------------- |
| Polars incompatibility with inewave DataFrames | Low         | High   | Keep pandas at boundary, convert only in hot path                   |
| Test suite breaks during refactoring           | Medium      | High   | Each ticket is independently testable; run tests after every change |
| inewave API changes during development         | Low         | Medium | Pin inewave version; adapter pattern isolates changes               |
| Polars quantile results differ from pandas     | Medium      | Medium | Validate numerical equivalence in tests with tolerance              |
| Multiprocessing serialization overhead         | Medium      | Medium | Profile serialization cost; use fork-based sharing on Linux         |
| numba removal breaks edge cases                | Low         | Medium | Keep numba as fallback until Polars path is validated               |

## Success Metrics

1. **Runtime**: Full operation synthesis completes in under 5 minutes for a case with 200 UHEs, 2000 scenarios, 120 stages, 3 blocks
2. **Memory**: Peak memory usage does not exceed 2x the size of the output Parquet files
3. **Correctness**: All existing tests pass; output Parquet files are byte-identical (or numerically equivalent within floating-point tolerance) to current output
4. **Code quality**: No file exceeds 500 lines; all public functions have type annotations; ruff passes clean
5. **Test coverage**: Each new module has corresponding unit tests
