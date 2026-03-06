# Master Plan: Polars-Native Migration

## Executive Summary

Migrate sintetizador-newave from a hybrid pandas/polars architecture (where pandas is the internal representation with polars used only for hot-path operations) to a polars-native architecture where polars is the internal representation throughout, with pandas used only at the inewave boundary. This eliminates ~6 unnecessary DataFrame copies per variable, ~30 pd/pl conversion call sites, and ~70 lines of dead fallback code.

## Goals & Non-Goals

### Goals

- Eliminate all unnecessary pd-to-pl and pl-to-pd conversions in the data pipeline
- Make `pl.DataFrame` the return type of all Deck facade methods that currently return `pd.DataFrame`
- Remove defensive `.copy()` calls in accessors.py (polars DataFrames are immutable)
- Remove pandas fallback paths in pipeline.py and resolution modules
- Remove dual implementations (e.g., `resolve_starting_stage` + `resolve_starting_stage_polars`)
- Convert DeckContext to store polars DataFrames
- Maintain 100% test pass rate throughout the migration

### Non-Goals

- Modifying the inewave library (external dependency, returns pandas)
- Adding new synthesis variables or features
- Changing the Parquet/CSV export format
- Optimizing the inewave file-reading layer (readers.py stays pandas internally)
- Rewriting the parallelism architecture

## Architecture Overview

### Current State

```
inewave (pandas) --> accessors cache (pandas + .copy()) --> domain modules (pandas)
--> pipeline: pd_to_pl() --> polars processing --> pl_to_pd() --> cache (pandas)
--> export: pd_to_pl() --> sort --> pl_to_pd() --> pd_to_pl() --> parquet write
```

Key problems:

- 7 defensive `.copy()` calls in accessors.py returning pandas DataFrames
- ~30 `pd_to_pl()` / `pl_to_pd()` conversion call sites
- ~70 lines of try/except pandas fallback dead code in pipeline.py
- Dual implementations of `resolve_starting_stage` (pandas) and `resolve_starting_stage_polars` (polars)
- DeckContext stores pandas DataFrames that get converted to polars downstream
- Export pipeline converts pd->pl->pd->pl->parquet

### Target State

```
inewave (pandas) --> accessors: pl.from_pandas() ONCE --> cache (polars, immutable)
--> domain modules (polars) --> pipeline (polars, no conversions)
--> export: polars native parquet write
```

Key improvements:

- Single conversion point at accessors.py boundary
- No `.copy()` calls needed (polars immutability)
- No `pd_to_pl()` / `pl_to_pd()` utility functions needed
- Single implementation of each function (polars only)
- DeckContext stores polars DataFrames
- Direct polars-to-parquet export path

### Key Design Decisions

1. **Conversion boundary at accessors.py**: Each accessor function that returns a DataFrame will call `pl.from_pandas()` on the inewave result before caching. The cached value is a `pl.DataFrame`. No `.copy()` needed.

2. **Deck facade return types change**: All `Deck` classmethods that currently return `pd.DataFrame` will return `pl.DataFrame`. This is a breaking change for all consumers (pipeline, bounds, spatial resolution, scenario, export, tests).

3. **DeckContext stores polars**: The `DeckContext` dataclass fields `block_lengths`, `eer_submarket_map`, and `hydro_eer_submarket_map` change from `pd.DataFrame` to `pl.DataFrame`.

4. **Indexed DataFrames become regular columns**: pandas `set_index()` patterns (used extensively in entities.py, hydro.py, storage.py) have no polars equivalent. Index columns become regular columns, and `.at[idx, col]` lookups become `.filter().select()` or join operations.

5. **Export adapter**: `read_df()` stays pandas (reads existing parquet files for append operations). `synthetize_pl()` is already implemented and will become the primary write path.

## Technical Approach

### Tech Stack

- Python >= 3.10
- Polars >= 1.0.0 (primary DataFrame library)
- pandas (inewave boundary only, in readers.py and accessors.py pre-conversion)
- pyarrow >= 18 (Parquet export)
- inewave >= 1.9.2 (external, returns pandas)

### Component/Module Breakdown

**Layer 1 - Conversion Boundary (Epic 1)**:

- `accessors.py`: Convert inewave pandas results to polars at cache insertion
- `deck.py`: Update return type annotations
- `context.py`: Store polars DataFrames

**Layer 2 - Deck Domain Modules (Epic 2)**:

- `entities.py`: Port entity catalog functions to polars
- `temporal.py`: Port temporal functions (mostly scalar returns, some DataFrames)
- `misc.py`: Port block lengths, costs
- `exchange.py`: Port exchange bounds
- `energy.py`: Port energy bounds
- `storage.py`: Port storage computation
- `hydro.py`: Port hydro bounds (most complex)
- `thermal.py`: Port thermal bounds (includes resample/ffill)
- `policy.py`: Port policy coefficients

**Layer 3 - Synthesis Pipeline (Epic 3)**:

- `pipeline.py`: Remove fallback paths, accept polars throughout
- `bounds.py` (synthesis): Remove pd/pl conversion wrapper
- `export.py`: Remove pd/pl conversions, use polars natively
- `cache.py`: Store polars instead of pandas
- `spatial.py`: Update return types
- `resolution_*.py`: Update to accept/return polars
- `operations.py`: Accept polars directly in calc_statistics

**Layer 4 - Scenario & Cleanup (Epic 4)**:

- `scenario.py`: Port to polars
- `dataframe.py`: Remove pd_to_pl/pl_to_pd utilities
- All modules: Remove dead pandas imports
- Tests: Update mocks and assertions

### Data Flow (Target)

```
readers.py (pandas from inewave)
    |
    v
accessors.py: pl.from_pandas(pandas_result) --> cache[key] = pl.DataFrame
    |
    v
Deck facade --> pl.DataFrame to all consumers
    |
    +----> domain modules (entities, temporal, hydro, thermal, etc.)
    |          all internal operations in polars
    |          return pl.DataFrame
    |
    +----> DeckContext(block_lengths=pl.DataFrame, ...)
    |
    +----> pipeline.py
    |          resolve_temporal_resolution: pl.DataFrame in, pl.DataFrame out
    |          post_resolve: pl.concat() + sort, returns pl.DataFrame
    |
    +----> export.py
    |          export_scenario_synthesis: pl sort + synthetize_pl()
    |          calc_statistics: accepts pl.DataFrame directly
    |
    +----> scenario.py
               all operations in polars
```

### Testing Strategy

- Every ticket includes test updates to maintain the 349+ existing test pass rate
- Tests that mock DataFrames must be updated to use `pl.DataFrame` instead of `pd.DataFrame`
- Assertion patterns change from `pd.testing.assert_frame_equal` to polars equivalents or `.to_pandas()` for comparison
- Each ticket is independently verifiable by running the test suite

## Phases & Milestones

| Phase | Epic   | Description         | Milestone                                            |
| ----- | ------ | ------------------- | ---------------------------------------------------- |
| 1     | Epic 1 | Conversion Boundary | Deck returns polars, all consumers updated to accept |
| 2     | Epic 2 | Deck Domain Modules | All deck modules use polars internally               |
| 3     | Epic 3 | Synthesis Pipeline  | Pipeline, export, cache all native polars            |
| 4     | Epic 4 | Scenario & Cleanup  | scenario.py migrated, dead code removed              |

## Risk Analysis

| Risk                                                    | Impact | Likelihood | Mitigation                                                                   |
| ------------------------------------------------------- | ------ | ---------- | ---------------------------------------------------------------------------- |
| pandas index patterns hard to port                      | Medium | High       | Use polars join/filter patterns; document index-to-column mapping per module |
| `.apply(lambda)` patterns slow in polars `map_elements` | Medium | Medium     | Rewrite as vectorized `with_columns()` expressions where possible            |
| Test mock updates are extensive                         | Low    | High       | Systematic approach: one ticket per epic for test updates                    |
| `resample().ffill()` in thermal.py                      | Low    | Low        | Use `group_by_dynamic()` + `forward_fill()` or `upsample()`                  |
| DeckContext pickling changes                            | Medium | Low        | polars DataFrames are picklable; verify in ProcessPoolExecutor context       |

## Success Metrics

- Zero `pd_to_pl()` / `pl_to_pd()` calls remaining in codebase
- Zero `.copy()` calls on DataFrame returns in accessors.py
- Zero pandas fallback paths in pipeline.py
- All 349+ existing tests pass
- No pandas imports outside of: readers.py, accessors.py (pre-conversion), export adapter (read_df), test fixtures
