# Epic 02: Statistics & Data Pipeline Optimization

## Goal

Introduce Polars as a dependency and migrate the statistics computation and DataFrame concatenation hot paths from pandas to Polars. This epic targets the two remaining highest-impact bottlenecks after Epic 01: the slow pandas groupby/quantile operations and the inefficient pd.concat patterns.

## Scope

1. **Add Polars dependency and create a conversion utility**: Add `polars` to `pyproject.toml`, create a thin utility module for pandas-to-Polars-to-pandas conversion at the pipeline boundaries.

2. **Rewrite `calc_statistics()` using Polars**: Replace the pandas `groupby().quantile()` and `groupby().mean()/std()` with Polars equivalents, which are natively multi-threaded and significantly faster.

3. **Optimize DataFrame concatenation patterns**: Replace the accumulate-and-concat-in-loop pattern with Polars' native `pl.concat()` and lazy evaluation.

4. **Eliminate unnecessary `df.copy()` calls in cache**: The cache currently stores and retrieves `df.copy()`, doubling memory. Use copy-on-write semantics or Polars immutable DataFrames.

## Dependencies

- Epic 01 must be complete (statistics are computed post-concatenation, chdir is removed, deck context exists)

## Success Criteria

- All existing tests pass
- `calc_statistics()` runs at least 5x faster on a representative DataFrame (120 stages x 200 UHEs x 2000 scenarios x 3 blocks)
- Peak memory usage during statistics computation is reduced by at least 30%
- Polars appears in `pyproject.toml` dependencies
- pandas-to-Polars conversion happens exactly at pipeline entry; Polars-to-pandas conversion happens exactly at export boundary

## Tickets

| Ticket     | Title                                           | Points | Depends On             |
| ---------- | ----------------------------------------------- | ------ | ---------------------- |
| ticket-004 | Add Polars dependency and conversion utilities  | 2      | ticket-003             |
| ticket-005 | Rewrite calc_statistics using Polars            | 3      | ticket-004             |
| ticket-006 | Optimize DataFrame concatenation with Polars    | 3      | ticket-004             |
| ticket-007 | Eliminate unnecessary DataFrame copies in cache | 2      | ticket-005, ticket-006 |
