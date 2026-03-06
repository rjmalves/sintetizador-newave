# ticket-011 Migrate Parquet Export to Polars Native Writer

## Context

### Background

The current Parquet export path in `ParquetExportRepository.synthetize_df` (line 41 of `app/adapters/repository/export.py`) converts a pandas DataFrame to a PyArrow table via `pa.Table.from_pandas(enforce_utc(df))`, then writes it with `pq.write_table()` using `flavor="spark"`, `write_statistics=False`, `coerce_timestamps="ms"`, and `allow_truncated_timestamps=True`. After tickets 008-010, the pipeline operates on Polars internally but converts to pandas before calling `synthetize_df`. Using Polars' native `write_parquet()` eliminates the pandas-to-PyArrow conversion step.

### Relation to Epic

This is the final ticket of Epic 03. It completes the Polars migration by replacing the export path, so the hot pipeline from nwlistop data through temporal resolution, entity processing, bounds, and export uses Polars with minimal pandas conversion.

### Current State

- `ParquetExportRepository` in `app/adapters/repository/export.py` has method `synthetize_df(self, df: pd.DataFrame, filename: str) -> bool`.
- `enforce_utc` in `app/utils/tz.py` localizes naive `datetime64[ns]` columns to UTC.
- `_export_scenario_synthesis` in `operation.py` (line ~2507) sorts the DataFrame with Polars, converts back to pandas, calls `calc_statistics`, stores in cache, then calls `uow.export.synthetize_df(scenarios_df[columns], str(s))`.
- `_export_stats` in `operation.py` (line ~2540) concatenates statistics DataFrames and calls `uow.export.synthetize_df`.
- The `AbstractExportRepository` interface defines `synthetize_df(self, df: pd.DataFrame, filename: str) -> bool`.
- `CSVExportRepository` and `TestExportRepository` also implement `synthetize_df` with pandas.
- `read_df` returns `pd.DataFrame | None` and is used for reading back existing metadata/stats files.

## Specification

### Requirements

1. Add a `synthetize_pl` method to `ParquetExportRepository` that accepts `pl.DataFrame` and writes Parquet using Polars' native `write_parquet()`. This method handles UTC enforcement for datetime columns in Polars.
2. Configure Polars' `write_parquet()` to produce compatible output: `use_pyarrow=True`, `pyarrow_options={"flavor": "spark", "write_statistics": False, "coerce_timestamps": "ms", "allow_truncated_timestamps": True}`. This delegates to PyArrow under the hood but avoids the pandas-to-PyArrow conversion.
3. Add `synthetize_pl` to `AbstractExportRepository` with a default implementation that falls back to converting to pandas and calling `synthetize_df`.
4. Modify `_export_scenario_synthesis` in `operation.py` to call `synthetize_pl` with a Polars DataFrame instead of converting to pandas for the export step. The stats computation and cache storage still use pandas.
5. Modify `_export_stats` in `operation.py` to call `synthetize_pl` with a Polars DataFrame for the final stats export.
6. Do NOT modify `CSVExportRepository`, `TestExportRepository`, or `read_df` methods.

### Inputs/Props

- `synthetize_pl(self, df: pl.DataFrame, filename: str) -> bool`
- The `pl.DataFrame` has datetime columns that may be timezone-naive.

### Outputs/Behavior

- Writes a Parquet file at `self.path / (filename + ".parquet")`.
- The Parquet file schema must be byte-compatible with the current PyArrow output (same column names, same types, same timestamp precision).
- Returns `True` on success.

### Error Handling

- If `write_parquet` fails, fall back to converting to pandas and calling the existing `synthetize_df`.
- Log a warning on fallback.

## Acceptance Criteria

- [ ] Given a `pl.DataFrame` with columns `[STAGE_COL, START_DATE_COL, END_DATE_COL, SCENARIO_COL, BLOCK_COL, BLOCK_DURATION_COL, VALUE_COL]` where `START_DATE_COL` and `END_DATE_COL` are `datetime[us]`, when `synthetize_pl` is called, then a Parquet file is written at the expected path.
- [ ] Given the Parquet file written by `synthetize_pl`, when read back with `pd.read_parquet`, then the DataFrame has identical column names, dtypes (datetime columns as `datetime64[ms, UTC]`), and values compared to a file written by the current `synthetize_df` from the same data.
- [ ] Given `_export_scenario_synthesis` processes a synthesis variable, when it calls the export method, then it calls `synthetize_pl` with a `pl.DataFrame` (verified by checking the method name in the call or by type assertion in a test mock).
- [ ] Given `AbstractExportRepository.synthetize_pl` is called on a `CSVExportRepository` instance, then the default implementation converts to pandas and calls `synthetize_df` without error.
- [ ] Given the full test suite, when `pytest tests/ -x` runs, then no regressions occur in previously passing tests.

## Implementation Guide

### Suggested Approach

1. **Add `synthetize_pl` to `AbstractExportRepository`**:

   ```python
   def synthetize_pl(self, df: pl.DataFrame, filename: str) -> bool:
       """Default: convert to pandas and use existing path."""
       return self.synthetize_df(df.to_pandas(), filename)
   ```

   Add `import polars as pl` to `export.py`.

2. **Implement `synthetize_pl` in `ParquetExportRepository`**:

   ```python
   def synthetize_pl(self, df: pl.DataFrame, filename: str) -> bool:
       # Enforce UTC on datetime columns
       for col_name in df.columns:
           if df[col_name].dtype in (pl.Datetime, pl.Date):
               dtype = df[col_name].dtype
               if isinstance(dtype, pl.Datetime) and dtype.time_zone is None:
                   df = df.with_columns(
                       pl.col(col_name).dt.replace_time_zone("UTC")
                   )
       df.write_parquet(
           self.path.joinpath(filename + ".parquet"),
           use_pyarrow=True,
           pyarrow_options={
               "flavor": "spark",
               "write_statistics": False,
               "coerce_timestamps": "ms",
               "allow_truncated_timestamps": True,
           },
       )
       return True
   ```

3. **Modify `_export_scenario_synthesis` in `operation.py`**:
   - After sorting with Polars, keep the result as `pl.DataFrame` for export.
   - Convert to pandas only for `calc_statistics` and cache storage.

   ```python
   scenarios_pl = pd_to_pl(df.astype({SCENARIO_COL: int})).sort(
       s.spatial_resolution.sorting_synthesis_df_columns,
       maintain_order=True,
   )
   scenarios_df = pl_to_pd(scenarios_pl).reset_index(drop=True)
   stats_df = calc_statistics(scenarios_df)
   cls._add_synthesis_stats(s, stats_df)
   cls.__store_in_cache_if_needed(s, scenarios_df)
   # Export using Polars native writer
   with uow:
       uow.export.synthetize_pl(
           scenarios_pl.select(s.spatial_resolution.all_synthesis_df_columns),
           str(s),
       )
   ```

4. **Modify `_export_stats` in `operation.py`**:
   - After concatenating stats DataFrames and deduplicating, convert to Polars for export:
   ```python
   df_pl = pd_to_pl(df)
   uow.export.synthetize_pl(df_pl, stats_filename)
   ```

### Key Files to Modify

- `app/adapters/repository/export.py` — Add `synthetize_pl` to `AbstractExportRepository` and `ParquetExportRepository`.
- `app/services/synthesis/operation.py` — `_export_scenario_synthesis` and `_export_stats` methods.

### Patterns to Follow

- `use_pyarrow=True` with `pyarrow_options` dict in Polars `write_parquet` for feature parity with the current PyArrow writer.
- `pl.col(...).dt.replace_time_zone("UTC")` for timezone enforcement in Polars.
- Try/except with pandas fallback.

### Pitfalls to Avoid

- Do NOT remove the existing `synthetize_df` method; it is used by `CSVExportRepository`, `TestExportRepository`, and potentially by other callers.
- Do NOT modify `read_df`; it returns `pd.DataFrame` and is used for reading metadata/stats.
- Do NOT modify `enforce_utc` in `app/utils/tz.py`; it remains for the pandas path.
- The `flavor="spark"` option produces Spark-compatible Parquet with specific timestamp handling; omitting it could break downstream consumers.
- `pl.Datetime` dtype check: use `isinstance(dtype, pl.Datetime)` not `dtype == pl.Datetime` because Polars datetime types carry time_unit and time_zone parameters.

## Testing Requirements

### Unit Tests

- Add a test in `tests/app/adapters/repository/test_export_polars.py` that:
  1. Creates a `ParquetExportRepository` with a temporary directory.
  2. Constructs a `pl.DataFrame` with one datetime column (naive) and one float column.
  3. Calls `synthetize_pl` and verifies the file exists.
  4. Reads the file back with `pd.read_parquet` and verifies the datetime column is UTC and values match.
  5. Compares the Parquet file bytes/schema with one produced by the current `synthetize_df` from the same data (converted to pandas).

### Integration Tests

- Run `pytest tests/ -x` and verify no regressions.

### E2E Tests (if applicable)

- Not applicable for this ticket.

## Dependencies

- **Blocked By**: ticket-010-migrate-bounds-computation-polars.md
- **Blocks**: None

## Effort Estimate

**Points**: 2
**Confidence**: High

## Out of Scope

- Modifying `CSVExportRepository` or `TestExportRepository` to accept Polars.
- Modifying `read_df` to return Polars DataFrames.
- Removing `pyarrow` dependency (still used by Polars' `use_pyarrow=True` path).
- Modifying `scenario.py` export path.
- Removing the `enforce_utc` utility function.
