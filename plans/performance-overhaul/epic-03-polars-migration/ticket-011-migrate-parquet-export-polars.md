# ticket-011 Migrate Parquet Export to Polars Native Writer

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Replace the current Parquet export path (`pyarrow.Table.from_pandas(df)` followed by `pq.write_table()` in `app/adapters/repository/export.py`) with Polars' native `write_parquet()`. After tickets 008-010, the pipeline operates on Polars DataFrames until the export boundary. Using Polars' native Parquet writer avoids the Polars-to-pandas-to-pyarrow conversion chain and leverages Polars' optimized writer. The output Parquet schema must remain backward-compatible.

## Anticipated Scope

- **Files likely to be modified**: `app/adapters/repository/export.py` (new `PolarsParquetExportRepository` or modified `ParquetExportRepository`), `app/services/synthesis/operation.py` (`_export_scenario_synthesis` and `_export_stats`)
- **Key decisions needed**: Whether to create a new repository class or modify the existing one. Whether the export interface changes to accept `pl.DataFrame` instead of `pd.DataFrame`. How to handle the `enforce_utc()` timezone conversion currently applied before pandas export. Whether the `write_statistics=False` and `flavor="spark"` options in the current pyarrow writer have Polars equivalents.
- **Open questions**: Does Polars `write_parquet()` produce byte-compatible output with the current pyarrow writer using `flavor="spark"`? Can coerce_timestamps and allow_truncated_timestamps be replicated? How do downstream consumers (analytics tools) handle any schema differences?

## Dependencies

- **Blocked By**: ticket-010-migrate-bounds-computation-polars.md
- **Blocks**: None

## Effort Estimate

**Points**: 2
**Confidence**: Low (will be re-estimated during refinement)
