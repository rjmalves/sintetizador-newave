# Epic 03: Full Hot-Path Polars Migration

## Goal

Migrate the entire data processing pipeline -- from inewave DataFrame reception through post-processing and export -- to use Polars internally. Epics 01 and 02 introduced Polars at the statistics and concatenation boundaries. This epic pushes Polars deeper into the entity resolution methods, temporal resolution, bounds computation, and export, eliminating most pandas-to-Polars conversions in the hot path.

## Scope

1. **Migrate `_resolve_temporal_resolution` to Polars**: The temporal resolution method adds stage, date, scenario, and block duration columns using numpy array manipulation and pandas assignment. Convert to Polars columnar operations.

2. **Migrate `_post_resolve_entity` pipeline to Polars**: Entity enrichment (adding entity code columns, starting stage adjustment) and the full post-processing pipeline per entity.

3. **Migrate bounds computation to Polars**: The `OperationVariableBounds.resolve_bounds()` method adds upper and lower bounds columns based on deck data. Convert to Polars join/map operations.

4. **Migrate Parquet export to use Polars native writer**: Replace `pyarrow.Table.from_pandas(df)` with Polars' native `write_parquet()`, which avoids the pandas-to-pyarrow conversion overhead.

## Dependencies

- Epic 02 must be complete (Polars dependency exists, statistics use Polars, concatenation uses Polars)

## Success Criteria

- The hot path (from nwlistop DataFrame to Parquet export) performs zero pandas-to-Polars or Polars-to-pandas conversions in the inner loop
- pandas is used ONLY at the inewave boundary (receiving DataFrames from `BlockFile.read().valores`)
- All existing tests pass
- Runtime improvement of 2-4x over the Epic 02 baseline

## Tickets

| Ticket     | Title                                             | Points | Depends On |
| ---------- | ------------------------------------------------- | ------ | ---------- |
| ticket-008 | Migrate temporal resolution to Polars             | 3      | ticket-007 |
| ticket-009 | Migrate entity post-processing pipeline to Polars | 3      | ticket-008 |
| ticket-010 | Migrate bounds computation to Polars              | 3      | ticket-009 |
| ticket-011 | Migrate Parquet export to Polars native writer    | 2      | ticket-010 |
