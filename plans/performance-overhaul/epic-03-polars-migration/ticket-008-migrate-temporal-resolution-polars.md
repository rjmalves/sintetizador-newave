# ticket-008 Migrate Temporal Resolution to Polars

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Convert the `_resolve_temporal_resolution` static method in `OperationSynthetizer` (lines 308-426 of `app/services/synthesis/operation.py`) from pandas/numpy operations to Polars columnar operations. This method is called once per entity in the inner loop and performs column renaming, sorting, scenario index replacement, stage/date assignment, and block duration computation using `np.tile`, `np.repeat`, and `np.arange` patterns. These can be expressed as Polars expressions for better performance and to eliminate the pandas-Polars conversion boundary inside the entity loop.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation.py` (the `_resolve_temporal_resolution` method and its nested functions), possibly `app/services/deck/context.py` (DeckContext may need Polars-typed fields)
- **Key decisions needed**: Whether `DeckContext` fields should store Polars DataFrames/Series instead of pandas (depends on learnings from ticket-002 about context design). Whether the method signature changes to accept/return `pl.DataFrame` or stays as `pd.DataFrame` with internal conversion.
- **Open questions**: How does the `block_lengths` DataFrame interact with Polars join semantics? Does the `np.tile`/`np.repeat` pattern for scenario replacement have a clean Polars equivalent?

## Dependencies

- **Blocked By**: ticket-007-eliminate-unnecessary-copies.md
- **Blocks**: ticket-009-migrate-entity-post-processing-polars.md

## Effort Estimate

**Points**: 3
**Confidence**: Low (will be re-estimated during refinement)
