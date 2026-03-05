# ticket-010 Migrate Bounds Computation to Polars

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Convert the `OperationVariableBounds.resolve_bounds()` method in `app/services/deck/bounds.py` to use Polars for all DataFrame operations. The bounds computation adds `limite_inferior` and `limite_superior` columns to the synthesis DataFrame based on deck configuration data (min/max volumes, flows, capacities). This involves joins, conditional column assignment, and aggregations that are natural Polars operations.

## Anticipated Scope

- **Files likely to be modified**: `app/services/deck/bounds.py` (the `resolve_bounds` method and its helper methods), `app/services/synthesis/operation.py` (`_resolve_bounds` caller)
- **Key decisions needed**: Whether bounds data from Deck should be pre-converted to Polars as part of DeckContext (depends on learnings from ticket-002 context design). Whether the bounds method receives `pl.DataFrame` or converts internally.
- **Open questions**: How complex is the bounds logic? How many conditional paths exist? Does the bounds computation use pandas merge/join operations that would benefit from Polars? What is the performance profile -- is bounds computation a significant fraction of total runtime, or is this mainly a code consistency improvement?

## Dependencies

- **Blocked By**: ticket-009-migrate-entity-post-processing-polars.md
- **Blocks**: ticket-011-migrate-parquet-export-polars.md

## Effort Estimate

**Points**: 3
**Confidence**: Low (will be re-estimated during refinement)
