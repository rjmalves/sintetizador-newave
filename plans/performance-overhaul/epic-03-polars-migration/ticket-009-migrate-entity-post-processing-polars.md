# ticket-009 Migrate Entity Post-Processing Pipeline to Polars

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Convert the entity resolution methods (`_resolve_SBM_entity`, `_resolve_REE_entity`, `_resolve_UHE_entity`, `_resolve_SBP_entity`, `_resolve_UTE_entity`, `_resolve_PEE_entity`) and `_post_resolve_entity` to work with Polars DataFrames throughout. After ticket-008, `_resolve_temporal_resolution` returns Polars. This ticket extends that to the entity enrichment (adding entity code columns), starting stage resolution, and the internal stub dispatch (e.g., `_calc_block_0_weighted_mean`). The goal is that entity resolution returns `pl.DataFrame` to `_post_resolve`, eliminating per-entity pandas-Polars conversion.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation.py` (entity resolution methods, `_post_resolve_entity`, `_resolve_starting_stage`, internal stubs like `_calc_block_0_weighted_mean`), `app/services/synthesis/scenario.py` (equivalent entity methods)
- **Key decisions needed**: Whether inewave's `pd.DataFrame` output is converted to Polars at the file-reading boundary (in `get_nwlistop`) or in the entity resolution methods. Whether the internal stubs should accept Polars or pandas DataFrames.
- **Open questions**: How many distinct internal stub patterns exist in UHE resolution? Do the hooks in `_post_resolve` (early_hooks, late_hooks) need Polars interfaces? What is the performance impact of Polars-ifying the relatively small entity DataFrames vs. only the concatenated result?

## Dependencies

- **Blocked By**: ticket-008-migrate-temporal-resolution-polars.md
- **Blocks**: ticket-010-migrate-bounds-computation-polars.md

## Effort Estimate

**Points**: 3
**Confidence**: Low (will be re-estimated during refinement)
