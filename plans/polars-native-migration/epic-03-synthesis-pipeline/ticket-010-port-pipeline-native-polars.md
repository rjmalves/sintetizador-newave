# ticket-010 Port pipeline.py to native polars

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Port `app/services/synthesis/operation/pipeline.py` to native polars by removing all pandas fallback paths, the dual `resolve_starting_stage`/`resolve_starting_stage_polars` implementations, and converting `post_resolve()`, `initial_stored_energy_df()`, `generate_scenarios()`, and `resolve_temporal_resolution_GTER_UTE()` to accept and return polars DataFrames. Also port `app/utils/operations.py` (`calc_statistics`) to accept `pl.DataFrame` directly instead of going through `pd_to_pl()`.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation/pipeline.py`, `app/utils/operations.py`
- **Key decisions needed**: Whether `post_resolve()` should return `pl.DataFrame` (changing the cache and export contract) or stay as `pd.DataFrame` (deferring to ticket-011). Depends on what pattern was established in Epic 2 for the Deck-to-pipeline boundary.
- **Open questions**:
  - After Epic 2, does DeckContext.block_lengths flow through as polars end-to-end or does `_fetch_temporal_deck_data` need updates?
  - Should the `_replace_scenario_info`, `_add_stage_info`, `_add_block_duration_info` pandas helper functions be removed (they are only used by fallback paths) or kept for any remaining callers?
  - How should `get_unique_column_values_in_order()` work -- it currently expects `pd.DataFrame` with `.unique().tolist()`. What is the polars equivalent established in earlier tickets?

## Dependencies

- **Blocked By**: ticket-009-port-policy-polars.md
- **Blocks**: ticket-011-port-synthesis-bounds-cache-export-polars.md

## Effort Estimate

**Points**: 5
**Confidence**: Low (will be re-estimated during refinement)
