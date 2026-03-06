# ticket-012 Port resolution modules and spatial dispatch to polars

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Port `app/services/synthesis/operation/spatial.py`, `resolution_sbm.py`, `resolution_ree.py`, `resolution_uhe.py`, `resolution_ute.py`, `resolution_sin.py`, `resolution_sbp.py`, `resolution_pee.py`, `stubs.py`, and `_stubs_market.py` to work with polars DataFrames from Deck and the pipeline. Remove `.to_pandas()` shims for Deck entity lookups in resolution modules. Update return types from `pd.DataFrame` to `pl.DataFrame` where applicable.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation/spatial.py`, `resolution_sbm.py`, `resolution_ree.py`, `resolution_uhe.py`, `resolution_ute.py`, `resolution_sin.py`, `resolution_sbp.py`, `resolution_pee.py`, `stubs.py`, `_stubs_market.py`
- **Key decisions needed**: Whether `resolve_spatial_resolution()` in spatial.py should return `pl.DataFrame` or `pd.DataFrame`. This affects the bounds.py wrapper in ticket-011.
- **Open questions**:
  - After ticket-010, what does `post_resolve()` return? The resolution modules call it indirectly.
  - The resolution modules use `Deck.submarkets(uow).reset_index()` and `.loc[]` -- after entities returns polars (no index), what is the pattern for filtering real submarkets?
  - Do stubs modules need full migration or just shim removal?

## Dependencies

- **Blocked By**: ticket-011-port-synthesis-bounds-cache-export-polars.md
- **Blocks**: ticket-013-port-scenario-polars.md

## Effort Estimate

**Points**: 3
**Confidence**: Low (will be re-estimated during refinement)
