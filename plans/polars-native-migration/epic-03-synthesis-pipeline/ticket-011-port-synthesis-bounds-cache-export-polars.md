# ticket-011 Port synthesis bounds, cache, and export to polars

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Port `app/services/synthesis/operation/bounds.py` (synthesis wrapper), `app/services/synthesis/operation/cache.py`, and `app/services/synthesis/operation/export.py` to accept and store `pl.DataFrame` natively. Remove the `pd_to_pl()`/`pl_to_pd()` conversion pairs in bounds.py and export.py. Change the cache to store `pl.DataFrame` instead of `pd.DataFrame`. Update `orchestrator.py` type annotations.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation/bounds.py`, `app/services/synthesis/operation/cache.py`, `app/services/synthesis/operation/export.py`, `app/services/synthesis/operation/orchestrator.py`
- **Key decisions needed**: Whether `export_metadata()` should stay pandas (it builds a small metadata DataFrame) or be ported to polars. Whether `export_stats()` which uses `pd.concat` and pandas sort should be ported or kept.
- **Open questions**:
  - Does `calc_statistics()` now return `pl.DataFrame` after ticket-010, or does it still return `pd.DataFrame`?
  - What is the contract for `store_in_cache_if_needed()` -- does it receive `pl.DataFrame` from `post_resolve()` after ticket-010?
  - Should `export_scenario_synthesis()` eliminate the intermediate pandas round-trip entirely, or is it needed for `calc_statistics`?

## Dependencies

- **Blocked By**: ticket-010-port-pipeline-native-polars.md
- **Blocks**: ticket-012-port-resolution-modules-polars.md

## Effort Estimate

**Points**: 3
**Confidence**: Low (will be re-estimated during refinement)
