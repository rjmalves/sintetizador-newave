# ticket-013 Port scenario.py to polars

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Port `app/services/synthesis/scenario.py` (~1600 lines) to use polars DataFrames throughout. This module implements scenario synthesis (energy scenarios, flow scenarios, LTA computations) and currently uses pandas extensively for DataFrame construction, groupby, concat, apply, and join operations. It also uses `pd_to_pl()`/`pl_to_pd()` for polars integration.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/scenario.py`
- **Key decisions needed**: Whether the export path in scenario.py should mirror the pattern established in operation export (ticket-011) or have its own approach. Whether the cached synthesis and cached MLT values should store `pl.DataFrame`.
- **Open questions**:
  - What pattern was established in earlier epics for `pd.date_range()` equivalent? scenario.py uses it for generating date sequences.
  - How should `_eval_monthly_lta()` with `df.apply(lambda: date.month)` and `groupby([month]).mean()` be written in polars?
  - scenario.py uses `pd_to_pl` and `pl_to_pd` for concat/sort -- after Epic 3, are these still used anywhere else?
  - What is the polars pattern for the `resolve_starting_stage` equivalent that scenario.py calls?

## Dependencies

- **Blocked By**: ticket-012-port-resolution-modules-polars.md
- **Blocks**: ticket-014-remove-conversion-utilities-dead-imports.md

## Effort Estimate

**Points**: 5
**Confidence**: Low (will be re-estimated during refinement)
