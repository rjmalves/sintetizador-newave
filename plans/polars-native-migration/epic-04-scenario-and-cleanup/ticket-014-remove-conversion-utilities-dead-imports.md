# ticket-014 Remove conversion utilities and dead pandas imports

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Remove the `pd_to_pl()` and `pl_to_pd()` utility functions from `app/utils/dataframe.py`, remove all dead `import pandas as pd` statements from migrated modules, remove any remaining `# SHIM:` comments, and verify that pandas is only imported in boundary code (readers.py, accessors.py pre-conversion section, export adapter `read_df`). Update or remove test files for the deleted utilities.

## Anticipated Scope

- **Files likely to be modified**: `app/utils/dataframe.py`, all previously migrated modules (to remove dead imports), `tests/app/utils/test_dataframe.py`
- **Key decisions needed**: Whether `dataframe.py` should be deleted entirely or kept with other utility functions. Whether `fast_group_df()` in `operations.py` (which uses pandas groupby) should also be removed or was already ported.
- **Open questions**:
  - Are there any remaining callers of `pd_to_pl`/`pl_to_pd` that were missed in earlier epics?
  - Should the export adapter's `read_df()` (which returns `pd.DataFrame`) be changed to return `pl.DataFrame`?
  - Are there any `# SHIM:` comments remaining from ticket-003 that were not cleaned up during their module's migration?

## Dependencies

- **Blocked By**: ticket-013-port-scenario-polars.md
- **Blocks**: None

## Effort Estimate

**Points**: 2
**Confidence**: Low (will be re-estimated during refinement)
