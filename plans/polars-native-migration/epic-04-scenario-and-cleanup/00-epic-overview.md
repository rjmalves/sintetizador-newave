# Epic 04: Scenario Synthesis & Final Cleanup

## Goal

Complete the migration by porting `scenario.py` to polars and performing final cleanup: removing the `pd_to_pl`/`pl_to_pd` utility functions, removing all dead pandas imports, and verifying no pandas usage remains outside the inewave boundary.

## Scope

- Port `scenario.py` (~1600 lines) to use polars DataFrames throughout
- Remove `pd_to_pl()` and `pl_to_pd()` from `app/utils/dataframe.py`
- Remove dead `import pandas as pd` statements from all migrated modules
- Update any remaining test files that use pandas mocks
- Final verification: grep for remaining pandas usage and ensure it's only in boundary code

## Tickets

| ID         | Title                                               | Effort |
| ---------- | --------------------------------------------------- | ------ |
| ticket-013 | Port scenario.py to polars                          | 5      |
| ticket-014 | Remove conversion utilities and dead pandas imports | 2      |

## Dependencies

- Depends on Epic 3 (synthesis pipeline uses polars)

## Success Criteria

- `scenario.py` uses polars throughout
- `pd_to_pl()` and `pl_to_pd()` functions removed
- No pandas imports outside boundary code (readers.py, accessors.py pre-conversion, export adapter read_df)
- All 349+ existing tests pass
