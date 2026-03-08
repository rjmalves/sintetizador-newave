# ticket-006 Optimize DataFrame Concatenation with Polars

## Context

### Background

The `_post_resolve()` method in `OperationSynthetizer` (line 261-306 of `app/services/synthesis/operation.py`) concatenates entity DataFrames using `pd.concat(valid_dfs, ignore_index=True)`. After Epic 01 ticket-003, these DataFrames contain only scenario data (no statistics rows), but for UHE variables with 200 plants, concatenating 200 DataFrames of ~720,000 rows each produces a DataFrame of ~144 million rows. The `pd.concat` call copies all data into a new contiguous block.

Additionally, `_export_scenario_synthesis()` performs sorting (`df.sort_values(...)`) on the full DataFrame before export. Both concatenation and sorting are significantly faster in Polars due to its columnar memory layout and multi-threaded operations.

### Relation to Epic

This is the third ticket in Epic 02 and depends on ticket-004 (Polars dependency). It can be implemented in parallel with ticket-005.

### Current State

- `_post_resolve()` at line 280: `df = pd.concat(valid_dfs, ignore_index=True)` -- concatenates all entity DataFrames
- `_post_resolve()` at lines 288-290: `df = df.sort_values(...).reset_index(drop=True)` -- sorts the full DataFrame
- `_export_scenario_synthesis()` at lines 2379-2381: `scenarios_df = scenarios_df.sort_values(...).reset_index(drop=True)` -- sorts again before export
- Each entity DataFrame from `_post_resolve_entity()` is a standard pandas DataFrame

## Specification

### Requirements

1. In `_post_resolve()`, convert each entity DataFrame to Polars before concatenation, use `pl.concat()` for concatenation, and perform sorting using Polars' multi-threaded sort
2. Return the result as a pandas DataFrame from `_post_resolve()` to maintain the existing interface with callers
3. In `_export_scenario_synthesis()`, convert to Polars for the final sort before export, then convert back to pandas for the Parquet write
4. The output data (column names, values, ordering) must be identical to the current implementation
5. All existing tests must pass
6. Apply the same optimization to `ScenarioSynthetizer._post_resolve()` equivalent if it exists

### Inputs/Props

- `_post_resolve()` receives `resolve_responses: Dict[str, Optional[pd.DataFrame]]` -- a dict of entity name to DataFrame
- Each DataFrame has columns matching the spatial resolution's `all_synthesis_df_columns`

### Outputs/Behavior

- `_post_resolve()` returns a sorted `pd.DataFrame` with all entities concatenated, identical to the current output
- Sorting is by `spatial_resolution.sorting_synthesis_df_columns`
- The entity column ordering extraction (`_get_unique_column_values_in_order`) must still work correctly

### Error Handling

- If all entity DataFrames are None, return None (current behavior preserved)
- If Polars concatenation fails, let the exception propagate (no silent fallback)

## Acceptance Criteria

- [ ] Given `_post_resolve()` is called with 200 entity DataFrames of 720,000 rows each, when it returns, then the result has 144,000,000 rows sorted by the spatial resolution columns
- [ ] Given the output of `_post_resolve()` is compared to the previous pandas-only implementation on the same input, when row values are compared, then all values match exactly
- [ ] Given `_post_resolve()` is timed on 200 entity DataFrames, when compared to the pandas-only implementation, then it completes in less than 50% of the original time
- [ ] Given `pytest tests/` is run, when all tests execute, then all tests pass with zero failures

## Implementation Guide

### Suggested Approach

1. In `_post_resolve()` (line 261-306 of operation.py), modify the concatenation and sorting:

   ```python
   from app.utils.dataframe import pd_to_pl, pl_to_pd

   # Inside _post_resolve:
   valid_dfs = [df for df in resolve_responses.values() if df is not None]
   if len(valid_dfs) > 0:
       pl_dfs = [pd_to_pl(df) for df in valid_dfs]
       pl_combined = pl.concat(pl_dfs)
       sorting_cols = spatial_resolution.sorting_synthesis_df_columns
       pl_combined = pl_combined.sort(sorting_cols)
       df = pl_to_pd(pl_combined)
       df = df.reset_index(drop=True)
   else:
       return None
   ```

2. The `_get_unique_column_values_in_order` call (line 292-301) extracts ordered unique values for entity and non-entity columns. This must still operate on the sorted result. Since the conversion back to pandas happens before this call, no change is needed to this extraction logic.

3. In `_export_scenario_synthesis()`, apply the same pattern for the final sort:

   ```python
   pl_scenarios = pd_to_pl(scenarios_df)
   pl_scenarios = pl_scenarios.sort(s.spatial_resolution.sorting_synthesis_df_columns)
   scenarios_df = pl_to_pd(pl_scenarios).reset_index(drop=True)
   ```

4. The early hooks and late hooks in `_post_resolve()` operate on pandas DataFrames. Keep the hooks operating on the pandas result (after `pl_to_pd` conversion).

### Key Files to Modify

- `app/services/synthesis/operation.py` (`_post_resolve`, `_export_scenario_synthesis`)
- `app/services/synthesis/scenario.py` (equivalent methods if they exist)

### Patterns to Follow

- Convert to Polars at the start of the heavy operation, compute, convert back to pandas
- Use `pl.concat()` without `rechunk=True` for performance (Polars handles non-contiguous memory)
- Use `.sort()` with column names, which triggers multi-threaded sorting

### Pitfalls to Avoid

- Do NOT convert to Polars inside the entity resolution methods -- they are called in subprocesses and Polars import overhead should be minimized there
- Do NOT change the return type of `_post_resolve()` -- it must still return `pd.DataFrame` for compatibility with hooks, bounds resolution, and caching
- The `_get_unique_column_values_in_order` call uses `df[col].unique().tolist()` on a pandas DataFrame -- this must remain pandas-based since it feeds into `ORDERED_SYNTHESIS_ENTITIES` which is accessed elsewhere
- Verify that `pl.sort()` maintains stable sort order for equal values -- if not, this could cause test failures. Polars sort is NOT stable by default; use `.sort(..., maintain_order=True)` if stable ordering is required

## Testing Requirements

### Unit Tests

- Test that `pl.concat` followed by `pl.sort` produces the same ordering as `pd.concat` followed by `pd.sort_values`
- Test with empty input (all None responses)

### Integration Tests

- Run `pytest tests/` -- all must pass

### E2E Tests (if applicable)

- Not required

## Dependencies

- **Blocked By**: ticket-004-add-polars-dependency.md
- **Blocks**: ticket-007-eliminate-unnecessary-copies.md

## Effort Estimate

**Points**: 3
**Confidence**: High

## Out of Scope

- Migrating the entity resolution methods to use Polars internally
- Changing the hooks interface to use Polars DataFrames
- Migrating the Parquet export to use Polars native writer
