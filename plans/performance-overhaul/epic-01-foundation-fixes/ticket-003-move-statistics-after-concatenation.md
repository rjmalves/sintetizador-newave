# ticket-003 Move Statistics Computation After Entity Concatenation

## Context

### Background

The `calc_statistics()` function (`app/utils/operations.py`) computes 21 quantiles (0%, 5%, 10%, ..., 100%) plus mean and standard deviation for every entity's DataFrame individually. This happens inside `_post_resolve_entity()` (line 233-258 of `app/services/synthesis/operation.py`), which is called once per entity (per UHE, per REE, per SBM, etc.). For a UHE synthesis with 200 plants, `calc_statistics()` is called 200 times -- each computing quantiles over a relatively small DataFrame (120 stages x 2000 scenarios x 3 blocks = 720,000 rows). Then all entity DataFrames (including their statistics rows) are concatenated.

The statistics rows are later separated from scenario rows in `_export_scenario_synthesis()` (line 2375-2376) using the `STATS_OR_SCENARIO_COL` boolean flag. This means the per-entity statistics are carried through concatenation and sorting only to be split out again.

Moving statistics computation to after concatenation means: (a) a single `calc_statistics()` call over the full DataFrame, which is more efficient for groupby operations, and (b) eliminating the `STATS_OR_SCENARIO_COL` flag and the DataFrame bloat from carrying statistics rows through the pipeline.

### Relation to Epic

This is the third ticket in Epic 01. It is independent of tickets 001 and 002 and can be implemented in parallel.

### Current State

- `_post_resolve_entity()` (line 233-258 of operation.py): calls `calc_statistics(df)` at line 253, appends a `STATS_OR_SCENARIO_COL` boolean flag, concatenates scenario + stats rows into a single DataFrame
- `_post_resolve()` (line 261-306 of operation.py): concatenates all entity DataFrames (each containing both scenario and stats rows)
- `_export_scenario_synthesis()` (line 2361-2397 of operation.py): splits the concatenated DataFrame into `scenarios_df` and `stats_df` using the boolean flag, then calls `calc_statistics()` again if `stats_df` is empty (line 2383)
- `calc_statistics()` (line 110-120 of operations.py): calls `_calc_quantiles()` and `_calc_mean_std()`, both using pandas groupby
- `_calc_quantiles()` (line 58-85 of operations.py): uses `.groupby(...).quantile(quantiles)` which is a slow pandas operation
- `ScenarioSynthetizer` (`app/services/synthesis/scenario.py`) likely has the same pattern -- needs verification

## Specification

### Requirements

1. Remove the `calc_statistics(df)` call from `_post_resolve_entity()` in `OperationSynthetizer`
2. Remove the `STATS_OR_SCENARIO_COL` column assignment and the `pd.concat([df, df_stats])` from `_post_resolve_entity()`
3. In `_export_scenario_synthesis()`, compute statistics after the full DataFrame is assembled -- call `calc_statistics(scenarios_df)` on the complete scenarios DataFrame instead of relying on per-entity pre-computed stats
4. Remove the `STATS_OR_SCENARIO_COL` splitting logic from `_export_scenario_synthesis()`
5. Apply the same pattern to `ScenarioSynthetizer` if it follows the same per-entity statistics pattern
6. All existing tests must pass
7. Output Parquet files must contain the same data (statistics and scenarios) as before

### Inputs/Props

- `_post_resolve_entity()` receives a DataFrame with scenario data for a single entity
- `_export_scenario_synthesis()` receives the concatenated DataFrame for all entities of a variable

### Outputs/Behavior

- `_post_resolve_entity()` returns ONLY the scenario DataFrame (no statistics rows, no `STATS_OR_SCENARIO_COL` column)
- `_export_scenario_synthesis()` computes statistics from the full scenarios DataFrame and exports both
- The exported Parquet files for scenarios and statistics have identical content to before
- The `STATS_OR_SCENARIO_COL` constant can be removed from `constants.py` if no other code references it

### Error Handling

- If `calc_statistics()` fails on the full DataFrame, the existing exception handling in `_synthetize_single_variable()` (line 2496-2503) catches and logs the error

## Acceptance Criteria

- [ ] Given `_post_resolve_entity()` is called for one entity, when it returns, then the returned DataFrame does NOT contain a column named `estatistica_ou_cenario`
- [ ] Given `_post_resolve_entity()` is called for one entity with 720,000 scenario rows, when it returns, then the returned DataFrame has exactly 720,000 rows (no statistics rows appended)
- [ ] Given `_export_scenario_synthesis()` is called with the full concatenated DataFrame, when statistics are computed, then `calc_statistics()` is called exactly once for the complete DataFrame
- [ ] Given `pytest tests/` is run, when all tests execute, then all tests pass with zero failures
- [ ] Given `grep -rn "STATS_OR_SCENARIO_COL" app/` is run after the change, when examining results, then the constant is referenced only in its definition in `constants.py` (or is fully removed)

## Implementation Guide

### Suggested Approach

1. In `_post_resolve_entity()` (line 233-258 of operation.py):
   - Remove line 253: `df_stats = calc_statistics(df)`
   - Remove line 254: `df[STATS_OR_SCENARIO_COL] = False`
   - Remove line 255: `df_stats[STATS_OR_SCENARIO_COL] = True`
   - Remove line 256: `df = pd.concat([df, df_stats], ignore_index=True)`
   - Remove line 257: `df = df.astype({SCENARIO_COL: STRING_DF_TYPE})`
   - The method now returns just the scenario DataFrame with entity columns added and temporal resolution applied

2. In `_export_scenario_synthesis()` (line 2361-2397 of operation.py):
   - Remove the `STATS_OR_SCENARIO_COL` splitting logic (lines 2375-2376)
   - The input `df` is now purely scenario data (no stats rows mixed in)
   - Compute statistics: `stats_df = calc_statistics(df)`
   - Continue with the existing export logic for both `df` (scenarios) and `stats_df`
   - Remove the `if stats_df.empty:` fallback (line 2382-2383) since stats are always computed here now

3. In `_post_resolve()` (line 261-306 of operation.py):
   - The DataFrame being concatenated is now smaller (no stats rows per entity)
   - No logic changes needed -- it concatenates entity DataFrames as before

4. Update the `_add_synthesis_stats()` method (line 2348-2358) -- it receives `stats_df` from `_export_scenario_synthesis()`, which is now computed differently. Verify the stats DataFrame format is compatible.

5. In `_export_scenario_synthesis()`, after computing stats, cast `SCENARIO_COL` to `STRING_DF_TYPE` in the stats DataFrame (since it now contains string labels like "mean", "std", "p5", etc.).

6. Search for `STATS_OR_SCENARIO_COL` usage across the codebase. Remove the import from operation.py. If no other file uses it, consider removing it from `constants.py`.

7. Check `ScenarioSynthetizer` in `app/services/synthesis/scenario.py` for the same pattern and apply equivalent changes if found.

### Key Files to Modify

- `app/services/synthesis/operation.py` (remove per-entity stats, add post-concat stats in export)
- `app/services/synthesis/scenario.py` (apply same pattern if applicable)
- `app/internal/constants.py` (potentially remove `STATS_OR_SCENARIO_COL` if unused)

### Patterns to Follow

- The `calc_statistics()` function itself is unchanged -- it is called on a larger DataFrame but with the same interface
- The export pattern of separating scenarios and statistics into different outputs is preserved

### Pitfalls to Avoid

- Do NOT change the `calc_statistics()` function itself -- it works correctly, it is just called in the wrong place
- Do NOT remove the `__store_in_cache_if_needed()` call in `_export_scenario_synthesis()` -- the cache stores scenario data (without stats), and it is used by stub synthesis methods
- Watch for the `SCENARIO_COL` dtype -- in the current code, `df.astype({SCENARIO_COL: STRING_DF_TYPE})` is called in `_post_resolve_entity()` because stats rows have string labels in SCENARIO_COL. After the change, this cast must happen in the stats DataFrame only, while the scenarios DataFrame keeps integer scenario IDs
- The `_resolve_bounds()` method (line 2268-2286) operates on the full DataFrame. After this change, bounds are applied to scenario-only data, which is correct since bounds are per-scenario values

## Testing Requirements

### Unit Tests

- Test that `_post_resolve_entity()` returns a DataFrame without the `STATS_OR_SCENARIO_COL` column
- Test that `_export_scenario_synthesis()` produces identical statistics output as before (compare quantile values)

### Integration Tests

- Run the existing test suite (`pytest tests/`) -- all must pass
- Specifically verify that `test_operation.py` tests pass, as they validate the full synthesis pipeline including statistics

### E2E Tests (if applicable)

- Not required for this ticket

## Dependencies

- **Blocked By**: None
- **Blocks**: None

## Effort Estimate

**Points**: 3
**Confidence**: High

## Out of Scope

- Changing the statistics computation algorithm (that is Epic 02)
- Migrating from pandas groupby to Polars (that is Epic 02/03)
- Changing the Parquet export format
- Optimizing `calc_statistics()` itself
