# ticket-007 Eliminate Unnecessary DataFrame Copies in Cache

## Context

### Background

The `OperationSynthetizer` cache mechanism stores and retrieves DataFrames with explicit `.copy()` calls. In `__store_in_cache_if_needed()` (line 2253-2265 of operation.py), the DataFrame is stored as `cls.CACHED_SYNTHESIS[s] = df.copy()`. In `_get_from_cache()` (line 2070-2084), it is retrieved as `return res.copy()`. This means every cached DataFrame exists in memory twice: once in the cache and once as the working copy. For UHE variables with ~144M rows, this doubles memory usage (~2-4 GB per variable).

After tickets 005 and 006 introduce Polars in the pipeline, the cache can store Polars DataFrames (which are immutable and reference-counted) or the copy-on-write pattern can be adopted for pandas DataFrames.

### Relation to Epic

This is the final ticket in Epic 02. It depends on tickets 005 and 006 because the cache interaction changes when statistics are computed via Polars and concatenation uses Polars.

### Current State

- `__store_in_cache_if_needed()` (line 2253-2265 of operation.py): `cls.CACHED_SYNTHESIS[s] = df.copy()`
- `_get_from_cache()` (line 2070-2084 of operation.py): `return res.copy()`
- Cache is used by stub methods (`__stub_QDEF`, `__stub_VDEF`, `__stub_CTO`, etc.) which read a cached synthesis and modify the `VALUE_COL` to compute derived variables
- The `.copy()` exists to prevent stub methods from mutating the cached data
- `CACHED_SYNTHESIS: Dict[OperationSynthesis, pd.DataFrame] = {}` class-level dict

## Specification

### Requirements

1. Replace `df.copy()` in cache storage with direct assignment (no copy)
2. In `_get_from_cache()`, return the cached DataFrame directly (no copy)
3. Modify stub methods that mutate the cached DataFrame to create their own copy ONLY of the column they modify (not the entire DataFrame)
4. Alternatively, store Polars DataFrames in the cache (immutable by nature) and convert to pandas only when stubs need to mutate
5. Memory usage during synthesis must decrease measurably
6. All existing tests must pass

### Inputs/Props

- Cache stores `pd.DataFrame` keyed by `OperationSynthesis`
- Stub methods read from cache via `_get_from_cache()` and modify `VALUE_COL`

### Outputs/Behavior

- Cached DataFrames are not copied on store or retrieve
- Stub methods that derive new variables create copies only of the columns they modify
- All synthesis results are numerically identical to before

### Error Handling

- If a stub method accidentally mutates the cached DataFrame (regression), the next read of that cache entry would return corrupted data. Guard against this by adding an assertion in debug mode that verifies cache integrity after stub execution.

## Acceptance Criteria

- [ ] Given `__store_in_cache_if_needed()` is called, when the code is inspected, then NO `.copy()` call appears on the stored DataFrame
- [ ] Given `_get_from_cache()` is called, when the code is inspected, then NO `.copy()` call appears on the returned DataFrame
- [ ] Given a stub method (`__stub_QDEF`, `__stub_CTO`, etc.) modifies `VALUE_COL`, when the original cached DataFrame is read again, then the cached values are unchanged
- [ ] Given `pytest tests/` is run, when all tests execute, then all tests pass with zero failures

## Implementation Guide

### Suggested Approach

1. In `__store_in_cache_if_needed()`, remove the `.copy()`:

   ```python
   cls.CACHED_SYNTHESIS[s] = df  # was: df.copy()
   ```

2. In `_get_from_cache()`, remove the `.copy()`:

   ```python
   return res  # was: res.copy()
   ```

3. In each stub method that reads from cache and modifies `VALUE_COL`, create a shallow copy of only the necessary data:
   - `__stub_QDEF` (line 841): reads turbined_df and spilled_df from cache, modifies spilled_df VALUE_COL
     - Change to: `result_df = spilled_df.copy()` then modify `result_df[VALUE_COL]`
   - `__stub_VDEF` (line 865): same pattern
   - `__stub_VEVAP` (line 889): same pattern
   - `__stub_CTO` (line 914): same pattern
   - `__stub_EVER` (line 939): same pattern
   - `_convert_volume_to_flow` (line 791): modifies `df[VALUE_COL]` from cache
   - `_convert_flow_to_volume` (line 815): same pattern

   For each of these, the fix is: `df = cached_df.assign(**{VALUE_COL: new_values})` which creates a new DataFrame with shared columns except the modified one. This is cheaper than a full `.copy()`.

4. Alternatively, use `pd.DataFrame.copy(deep=False)` (shallow copy) in stubs -- this copies the DataFrame structure but shares the underlying column arrays. Then when a column is modified via `.loc[:, col] = ...`, pandas triggers copy-on-write for that column only.

### Key Files to Modify

- `app/services/synthesis/operation.py` (cache store/retrieve and all stub methods)

### Patterns to Follow

- Use `df.assign(**{col: values})` to create a new DataFrame with one column replaced
- This pattern is explicit about which columns are new vs shared

### Pitfalls to Avoid

- Do NOT remove `.copy()` from stubs without verifying they create their own modified copy -- this would corrupt the cache
- Do NOT change `_hydro_resolution_variable_map()` and `_flow_volume_hydro_variable_map()` -- they read from cache but pass the DataFrame to bounds resolution which may or may not mutate it. Verify whether bounds resolution mutates the DataFrame.
- Be careful with pandas 2.x copy-on-write behavior -- it is opt-in and may not be active in the project's pandas version

## Testing Requirements

### Unit Tests

- Test that modifying a DataFrame retrieved from cache does not affect subsequent retrievals
- Test that stub methods produce correct results with the new copy strategy

### Integration Tests

- Run `pytest tests/` -- all must pass

### E2E Tests (if applicable)

- Not required

## Dependencies

- **Blocked By**: ticket-005-rewrite-calc-statistics-polars.md, ticket-006-optimize-concat-with-polars.md
- **Blocks**: None

## Effort Estimate

**Points**: 2
**Confidence**: Medium (need to verify all mutation paths in stub methods)

## Out of Scope

- Changing the cache data structure (e.g., to use Polars DataFrames as cache values)
- Implementing an LRU eviction policy for the cache
- Changing the cache key type or lookup mechanism
