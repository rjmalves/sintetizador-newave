# ticket-009 Port policy.py to polars

## Context

### Background

`app/services/deck/policy.py` (495 lines) builds policy coefficient DataFrames from NWLISTCF binary files. It constructs DataFrames for policy variable mapping and coefficient expansion using `pd.DataFrame()` construction, `np.repeat()`/`np.tile()` for array expansion, `pd.concat()`, and `.apply(lambda)` for unit assignment. It depends on entities.py, misc.py, and temporal.py (all ported in previous tickets).

### Relation to Epic

This is the sixth and final ticket of Epic 2. Policy.py is the last deck domain module. After this ticket, all deck domain modules use polars internally.

### Current State

`app/services/deck/policy.py` key patterns:

- `pd.DataFrame(data={...})` construction with numpy arrays -- 3 occurrences
- `np.repeat()` / `np.tile()` for expanding arrays to match DataFrame rows -- 5 occurrences
- `pd.concat([dfs])` -- 2 occurrences
- `.apply(lambda, axis=1)` for unit lookup -- 1 occurrence
- `.sort_values()` -- 2 occurrences
- `.drop_duplicates()` -- 1 occurrence
- `.rename(columns={...})` -- 1 occurrence

The module reads binary nwlistcf files via `readers.get_nwlistcfrel()` and `readers.get_estados()` which return inewave objects. The `.coeficientes` and `.estados` attributes return `pd.DataFrame` instances.

## Specification

### Requirements

1. All functions in policy.py must use polars operations
2. Remove `import pandas as pd`, add `import polars as pl`
3. Remove `.copy()` calls
4. DataFrame construction must use `pl.DataFrame({...})` instead of `pd.DataFrame(data={...})`
5. The `apply(lambda)` for unit lookup must be replaced with a polars join or map expression
6. `np.repeat()` / `np.tile()` patterns can be replaced with polars `pl.Series` construction or kept with numpy (polars accepts numpy arrays in constructors)

### Inputs/Props

- inewave nwlistcf/estados objects (accessed via readers)
- `pl.DataFrame` from entities, misc, temporal

### Outputs/Behavior

- `common_policy_df()` returns `pl.DataFrame` with policy coefficient data
- `policy_variable_units()` returns `pl.DataFrame` with variable-to-unit mapping

### Error Handling

- Same as current -- readers.validate_data for initial validation
- If nwlistcf files are missing, the function returns early (existing behavior preserved)

## Acceptance Criteria

- [ ] Given `policy.py`, when searching for `import pandas`, then zero matches are found
- [ ] Given `policy.common_policy_df()`, when called, then it returns a `pl.DataFrame` with columns including `STAGE_COL`, `CUT_INDEX_COL`, `COEF_TYPE_COL`, `COEF_VALUE_COL`
- [ ] Given `policy.policy_variable_units()`, when called, then it returns a `pl.DataFrame` mapping coefficient types to their units
- [ ] Given the full test suite, when run after this ticket, then all tests pass

## Implementation Guide

### Suggested Approach

1. Replace `pd.DataFrame(data={...})` with `pl.DataFrame({...})`. Polars constructors accept numpy arrays directly, so `np.repeat()` and `np.tile()` patterns work unchanged:

   ```python
   df = pl.DataFrame({
       STAGE_COL: np.repeat(stages, num_entries),
       CUT_INDEX_COL: cut_indexes,
   })
   ```

2. Replace `pd.concat([dfs])` with `pl.concat([dfs])`

3. Replace `.sort_values(cols)` with `.sort(cols)`

4. Replace `.drop_duplicates()` with `.unique()`

5. Replace `.rename(columns={...})` with `.rename({...})`

6. Replace the `apply(lambda)` for unit lookup with a polars join:

   ```python
   # Build a unit mapping DataFrame
   unit_map = pl.DataFrame({
       COEF_TYPE_COL: [k for k in unit_dict.keys()],
       "unit": [v for v in unit_dict.values()],
   })
   df = df.join(unit_map, on=COEF_TYPE_COL, how="left")
   ```

7. Convert inewave binary file results (`.coeficientes`, `.estados`) from pandas to polars with `pl.from_pandas()`

### Key Files to Modify

- `app/services/deck/policy.py` -- full rewrite to polars

### Patterns to Follow

- `pl.DataFrame({col: numpy_array})` for construction
- `pl.concat([dfs])` for concatenation
- `df.join(lookup_table, on=key)` for lookup operations
- `.sort()`, `.unique()`, `.rename()` for basic operations

### Pitfalls to Avoid

- The numpy arrays used in DataFrame construction (`np.repeat`, `np.tile`) work directly in polars constructors -- no need to convert to lists
- The `_policy_df_building_block()` function is cached -- ensure the cached value is a `pl.DataFrame`
- Policy.py imports from `inewave.nwlistcf` -- these imports stay unchanged
- The `_COEF_SHORT` and `_COEF_LONG` dictionaries are Python dicts used for coefficient type naming -- they stay unchanged

## Testing Requirements

### Unit Tests

- Run full test suite to verify no regressions
- Verify policy DataFrame has expected coefficient types and structure

### Integration Tests

- Policy data is used in scenario synthesis -- verified through existing tests

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-004-port-entities-polars.md, ticket-005-port-temporal-misc-polars.md
- **Blocks**: ticket-010-port-pipeline-native-polars.md (Epic 3)

## Effort Estimate

**Points**: 3
**Confidence**: High (straightforward pandas-to-polars translation with no complex patterns like resample or graph traversal)

## Out of Scope

- Porting the synthesis pipeline (Epic 3)
- Modifying inewave nwlistcf parsing
- Optimizing policy coefficient computation
