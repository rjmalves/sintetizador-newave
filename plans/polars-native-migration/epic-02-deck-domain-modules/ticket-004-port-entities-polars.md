# ticket-004 Port entities.py to polars

## Context

### Background

`app/services/deck/entities.py` provides entity catalog functions (submarkets, eers, hydros, thermals, and their relationship maps). After Epic 1, accessors return polars DataFrames but entities.py still operates in pandas (with `.to_pandas()` shims from ticket-003). This ticket removes those shims and ports all entity functions to native polars.

### Relation to Epic

This is the first ticket of Epic 2 (Deck Domain Modules). Entities is the foundational catalog module -- many other domain modules (energy, hydro, storage, thermal) depend on entity functions like `eers()`, `hydros()`, `eer_submarket_map()`, and `hydro_eer_submarket_map()`. Porting entities first unlocks all subsequent domain module migrations.

### Current State

`app/services/deck/entities.py` has 13 functions using pandas operations. Key patterns:

- `df.rename(columns={...})` -- 6 occurrences
- `df.drop_duplicates(subset=[...]).reset_index(drop=True)` -- 2 occurrences
- `df.astype({col: type})` -- 4 occurrences
- `df.set_index(col)` -- 7 occurrences (returns indexed DataFrames used with `.at[idx, col]`)
- `val.copy()` -- 8 return statements with `.copy()`
- `df.join(other, on=col)` -- 5 occurrences
- `df.apply(lambda, axis=1)` -- 2 occurrences
- `df.groupby(...).sum()` -- 2 occurrences
- `pd.concat([...])` -- 3 occurrences
- `df.sort_values(...)` -- 2 occurrences
- `df.isna().sum()` -- 1 occurrence (in `hybrid_policy`)

Critical design change: Entities currently use `set_index()` to create indexed DataFrames that downstream consumers access with `.at[idx, col]`. Polars has no index concept. These must become regular-column DataFrames, and `.at[idx, col]` lookups in downstream code must change to polars `.filter()` + `.item()` or join patterns.

## Specification

### Requirements

1. All 13 functions in `entities.py` must use polars operations instead of pandas
2. Remove all `.to_pandas()` shims added in ticket-003 for this file
3. Remove `import pandas as pd` (replace with `import polars as pl`)
4. Remove all `.copy()` return calls (polars immutability)
5. Cached values must be `pl.DataFrame` instances
6. Functions that previously returned indexed DataFrames must return regular `pl.DataFrame` with the index column as a regular column
7. The `hydro_eer_submarket_map` and `eer_submarket_map` functions must return `pl.DataFrame` with join-friendly structure (no index)

### Inputs/Props

- `pl.DataFrame` from accessors (confhd, eers raw data, etc.)
- Cache dictionary for memoization

### Outputs/Behavior

- All entity functions return `pl.DataFrame`
- Entity code order functions (`eer_code_order`, `hydro_code_order`) continue to return `List[int]`
- `hybrid_policy` continues to return `bool`
- Downstream consumers that previously used `.at[idx, col]` patterns on entity DataFrames will need to be updated (in their own migration tickets)

### Error Handling

- Same validation as current (readers.validate_data is called before polars conversion)
- `isna().sum()` in `hybrid_policy` becomes `pl.col(...).is_null().sum()`

## Acceptance Criteria

- [ ] Given `entities.py`, when searching for `import pandas`, then zero matches are found
- [ ] Given `entities.py`, when searching for `.copy()`, then zero matches are found
- [ ] Given `entities.submarkets()`, when called, then it returns a `pl.DataFrame` with columns `[SUBMARKET_CODE_COL, SUBMARKET_NAME_COL, ...]` (SUBMARKET_CODE_COL is a regular column, not an index)
- [ ] Given `entities.hydro_eer_submarket_map()`, when called, then it returns a `pl.DataFrame` that can be joined on `HYDRO_CODE_COL` using `pl.DataFrame.join()`
- [ ] Given the full test suite, when run after this ticket plus shim updates for downstream consumers of entities, then all tests pass

## Implementation Guide

### Suggested Approach

Port each function following these pandas-to-polars translations:

| pandas                                                       | polars                                          |
| ------------------------------------------------------------ | ----------------------------------------------- |
| `df.rename(columns={"a": "b"})`                              | `df.rename({"a": "b"})`                         |
| `df.drop_duplicates(subset=[col])`                           | `df.unique(subset=[col])`                       |
| `df.astype({col: str})`                                      | `df.with_columns(pl.col(col).cast(pl.Utf8))`    |
| `df.set_index(col)`                                          | Keep col as regular column (no-op)              |
| `val.copy()`                                                 | Remove (polars is immutable)                    |
| `df.join(other, on=col)`                                     | `df.join(other, on=col)` (same in polars)       |
| `df.apply(lambda line: other.at[line[col], target], axis=1)` | `df.join(other.select([col, target]), on=col)`  |
| `df.groupby(cols).sum()`                                     | `df.group_by(cols).agg(pl.col(val).sum())`      |
| `pd.concat([dfs])`                                           | `pl.concat([dfs])`                              |
| `df.sort_values(cols)`                                       | `df.sort(cols)`                                 |
| `df.isna().sum()`                                            | `df.select(pl.col(col).is_null().sum()).item()` |
| `df.reset_index(drop=True)`                                  | No-op (polars has no index)                     |
| `df.index.tolist()`                                          | `df[col].to_list()`                             |
| `df.drop(columns="col")`                                     | `df.drop("col")`                                |

**Key functions requiring careful attention:**

1. `thermal_submarket_map()` (line 186): Uses pandas `.set_index()` then `.index.tolist()` and `.apply(lambda)`. Rewrite as polars join between thermals and submarkets.

2. `hydro_eer_submarket_map()` (line 148): Chains three `.join()` calls with indexed DataFrames. In polars, use explicit `on=` column in each join.

3. `flow_diversion()` (line 227): Complex function with nested helper functions using pandas groupby, concat, apply, loc. Port each helper to polars.

4. `non_simulated_generation()` (line 293): Complex function with multiple nested helpers. Port each helper to polars.

**Important**: After porting entities.py, update the `.to_pandas()` shims in downstream consumers that now receive polars from entity functions. For domain modules not yet migrated (hydro.py, energy.py, storage.py, etc.), replace the `Deck.eers(uow).to_pandas()` shim pattern with calling `entities.eers(...)` directly and adding `.to_pandas()` there. Since entities functions are called both through Deck facade and directly, ensure both paths return polars.

### Key Files to Modify

- `app/services/deck/entities.py` -- full rewrite to polars
- `app/services/deck/hydro.py` -- update shims for entities calls (temporary, pending Epic 2 migration)
- `app/services/deck/energy.py` -- update shims for entities calls
- `app/services/deck/storage.py` -- update shims for entities calls
- `app/services/deck/thermal.py` -- update shims for entities calls

### Patterns to Follow

- Use `pl.col()` expressions for column operations
- Use `.join()` instead of indexed lookups
- Use `pl.concat()` for DataFrame concatenation
- Use `.sort()` for sorting
- Cache polars DataFrames directly (no copy needed)

### Pitfalls to Avoid

- The current code uses `set_index()` extensively for `.at[idx, col]` lookups. When removing the index, downstream consumers that use `.at[]` on entity returns will need shims updated to `.to_pandas()` before the `.at[]` call, or the downstream module itself must be migrated first
- `flow_diversion()` and `non_simulated_generation()` are the most complex functions -- they have nested helpers that each need porting
- The `"ficticio"` column filter in `resolution_sbm.py` uses `submarkets.loc[submarkets["ficticio"] == 0]` -- this will need polars `.filter()` in the resolution module shim update

## Testing Requirements

### Unit Tests

- Run the full test suite to verify no regressions
- Verify entity functions return `pl.DataFrame` with correct column names and dtypes

### Integration Tests

- The test suite exercises entity functions through the Deck facade

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-003-add-polars-compatibility-shims.md
- **Blocks**: ticket-005-port-temporal-misc-polars.md, ticket-006-port-exchange-energy-polars.md, ticket-007-port-hydro-polars.md, ticket-008-port-storage-thermal-polars.md

## Effort Estimate

**Points**: 3
**Confidence**: High

## Out of Scope

- Porting temporal.py, misc.py, or other domain modules (separate tickets)
- Porting synthesis pipeline modules (Epic 3)
- Removing `.to_pandas()` shims in modules not being migrated in this ticket
