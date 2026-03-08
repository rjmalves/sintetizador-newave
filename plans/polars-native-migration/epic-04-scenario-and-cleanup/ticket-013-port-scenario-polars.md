# ticket-013 Port scenario.py, execution.py, and system.py to polars

## Context

### Background

`app/services/synthesis/scenario.py` (1610 lines) is the scenario synthesis module. It constructs inflow, energy, and policy scenario DataFrames using extensive pandas operations: `pd.DataFrame()` construction, `.groupby().mean()`, `.apply(lambda)`, `pd.concat()`, `.sort_values()`, `.merge()`, `.loc[]` filtering, and `pd_to_pl()`/`pl_to_pd()` for polars integration at the export boundary. It has 17 `# SHIM: remove after polars migration` annotations for `.to_pandas()` calls on Deck accessors.

`app/services/synthesis/execution.py` (~180 lines) and `app/services/synthesis/system.py` (~230 lines) are smaller synthesizers with 2 and 5 SHIM annotations respectively. They use simple `.to_pandas()` calls on Deck accessors.

### Relation to Epic

This is the first ticket of Epic 4. After this ticket, all three synthesis modules use polars internally. The only remaining pandas usage will be in boundary code (readers, accessors, export adapters).

### Current State

**scenario.py key patterns:**

- `Deck.hydros(uow).to_pandas().reset_index(drop=True)` — 1 occurrence (line 133)
- `Deck.vazoes(uow).to_pandas()` with `int(c) for c in vazoes.columns` int-column-name fix — 1 occurrence (lines 135-138)
- `Deck.configurations(uow).to_pandas()` — 1 occurrence (line 391)
- `Deck.eer_submarket_map(uow).to_pandas().set_index(EER_CODE_COL)` — 2 occurrences (lines 500, 702)
- `Deck.energiaf(it, uow).to_pandas()` — 1 occurrence (line 937)
- `Deck.enavazf(it, uow).to_pandas()` — 1 occurrence (line 940)
- `Deck.internal_stages_starting_dates_*().to_pandas()` — 8 occurrences (various)
- `pd_to_pl()`/`pl_to_pd()` at export boundary — 3 occurrences (lines 971, 1477-1482)
- `pd.concat()` for stats — 1 occurrence (line 1500)
- `pd.DataFrame()` construction — multiple occurrences throughout
- `.groupby().mean()` for LTA computation — multiple occurrences
- `.apply(lambda: date.month)` for month extraction — multiple occurrences
- `.sort_values()`, `.merge()`, `.loc[]` — throughout

**execution.py key patterns:**

- `Deck.costs(uow).to_pandas()` — 1 occurrence (line 129)
- `Deck.runtimes(uow).to_pandas()` with `.dt.total_seconds()` and `.loc[]` filter — 1 occurrence (lines 136-138)

**system.py key patterns:**

- `Deck.block_lengths(uow).to_pandas()` with column mutation — 1 occurrence (line 130)
- `Deck.submarkets(uow).to_pandas().reset_index(drop=True)` — 1 occurrence (line 138)
- `Deck.eer_submarket_map(uow).to_pandas().reset_index(drop=True)` — 1 occurrence (line 145)
- `Deck.thermal_submarket_map(uow).to_pandas().reset_index(drop=True)` — 1 occurrence (line 152)
- `Deck.hydro_eer_submarket_map(uow).to_pandas().reset_index(drop=True)` — 1 occurrence (line 164)

## Specification

### Requirements

1. **Port `scenario.py` Deck accessor shims**: Remove all 17 `.to_pandas()` SHIM calls. Use Deck accessors natively (they return `pl.DataFrame` since epic-02). For `Deck.vazoes()`, the integer-column-name issue is resolved by keeping columns as strings in polars and using string keys.
2. **Port `scenario.py` DataFrame construction**: Replace `pd.DataFrame({...})` with `pl.DataFrame({...})`. Replace `pd.concat()` with `pl.concat()`.
3. **Port `scenario.py` groupby/apply**: Replace `.groupby(cols).mean()` with `.group_by(cols).agg(pl.mean(...))`. Replace `.apply(lambda row: row[DATE_COL].month)` with `pl.col(DATE_COL).dt.month()`.
4. **Port `scenario.py` filtering/sorting**: Replace `.loc[condition]` with `.filter()`. Replace `.sort_values()` with `.sort()`. Replace `.merge()` with `.join()`.
5. **Port `scenario.py` export boundary**: Replace `pd_to_pl()`/`pl_to_pd()` with native polars operations. Use `uow.export.synthetize_pl()` for parquet export (already available). For stats export, use `pl.concat()` instead of `pd.concat()`.
6. **Port `execution.py`**: Remove 2 SHIM calls. Use Deck accessors natively. Port `_resolve_cost()` and `_resolve_runtime()` column selection/filtering to polars.
7. **Port `system.py`**: Remove 5 SHIM calls. Use Deck accessors natively. Port `__resolve_PAT()`, `__resolve_SBM()`, `__resolve_REE()`, `__resolve_UTE()`, `__resolve_UHE()` to return `pl.DataFrame`. Port the export path to use polars.
8. **Remove `import pandas as pd`** from all three files where no longer needed. Keep `import pandas as pd` in scenario.py if `export_metadata()` still uses pandas (same pattern as operation export).

### Inputs/Props

- All Deck accessor calls return `pl.DataFrame` natively
- `calc_statistics()` accepts/returns `pl.DataFrame` (ported in ticket-010)
- `uow.export.synthetize_pl()` accepts `pl.DataFrame` for parquet export
- `uow.export.synthetize_df()` accepts `pd.DataFrame` for metadata export
- `uow.export.read_df()` returns `Optional[pd.DataFrame]`

### Outputs/Behavior

- All scenario synthesis functions return `pl.DataFrame` internally
- Export uses `synthetize_pl()` for data, `synthetize_df()` for metadata (stays pandas)
- execution.py resolve functions return `pl.DataFrame`
- system.py resolve functions return `pl.DataFrame`

### Error Handling

- Same as current — errors propagate naturally

## Acceptance Criteria

- [ ] Given `scenario.py` is open, when searching for `# SHIM`, then zero matches are found
- [ ] Given `execution.py` is open, when searching for `# SHIM`, then zero matches are found
- [ ] Given `system.py` is open, when searching for `# SHIM`, then zero matches are found
- [ ] Given `scenario.py` is open, when searching for `pd_to_pl` or `pl_to_pd`, then zero matches are found
- [ ] Given all existing tests are executed via `pytest`, when the test suite completes, then all tests pass with zero failures

## Implementation Guide

### Suggested Approach

**Phase A: execution.py and system.py (simplest)**

1. `execution.py _resolve_cost()`: Replace `Deck.costs(uow).to_pandas()` with `Deck.costs(uow)`. Port `df[["parcela", "valor_esperado", "desvio_padrao"]]` to `df.select(["parcela", "valor_esperado", "desvio_padrao"])`.

2. `execution.py _resolve_runtime()`: Replace `Deck.runtimes(uow).to_pandas()` with `Deck.runtimes(uow)`. Port `.dt.total_seconds()` to polars: `pl.col("tempo").dt.total_seconds()`. Port `.loc[df["etapa"] != "Tempo Total"]` to `.filter(pl.col("etapa") != "Tempo Total")`.

3. `system.py __resolve_PAT()`: Replace `Deck.block_lengths(uow).to_pandas()` with `Deck.block_lengths(uow)`. Port `df[VALUE_COL] *= STAGE_DURATION_HOURS` to `df.with_columns(pl.col(VALUE_COL) * STAGE_DURATION_HOURS)`.

4. `system.py __resolve_SBM/REE/UTE/UHE()`: Remove `.to_pandas().reset_index(drop=True)`. Return polars DataFrame directly. Port column selection with `.select()`.

5. For execution.py and system.py export paths: check whether they use `synthetize_df()` (pandas) or `synthetize_pl()` (polars). If they use `synthetize_df()`, either convert at the export boundary with `.to_pandas()` or switch to `synthetize_pl()`.

**Phase B: scenario.py (largest)**

6. **Deck accessor shims**: Replace all `.to_pandas()` calls with native polars usage. For `Deck.vazoes()`, keep string column names (polars default) and use string keys for station lookup.

7. **DataFrame construction**: Replace `pd.DataFrame({col: values})` with `pl.DataFrame({col: values})`. Numpy arrays work directly in polars constructors.

8. **Groupby/LTA**: Replace `.groupby(cols).mean()` with:

   ```python
   df.group_by(cols).agg(pl.col(VALUE_COL).mean())
   ```

   Replace `df[DATE_COL].apply(lambda x: x.month)` with:

   ```python
   df.with_columns(pl.col(DATE_COL).dt.month().alias(MONTH_COL))
   ```

9. **Filtering**: Replace `.loc[condition]` with `.filter()`. Replace `.iloc[0]` scalar extraction with `.item(0, col)`.

10. **Export boundary**: Replace `pd_to_pl(scenarios_df).sort(...)` with native polars sort. Replace `pl_to_pd(calc_statistics(scenarios_pl))` — `calc_statistics` already returns polars. Use `synthetize_pl()` for parquet export. For stats `pd.concat(dfs)` → `pl.concat(dfs)`.

11. **`_generate_hydro_incremental_inflow_dataframe`**: This is the most complex function. It accesses `Deck.vazoes()` with integer column names (now strings in polars). Use string keys: `vazoes[str(inflow_station)]`. Port the upstream flow subtraction loop to polars.

### Key Files to Modify

- `app/services/synthesis/scenario.py` — full polars migration (17 SHIMs, pd_to_pl/pl_to_pd removal)
- `app/services/synthesis/execution.py` — remove 2 SHIMs, port to polars
- `app/services/synthesis/system.py` — remove 5 SHIMs, port to polars

### Patterns to Follow

- `pl.DataFrame({col: numpy_array})` for construction
- `.group_by(cols).agg(pl.col(COL).mean())` for groupby mean
- `pl.col(DATE_COL).dt.month()` for month extraction
- `.filter(condition)` for row filtering
- `.select(cols)` for column selection
- `.join(other, on=key)` for merges
- `dict(zip(df[KEY].to_list(), df[VAL].to_list()))` for scalar lookups in loops

### Pitfalls to Avoid

- **`Deck.vazoes()` integer column names**: After `pl.from_pandas()`, integer column names become strings. Use `str(station_code)` as the column key. Do not cast back to int.
- **`_eval_monthly_lta()` pattern**: Uses `.apply(lambda)` for month extraction and `.groupby([month]).mean()`. Port to `pl.col(DATE_COL).dt.month()` + `.group_by().agg()`.
- **`_post_resolve_energy_iteration()` complexity**: This function builds DataFrames from multiple sources. Port carefully, section by section.
- **execution.py `_resolve_runtime()` timedelta**: `Deck.runtimes()` returns a column with `timedelta` type. In polars, use `.dt.total_seconds()` which works on Duration type.
- **system.py export**: Check whether `_export_data()` uses `synthetize_df()` or `synthetize_pl()`. May need a `.to_pandas()` at the export boundary if the adapter expects pandas.
- **`export_metadata()` stays pandas**: Same pattern as operation export — metadata is a tiny DataFrame built with `pd.DataFrame` and exported via `synthetize_df()`.

## Testing Requirements

### Unit Tests

- Existing `test_scenario.py` exercises the full scenario synthesis path
- Existing `test_system.py` and `test_execution.py` exercise those synthesizers

### Integration Tests

- All existing tests must pass via `pytest` with zero failures

## Dependencies

- **Blocked By**: ticket-012-port-resolution-modules-polars.md
- **Blocks**: ticket-014-remove-conversion-utilities-dead-imports.md

## Effort Estimate

**Points**: 8
**Confidence**: Medium (scenario.py is 1610 lines with complex groupby/apply patterns)
