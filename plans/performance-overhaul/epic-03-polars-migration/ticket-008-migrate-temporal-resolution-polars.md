# ticket-008 Migrate Temporal Resolution to Polars

## Context

### Background

The `_resolve_temporal_resolution` static method in `OperationSynthetizer` is called once per entity in the inner loop. It receives a raw pandas DataFrame from `inewave`'s `get_nwlistop`, renames columns, sorts, replaces scenario indices with `np.tile`/`np.repeat`, adds stage and end-date columns with `np.repeat`, and computes block-duration columns with a Python `for` loop over start dates combined with `np.tile`. After Epics 01 and 02 established Polars at the statistics and concatenation boundaries, this method remains a pandas/numpy hot path that forces a pd-to-pl conversion later in `_post_resolve`.

### Relation to Epic

Epic 03 pushes Polars deeper into the entity resolution pipeline to eliminate per-entity pandas-Polars conversions. This ticket is the first step: converting the temporal resolution method so it returns a `pl.DataFrame`. Subsequent tickets (009-011) build on this by consuming Polars DataFrames throughout.

### Current State

- `_resolve_temporal_resolution` (line ~310 of `app/services/synthesis/operation.py`) accepts `pd.DataFrame`, returns `pd.DataFrame`.
- Internally it defines 4 nested functions: `_replace_scenario_info`, `_add_stage_info`, `_add_block_duration_info`, `_add_temporal_info`.
- All use `np.tile`, `np.repeat`, `np.arange`, and direct `df[col] = array` assignment.
- `DeckContext` fields (`block_lengths`, `starting_dates`, `ending_dates`, `num_scenarios`, etc.) are pandas/Python types.
- The method selects `OPERATION_SYNTHESIS_COMMON_COLUMNS` = `[STAGE_COL, START_DATE_COL, END_DATE_COL, SCENARIO_COL, BLOCK_COL, BLOCK_DURATION_COL, VALUE_COL]` at the end.
- `pd_to_pl` and `pl_to_pd` from `app/utils/dataframe.py` are the established conversion utilities.
- There is also `_resolve_temporal_resolution_GTER_UTE` (line ~1830) with an analogous pattern for thermal generation per UTE; this ticket covers only the standard method.

## Specification

### Requirements

1. Create a `_resolve_temporal_resolution_polars` internal implementation that operates entirely on `pl.DataFrame` and returns `pl.DataFrame`.
2. The public `_resolve_temporal_resolution` method keeps its `pd.DataFrame -> pd.DataFrame` signature unchanged. It converts input to Polars at entry, calls the Polars implementation, and converts back at exit.
3. Replace `np.tile`/`np.repeat`/`np.arange` patterns with Polars expressions: `pl.arange`, `pl.repeat`, `pl.lit`, and computed columns via `with_columns`.
4. Replace the Python `for` loop over start dates in `_add_block_duration_info` with a Polars `join` between the main DataFrame and `block_lengths` on `(START_DATE_COL, BLOCK_COL)`, followed by a `with_columns` to multiply by `STAGE_DURATION_HOURS`.
5. Wrap the Polars path in try/except with logger warning and pandas fallback, following the pattern established in `calc_statistics` (`app/utils/operations.py` lines 171-194).
6. Use `maintain_order=True` on any Polars `sort` call.

### Inputs/Props

- `df: pd.DataFrame` from `inewave`'s `get_nwlistop` with columns `["data", "serie", BLOCK_COL, VALUE_COL]`.
- `uow: AbstractUnitOfWork` for fallback Deck calls.
- `deck_context: Optional[DeckContext]` with pre-computed `block_lengths` (pd.DataFrame), `num_scenarios` (int), `starting_dates` (List[datetime]), `ending_dates` (List[datetime]).

### Outputs/Behavior

- Returns `pd.DataFrame` with exactly columns `OPERATION_SYNTHESIS_COMMON_COLUMNS` in that order: `[STAGE_COL, START_DATE_COL, END_DATE_COL, SCENARIO_COL, BLOCK_COL, BLOCK_DURATION_COL, VALUE_COL]`.
- Row values must be numerically identical to the current pandas/numpy implementation.
- Row ordering must match the current implementation (sorted by `START_DATE_COL, SCENARIO_COL, BLOCK_COL`).

### Error Handling

- If the Polars path raises any exception, log a warning and fall back to the existing pandas/numpy implementation (which is preserved as-is).
- Do not swallow the exception silently; use `logger.warning(...)` with the exception message.

## Acceptance Criteria

- [ ] Given a DataFrame from `get_nwlistop` with 60 stages, 2000 scenarios, and 3 blocks, when `_resolve_temporal_resolution` is called, then the returned DataFrame has exactly `60 * 2000 * 3 = 360000` rows with columns `OPERATION_SYNTHESIS_COMMON_COLUMNS`.
- [ ] Given the same input, when comparing the Polars-path output to the pandas-fallback output row-by-row, then all values in `VALUE_COL`, `STAGE_COL`, `SCENARIO_COL`, `BLOCK_COL`, `BLOCK_DURATION_COL`, `START_DATE_COL`, `END_DATE_COL` are identical.
- [ ] Given `deck_context` is provided, when `_resolve_temporal_resolution` executes the Polars path, then no call to `Deck.num_scenarios_final_simulation`, `Deck.internal_stages_starting_dates_final_simulation`, `Deck.internal_stages_ending_dates_final_simulation`, or `Deck.block_lengths` is made.
- [ ] Given the Polars path raises a `pl.ComputeError`, when `_resolve_temporal_resolution` catches it, then a warning is logged containing the string "falling back to pandas" and the pandas fallback result is returned.
- [ ] Given the file `app/services/synthesis/operation.py`, when inspected, then the existing `_replace_scenario_info`, `_add_stage_info`, `_add_block_duration_info`, `_add_temporal_info` functions are preserved unchanged (they serve as the fallback).

## Implementation Guide

### Suggested Approach

1. Inside `_resolve_temporal_resolution`, after the existing `_add_temporal_info` definition, add a new `_add_temporal_info_polars` function that:
   a. Converts `df` to Polars via `pd_to_pl(df.rename(columns={"data": START_DATE_COL, "serie": SCENARIO_COL}))`.
   b. Sorts by `[START_DATE_COL, SCENARIO_COL, BLOCK_COL]` with `maintain_order=True`.
   c. Computes `num_stages`, `blocks`, `num_blocks` from the Polars DataFrame using `n_unique()` and `unique()`.
   d. Replaces scenarios: uses `pl.arange(1, num_scenarios + 1)` tiled via `.repeat_by()` or constructs a Series of length `num_stages * num_scenarios * num_blocks` and assigns with `with_columns`.
   e. Adds stage info: constructs a `pl.Series` for `STAGE_COL` and `END_DATE_COL` using `pl.arange` and `pl.Series(end_dates)`, each repeated `num_scenarios * num_blocks` times.
   f. Adds block duration: converts `deck_context.block_lengths` to Polars, filters by blocks in use, joins on `(START_DATE_COL, BLOCK_COL)`, multiplies `VALUE_COL` by `STAGE_DURATION_HOURS` to produce `BLOCK_DURATION_COL`, drops the joined value column.
   g. Selects `OPERATION_SYNTHESIS_COMMON_COLUMNS` and returns `pl_to_pd(result)`.

2. Modify the final `return` of `_resolve_temporal_resolution` to try the Polars path first:

   ```python
   if df is None:
       return None
   try:
       return _add_temporal_info_polars(df, uow, deck_context)
   except Exception as exc:
       OperationSynthetizer._log(
           f"_resolve_temporal_resolution: Polars path failed ({exc}), "
           "falling back to pandas",
           WARNING,
       )
       return _add_temporal_info(df, uow, deck_context)
   ```

3. For the block-duration join, the key insight is that `block_lengths` has columns `[START_DATE_COL, BLOCK_COL, VALUE_COL]`. The join replaces the Python for-loop:
   ```python
   bl_pl = pd_to_pl(df_block_lengths).filter(
       pl.col(BLOCK_COL).is_in(blocks)
   ).rename({VALUE_COL: BLOCK_DURATION_COL})
   result = result.join(bl_pl, on=[START_DATE_COL, BLOCK_COL], how="left")
   result = result.with_columns(
       (pl.col(BLOCK_DURATION_COL) * STAGE_DURATION_HOURS).alias(BLOCK_DURATION_COL)
   )
   ```

### Key Files to Modify

- `app/services/synthesis/operation.py` — `_resolve_temporal_resolution` method (lines ~310-450), adding `_add_temporal_info_polars` and try/except wrapper.

### Patterns to Follow

- Try/except with pandas fallback, as in `calc_statistics` in `app/utils/operations.py` (lines 171-194).
- `pd_to_pl` / `pl_to_pd` for boundary conversions, from `app/utils/dataframe.py`.
- `maintain_order=True` on all Polars `sort` and `group_by` calls.
- `_polars` suffix for the internal Polars implementation name.

### Pitfalls to Avoid

- Do NOT modify `_resolve_temporal_resolution_GTER_UTE` in this ticket; that is a separate method with different column structure (includes `THERMAL_CODE_COL`).
- Do NOT change the method signature from `pd.DataFrame -> pd.DataFrame`; ticket-009 handles the boundary shift.
- Do NOT import `polars` at module top level in ways that would break subprocess dispatch; `polars` is already imported at the top of `operation.py` (line 10) so this is fine for the main module, but be aware the entity resolution methods run in subprocesses.
- The `block_lengths` DataFrame from `DeckContext` uses `VALUE_COL` for durations; rename it before joining to avoid collision with the main DataFrame's `VALUE_COL`.

## Testing Requirements

### Unit Tests

- Add a test in `tests/app/services/synthesis/test_temporal_resolution.py` that:
  1. Constructs a small synthetic DataFrame (3 stages, 5 scenarios, 2 blocks) with known values.
  2. Constructs a mock `DeckContext` with matching `block_lengths`, `num_scenarios`, `starting_dates`, `ending_dates`.
  3. Calls `_resolve_temporal_resolution` and asserts output shape is `3 * 5 * 2 = 30` rows.
  4. Asserts `SCENARIO_COL` contains values 1-5 repeated correctly.
  5. Asserts `STAGE_COL` contains values 1-3 repeated correctly.
  6. Asserts `BLOCK_DURATION_COL` values equal `block_lengths_value * STAGE_DURATION_HOURS`.

### Integration Tests

- Run the existing test suite (`pytest tests/ -x`) and verify no regressions in the passing tests.

### E2E Tests (if applicable)

- Not applicable for this ticket.

## Dependencies

- **Blocked By**: ticket-007-eliminate-unnecessary-copies.md
- **Blocks**: ticket-009-migrate-entity-post-processing-polars.md

## Effort Estimate

**Points**: 3
**Confidence**: High

## Out of Scope

- Migrating `_resolve_temporal_resolution_GTER_UTE` (thermal UTE variant).
- Changing the method signature to accept/return `pl.DataFrame` (done in ticket-009).
- Modifying `DeckContext` fields to store Polars types.
- Modifying `scenario.py` temporal resolution (different synthesizer, different epic scope).
