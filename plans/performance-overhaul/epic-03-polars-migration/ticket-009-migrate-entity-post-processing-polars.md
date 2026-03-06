# ticket-009 Migrate Entity Post-Processing Pipeline to Polars

## Context

### Background

After ticket-008, `_resolve_temporal_resolution` internally uses Polars but still returns `pd.DataFrame`. The entity resolution methods (`_resolve_SBM_entity`, `_resolve_REE_entity`, `_resolve_UHE_entity`, `_resolve_SBP_entity`, `_resolve_UTE_entity`, `_resolve_PEE_entity`) each call `_post_resolve_entity`, which calls `_resolve_temporal_resolution`, adds entity code columns, calls `_resolve_starting_stage`, and dispatches internal stubs. All of this currently operates on pandas DataFrames. The `_post_resolve` method then converts each entity's `pd.DataFrame` to Polars for concatenation via `pl.concat`, then converts back to pandas. This ticket eliminates the round-trip by making the entity pipeline produce `pl.DataFrame` throughout.

### Relation to Epic

This is the second step of Epic 03. It extends the Polars boundary from temporal resolution (ticket-008) through the full entity post-processing pipeline, so that `_post_resolve` receives `pl.DataFrame` objects and can concatenate without conversion.

### Current State

- `_post_resolve_entity` (line ~238 of `app/services/synthesis/operation.py`) accepts and returns `Optional[pd.DataFrame]`.
- It calls `_resolve_temporal_resolution` (returns pd.DataFrame), assigns entity columns via `df[col] = val`, calls `_resolve_starting_stage` (line ~2115, subtracts starting month from STAGE_COL and filters), and dispatches internal stubs.
- `_post_resolve` (line ~262) converts each entity DataFrame to Polars via `pd_to_pl(df)`, calls `pl.concat`, sorts, and converts back via `pl_to_pd`.
- Internal stubs like `_calc_block_0_weighted_mean` (line ~739 in UHE resolution) operate on pandas DataFrames.
- Entity resolution methods are dispatched in subprocesses via `multiprocessing.Pool` for SBM, REE, UHE, SBP, UTE, PEE spatial resolutions. SIN runs in the main process.

## Specification

### Requirements

1. Modify `_resolve_temporal_resolution` to return `pl.DataFrame` instead of `pd.DataFrame`. The try/except fallback converts the pandas result to Polars before returning. This changes the internal contract but not the callers outside `operation.py`.
2. Modify `_post_resolve_entity` to work with `pl.DataFrame` throughout:
   - Receive the `pl.DataFrame` from `_resolve_temporal_resolution`.
   - Add entity code columns using `with_columns(pl.lit(val).alias(col))`.
   - Call a Polars version of `_resolve_starting_stage`.
   - Return `pl.DataFrame`.
3. Create `_resolve_starting_stage_polars` that filters `pl.col(STAGE_COL) > 0` after subtracting `starting_month - 1`. Wrap in try/except with pandas fallback.
4. Modify `_post_resolve` to accept `pl.DataFrame` objects directly, removing the `pd_to_pl(df)` conversion per entity. The `pl.concat` and `pl.sort` calls remain. The final `pl_to_pd` conversion at the end of `_post_resolve` stays because downstream hooks and cache operate on pandas.
5. Convert internal stubs (`_calc_block_0_weighted_mean` and any similar per-entity stubs in UHE, UTE resolution) to accept `pl.DataFrame` and return `pl.DataFrame`. These operate on small per-entity data, so the conversion overhead is minimal; the primary goal is eliminating the pl->pd->pl round-trip in the entity pipeline.
6. Maintain the cache safety pattern: when reading from `CACHED_SYNTHESIS` (which stores `pd.DataFrame`), convert to Polars if needed. When storing, convert back to pandas. This ticket does NOT change the cache storage format.

### Inputs/Props

- Entity resolution methods receive `uow`, `synthesis`, entity identifiers, and `deck_context`.
- `_post_resolve_entity` receives `Optional[pl.DataFrame]` (changed from pd), synthesis info, entity column values dict, uow, internal stubs, deck_context.
- `_post_resolve` receives `Dict[str, Optional[pl.DataFrame]]` (changed from pd).

### Outputs/Behavior

- Entity resolution methods (`_resolve_SBM_entity`, etc.) return `Optional[pl.DataFrame]`.
- `_post_resolve` returns `Optional[pd.DataFrame]` (unchanged; conversion at the end).
- Row values and ordering must be identical to the current implementation.

### Error Handling

- Each Polars operation in the entity pipeline has a try/except fallback to the pandas equivalent.
- If `_resolve_starting_stage_polars` fails, fall back to `_resolve_starting_stage` (pandas), converting to/from pd at the boundary.

## Acceptance Criteria

- [ ] Given a SBM entity resolution with 60 stages, 2000 scenarios, 3 blocks, when `_resolve_SBM_entity` completes, then it returns a `pl.DataFrame` (verified by `isinstance(result, pl.DataFrame)`) with columns including `SUBMARKET_CODE_COL`.
- [ ] Given `_post_resolve` receives a dict of `pl.DataFrame` objects from 4 submarkets, when it concatenates them, then no call to `pd_to_pl` occurs inside `_post_resolve` (the `pd_to_pl(df) for df in valid_dfs` list comprehension is removed).
- [ ] Given the UHE entity resolution path with `_calc_block_0_weighted_mean` stub, when the stub processes a `pl.DataFrame`, then the returned DataFrame has a `BLOCK_COL == 0` row with `VALUE_COL` equal to the weighted mean of block values by block duration.
- [ ] Given `_resolve_starting_stage_polars` receives a `pl.DataFrame` with `STAGE_COL` values `[0, 1, 2, 3]` and `starting_month = 2`, then the returned DataFrame has `STAGE_COL` values `[1, 2]` (stages 0 and -1 filtered out after subtracting 1).
- [ ] Given the full test suite, when `pytest tests/ -x` runs, then no regressions occur in previously passing tests.

## Implementation Guide

### Suggested Approach

1. **Change `_resolve_temporal_resolution` return type**: In the method body, after the Polars path produces a `pl.DataFrame`, return it directly (remove the `pl_to_pd` at the end of `_add_temporal_info_polars` from ticket-008). In the fallback path, wrap the pandas result: `return pd_to_pl(_add_temporal_info(df, uow, deck_context))`.

2. **Create `_resolve_starting_stage_polars`**:

   ```python
   @staticmethod
   def _resolve_starting_stage_polars(
       df: pl.DataFrame,
       deck_context: Optional[DeckContext],
       uow: AbstractUnitOfWork,
   ) -> pl.DataFrame:
       if deck_context is not None:
           starting_month = deck_context.study_period_starting_month
       else:
           starting_month = Deck.study_period_starting_month(uow)
       return df.with_columns(
           (pl.col(STAGE_COL) - (starting_month - 1)).alias(STAGE_COL)
       ).filter(pl.col(STAGE_COL) > 0)
   ```

3. **Modify `_post_resolve_entity`** to use Polars:

   ```python
   if df is None:
       return df
   df_pl = cls._resolve_temporal_resolution(df, uow, deck_context)  # now returns pl.DataFrame
   df_pl = df_pl.with_columns(
       [pl.lit(val).alias(col) for col, val in entity_column_values.items()]
   )
   df_pl = cls._resolve_starting_stage_polars(df_pl, deck_context, uow)
   if s.variable in internal_stubs:
       df_pl = internal_stubs[s.variable](df_pl, uow)
   return df_pl
   ```

4. **Modify `_post_resolve`**: Change the list comprehension from `pd_to_pl(df)` to just `df`:

   ```python
   df = pl_to_pd(
       pl.concat(valid_dfs).sort(
           s.spatial_resolution.sorting_synthesis_df_columns,
           maintain_order=True,
       )
   ).reset_index(drop=True)
   ```

5. **Convert `_calc_block_0_weighted_mean`** (UHE stub) to Polars. This is the most complex stub; it creates a block-0 row as a weighted mean. The Polars equivalent uses `group_by` over `[HYDRO_CODE_COL, STAGE_COL, SCENARIO_COL]` to sum `VALUE_COL * BLOCK_DURATION_COL / STAGE_DURATION_HOURS`, then concatenates with the original.

6. **Update type hints** on modified methods to reflect `pl.DataFrame` where applicable. Use `Union[pd.DataFrame, pl.DataFrame]` only if the fallback path returns pandas; otherwise use `pl.DataFrame`.

### Key Files to Modify

- `app/services/synthesis/operation.py` — `_post_resolve_entity`, `_post_resolve`, `_resolve_starting_stage` (add Polars variant), `_resolve_temporal_resolution` (change return type), `_calc_block_0_weighted_mean`, entity resolution methods return type annotations.

### Patterns to Follow

- `pl.lit(val).alias(col)` for adding constant columns, as established in Polars idioms.
- `maintain_order=True` on all `sort` and `group_by` calls.
- Try/except with pandas fallback at each new Polars conversion point.
- `pd_to_pl` / `pl_to_pd` for boundary conversions only.

### Pitfalls to Avoid

- Do NOT change `CACHED_SYNTHESIS` storage format from pandas; the cache is read by `_resolve_bounds` (ticket-010) and `_export_scenario_synthesis` which expect pandas.
- Do NOT modify `scenario.py` in this ticket; it has its own `_post_resolve` that follows a similar pattern but is out of scope.
- The `early_hooks` and `late_hooks` in `_post_resolve` receive `pd.DataFrame` after the `pl_to_pd` conversion at the end; do not change their interface.
- `_resolve_temporal_resolution_GTER_UTE` and `_post_resolve_GTER_UTE_entity` are separate paths for thermal UTE generation; do not modify them in this ticket.
- Entity resolution methods run in subprocesses; Polars is already imported at module level in `operation.py`, so `pl.DataFrame` can be pickled across the process boundary without issue.

## Testing Requirements

### Unit Tests

- Add a test in `tests/app/services/synthesis/test_entity_pipeline.py` that:
  1. Constructs a synthetic `pl.DataFrame` with `OPERATION_SYNTHESIS_COMMON_COLUMNS` plus `VALUE_COL`.
  2. Calls `_resolve_starting_stage_polars` with `starting_month = 3` and verifies `STAGE_COL` is shifted by 2 and rows with `STAGE_COL <= 0` are removed.
  3. Verifies the result is a `pl.DataFrame`.

- Add a test for `_calc_block_0_weighted_mean` Polars version:
  1. Constructs a small DataFrame with 2 blocks, 2 scenarios, 1 stage for 1 UHE.
  2. Verifies block-0 row `VALUE_COL` equals `sum(value * duration) / STAGE_DURATION_HOURS`.

### Integration Tests

- Run `pytest tests/ -x` and verify no regressions.

### E2E Tests (if applicable)

- Not applicable for this ticket.

## Dependencies

- **Blocked By**: ticket-008-migrate-temporal-resolution-polars.md
- **Blocks**: ticket-010-migrate-bounds-computation-polars.md

## Effort Estimate

**Points**: 3
**Confidence**: Medium

## Out of Scope

- Modifying `scenario.py` entity resolution or `_post_resolve`.
- Modifying `_resolve_temporal_resolution_GTER_UTE` or `_post_resolve_GTER_UTE_entity`.
- Changing `CACHED_SYNTHESIS` storage format from pandas to Polars.
- Changing the `early_hooks` / `late_hooks` interface in `_post_resolve`.
