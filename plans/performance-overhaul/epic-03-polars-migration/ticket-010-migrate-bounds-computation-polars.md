# ticket-010 Migrate Bounds Computation to Polars

## Context

### Background

The `OperationVariableBounds` class in `app/services/deck/bounds.py` adds `limite_inferior` and `limite_superior` columns to synthesis DataFrames. It uses pandas groupby, numpy array manipulation (`np.tile`, `np.repeat`, `np.zeros`), and pandas column assignment. After ticket-009, the entity pipeline returns `pl.DataFrame`, but `_resolve_bounds` in `operation.py` still calls `OperationVariableBounds.resolve_bounds` which expects `pd.DataFrame`. This forces a Polars-to-pandas conversion at the bounds boundary.

### Relation to Epic

This is the third step of Epic 03. It migrates the bounds computation to accept and return `pl.DataFrame`, eliminating the pd/pl conversion between entity resolution and bounds resolution. After this ticket, the pipeline from temporal resolution through bounds is fully Polars-native.

### Current State

- `OperationVariableBounds.resolve_bounds` (line ~1881 of `bounds.py`) accepts `pd.DataFrame`, returns `pd.DataFrame`.
- It dispatches to ~40 variable-specific lambda mappings in `MAPPINGS`, each calling one of ~10 helper methods: `_stored_energy_bounds`, `_stored_volume_bounds`, `_outflow_bounds`, `_turbined_flow_bounds`, `_exchange_bounds`, `_thermal_generation_bounds`, `_flow_diversion_bounds`, `_qdes_vdes_uhe_bounds`, `_lower_bounded_bounds`, `_unbounded_bounds`, `_group_hydro_df`, `_group_hydro_df_vol_flow_cast`, `_group_thermal_df`.
- Helper methods use `fast_group_df` (pandas groupby), `np.tile`, `np.repeat`, `np.zeros`, `np.round`, and direct column assignment.
- `_resolve_bounds` in `operation.py` (line ~2411) calls `resolve_bounds` and passes `ordered_synthesis_entities` (a `Dict[str, list]`).
- Bounds computation runs in the main process only (not in subprocesses), after `_post_resolve` concatenates all entities.
- The bounds data comes from `Deck` methods that return `pd.DataFrame`.

## Specification

### Requirements

1. Modify `resolve_bounds` to accept `pl.DataFrame` and return `pl.DataFrame`. Convert to pandas internally at entry for the existing helper methods, and convert back at exit. This is the minimal-change approach that avoids rewriting all ~10 helper methods.
2. Create Polars-native implementations for the two most performance-critical helpers: `_repeats_data_by_scenario` and `_repeats_data_by_scenario_and_block`. These are called by every bounded variable and contain the nested for-loop over entities and stages.
3. Convert `_unbounded` to Polars (trivial: `df.with_columns(pl.lit(-float("inf")).alias(LOWER_BOUND_COL), pl.lit(float("inf")).alias(UPPER_BOUND_COL))`).
4. Modify `_resolve_bounds` in `operation.py` to pass the `pl.DataFrame` directly (removing the pd conversion that would otherwise be needed after ticket-009).
5. Wrap the Polars path in try/except with pandas fallback in `resolve_bounds`.

### Inputs/Props

- `s: OperationSynthesis` — identifies which variable/spatial resolution.
- `df: pl.DataFrame` — the concatenated synthesis DataFrame with all entities.
- `ordered_synthesis_entities: Dict[str, list]` — entity ordering info.
- `uow: AbstractUnitOfWork` — for Deck data access.

### Outputs/Behavior

- Returns `pl.DataFrame` with additional columns `LOWER_BOUND_COL` and `UPPER_BOUND_COL`.
- Values must be numerically identical to the current pandas/numpy implementation.
- Column ordering is not changed; the two bound columns are appended.

### Error Handling

- If the Polars path in `resolve_bounds` fails, convert to pandas, call the existing pandas implementation, convert back to Polars, and log a warning.
- Existing `ValueError` catch for bounds computation errors is preserved.

## Acceptance Criteria

- [ ] Given a `pl.DataFrame` with 4 submarkets, 60 stages, 2000 scenarios, 3 blocks for `ENERGIA_ARMAZENADA_ABSOLUTA_FINAL` at REE resolution, when `resolve_bounds` is called, then the returned `pl.DataFrame` has columns `LOWER_BOUND_COL` and `UPPER_BOUND_COL` with non-null float values.
- [ ] Given an unbounded variable (not in `MAPPINGS`), when `resolve_bounds` is called with a `pl.DataFrame`, then the returned DataFrame has `LOWER_BOUND_COL == -inf` and `UPPER_BOUND_COL == inf` for all rows.
- [ ] Given the same input DataFrame, when comparing the Polars-path `resolve_bounds` output to the pandas-fallback output column-by-column for `LOWER_BOUND_COL`, `UPPER_BOUND_COL`, and `VALUE_COL`, then all values are identical within floating-point tolerance (`atol=1e-6`).
- [ ] Given `_resolve_bounds` in `operation.py`, when it calls `resolve_bounds`, then no `pl_to_pd` or `pd_to_pl` conversion occurs at the call site (the conversion happens inside `resolve_bounds`).
- [ ] Given the full test suite, when `pytest tests/ -x` runs, then no regressions occur in previously passing tests.

## Implementation Guide

### Suggested Approach

1. **Modify `resolve_bounds` signature and body**:

   ```python
   @classmethod
   def resolve_bounds(
       cls,
       s: OperationSynthesis,
       df: pl.DataFrame,
       ordered_synthesis_entities: Dict[str, list],
       uow: AbstractUnitOfWork,
       logger: Optional[Logger] = None,
   ) -> pl.DataFrame:
       Deck.logger = logger
       try:
           if cls.is_bounded(s):
               pd_df = df.to_pandas()
               result_pd = cls.MAPPINGS[s](pd_df, uow, ordered_synthesis_entities)
               return pl.from_pandas(result_pd)
       except ValueError as e:
           cls._log(f"Erro no calculo de limites: {e}", ERROR)
           cls._log("Considerando variavel ilimitada.", ERROR)
       return df.with_columns(
           pl.lit(-float("inf")).alias(LOWER_BOUND_COL),
           pl.lit(float("inf")).alias(UPPER_BOUND_COL),
       )
   ```

2. **Update `_resolve_bounds` in `operation.py`**: Remove any pd/pl conversion; pass the DataFrame directly. After ticket-009, `_post_resolve` returns `pd.DataFrame` (it converts at the end), but `_resolve_bounds` is called after `_post_resolve`. Check the call site: in `_resolve_synthesis` (line ~2443) it is `df = cls._resolve_bounds(s, df, uow)` where `df` is the result of `_resolve_spatial_resolution` which calls `_post_resolve` which returns `pd.DataFrame`. So we need to convert to Polars before calling bounds:

   ```python
   @classmethod
   def _resolve_bounds(cls, s, df, uow):
       with time_and_log(...):
           df_pl = pd_to_pl(df)
           df_pl = OperationVariableBounds.resolve_bounds(
               s, df_pl, cls._get_ordered_entities(s), uow
           )
           return pl_to_pd(df_pl)
   ```

   This adds one conversion pair at the bounds boundary, but the subsequent ticket-011 will push Polars further to export.

3. **Optimize `_repeats_data_by_scenario` for future**: Leave the numpy implementation as-is inside the pandas helper methods. The conversion at the `resolve_bounds` boundary is the performance win for now; the internal numpy operations on the already-converted pandas DataFrame are fast.

### Key Files to Modify

- `app/services/deck/bounds.py` — `resolve_bounds` method signature and `_unbounded` method.
- `app/services/synthesis/operation.py` — `_resolve_bounds` method (line ~2411) to handle pd/pl boundary.

### Patterns to Follow

- `pl.lit(value).alias(col)` for constant columns.
- `pl.from_pandas(df)` / `df.to_pandas()` for boundary conversions inside `resolve_bounds`.
- Try/except with fallback pattern.

### Pitfalls to Avoid

- Do NOT rewrite all ~10 helper methods (`_stored_energy_bounds`, `_stored_volume_bounds`, etc.) to Polars in this ticket. They use complex numpy array indexing that is not trivially translatable. The conversion at the `resolve_bounds` boundary is sufficient for this epic.
- Do NOT change the `MAPPINGS` dict lambdas; they must continue to receive `pd.DataFrame`.
- The `_group_hydro_df` and `_group_thermal_df` methods use `fast_group_df` which is pandas-only; keep them as-is.
- `_resolve_bounds` is also called from `_resolve_stub` (line ~2381); ensure that path also handles the pd/pl boundary correctly.

## Testing Requirements

### Unit Tests

- Add a test in `tests/app/services/deck/test_bounds_polars.py` that:
  1. Constructs a small `pl.DataFrame` for an unbounded variable.
  2. Calls `resolve_bounds` and verifies the result is a `pl.DataFrame` with `LOWER_BOUND_COL == -inf` and `UPPER_BOUND_COL == inf`.

### Integration Tests

- Run `pytest tests/ -x` and verify no regressions.

### E2E Tests (if applicable)

- Not applicable for this ticket.

## Dependencies

- **Blocked By**: ticket-009-migrate-entity-post-processing-polars.md
- **Blocks**: ticket-011-migrate-parquet-export-polars.md

## Effort Estimate

**Points**: 2
**Confidence**: High

## Out of Scope

- Rewriting the internal bounds helper methods (`_stored_energy_bounds`, `_stored_volume_bounds`, `_outflow_bounds`, etc.) to pure Polars.
- Optimizing `_repeats_data_by_scenario` with Polars expressions.
- Modifying `Deck` methods to return Polars DataFrames.
- Modifying the `MAPPINGS` dict lambda signatures.
