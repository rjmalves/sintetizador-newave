# ticket-005 Rewrite calc_statistics Using Polars

## Context

### Background

The `calc_statistics()` function in `app/utils/operations.py` is called once per variable (after Epic 01 ticket-003 moves it post-concatenation). It computes 21 quantiles (0%, 5%, ..., 100%) plus mean and standard deviation, grouped by all columns except `cenario` and `valor`. For a typical UHE variable with 200 plants x 120 stages x 3 blocks = 72,000 groups, each containing 2000 scenarios, the pandas `groupby().quantile()` operation is extremely slow -- it is single-threaded and creates many intermediate Python objects.

Polars provides a natively multi-threaded `group_by().agg()` with quantile support that is 10-50x faster for this workload pattern.

### Relation to Epic

This is the second ticket in Epic 02 and depends on ticket-004 (Polars dependency and conversion utilities). It rewrites the core statistics computation to use Polars while maintaining the same input/output interface (pandas DataFrames at boundaries).

### Current State

- `calc_statistics()` (line 110-120 of `app/utils/operations.py`): calls `_calc_quantiles()` and `_calc_mean_std()`, returns `pd.concat([df_q, df_m])`
- `_calc_quantiles()` (line 58-85): uses `df.groupby(grouping_columns).quantile(quantiles)` -- the slowest operation in the entire pipeline
- `_calc_mean_std()` (line 88-107): uses `fast_group_df()` which calls `df.groupby().mean(engine=PANDAS_GROUPING_ENGINE)` and `.std()` with numba engine
- `fast_group_df()` (line 14-40): wraps pandas groupby with engine selection (numba or cython)
- `QUANTILES_FOR_STATISTICS` (constants.py line 139): `[0.05 * i for i in range(21)]` = [0.0, 0.05, 0.10, ..., 1.0]
- The functions are called from `_export_scenario_synthesis()` in operation.py and from scenario.py

## Specification

### Requirements

1. Rewrite `calc_statistics()` to internally convert the input pandas DataFrame to Polars, perform all groupby/quantile/mean/std operations using Polars, and convert the result back to a pandas DataFrame
2. The function signature must remain `calc_statistics(df: pd.DataFrame) -> pd.DataFrame` -- callers are unchanged
3. The output DataFrame must have the same columns, column order, dtypes, and row semantics as the current implementation
4. The quantile labels must match exactly: "min", "p5", "p10", ..., "p95", "max", "mean", "std" in the `cenario` column
5. Numerical values must be equivalent within floating-point tolerance (1e-6 relative, since groupby quantile implementations may use slightly different interpolation)
6. The `fast_group_df()` function is retained for any remaining pandas-based callers outside the statistics path
7. All existing tests must pass

### Inputs/Props

- Input DataFrame has columns like: `[codigo_usina, codigo_ree, codigo_submercado, estagio, data_inicio, data_fim, cenario, patamar, duracao_patamar, valor, limite_inferior, limite_superior]`
- Grouping columns: all columns except `cenario` and `valor`
- The `cenario` column contains integer scenario IDs (1 to num_scenarios)
- The `valor` column contains float64 values

### Outputs/Behavior

- Output DataFrame has the same columns as input, but `cenario` contains string labels ("min", "p5", ..., "max", "mean", "std") and `valor` contains the computed statistics
- One row per (group x statistic), so for 72,000 groups x 23 statistics = 1,656,000 rows

### Error Handling

- If the input DataFrame is empty, return an empty DataFrame with the same columns
- If the Polars conversion fails, fall back to the original pandas implementation with a logged warning

## Acceptance Criteria

- [ ] Given `calc_statistics()` is called with a DataFrame of 72,000 groups x 2,000 scenarios, when it returns, then the output has exactly 72,000 x 23 = 1,656,000 rows
- [ ] Given the output of the new `calc_statistics()` and the old pandas-only implementation are compared on the same input, when values are compared, then all values match within 1e-6 relative tolerance
- [ ] Given `calc_statistics()` is called with a representative large DataFrame, when execution time is measured, then it completes in less than 20% of the time taken by the pandas-only implementation
- [ ] Given `pytest tests/` is run, when all tests execute, then all tests pass with zero failures
- [ ] Given `calc_statistics()` is called with an empty DataFrame, when it returns, then the result is an empty DataFrame with identical columns

## Implementation Guide

### Suggested Approach

1. In `app/utils/operations.py`, modify `calc_statistics()`:

   ```python
   import polars as pl
   from app.utils.dataframe import pd_to_pl, pl_to_pd

   def calc_statistics(df: pd.DataFrame) -> pd.DataFrame:
       if df.empty:
           return df.drop(columns=[]).head(0)  # empty with same columns

       value_columns = [SCENARIO_COL, VALUE_COL]
       grouping_columns = [c for c in df.columns if c not in value_columns]

       pl_df = pd_to_pl(df)

       # Compute quantiles
       quantile_frames = []
       for q in QUANTILES_FOR_STATISTICS:
           label = quantile_scenario_labels(q)
           q_df = (
               pl_df
               .group_by(grouping_columns, maintain_order=True)
               .agg(pl.col(VALUE_COL).quantile(q, interpolation="linear").alias(VALUE_COL))
               .with_columns(pl.lit(label).alias(SCENARIO_COL))
           )
           quantile_frames.append(q_df)

       # Compute mean and std
       mean_df = (
           pl_df
           .group_by(grouping_columns, maintain_order=True)
           .agg(pl.col(VALUE_COL).mean().alias(VALUE_COL))
           .with_columns(pl.lit("mean").alias(SCENARIO_COL))
       )
       std_df = (
           pl_df
           .group_by(grouping_columns, maintain_order=True)
           .agg(pl.col(VALUE_COL).std().alias(VALUE_COL))
           .with_columns(pl.lit("std").alias(SCENARIO_COL))
       )

       result = pl.concat(quantile_frames + [mean_df, std_df])
       return pl_to_pd(result)
   ```

2. The above approach calls `group_by` 23 times (21 quantiles + mean + std). An alternative is to compute all quantiles in a single aggregation expression:

   ```python
   agg_exprs = [
       pl.col(VALUE_COL).quantile(q, interpolation="linear").alias(f"q_{i}")
       for i, q in enumerate(QUANTILES_FOR_STATISTICS)
   ] + [
       pl.col(VALUE_COL).mean().alias("mean_val"),
       pl.col(VALUE_COL).std().alias("std_val"),
   ]
   agg_df = pl_df.group_by(grouping_columns, maintain_order=True).agg(agg_exprs)
   # Then unpivot to get one row per statistic
   ```

   This single-pass approach is preferred if Polars supports it efficiently. Test both approaches.

3. Ensure the output column order matches the existing implementation. The current output has grouping columns first, then `SCENARIO_COL`, then `VALUE_COL`.

4. The `quantile_scenario_labels()` function (line 43-55) is unchanged -- reuse it.

### Key Files to Modify

- `app/utils/operations.py` (rewrite `calc_statistics`, `_calc_quantiles`, `_calc_mean_std`)

### Patterns to Follow

- Convert to Polars at function entry, compute, convert back to pandas at function exit
- Use `maintain_order=True` in `group_by` to preserve deterministic output ordering

### Pitfalls to Avoid

- Do NOT change the function signature -- callers pass and expect `pd.DataFrame`
- Do NOT change `fast_group_df()` -- it may still be used by other code paths
- Watch for Polars' default quantile interpolation method -- ensure it matches pandas' default ("linear")
- The `cenario` column in the output must be of type compatible with `STRING_DF_TYPE` (pyarrow string) -- verify the string type propagation
- Polars `std()` uses N-1 (sample std) by default, same as pandas -- verify this matches
- The grouping columns may include datetime columns -- Polars handles these natively but verify conversion fidelity

## Testing Requirements

### Unit Tests

- Add a test that compares the output of the new Polars-based `calc_statistics()` to the old pandas-based implementation on a representative DataFrame
- Test with a small DataFrame (10 groups x 100 scenarios) for correctness
- Test with an empty DataFrame
- Test that the output column order matches the expected format

### Integration Tests

- Run `pytest tests/` -- all must pass
- Specifically run `test_operation.py` and `test_scenario.py` which exercise the statistics pipeline

### E2E Tests (if applicable)

- Not required, but a manual benchmark comparison is recommended

## Dependencies

- **Blocked By**: ticket-004-add-polars-dependency.md
- **Blocks**: ticket-007-eliminate-unnecessary-copies.md

## Effort Estimate

**Points**: 3
**Confidence**: Medium (Polars quantile API nuances may require iteration)

## Out of Scope

- Migrating the full synthesis pipeline to Polars (that is Epic 03)
- Changing the `fast_group_df()` function
- Removing the numba dependency
- Changing the quantile list or statistics computed
