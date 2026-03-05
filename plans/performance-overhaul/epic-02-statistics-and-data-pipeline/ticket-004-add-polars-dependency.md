# ticket-004 Add Polars Dependency and Conversion Utilities

## Context

### Background

The sintetizador-newave pipeline currently uses pandas exclusively for all DataFrame operations. pandas' groupby and quantile operations are single-threaded and are the dominant cost in statistics computation. Polars provides multi-threaded, vectorized implementations of these operations and lazy evaluation to minimize memory allocations. Before migrating any hot-path code, we need to add the Polars dependency and establish the conversion boundary pattern.

The inewave library returns `pd.DataFrame` from its file readers. The export layer writes Parquet via `pyarrow.Table.from_pandas()`. These boundaries will remain pandas-based. The conversion utility module establishes the pattern for converting at pipeline entry and exit.

### Relation to Epic

This is the first ticket in Epic 02 and a prerequisite for all subsequent Polars migration tickets (005, 006, 007).

### Current State

- `pyproject.toml` lists dependencies: `pandas`, `pyarrow>=18.0.0`, `click>=8.1.7`, `numba>=0.60.0`, `inewave>=1.9.2`
- No Polars dependency exists
- All DataFrame operations throughout the codebase use pandas
- `app/internal/constants.py` defines `PANDAS_GROUPING_ENGINE` and `STRING_DF_TYPE = pandas.StringDtype(storage="pyarrow")`

## Specification

### Requirements

1. Add `polars>=1.0.0` to the `[project] dependencies` list in `pyproject.toml`
2. Create a utility module `app/utils/dataframe.py` with functions:
   - `pd_to_pl(df: pd.DataFrame) -> pl.DataFrame` -- converts a pandas DataFrame to a Polars DataFrame
   - `pl_to_pd(df: pl.DataFrame) -> pd.DataFrame` -- converts a Polars DataFrame to a pandas DataFrame
   - `pd_to_pl_lazy(df: pd.DataFrame) -> pl.LazyFrame` -- converts a pandas DataFrame to a Polars LazyFrame for deferred execution
3. The conversion functions must handle the column types used in the codebase: `int64`, `float64`, `datetime64[ns]`, `object` (strings), and `pandas.StringDtype(storage="pyarrow")`
4. All existing tests must pass (Polars is added but not yet used in any production path)

### Inputs/Props

- `pd_to_pl()` receives a standard pandas DataFrame as returned by inewave readers or by the synthesis pipeline
- `pl_to_pd()` receives a Polars DataFrame with equivalent column types

### Outputs/Behavior

- Round-trip conversion (`pd_to_pl` followed by `pl_to_pd`) produces a DataFrame with identical column names, dtypes (mapped to equivalent pandas dtypes), and values
- The `pd.StringDtype(storage="pyarrow")` type in pandas maps to `pl.Utf8` in Polars
- `datetime64[ns]` in pandas maps to `pl.Datetime` in Polars
- `int64` columns remain `pl.Int64`; `float64` columns remain `pl.Float64`

### Error Handling

- If a pandas DataFrame contains a column type not supported by Polars (e.g., `pd.Categorical`), the conversion should let Polars' built-in `from_pandas()` handle it, which raises a clear error

## Acceptance Criteria

- [ ] Given `pyproject.toml` is read, when the `[project] dependencies` list is examined, then `polars>=1.0.0` appears in the list
- [ ] Given `python -c "import polars; print(polars.__version__)"` is run in the project environment, when the command completes, then a version >= 1.0.0 is printed
- [ ] Given a pandas DataFrame with columns `[int64, float64, datetime64[ns], object]` is passed to `pd_to_pl()`, when the result is examined, then it is a `pl.DataFrame` with columns `[Int64, Float64, Datetime, Utf8]`
- [ ] Given a Polars DataFrame is passed to `pl_to_pd()`, when the result is compared to the original pandas DataFrame (pre-conversion), then values are numerically identical within floating-point tolerance (1e-10)
- [ ] Given `pytest tests/` is run, when all tests execute, then all tests pass with zero failures

## Implementation Guide

### Suggested Approach

1. Edit `pyproject.toml` to add `polars>=1.0.0` to the dependencies list:

   ```toml
   dependencies = [
       "pandas",
       "polars>=1.0.0",
       "pyarrow>=18.0.0",
       "click>=8.1.7",
       "numba>=0.60.0",
       "inewave>=1.9.2",
   ]
   ```

2. Create `app/utils/dataframe.py`:

   ```python
   import pandas as pd
   import polars as pl


   def pd_to_pl(df: pd.DataFrame) -> pl.DataFrame:
       return pl.from_pandas(df)


   def pl_to_pd(df: pl.DataFrame) -> pd.DataFrame:
       return df.to_pandas()


   def pd_to_pl_lazy(df: pd.DataFrame) -> pl.LazyFrame:
       return pl.from_pandas(df).lazy()
   ```

3. The implementation is intentionally minimal -- Polars' `from_pandas()` and `to_pandas()` handle all standard type conversions. The utility module exists to establish the pattern and provide a single import location.

4. Run `pip install -e .` or equivalent to install the new dependency, then run `pytest tests/`.

### Key Files to Modify

- `pyproject.toml` (add polars dependency)
- `app/utils/dataframe.py` (new file)

### Patterns to Follow

- Follow the existing utility module pattern in `app/utils/` (e.g., `operations.py`, `timing.py`)
- Keep functions stateless and side-effect-free

### Pitfalls to Avoid

- Do NOT add Polars imports to any existing production code in this ticket -- only create the utility module
- Do NOT remove pandas or numba -- they are still used everywhere
- Do NOT pin Polars to a specific minor version unless a known incompatibility exists -- use `>=1.0.0` for flexibility
- Do NOT add `polars` to the `[project.optional-dependencies] dev` section -- it is a runtime dependency

## Testing Requirements

### Unit Tests

- Add tests in `tests/app/utils/test_dataframe.py` that verify:
  - `pd_to_pl` produces correct Polars types for all column types used in the project
  - `pl_to_pd` round-trips values correctly
  - `pd_to_pl_lazy` returns a `pl.LazyFrame`
  - Empty DataFrames convert without error

### Integration Tests

- Run `pytest tests/` -- all existing tests must pass

### E2E Tests (if applicable)

- Not required for this ticket

## Dependencies

- **Blocked By**: ticket-003-move-statistics-after-concatenation.md (Epic 01 statistics relocation must be complete before rewriting stats with Polars)
- **Blocks**: ticket-005-rewrite-calc-statistics-polars.md, ticket-006-optimize-concat-with-polars.md

## Effort Estimate

**Points**: 2
**Confidence**: High

## Out of Scope

- Using Polars in any production code path (only the utility module is created)
- Removing pandas or numba dependencies
- Changing the inewave reader output format
- Changing the Parquet export format
