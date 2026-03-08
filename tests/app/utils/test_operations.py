"""Unit tests for app.utils.operations.calc_statistics (Polars native)."""

import numpy as np
import polars as pl

from app.internal.constants import (
    QUANTILES_FOR_STATISTICS,
    SCENARIO_COL,
    VALUE_COL,
)
from app.utils.operations import (
    calc_statistics,
    quantile_scenario_labels,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EXPECTED_STAT_LABELS = [
    quantile_scenario_labels(q) for q in QUANTILES_FOR_STATISTICS
] + ["mean", "std"]
NUM_STATISTICS = len(EXPECTED_STAT_LABELS)  # 23


def _make_df(
    num_groups: int,
    num_scenarios: int,
    *,
    seed: int = 42,
    extra_grouping_cols: bool = False,
) -> pl.DataFrame:
    """
    Build a representative input DataFrame for calc_statistics.

    Parameters
    ----------
    num_groups:
        Number of unique (codigo_usina, estagio) combinations.
    num_scenarios:
        Number of scenarios per group.
    seed:
        Random seed for reproducibility.
    extra_grouping_cols:
        When True, add an extra categorical grouping column to stress-test
        column ordering in the output.
    """
    rng = np.random.default_rng(seed)
    total_rows = num_groups * num_scenarios

    usina_ids = np.repeat(np.arange(1, num_groups + 1), num_scenarios)
    estagio = np.ones(total_rows, dtype=np.int64)
    cenario = np.tile(np.arange(1, num_scenarios + 1), num_groups)
    valor = rng.random(total_rows)

    data: dict = {
        "codigo_usina": usina_ids.tolist(),
        "estagio": estagio.tolist(),
        SCENARIO_COL: cenario.tolist(),
        VALUE_COL: valor.tolist(),
    }
    if extra_grouping_cols:
        data["patamar"] = [1] * total_rows

    return pl.DataFrame(data)


# ---------------------------------------------------------------------------
# Tests: output shape
# ---------------------------------------------------------------------------


def test_output_row_count_small():
    """10 groups x 100 scenarios → 10 x 23 = 230 rows."""
    df = _make_df(num_groups=10, num_scenarios=100)
    result = calc_statistics(df)
    assert result.shape[0] == 10 * NUM_STATISTICS


def test_output_row_count_matches_groups_times_statistics():
    """Generalised check: any (G, S) → G * 23 rows."""
    df = _make_df(num_groups=5, num_scenarios=50)
    result = calc_statistics(df)
    assert result.shape[0] == 5 * NUM_STATISTICS


# ---------------------------------------------------------------------------
# Tests: column order
# ---------------------------------------------------------------------------


def test_output_column_order():
    """Grouping columns must come before SCENARIO_COL and VALUE_COL."""
    df = _make_df(num_groups=3, num_scenarios=20)
    result = calc_statistics(df)
    grouping_cols = [
        c for c in df.columns if c not in (SCENARIO_COL, VALUE_COL)
    ]
    expected_cols = grouping_cols + [SCENARIO_COL, VALUE_COL]
    assert list(result.columns) == expected_cols


def test_output_column_order_with_extra_grouping_col():
    """Column order is correct even when the input has more grouping columns."""
    df = _make_df(num_groups=3, num_scenarios=20, extra_grouping_cols=True)
    result = calc_statistics(df)
    grouping_cols = [
        c for c in df.columns if c not in (SCENARIO_COL, VALUE_COL)
    ]
    expected_cols = grouping_cols + [SCENARIO_COL, VALUE_COL]
    assert list(result.columns) == expected_cols


# ---------------------------------------------------------------------------
# Tests: cenario labels
# ---------------------------------------------------------------------------


def test_output_cenario_labels():
    """The cenario column must contain exactly the 23 expected string labels."""
    df = _make_df(num_groups=2, num_scenarios=50)
    result = calc_statistics(df)
    observed_labels = set(result[SCENARIO_COL].unique().to_list())
    assert observed_labels == set(EXPECTED_STAT_LABELS)


# ---------------------------------------------------------------------------
# Tests: numerical correctness vs. reference pandas implementation
# ---------------------------------------------------------------------------


def test_numerical_correctness_of_statistics():
    """
    Computed statistic values must be numerically correct.

    Verifies mean and quantile values against direct numpy/pandas computation
    on the raw data.
    """
    df = _make_df(num_groups=10, num_scenarios=100)
    result = calc_statistics(df)

    grouping_cols = [
        c for c in df.columns if c not in (SCENARIO_COL, VALUE_COL)
    ]

    # Convert to pandas for reference comparison
    df_pd = df.to_pandas()

    # Verify 'mean' label: compare against pandas groupby mean
    mean_result = (
        result.filter(pl.col(SCENARIO_COL) == "mean")
        .sort(grouping_cols)[VALUE_COL]
        .to_numpy()
    )
    ref_mean = (
        df_pd.groupby(grouping_cols, sort=False)[VALUE_COL]
        .mean()
        .reset_index()
        .sort_values(grouping_cols)
        .reset_index(drop=True)[VALUE_COL]
        .values
    )
    assert len(mean_result) == len(ref_mean)
    np.testing.assert_allclose(
        mean_result,
        ref_mean,
        rtol=1e-6,
        err_msg="Numerical mismatch for statistic 'mean'",
    )

    # Verify median (q=0.5): compare against pandas quantile
    median_result = (
        result.filter(pl.col(SCENARIO_COL) == "median")
        .sort(grouping_cols)[VALUE_COL]
        .to_numpy()
    )
    ref_median = (
        df_pd.groupby(grouping_cols, sort=False)[VALUE_COL]
        .quantile(0.5)
        .reset_index()
        .sort_values(grouping_cols)
        .reset_index(drop=True)[VALUE_COL]
        .values
    )
    assert len(median_result) == len(ref_median)
    np.testing.assert_allclose(
        median_result,
        ref_median,
        rtol=1e-4,
        err_msg="Numerical mismatch for statistic 'median'",
    )


# ---------------------------------------------------------------------------
# Tests: empty DataFrame
# ---------------------------------------------------------------------------


def test_empty_dataframe_returns_empty_with_same_columns():
    """An empty input must produce an empty output with identical columns."""
    df = _make_df(num_groups=3, num_scenarios=20)
    empty_df = df.head(0)
    result = calc_statistics(empty_df)
    assert result.is_empty()
    assert list(result.columns) == list(df.columns)


def test_empty_dataframe_zero_rows():
    """An empty input returns a DataFrame with exactly 0 rows."""
    df = _make_df(num_groups=3, num_scenarios=20).head(0)
    result = calc_statistics(df)
    assert len(result) == 0
