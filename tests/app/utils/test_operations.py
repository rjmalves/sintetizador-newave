"""Unit tests for app.utils.operations.calc_statistics (Polars rewrite)."""

import numpy as np
import pandas as pd

from app.internal.constants import (
    QUANTILES_FOR_STATISTICS,
    SCENARIO_COL,
    VALUE_COL,
)
from app.utils.operations import (
    _calc_mean_std,
    _calc_quantiles,
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
) -> pd.DataFrame:
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
        "codigo_usina": usina_ids,
        "estagio": estagio,
        SCENARIO_COL: cenario,
        VALUE_COL: valor,
    }
    if extra_grouping_cols:
        data["patamar"] = np.ones(total_rows, dtype=np.int64)

    return pd.DataFrame(data)


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
    observed_labels = set(result[SCENARIO_COL].unique())
    assert observed_labels == set(EXPECTED_STAT_LABELS)


# ---------------------------------------------------------------------------
# Tests: numerical correctness vs. reference pandas implementation
# ---------------------------------------------------------------------------


def test_numerical_equivalence_to_pandas_reference():
    """
    All computed statistic values must match the legacy pandas implementation
    within a relative tolerance of 1e-6.
    """
    df = _make_df(num_groups=10, num_scenarios=100)

    # Reference: legacy pandas implementation
    df_q = _calc_quantiles(df, QUANTILES_FOR_STATISTICS)
    df_m = _calc_mean_std(df)
    reference = pd.concat([df_q, df_m], ignore_index=True)

    result = calc_statistics(df)

    grouping_cols = [
        c for c in df.columns if c not in (SCENARIO_COL, VALUE_COL)
    ]

    for label in EXPECTED_STAT_LABELS:
        ref_vals = (
            reference[reference[SCENARIO_COL] == label]
            .sort_values(grouping_cols)
            .reset_index(drop=True)[VALUE_COL]
            .values
        )
        new_vals = (
            result[result[SCENARIO_COL] == label]
            .sort_values(grouping_cols)
            .reset_index(drop=True)[VALUE_COL]
            .values
        )
        assert len(ref_vals) == len(new_vals), (
            f"Row count mismatch for label '{label}': "
            f"reference={len(ref_vals)}, result={len(new_vals)}"
        )
        np.testing.assert_allclose(
            new_vals,
            ref_vals,
            rtol=1e-6,
            err_msg=f"Numerical mismatch for statistic '{label}'",
        )


# ---------------------------------------------------------------------------
# Tests: empty DataFrame
# ---------------------------------------------------------------------------


def test_empty_dataframe_returns_empty_with_same_columns():
    """An empty input must produce an empty output with identical columns."""
    df = _make_df(num_groups=3, num_scenarios=20)
    empty_df = df.head(0)
    result = calc_statistics(empty_df)
    assert result.empty
    assert list(result.columns) == list(df.columns)


def test_empty_dataframe_zero_rows():
    """An empty input returns a DataFrame with exactly 0 rows."""
    df = _make_df(num_groups=3, num_scenarios=20).head(0)
    result = calc_statistics(df)
    assert len(result) == 0
