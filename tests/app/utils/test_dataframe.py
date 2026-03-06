import numpy as np
import pandas as pd
import polars as pl
import pytest

from app.utils.dataframe import pd_to_pl, pd_to_pl_lazy, pl_to_pd


@pytest.fixture
def sample_pd_df():
    return pd.DataFrame(
        {
            "int_col": pd.array([1, 2, 3], dtype="int64"),
            "float_col": pd.array([1.1, 2.2, 3.3], dtype="float64"),
            "datetime_col": pd.to_datetime(
                ["2023-01-01", "2023-02-01", "2023-03-01"]
            ),
            "str_col": ["a", "b", "c"],
        }
    )


def test_pd_to_pl_types(sample_pd_df):
    result = pd_to_pl(sample_pd_df)
    assert isinstance(result, pl.DataFrame)
    assert result.schema["int_col"] == pl.Int64
    assert result.schema["float_col"] == pl.Float64
    assert result.schema["datetime_col"] == pl.Datetime
    assert result.schema["str_col"] == pl.String


def test_pl_to_pd_round_trip(sample_pd_df):
    pl_df = pd_to_pl(sample_pd_df)
    result = pl_to_pd(pl_df)
    assert list(result.columns) == list(sample_pd_df.columns)
    np.testing.assert_allclose(
        result["float_col"].values,
        sample_pd_df["float_col"].values,
        atol=1e-10,
    )
    np.testing.assert_array_equal(
        result["int_col"].values, sample_pd_df["int_col"].values
    )


def test_pd_to_pl_lazy(sample_pd_df):
    result = pd_to_pl_lazy(sample_pd_df)
    assert isinstance(result, pl.LazyFrame)
    collected = result.collect()
    assert collected.shape == sample_pd_df.shape


def test_empty_dataframe():
    empty_df = pd.DataFrame()
    result = pd_to_pl(empty_df)
    assert isinstance(result, pl.DataFrame)
    assert result.shape == (0, 0)
    back = pl_to_pd(result)
    assert isinstance(back, pd.DataFrame)
    assert back.shape == (0, 0)
