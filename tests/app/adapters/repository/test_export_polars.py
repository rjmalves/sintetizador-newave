"""Unit tests for ParquetExportRepository.synthetize_pl."""

import pathlib
from datetime import datetime

import pandas as pd
import polars as pl
import pytest

from app.adapters.repository.export import (
    AbstractExportRepository,
    CSVExportRepository,
    ParquetExportRepository,
)
from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    END_DATE_COL,
    SCENARIO_COL,
    STAGE_COL,
    START_DATE_COL,
    VALUE_COL,
)


@pytest.fixture()
def tmp_repo(tmp_path: pathlib.Path) -> ParquetExportRepository:
    return ParquetExportRepository(str(tmp_path))


@pytest.fixture()
def sample_pl_df() -> pl.DataFrame:
    """Polars DataFrame with a timezone-naive datetime column and a float column."""
    return pl.DataFrame(
        {
            STAGE_COL: [1, 2],
            START_DATE_COL: [datetime(2023, 1, 1), datetime(2023, 2, 1)],
            END_DATE_COL: [datetime(2023, 1, 31), datetime(2023, 2, 28)],
            SCENARIO_COL: [1, 1],
            BLOCK_COL: [1, 1],
            BLOCK_DURATION_COL: [730.0, 672.0],
            VALUE_COL: [100.5, 200.75],
        },
        schema={
            STAGE_COL: pl.Int64,
            START_DATE_COL: pl.Datetime("us"),
            END_DATE_COL: pl.Datetime("us"),
            SCENARIO_COL: pl.Int64,
            BLOCK_COL: pl.Int64,
            BLOCK_DURATION_COL: pl.Float64,
            VALUE_COL: pl.Float64,
        },
    )


def test_synthetize_pl_creates_file(
    tmp_repo: ParquetExportRepository,
    sample_pl_df: pl.DataFrame,
) -> None:
    """synthetize_pl must write a .parquet file at the expected path."""
    result = tmp_repo.synthetize_pl(sample_pl_df, "test_output")
    assert result is True
    expected_path = tmp_repo.path / "test_output.parquet"
    assert expected_path.exists(), f"Expected file not found: {expected_path}"


def test_synthetize_pl_datetime_is_utc_on_readback(
    tmp_repo: ParquetExportRepository,
    sample_pl_df: pl.DataFrame,
) -> None:
    """Reading back the file with pd.read_parquet must yield UTC datetime columns."""
    tmp_repo.synthetize_pl(sample_pl_df, "test_utc")
    parquet_path = tmp_repo.path / "test_utc.parquet"
    df_back = pd.read_parquet(parquet_path)
    for col in [START_DATE_COL, END_DATE_COL]:
        dtype = df_back[col].dtype
        assert hasattr(dtype, "tz"), f"Column {col} has no timezone: {dtype}"
        assert str(dtype.tz) == "UTC", (
            f"Column {col} timezone is not UTC: {dtype.tz}"
        )


def test_synthetize_pl_values_match(
    tmp_repo: ParquetExportRepository,
    sample_pl_df: pl.DataFrame,
) -> None:
    """Values in the Polars-written Parquet must equal those in the original DataFrame."""
    tmp_repo.synthetize_pl(sample_pl_df, "test_values")
    parquet_path = tmp_repo.path / "test_values.parquet"
    df_back = pd.read_parquet(parquet_path)
    assert list(df_back.columns) == list(sample_pl_df.columns)
    assert df_back[VALUE_COL].tolist() == sample_pl_df[VALUE_COL].to_list()
    assert df_back[STAGE_COL].tolist() == sample_pl_df[STAGE_COL].to_list()


def test_synthetize_pl_schema_matches_synthetize_df(
    tmp_repo: ParquetExportRepository,
    sample_pl_df: pl.DataFrame,
) -> None:
    """Parquet file written by synthetize_pl must have same dtypes as synthetize_df."""
    # Write via the new Polars path
    tmp_repo.synthetize_pl(sample_pl_df, "via_polars")
    # Write via the existing pandas path from the same data
    tmp_repo.synthetize_df(sample_pl_df.to_pandas(), "via_pandas")

    df_pl_path = pd.read_parquet(tmp_repo.path / "via_polars.parquet")
    df_pd_path = pd.read_parquet(tmp_repo.path / "via_pandas.parquet")

    assert list(df_pl_path.columns) == list(df_pd_path.columns), (
        "Column names differ between Polars and pandas Parquet paths"
    )
    for col in df_pl_path.columns:
        assert df_pl_path[col].dtype == df_pd_path[col].dtype, (
            f"Column '{col}' dtype mismatch: "
            f"polars_path={df_pl_path[col].dtype}, pandas_path={df_pd_path[col].dtype}"
        )


def test_abstract_repo_synthetize_pl_falls_back_to_pandas(
    tmp_path: pathlib.Path,
) -> None:
    """AbstractExportRepository.synthetize_pl default must call synthetize_df."""
    called_with: list[tuple] = []

    class _ConcreteRepo(AbstractExportRepository):
        def read_df(self, filename: str) -> pd.DataFrame | None:
            return None

        def synthetize_df(self, df: pd.DataFrame, filename: str) -> bool:
            called_with.append((type(df).__name__, filename))
            return True

    repo = _ConcreteRepo()
    sample = pl.DataFrame({VALUE_COL: [1.0, 2.0]})
    result = repo.synthetize_pl(sample, "fallback_test")
    assert result is True
    assert len(called_with) == 1
    pandas_type, fname = called_with[0]
    assert pandas_type == "DataFrame", (
        "Expected pandas DataFrame to be passed to synthetize_df"
    )
    assert fname == "fallback_test"


def test_csv_repo_synthetize_pl_uses_default_fallback(
    tmp_path: pathlib.Path,
) -> None:
    """CSVExportRepository inherits the default synthetize_pl and writes a CSV."""
    repo = CSVExportRepository(str(tmp_path))
    sample = pl.DataFrame({VALUE_COL: [42.0]})
    # The default implementation converts to pandas then calls synthetize_df,
    # which writes a CSV file.
    result = repo.synthetize_pl(sample, "csv_fallback")
    assert result is True
    expected_csv = tmp_path / "csv_fallback.csv"
    assert expected_csv.exists(), f"Expected CSV file not found: {expected_csv}"
