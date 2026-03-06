import pandas as pd
import polars as pl


def pd_to_pl(df: pd.DataFrame) -> pl.DataFrame:
    """Convert pandas DataFrame to Polars DataFrame."""
    return pl.from_pandas(df)


def pl_to_pd(df: pl.DataFrame) -> pd.DataFrame:
    """Convert Polars DataFrame to pandas DataFrame."""
    return df.to_pandas()


def pd_to_pl_lazy(df: pd.DataFrame) -> pl.LazyFrame:
    """Convert pandas DataFrame to Polars LazyFrame."""
    return pl.from_pandas(df).lazy()
