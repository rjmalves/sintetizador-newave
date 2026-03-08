import logging
from typing import Callable, Dict

import pandas as pd
import polars as pl

from app.internal.constants import (
    PANDAS_GROUPING_ENGINE,
    QUANTILES_FOR_STATISTICS,
    SCENARIO_COL,
    VALUE_COL,
)

logger = logging.getLogger(__name__)


def fast_group_df(
    df: pd.DataFrame,
    grouping_columns: list,
    extract_columns: list,
    operation: str,
    reset_index: bool = True,
) -> pd.DataFrame:
    grouped_df = df.groupby(grouping_columns, sort=False)[extract_columns]

    operation_map: Dict[str, Callable[..., pd.DataFrame]] = {
        "mean": grouped_df.mean,
        "std": grouped_df.std,
        "sum": grouped_df.sum,
    }

    try:
        grouped_df = operation_map[operation](engine=PANDAS_GROUPING_ENGINE)
    except ZeroDivisionError:
        grouped_df = operation_map[operation](engine="cython")

    if reset_index:
        grouped_df = grouped_df.reset_index()
    return grouped_df


def quantile_scenario_labels(q: float) -> str:
    if q == 0:
        label = "min"
    elif q == 1:
        label = "max"
    elif q == 0.5:
        label = "median"
    else:
        label = f"p{int(100 * q)}"
    return label


def _calc_statistics_polars(df: pl.DataFrame) -> pl.DataFrame:
    value_columns = [SCENARIO_COL, VALUE_COL]
    grouping_columns = [c for c in df.columns if c not in value_columns]

    stat_col_names = [
        f"__q_{i}" for i in range(len(QUANTILES_FOR_STATISTICS))
    ] + [
        "__mean",
        "__std",
    ]

    label_map: dict[str, str] = {
        f"__q_{i}": quantile_scenario_labels(q)
        for i, q in enumerate(QUANTILES_FOR_STATISTICS)
    }
    label_map["__mean"] = "mean"
    label_map["__std"] = "std"

    agg_exprs = [
        pl.col(VALUE_COL).quantile(q, interpolation="linear").alias(f"__q_{i}")
        for i, q in enumerate(QUANTILES_FOR_STATISTICS)
    ] + [
        pl.col(VALUE_COL).mean().alias("__mean"),
        pl.col(VALUE_COL).std().alias("__std"),
    ]

    agg_df = df.group_by(grouping_columns, maintain_order=True).agg(agg_exprs)

    unpivoted = agg_df.unpivot(
        on=stat_col_names,
        index=grouping_columns,
        variable_name="__stat_col",
        value_name=VALUE_COL,
    )

    result = (
        unpivoted.with_columns(
            pl.col("__stat_col").replace(label_map).alias(SCENARIO_COL)
        )
        .drop("__stat_col")
        .select(grouping_columns + [SCENARIO_COL, VALUE_COL])
    )

    return result


def calc_statistics(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df.head(0)

    return _calc_statistics_polars(df)
