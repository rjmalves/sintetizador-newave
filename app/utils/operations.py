import pandas as pd  # type: ignore
from app.internal.constants import PANDAS_GROUPING_ENGINE
from typing import Dict, Callable


def fast_group_df(
    df: pd.DataFrame,
    grouping_columns: list,
    extract_columns: list,
    operation: str,
    reset_index: bool = True,
) -> pd.DataFrame:
    """
    Agrupa um DataFrame aplicando uma operação, tentando utilizar a engine mais
    adequada para o agrupamento.
    """
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
