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
from app.utils.dataframe import pd_to_pl, pl_to_pd

logger = logging.getLogger(__name__)


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


def quantile_scenario_labels(q: float) -> str:
    """
    Obtem um rótulo para um cenário baseado no quantil.
    """
    if q == 0:
        label = "min"
    elif q == 1:
        label = "max"
    elif q == 0.5:
        label = "median"
    else:
        label = f"p{int(100 * q)}"
    return label


def _calc_statistics_polars(df: pd.DataFrame) -> pd.DataFrame:
    """
    Implementação interna de calc_statistics usando Polars para
    melhor desempenho. Realiza um único group_by com todas as
    21 agregações de quantil mais média e desvio padrão, depois
    realiza unpivot para obter uma linha por (grupo x estatística).
    """
    value_columns = [SCENARIO_COL, VALUE_COL]
    grouping_columns = [c for c in df.columns if c not in value_columns]

    pl_df = pd_to_pl(df)

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

    agg_df = pl_df.group_by(grouping_columns, maintain_order=True).agg(
        agg_exprs
    )

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

    return pl_to_pd(result)


def calc_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza o pós-processamento de um DataFrame com dados da
    síntese da operação de uma determinada variável, calculando
    estatísticas como quantis e média para cada variável, em cada
    estágio e patamar.

    Utiliza Polars internamente para melhor desempenho (group_by
    single-pass multi-threaded). Mantém a interface pandas no
    limite da função: recebe pd.DataFrame e retorna pd.DataFrame.
    """
    if df.empty:
        return df.head(0)

    return _calc_statistics_polars(df)
