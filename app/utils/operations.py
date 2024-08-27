from typing import Callable, Dict, List

import polars as pl
from polars.dataframe.group_by import GroupBy

from app.internal.constants import (
    QUANTILES_FOR_STATISTICS,
    SCENARIO_COL,
    VALUE_COL,
)


def fast_group_df(
    df: pl.DataFrame,
    grouping_columns: list,
    extract_columns: list,
    operation: str,
) -> pl.DataFrame:
    """
    Agrupa um DataFrame aplicando uma operação, tentando utilizar a engine mais
    adequada para o agrupamento.
    """
    grouped_df: GroupBy = df.group_by(grouping_columns, maintain_order=True)[
        extract_columns
    ]

    operation_map: Dict[str, Callable[..., pl.DataFrame]] = {
        "mean": grouped_df.mean,
        "sum": grouped_df.sum,
    }

    grouped_df = operation_map[operation]()
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


def _calc_quantiles_mean(
    df: pl.DataFrame, quantiles: List[float]
) -> pl.DataFrame:
    """
    Realiza o pós-processamento para calcular uma lista de quantis
    de uma variável operativa dentre todos os estágios e patamares,
    agrupando de acordo com as demais colunas.
    """
    value_columns = [SCENARIO_COL, VALUE_COL]
    grouping_columns = [c for c in df.columns if c not in value_columns]
    dfs: list[pl.DataFrame] = []
    group_df = df.group_by(grouping_columns, maintain_order=True)
    for q in quantiles:
        dfq = group_df.quantile(q)
        dfq = dfq.with_columns(
            pl.lit(quantile_scenario_labels(q)).alias(SCENARIO_COL)
        )
        dfs.append(dfq)

    df_mean = group_df.mean()
    df_mean = df_mean.with_columns(pl.lit("mean").alias(SCENARIO_COL))

    df = pl.concat(dfs + [df_mean], parallel=False)
    return df


def calc_statistics(df: pl.DataFrame) -> pl.DataFrame:
    """
    Realiza o pós-processamento de um DataFrame com dados da
    síntese da operação de uma determinada variável, calculando
    estatísticas como quantis e média para cada variável, em cada
    estágio e patamar.
    """

    return _calc_quantiles_mean(df, QUANTILES_FOR_STATISTICS)
