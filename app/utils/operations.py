from typing import Callable, Dict, List

import numpy as np  # type: ignore
import pandas as pd  # type: ignore

from app.internal.constants import (
    PANDAS_GROUPING_ENGINE,
    QUANTILES_FOR_STATISTICS,
    SCENARIO_COL,
    VALUE_COL,
)


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


def _calc_quantiles(df: pd.DataFrame, quantiles: List[float]) -> pd.DataFrame:
    """
    Realiza o pós-processamento para calcular uma lista de quantis
    de uma variável operativa dentre todos os estágios e patamares,
    agrupando de acordo com as demais colunas.
    """
    value_columns = [SCENARIO_COL, VALUE_COL]
    grouping_columns = [c for c in df.columns if c not in value_columns]
    quantile_df = (
        df.groupby(grouping_columns, sort=False)[[SCENARIO_COL, VALUE_COL]]
        .quantile(quantiles)
        .reset_index()
    )

    level_column = [c for c in quantile_df.columns if "level_" in c]
    if len(level_column) != 1:
        raise RuntimeError()

    quantile_df = quantile_df.drop(columns=[SCENARIO_COL]).rename(
        columns={level_column[0]: SCENARIO_COL}
    )
    num_entities = quantile_df.shape[0] // len(quantiles)
    quantile_labels = np.tile(
        np.array([quantile_scenario_labels(q) for q in quantiles]),
        num_entities,
    )
    quantile_df[SCENARIO_COL] = quantile_labels
    return quantile_df


def _calc_mean_std(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza o pós-processamento para calcular o valor médio e o desvio
    padrão de uma variável operativa dentre todos os estágios e patamares,
    agrupando de acordo com as demais colunas.
    """
    value_columns = [SCENARIO_COL, VALUE_COL]
    grouping_columns = [c for c in df.columns if c not in value_columns]
    extract_columns = [VALUE_COL]
    df_mean = fast_group_df(
        df, grouping_columns, extract_columns, "mean", reset_index=True
    )
    df_mean[SCENARIO_COL] = "mean"

    df_std = fast_group_df(
        df, grouping_columns, extract_columns, "std", reset_index=True
    )
    df_std[SCENARIO_COL] = "std"

    return pd.concat([df_mean, df_std], ignore_index=True)


def calc_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza o pós-processamento de um DataFrame com dados da
    síntese da operação de uma determinada variável, calculando
    estatísticas como quantis e média para cada variável, em cada
    estágio e patamar.
    """
    df_q = _calc_quantiles(df, QUANTILES_FOR_STATISTICS)
    df_m = _calc_mean_std(df)
    df_stats = pd.concat([df_q, df_m], ignore_index=True)
    return df_stats
