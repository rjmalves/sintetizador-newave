from typing import TYPE_CHECKING, List

import pandas as pd

from app.internal.constants import (
    OPERATION_SYNTHESIS_METADATA_OUTPUT,
    OPERATION_SYNTHESIS_STATS_ROOT,
    SCENARIO_COL,
    STRING_DF_TYPE,
    VARIABLE_COL,
)
from app.model.operation.operationsynthesis import (
    SYNTHESIS_DEPENDENCIES,
    UNITS,
    OperationSynthesis,
)
from app.services.deck.bounds import OperationVariableBounds
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.dataframe import pd_to_pl, pl_to_pd
from app.utils.operations import calc_statistics
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def export_metadata(
    cls: "type[OperationSynthetizer]",
    success_synthesis: List[OperationSynthesis],
    uow: AbstractUnitOfWork,
) -> None:
    """
    Cria um DataFrame com os metadados das variáveis de síntese
    e realiza a exportação para um arquivo de metadados.
    """
    metadata_df = pd.DataFrame(
        columns=[
            "chave",
            "nome_curto_variavel",
            "nome_longo_variavel",
            "nome_curto_agregacao",
            "nome_longo_agregacao",
            "unidade",
            "calculado",
            "limitado",
        ]
    )
    for s in success_synthesis:
        metadata_df.loc[metadata_df.shape[0]] = [
            str(s),
            s.variable.short_name,
            s.variable.long_name,
            s.spatial_resolution.value,
            s.spatial_resolution.long_name,
            UNITS[s].value if s in UNITS else "",
            s in SYNTHESIS_DEPENDENCIES,
            OperationVariableBounds.is_bounded(s),
        ]
    with uow:
        existing_df = uow.export.read_df(OPERATION_SYNTHESIS_METADATA_OUTPUT)
        if existing_df is not None:
            metadata_df = pd.concat(
                [existing_df, metadata_df], ignore_index=True
            )
            metadata_df = metadata_df.drop_duplicates()
        uow.export.synthetize_df(
            metadata_df, OPERATION_SYNTHESIS_METADATA_OUTPUT
        )


def add_synthesis_stats(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    df: pd.DataFrame,
) -> None:
    """
    Adiciona um DataFrame com estatísticas de uma síntese ao
    DataFrame de estatísticas da agregação espacial em questão.
    """
    df[VARIABLE_COL] = s.variable.value

    if s.spatial_resolution not in cls.SYNTHESIS_STATS:
        cls.SYNTHESIS_STATS[s.spatial_resolution] = [df]
    else:
        cls.SYNTHESIS_STATS[s.spatial_resolution].append(df)


def export_scenario_synthesis(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    df: pd.DataFrame,
    uow: AbstractUnitOfWork,
) -> None:
    """
    Realiza a exportação dos dados para uma síntese da
    operação desejada. Opcionalmente, os dados são armazenados
    em cache para uso futuro e as estatísticas são adicionadas
    ao DataFrame de estatísticas da agregação espacial em questão.
    """
    from app.services.synthesis.operation.cache import store_in_cache_if_needed

    with time_and_log(
        message_root="Tempo para preparacao para exportacao",
        logger=cls.logger,
    ):
        scenarios_pl = pd_to_pl(df.astype({SCENARIO_COL: int})).sort(
            s.spatial_resolution.sorting_synthesis_df_columns,
            maintain_order=True,
        )
        scenarios_df = pl_to_pd(scenarios_pl).reset_index(drop=True)
        stats_df = calc_statistics(scenarios_df)
        add_synthesis_stats(cls, s, stats_df)
        store_in_cache_if_needed(cls, s, scenarios_df)
    with time_and_log(
        message_root="Tempo para exportacao dos dados", logger=cls.logger
    ):
        with uow:
            uow.export.synthetize_pl(
                scenarios_pl.select(
                    s.spatial_resolution.all_synthesis_df_columns
                ),
                str(s),
            )


def export_stats(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
) -> None:
    """
    Realiza a exportação dos dados de estatísticas de síntese
    da operação. As estatísticas são exportadas para um arquivo
    único por agregação espacial, de nome
    `OPERACAO_{agregacao}`.
    """
    for res, dfs in cls.SYNTHESIS_STATS.items():
        with time_and_log(
            message_root="Tempo para exportacao"
            + f" das estatisticas de {res.value}",
            logger=cls.logger,
        ):
            with uow:
                df = pd.concat(dfs, ignore_index=True)
                df = df[[VARIABLE_COL] + res.all_synthesis_df_columns]
                df = df.astype({VARIABLE_COL: STRING_DF_TYPE})
                df = df.sort_values(
                    [VARIABLE_COL] + res.sorting_synthesis_df_columns
                ).reset_index(drop=True)
                stats_filename = f"{OPERATION_SYNTHESIS_STATS_ROOT}_{res.value}"
                existing_df = uow.export.read_df(stats_filename)
                if existing_df is not None:
                    df = pd.concat([existing_df, df], ignore_index=True)
                    df = df.drop_duplicates()
                df_pl = pd_to_pl(df)
                uow.export.synthetize_pl(df_pl, stats_filename)
