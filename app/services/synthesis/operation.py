import logging
from datetime import datetime
from logging import DEBUG, ERROR, INFO, WARNING
from multiprocessing import Pool
from traceback import print_exc
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

import numpy as np
import pandas as pd  # type: ignore
import polars as pl

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    EER_CODE_COL,
    EER_NAME_COL,
    END_DATE_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    GROUPING_TMP_COL,
    HM3_M3S_MONTHLY_FACTOR,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    LOWER_BOUND_COL,
    OPERATION_SYNTHESIS_COMMON_COLUMNS,
    OPERATION_SYNTHESIS_METADATA_OUTPUT,
    OPERATION_SYNTHESIS_STATS_ROOT,
    OPERATION_SYNTHESIS_SUBDIR,
    PRODUCTIVITY_TMP_COL,
    SCENARIO_COL,
    STAGE_COL,
    STAGE_DURATION_HOURS,
    START_DATE_COL,
    STATS_OR_SCENARIO_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    THERMAL_CODE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
    VARIABLE_COL,
)
from app.model.operation.operationsynthesis import (
    SUPPORTED_SYNTHESIS,
    SYNTHESIS_DEPENDENCIES,
    UNITS,
    OperationSynthesis,
)
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.model.settings import Settings
from app.services.deck.bounds import OperationVariableBounds
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.graph import Graph
from app.utils.log import Log
from app.utils.operations import calc_statistics
from app.utils.regex import match_variables_with_wildcards
from app.utils.timing import time_and_log


class OperationSynthetizer:
    T = TypeVar("T")
    logger: Optional[logging.Logger] = None

    # Por padrão, todas as sínteses suportadas são consideradas
    DEFAULT_OPERATION_SYNTHESIS_ARGS: List[str] = SUPPORTED_SYNTHESIS

    # Todas as sínteses que forem dependências de outras sínteses
    # devem ser armazenadas em cache
    SYNTHESIS_TO_CACHE: List[OperationSynthesis] = list(
        set([p for pr in SYNTHESIS_DEPENDENCIES.values() for p in pr])
    )

    # Estratégias de cache para reduzir tempo total de síntese
    CACHED_SYNTHESIS: Dict[OperationSynthesis, pl.DataFrame] = {}
    ORDERED_SYNTHESIS_ENTITIES: Dict[OperationSynthesis, Dict[str, list]] = {}

    # Estatísticas das sínteses são armazenadas separadamente
    SYNTHESIS_STATS: Dict[SpatialResolution, List[pl.DataFrame]] = {}

    @classmethod
    def clear_cache(cls):
        """
        Limpa o cache de síntese de operação.
        """
        cls.CACHED_SYNTHESIS.clear()
        cls.ORDERED_SYNTHESIS_ENTITIES.clear()
        cls.SYNTHESIS_STATS.clear()

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _default_args(cls) -> List[OperationSynthesis]:
        """
        Retorna a lista de argumentos padrão para a síntese da operação,
        que é constituído de todos os objetos de síntese suportados.
        """
        args = [
            OperationSynthesis.factory(a)
            for a in cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        ]
        return [arg for arg in args if arg is not None]

    @classmethod
    def _match_wildcards(cls, variables: List[str]) -> List[str]:
        """
        Identifica se há variáveis de síntese que são suportadas
        dentro do padrão de wildcards (`*`) fornecidos.
        """
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[OperationSynthesis]:
        """
        Identifica se há objetos de síntese que sejam relacionados
        a variáveis de síntese dentro da lista de variáveis fornecidas.
        """
        args_data = [OperationSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        return valid_args

    @classmethod
    def _filter_valid_variables(
        cls, variables: List[OperationSynthesis], uow: AbstractUnitOfWork
    ) -> List[OperationSynthesis]:
        """
        Filtra as variáveis de síntese da operação que são válidas
        para o caso selecionado, considerando a presença de geração
        eólica e modelagem híbrida.
        """
        has_wind = Deck.models_wind_generation(uow)
        simulation_with_hydro = Deck.final_simulation_aggregation(uow)
        policy_with_hydro = Deck.hybrid_policy(uow)
        has_hydro = simulation_with_hydro or policy_with_hydro
        valid_variables: List[OperationSynthesis] = []
        for v in variables:
            if (
                v.variable
                in [
                    Variable.VELOCIDADE_VENTO,
                    Variable.GERACAO_EOLICA,
                    Variable.CORTE_GERACAO_EOLICA,
                ]
                and not has_wind
            ):
                continue
            if (
                v.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA
                and not has_hydro
            ):
                continue
            if (
                v.variable
                in [
                    Variable.VIOLACAO_FPHA,
                    Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                    Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                ]
                and not has_hydro
            ):
                continue
            if all([
                v.variable == Variable.VALOR_AGUA,
                v.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
                not has_hydro,
            ]):
                continue
            valid_variables.append(v)
        cls._log(f"Sinteses: {valid_variables}")
        return valid_variables

    @classmethod
    def _add_synthesis_dependencies(
        cls, synthesis: List[OperationSynthesis]
    ) -> List[OperationSynthesis]:
        """
        Adiciona objetos as dependências de síntese para uma lista de objetos
        de síntese que foram fornecidos.
        """

        def _add_synthesis_dependencies_recursive(
            current_synthesis: List[OperationSynthesis],
            todo_synthesis: OperationSynthesis,
        ):
            if todo_synthesis in SYNTHESIS_DEPENDENCIES.keys():
                for dep in SYNTHESIS_DEPENDENCIES[todo_synthesis]:
                    _add_synthesis_dependencies_recursive(
                        current_synthesis, dep
                    )
            if todo_synthesis not in current_synthesis:
                current_synthesis.append(todo_synthesis)

        result_synthesis: List[OperationSynthesis] = []
        for v in synthesis:
            _add_synthesis_dependencies_recursive(result_synthesis, v)
        return result_synthesis

    @classmethod
    def _get_unique_column_values_in_order(
        cls, df: pl.DataFrame, cols: List[str]
    ):
        """
        Extrai valores únicos na ordem em que aparecem para um
        conjunto de colunas de um DataFrame.
        """
        return {col: df[col].unique().to_list() for col in cols}

    @classmethod
    def _set_ordered_entities(
        cls, s: OperationSynthesis, entities: Dict[str, list]
    ):
        """
        Armazena um conjunto de entidades ordenadas para uma síntese.
        """
        cls.ORDERED_SYNTHESIS_ENTITIES[s] = entities

    @classmethod
    def _get_ordered_entities(cls, s: OperationSynthesis) -> Dict[str, list]:
        """
        Obtem um conjunto de entidades ordenadas para uma síntese.
        """
        return cls.ORDERED_SYNTHESIS_ENTITIES[s]

    @classmethod
    def _post_resolve_entity(
        cls,
        df: Optional[pl.DataFrame],
        s: OperationSynthesis,
        entity_column_values: Dict[str, Any],
        uow: AbstractUnitOfWork,
        internal_stubs: Dict[Variable, Callable] = {},
    ) -> Optional[pl.DataFrame]:
        """
        Realiza pós-processamento após a resolução da extração dados
        em um DataFrame de síntese extraído do NWLISTOP.
        """
        if df is None:
            return df
        df = cls._resolve_temporal_resolution(df, uow)
        df = df.with_columns(*[
            pl.lit(val).alias(col) for col, val in entity_column_values.items()
        ])

        df = cls._resolve_starting_stage(df, uow)
        if s.variable in internal_stubs:
            df = internal_stubs[s.variable](df, uow)
        df_stats = calc_statistics(df)
        df = df.with_columns(
            pl.col(SCENARIO_COL).cast(str),
            pl.lit(False).alias(STATS_OR_SCENARIO_COL),
        )
        df_stats = df_stats.with_columns(
            pl.lit(True).alias(STATS_OR_SCENARIO_COL)
        )
        df_stats = df_stats[df.columns]
        df = pl.concat([df, df_stats])
        return df

    @classmethod
    def _post_resolve(
        cls,
        resolve_responses: Dict[str, Optional[pl.DataFrame]],
        s: OperationSynthesis,
        uow: AbstractUnitOfWork,
        early_hooks: List[Callable] = [],
        late_hooks: List[Callable] = [],
    ) -> Optional[pl.DataFrame]:
        """
        Realiza pós-processamento após a resolução da extração
        de todos os dados de síntese extraídos do NWLISTOP para uma síntese.
        """
        with time_and_log(
            message_root="Tempo para compactacao dos dados", logger=cls.logger
        ):
            valid_dfs = [
                df for df in resolve_responses.values() if df is not None
            ]
            if len(valid_dfs) > 0:
                df = pl.concat(valid_dfs)
            else:
                return None

            spatial_resolution = s.spatial_resolution

            for c in early_hooks:
                df = c(s, df, uow)

            df = df.sort(spatial_resolution.sorting_synthesis_df_columns)

            entity_columns_order = cls._get_unique_column_values_in_order(
                df,
                spatial_resolution.sorting_synthesis_df_columns,
            )
            other_columns_order = cls._get_unique_column_values_in_order(
                valid_dfs[0],
                spatial_resolution.non_entity_sorting_synthesis_df_columns,
            )
            cls._set_ordered_entities(
                s, {**entity_columns_order, **other_columns_order}
            )

            for c in late_hooks:
                df = c(s, df, uow)
        return df

    @staticmethod
    def _resolve_temporal_resolution(
        df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Adiciona informação temporal a um DataFrame de síntese, utilizando
        as informações de duração dos patamares e datas de início dos estágios.
        """

        def _replace_scenario_info(
            df: pl.DataFrame,
            num_stages: int,
            num_scenarios: int,
            num_blocks: int,
        ) -> pl.DataFrame:
            """
            Substitui a informação de cenário por um intervalo fixo de `1`a
            `num_scenarios`, caso os dados fornecidos contenham informações
            de cenários de índices não regulares.
            """
            df = df.with_columns(
                pl.Series(
                    name=SCENARIO_COL,
                    values=np.tile(
                        np.repeat(np.arange(1, num_scenarios + 1), num_blocks),
                        num_stages,
                    ),
                )
            )
            return df

        def _add_stage_info(
            df: pl.DataFrame,
            num_stages: int,
            num_scenarios: int,
            num_blocks: int,
            end_dates: List[datetime],
        ) -> pl.DataFrame:
            """
            Adiciona informações de estágio a um DataFrame, utilizando o
            número de valores de data distintos fornecidos para definir uma
            faixa ordenada de estágios de `1` a `num_stages`.
            """
            stages = np.arange(1, num_stages + 1)
            stages_to_df_column = np.repeat(stages, num_scenarios * num_blocks)
            end_dates_to_df_column: np.ndarray = np.repeat(
                np.array(end_dates, dtype="datetime64[ns]"),
                num_scenarios * num_blocks,
            )
            df = df.with_columns(
                pl.Series(name=STAGE_COL, values=stages_to_df_column),
                pl.Series(name=END_DATE_COL, values=end_dates_to_df_column),
            )
            return df

        def _add_block_duration_info(
            df: pl.DataFrame,
            num_stages: int,
            num_scenarios: int,
            num_blocks: int,
            blocks: List[int],
            start_dates: List[datetime],
        ) -> pl.DataFrame:
            """
            Adiciona informações de duração de patamares a um DataFrame, utilizando
            as informações dos patamares e datas de início dos estágios.
            """
            df_block_lengths = Deck.block_lengths(uow)
            df_block_lengths = df_block_lengths.filter(
                pl.col(BLOCK_COL).is_in(blocks)
            )
            block_durations = np.zeros(
                (num_scenarios * num_blocks * num_stages,), dtype=np.float64
            )
            data_block_size = num_scenarios * num_blocks
            for i, d in enumerate(start_dates):
                date_durations = df_block_lengths.filter(
                    pl.col(START_DATE_COL) == d
                )[VALUE_COL].to_numpy()
                i_i = i * data_block_size
                i_f = i_i + data_block_size
                block_durations[i_i:i_f] = np.tile(
                    date_durations, num_scenarios
                )
            df = df.with_columns(
                pl.Series(
                    name=BLOCK_DURATION_COL,
                    values=block_durations * STAGE_DURATION_HOURS,
                )
            )
            return df

        def _add_temporal_info(
            df: pl.DataFrame, uow: AbstractUnitOfWork
        ) -> pl.DataFrame:
            """
            Adiciona informação temporal a um DataFrame de síntese.
            """
            df = df.rename({"data": START_DATE_COL, "serie": SCENARIO_COL})
            df = df.sort([START_DATE_COL, SCENARIO_COL, BLOCK_COL])
            num_stages = df[START_DATE_COL].unique().shape[0]
            num_scenarios = Deck.num_scenarios_final_simulation(uow)
            blocks = df[BLOCK_COL].unique().to_list()
            num_blocks = len(blocks)
            start_dates = Deck.internal_stages_starting_dates_final_simulation(
                uow
            )[:num_stages]
            end_dates = Deck.internal_stages_ending_dates_final_simulation(uow)[
                :num_stages
            ]
            df = _replace_scenario_info(
                df, num_stages, num_scenarios, num_blocks
            )
            df = _add_stage_info(
                df,
                num_stages,
                num_scenarios,
                num_blocks,
                end_dates,
            )
            df = _add_block_duration_info(
                df, num_stages, num_scenarios, num_blocks, blocks, start_dates
            )
            return df[OPERATION_SYNTHESIS_COMMON_COLUMNS]

        if df is None:
            return None
        return _add_temporal_info(df, uow)

    @classmethod
    def __resolve_SIN(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pl.DataFrame]:
        with time_and_log(
            message_root="Tempo para obter dados do SIN", logger=cls.logger
        ):
            with uow:
                df = uow.files.get_nwlistop(
                    synthesis.variable,
                    synthesis.spatial_resolution,
                    "",
                )
            df = cls._post_resolve_entity(df, synthesis, {}, uow)
        return cls._post_resolve({"SIN": df}, synthesis, uow)

    @classmethod
    def _resolve_SBM_entity(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        sbm_index: int,
        sbm_name: str,
    ) -> Optional[pl.DataFrame]:
        """
        Obtem os dados da síntese de operação para um submercado
        a partir do arquivo de saída do NWLISTOP.
        """

        logger_name = f"{synthesis.variable.value}_{sbm_name}"
        logger = Log.configure_process_logger(uow.queue, logger_name, sbm_index)
        with uow:
            logger.debug(
                f"Processando arquivo do submercado: {sbm_index} - {sbm_name}"
            )
            df = uow.files.get_nwlistop(
                synthesis.variable,
                synthesis.spatial_resolution,
                submercado=sbm_index,
            )
        return cls._post_resolve_entity(
            df,
            synthesis,
            {
                SUBMARKET_CODE_COL: sbm_index,
            },
            uow,
        )

    @classmethod
    def __resolve_SBM(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pl.DataFrame]:
        """
        Resolve a síntese de operação para uma variável operativa
        de um submercado a partir dos arquivos de saída do NWLISTOP.
        """

        submarkets = Deck.submarkets(uow).reset_index()
        real_submarkets = submarkets.loc[
            submarkets["ficticio"] == 0, :
        ].sort_values(SUBMARKET_CODE_COL)
        sbms_idx = real_submarkets[SUBMARKET_CODE_COL].unique()
        sbms_name = [
            real_submarkets.loc[
                real_submarkets[SUBMARKET_CODE_COL] == s, SUBMARKET_NAME_COL
            ].iloc[0]
            for s in sbms_idx
        ]

        n_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para obter dados de SBM", logger=cls.logger
        ):
            with Pool(processes=n_procs) as pool:
                async_res = {
                    idx: pool.apply_async(
                        cls._resolve_SBM_entity, (uow, synthesis, idx, name)
                    )
                    for idx, name in zip(sbms_idx, sbms_name)
                }
                dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}

        df = cls._post_resolve(
            dfs,
            synthesis,
            uow,
        )
        return df

    @classmethod
    def _resolve_SBP_entity(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        sbm1_index: int,
        sbm1_name: str,
        sbm2_index: int,
        sbm2_name: str,
    ) -> Optional[pl.DataFrame]:
        """
        Obtém os dados da síntese de operação para um par de submercados
        a partir do arquivo de saída do NWLISTOP.
        """
        if sbm1_index >= sbm2_index:
            return None
        logger_name = f"{synthesis.variable.value}_{sbm1_name}_{sbm2_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, sbm1_index + 10 * sbm2_index
        )
        with uow:
            logger.debug(
                "Processando arquivo do par de submercados:"
                + f" {sbm1_index}[{sbm1_name}] - {sbm2_index}[{sbm2_name}]"
            )
            df = uow.files.get_nwlistop(
                synthesis.variable,
                synthesis.spatial_resolution,
                submercados=(sbm1_index, sbm2_index),
            )
        return cls._post_resolve_entity(
            df,
            synthesis,
            {
                EXCHANGE_SOURCE_CODE_COL: sbm1_index,
                EXCHANGE_TARGET_CODE_COL: sbm2_index,
            },
            uow,
        )

    @classmethod
    def __resolve_SBP(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pl.DataFrame]:
        """
        Resolve a síntese de operação para uma variável operativa
        de um par de submercados a partir dos arquivos de saída do NWLISTOP.
        """

        submarkets = Deck.submarkets(uow).reset_index()
        sbms_idx = submarkets[SUBMARKET_CODE_COL].unique()
        sbms_name = [
            submarkets.loc[
                submarkets[SUBMARKET_CODE_COL] == s, SUBMARKET_NAME_COL
            ].iloc[0]
            for s in sbms_idx
        ]

        n_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para obter dados de SBP", logger=cls.logger
        ):
            with Pool(processes=n_procs) as pool:
                async_res = {
                    f"{idx1}-{idx2}": pool.apply_async(
                        cls._resolve_SBP_entity,
                        (uow, synthesis, idx1, name1, idx2, name2),
                    )
                    for idx1, name1 in zip(sbms_idx, sbms_name)
                    for idx2, name2 in zip(sbms_idx, sbms_name)
                }
                dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}

        df = cls._post_resolve(
            dfs,
            synthesis,
            uow,
        )
        return df

    @classmethod
    def _resolve_REE_entity(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        ree_index: int,
        ree_name: str,
    ) -> Optional[pl.DataFrame]:
        """
        Obtem os dados da síntese de operação para um REE
        a partir do arquivo de saída do NWLISTOP.
        """

        logger_name = f"{synthesis.variable.value}_{ree_name}"
        logger = Log.configure_process_logger(uow.queue, logger_name, ree_index)
        with uow:
            logger.debug(
                f"Processando arquivo do REE: {ree_index} - {ree_name}"
            )
            df = uow.files.get_nwlistop(
                synthesis.variable,
                synthesis.spatial_resolution,
                ree=ree_index,
            )

        aux_df = Deck.eer_submarket_map(uow)

        return cls._post_resolve_entity(
            df,
            synthesis,
            {
                EER_CODE_COL: ree_index,
                SUBMARKET_CODE_COL: aux_df.filter(
                    pl.col(EER_CODE_COL) == ree_index
                )[SUBMARKET_CODE_COL][0],
            },
            uow,
        )

    @classmethod
    def __resolve_REE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pl.DataFrame]:
        """
        Resolve a síntese de operação para uma variável operativa
        de um REE a partir dos arquivos de saída do NWLISTOP.
        """

        eers = Deck.eers(uow).reset_index().sort_values(EER_CODE_COL)
        eers_idx = eers[EER_CODE_COL]
        eers_name = eers[EER_NAME_COL]

        n_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para ler dados de REE", logger=cls.logger
        ):
            with Pool(processes=n_procs) as pool:
                async_res = {
                    idx: pool.apply_async(
                        cls._resolve_REE_entity, (uow, synthesis, idx, name)
                    )
                    for idx, name in zip(eers_idx, eers_name)
                }
                dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}

        df = cls._post_resolve(
            dfs,
            synthesis,
            uow,
        )
        return df

    @classmethod
    def _resolve_UHE_entity(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        uhe_index: int,
        uhe_name: str,
    ) -> Optional[pl.DataFrame]:
        """
        Obtem os dados da síntese de operação para uma UHE
        a partir do arquivo de saída do NWLISTOP.
        """

        def _calc_block_0_weighted_mean(
            df: pl.DataFrame, uow: AbstractUnitOfWork
        ) -> pl.DataFrame:
            """
            Calcula um valor médio ponderado para o estágio a partir
            de valores fornecidos por patamar de alguma variável operativa
            de uma UHE.
            """
            n_blocks = Deck.num_blocks(uow)
            unique_cols_for_block_0 = [HYDRO_CODE_COL, STAGE_COL, SCENARIO_COL]
            df_block_0 = df.with_columns(
                (
                    pl.col(VALUE_COL)
                    * pl.col(BLOCK_DURATION_COL)
                    / STAGE_DURATION_HOURS
                ).alias(VALUE_COL)
            )
            df_base = df.filter(pl.int_range(pl.len()) % n_blocks == 0)
            df_base = df_base.with_columns(
                pl.lit(0, dtype=pl.Int64).alias(BLOCK_COL),
                pl.lit(STAGE_DURATION_HOURS, dtype=pl.Float64).alias(
                    BLOCK_DURATION_COL
                ),
            )
            arr = df_block_0[VALUE_COL].to_numpy()
            n_linhas = arr.shape[0]
            n_elementos_distintos = n_linhas // n_blocks
            df_base = df_base.with_columns(
                pl.Series(
                    name=VALUE_COL,
                    values=arr.reshape((n_elementos_distintos, -1)).sum(axis=1),
                )
            )
            df_block_0 = pl.concat([df_base, df])
            df_block_0 = df_block_0.sort(unique_cols_for_block_0 + [BLOCK_COL])
            return df_block_0

        logger_name = f"{synthesis.variable.value}_{uhe_name}"
        logger = Log.configure_process_logger(uow.queue, logger_name, uhe_index)

        with uow:
            logger.debug(
                f"Processando arquivo da UHE: {uhe_index} - {uhe_name}"
            )
            df = uow.files.get_nwlistop(
                synthesis.variable,
                synthesis.spatial_resolution,
                uhe=uhe_index,
            )

        internal_stubs = {
            Variable.COTA_JUSANTE: _calc_block_0_weighted_mean,  # noqa
            Variable.QUEDA_LIQUIDA: _calc_block_0_weighted_mean,  # noqa
            Variable.VAZAO_TURBINADA: _calc_block_0_weighted_mean,  # noqa
            Variable.VAZAO_VERTIDA: _calc_block_0_weighted_mean,  # noqa
            Variable.VAZAO_DESVIADA: _calc_block_0_weighted_mean,  # noqa
        }

        aux_df = Deck.hydro_eer_submarket_map(uow)
        return cls._post_resolve_entity(
            df,
            synthesis,
            {
                HYDRO_CODE_COL: uhe_index,
                EER_CODE_COL: aux_df.filter(
                    pl.col(HYDRO_CODE_COL) == uhe_index
                )[EER_CODE_COL][0],
                SUBMARKET_CODE_COL: aux_df.filter(
                    pl.col(HYDRO_CODE_COL) == uhe_index
                )[SUBMARKET_CODE_COL][0],
            },
            uow,
            internal_stubs=internal_stubs,
        )

    @classmethod
    def __resolve_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pl.DataFrame]:
        """
        Resolve a síntese de operação para uma variável operativa
        de uma UHE a partir dos arquivos de saída do NWLISTOP.
        """

        def _limit_stages_with_hydro(
            s: OperationSynthesis, df: pl.DataFrame, uow: AbstractUnitOfWork
        ) -> pl.DataFrame:
            df = df.filter(
                pl.col(START_DATE_COL)
                < Deck.hydro_simulation_stages_ending_date_final_simulation(uow)
            )
            return df

        hydros = Deck.hydros(uow).reset_index().sort_values(HYDRO_CODE_COL)
        hydros_idx = hydros[HYDRO_CODE_COL]
        hydros_name = hydros[HYDRO_NAME_COL]

        n_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para ler dados de UHE",
            logger=cls.logger,
        ):
            with Pool(processes=n_procs) as pool:
                async_res = {
                    name: pool.apply_async(
                        cls._resolve_UHE_entity, (uow, synthesis, idx, name)
                    )
                    for idx, name in zip(hydros_idx, hydros_name)
                }
                dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}

        df = cls._post_resolve(
            dfs,
            synthesis,
            uow,
            early_hooks=[_limit_stages_with_hydro],
        )
        return df

    @classmethod
    def _convert_volume_to_flow(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza a conversão de síntese de volume para vazão.
        """
        variable_map = {
            Variable.VAZAO_RETIRADA: Variable.VOLUME_RETIRADO,
        }
        volume_synthesis = OperationSynthesis(
            variable_map[synthesis.variable],
            synthesis.spatial_resolution,
        )
        df = cls._get_from_cache(volume_synthesis)
        df = df.with_columns(
            (
                pl.col(VALUE_COL)
                * HM3_M3S_MONTHLY_FACTOR
                * STAGE_DURATION_HOURS
                / pl.col(BLOCK_DURATION_COL)
            ).alias(VALUE_COL)
        )

        return df

    @classmethod
    def _convert_flow_to_volume(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza a conversão de síntese de vazão para volume.
        """
        variable_map = {
            Variable.VOLUME_AFLUENTE: Variable.VAZAO_AFLUENTE,
            Variable.VOLUME_INCREMENTAL: Variable.VAZAO_INCREMENTAL,
            Variable.VOLUME_TURBINADO: Variable.VAZAO_TURBINADA,
            Variable.VOLUME_VERTIDO: Variable.VAZAO_VERTIDA,
            Variable.VOLUME_DESVIADO: Variable.VAZAO_DESVIADA,
        }
        flow_synthesis = OperationSynthesis(
            variable_map[synthesis.variable],
            synthesis.spatial_resolution,
        )
        df = cls._get_from_cache(flow_synthesis)
        df = df.with_columns(
            (
                pl.col(VALUE_COL)
                * pl.col(BLOCK_DURATION_COL)
                / (HM3_M3S_MONTHLY_FACTOR * STAGE_DURATION_HOURS)
            ).alias(VALUE_COL)
        )
        return df

    @classmethod
    def __stub_QDEF(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza o cálculo da vazão defluente a partir dos valores
        das vazões turbinada e vertida.
        """
        turbined_synthesis = OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            synthesis.spatial_resolution,
        )
        spilled_synthesis = OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            synthesis.spatial_resolution,
        )
        turbined_df = cls._get_from_cache(turbined_synthesis)
        spilled_df = cls._get_from_cache(spilled_synthesis)
        spilled_df = spilled_df.with_columns(
            (pl.col(VALUE_COL) + turbined_df[VALUE_COL]).alias(VALUE_COL)
        )
        return spilled_df

    @classmethod
    def __stub_VDEF(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza o cálculo do volume defluente a partir dos valores
        dos volumes turbinado e vertido.
        """
        turbined_synthesis = OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            synthesis.spatial_resolution,
        )
        spilled_synthesis = OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            synthesis.spatial_resolution,
        )
        turbined_df = cls._get_from_cache(turbined_synthesis)
        spilled_df = cls._get_from_cache(spilled_synthesis)

        spilled_df = spilled_df.with_columns(
            (pl.col(VALUE_COL) + turbined_df[VALUE_COL]).alias(VALUE_COL)
        )
        return spilled_df

    @classmethod
    def __stub_VEVAP(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza o cálculo da violação da evaporação a partir dos valores
        das violações positiva e negativa da evaporação.
        """
        positive_synthesis = OperationSynthesis(
            Variable.VIOLACAO_POSITIVA_EVAPORACAO,
            synthesis.spatial_resolution,
        )
        negative_synthesis = OperationSynthesis(
            Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
            synthesis.spatial_resolution,
        )
        positive_df = cls._get_from_cache(positive_synthesis)
        negative_df = cls._get_from_cache(negative_synthesis)

        positive_df = positive_df.with_columns(
            (pl.col(VALUE_COL) + negative_df[VALUE_COL]).alias(VALUE_COL)
        )
        return positive_df

    @classmethod
    def __stub_CTO(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza o cálculo do custo total a partir dos valores
        das parcelas de custo presente e futuro.
        """
        operation_cost_synthesis = OperationSynthesis(
            Variable.CUSTO_OPERACAO,
            synthesis.spatial_resolution,
        )
        future_cost_synthesis = OperationSynthesis(
            Variable.CUSTO_FUTURO,
            synthesis.spatial_resolution,
        )
        operation_df = cls._get_from_cache(operation_cost_synthesis)
        future_df = cls._get_from_cache(future_cost_synthesis)

        operation_df = operation_df.with_columns(
            (pl.col(VALUE_COL) + future_df[VALUE_COL]).alias(VALUE_COL)
        )

        return operation_df

    @classmethod
    def __stub_EVER(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza o cálculo da energia vertida a partir dos valores
        das energias vertidas em reservatório e fio d'água.
        """
        reservoir_synthesis = OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            synthesis.spatial_resolution,
        )
        run_of_river_synthesis = OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            synthesis.spatial_resolution,
        )
        reservoir_df = cls._get_from_cache(reservoir_synthesis)
        run_of_river_df = cls._get_from_cache(run_of_river_synthesis)

        reservoir_df = reservoir_df.with_columns(
            (pl.col(VALUE_COL) + run_of_river_df[VALUE_COL]).alias(VALUE_COL)
        )

        return reservoir_df

    @classmethod
    def _hydro_resolution_variable_map(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza o mapeamento de síntese de uma variável calculada
        a partir de uma agregação simples de variáveis de UHE.
        """

        hydro_synthesis = OperationSynthesis(
            variable=synthesis.variable,
            spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
        )
        hydro_df = cls._get_from_cache(hydro_synthesis)
        return hydro_df

    @classmethod
    def _flow_volume_hydro_variable_map(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza o mapeamento de uma síntese solicitada para variável
        em vazão para a variável correspondente em volume, obtendo os
        dados já processados e armazenados em cache.

        Regra de negócio: o df passado para resolução dos bounds é
        em unidade de volume e sempre a nível de UHE. As agregações
        e conversões de unidade são feitas na resolução dos bounds.
        O OperationSynthesis passado para o bounds é o original.
        """

        variable_map = {
            Variable.VAZAO_AFLUENTE: Variable.VOLUME_AFLUENTE,
            Variable.VAZAO_INCREMENTAL: Variable.VOLUME_INCREMENTAL,
            Variable.VAZAO_DEFLUENTE: Variable.VOLUME_DEFLUENTE,
            Variable.VAZAO_VERTIDA: Variable.VOLUME_VERTIDO,
            Variable.VAZAO_TURBINADA: Variable.VOLUME_TURBINADO,
            Variable.VAZAO_RETIRADA: Variable.VOLUME_RETIRADO,
            Variable.VAZAO_DESVIADA: Variable.VOLUME_DESVIADO,
        }

        volume_synthesis = OperationSynthesis(
            variable=variable_map[synthesis.variable],
            spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
        )
        volume_df = cls._get_from_cache(volume_synthesis)
        return volume_df

    @classmethod
    def _absolute_percent_volume_variable_map(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza o mapeamento de uma síntese solicitada para variável
        em volume percentuais para a em volume absoluto, obtendo os
        dados já processados e armazenados em cache.

        Regra de negócio: o df passado para resolução dos bounds é
        em unidade de volume e sempre a nível de UHE. As agregações
        e conversões de unidade são feitas na resolução dos bounds.
        O OperationSynthesis passado para o bounds é o original.
        """

        varmi = Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL
        varmf = Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL
        varpi = Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL
        varpf = Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL

        variable_map = {
            varpi: varmi,  # noqa
            varpf: varmf,  # noqa
        }

        absolute_synthesis = OperationSynthesis(
            variable=variable_map[synthesis.variable],
            spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
        )
        absolute_df = cls._get_from_cache(absolute_synthesis)

        return absolute_df

    @classmethod
    def _stub_resolve_initial_stored_energy(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Resolve a síntese de energias armazenadas iniciais para um REE,
        SBM ou para o SIN, processando informações existentes no `pmo.dat`
        e adequando o formato para serem utilizadas em conjunto com os
        resultados das energias armazenadas finais do NWLISTOP.
        """

        def _get_final_storage_synthesis_data(
            synthesis: OperationSynthesis,
        ) -> Tuple[pl.DataFrame, dict]:
            final_storage_synthesis = OperationSynthesis(
                variable=variable_map[synthesis.variable],
                spatial_resolution=synthesis.spatial_resolution,
            )
            final_storage_df = cls._get_from_cache(final_storage_synthesis)
            entities = cls._get_ordered_entities(final_storage_synthesis)
            return final_storage_df, entities

        def _get_initial_storage_values(
            initial_storage_data: pl.DataFrame,
            entities: dict,
        ) -> np.ndarray:
            value_column = (
                "valor_MWmes"
                if synthesis.variable == earmi
                else "valor_percentual"
            )
            groups = entities.get(
                grouping_col_map[synthesis.spatial_resolution]
            ) or [1]
            if (
                synthesis.spatial_resolution
                != SpatialResolution.SISTEMA_INTERLIGADO
            ):
                groups = [
                    g
                    for g in groups
                    if g in initial_storage_data[GROUPING_TMP_COL]
                ]
            initial_storage_values = initial_storage_data.filter(
                pl.col(GROUPING_TMP_COL).is_in(groups)
            )[value_column].to_numpy()
            return initial_storage_values

        def _get_initial_stage_indices(
            entities: dict, initial_storage_data: pl.DataFrame
        ) -> np.ndarray:
            groups = entities.get(
                grouping_col_map[synthesis.spatial_resolution]
            ) or [1]
            if (
                synthesis.spatial_resolution
                != SpatialResolution.SISTEMA_INTERLIGADO
            ):
                groups = [
                    g
                    for g in groups
                    if g in initial_storage_data[GROUPING_TMP_COL]
                ]
            num_groups = len(groups)
            scenarios = [
                s for s in entities[SCENARIO_COL] if str(s).isnumeric()
            ]
            num_scenarios = len(scenarios)
            stages = entities[STAGE_COL]
            num_stages = len(stages)
            offsets_groups = [
                i * num_scenarios * num_stages for i in range(num_groups)
            ]
            initial_stage_indices = np.tile(
                np.arange(num_scenarios), num_groups
            )
            initial_stage_indices += np.repeat(offsets_groups, num_scenarios)
            return initial_stage_indices

        def _fill_initial_storage_df(
            df: pl.DataFrame,
            indices: np.ndarray,
            values: np.ndarray,
            entities: dict,
        ) -> pl.DataFrame:
            num_scenarios = len([
                s for s in entities[SCENARIO_COL] if str(s).isnumeric()
            ])
            initial_storage_df = df
            initial_storage_values_df = initial_storage_df[VALUE_COL].to_numpy()
            initial_storage_values_df[num_scenarios:] = (
                initial_storage_values_df[:-num_scenarios]
            )
            initial_storage_values_df[indices] = np.repeat(
                values, num_scenarios
            )
            initial_storage_df = initial_storage_df.with_columns(
                pl.Series(name=VALUE_COL, values=initial_storage_values_df)
            )
            initial_storage_df = initial_storage_df.with_columns(
                pl.col(VALUE_COL).fill_nan(0.0)
            )
            return initial_storage_df

        earmi = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL
        earmf = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL
        earpi = Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL
        earpf = Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL
        variable_map = {
            earmi: earmf,
            earpi: earpf,
        }
        grouping_col_map = {
            SpatialResolution.RESERVATORIO_EQUIVALENTE: EER_CODE_COL,
            SpatialResolution.SUBMERCADO: SUBMARKET_CODE_COL,
            SpatialResolution.SISTEMA_INTERLIGADO: None,
        }

        initial_storage_data = pl.from_pandas(
            cls._initial_stored_energy_df(synthesis, uow)
        )
        final_storage_df, entities = _get_final_storage_synthesis_data(
            synthesis
        )
        initial_storage_values = _get_initial_storage_values(
            initial_storage_data, entities
        )
        initial_stage_indices = _get_initial_stage_indices(
            entities, initial_storage_data
        )
        initial_storage_df = _fill_initial_storage_df(
            final_storage_df,
            initial_stage_indices,
            initial_storage_values,
            entities,
        )
        return initial_storage_df

    @classmethod
    def __stub_resolve_initial_stored_volumes(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Resolve a síntese de volumes armazenados iniciais para uma UHE,
        processando informações existentes no `pmo.dat` e adequando
        o formato para serem utilizadas em conjunto com os resultados
        dos volumes armazenados finais do NWLISTOP.
        """

        def _get_final_storage_synthesis_data(
            synthesis: OperationSynthesis,
        ) -> Tuple[pl.DataFrame, dict]:
            final_storage_synthesis = OperationSynthesis(
                variable=variable_map[synthesis.variable],
                spatial_resolution=synthesis.spatial_resolution,
            )
            final_storage_df = cls._get_from_cache(final_storage_synthesis)
            entities = cls._get_ordered_entities(final_storage_synthesis)
            return final_storage_df, entities

        def _get_initial_storage_values(
            synthesis: OperationSynthesis,
            uow: AbstractUnitOfWork,
            entities: dict,
        ) -> np.ndarray:
            hydros = entities[HYDRO_CODE_COL]
            initial_storage_data = Deck.initial_stored_volume(uow)
            value_column = (
                "valor_hm3"
                if synthesis.variable == varmi
                else "valor_percentual"
            )
            if synthesis.variable == varmi:
                hidr = pl.from_pandas(Deck.hidr(uow).reset_index())
                initial_storage_data = initial_storage_data.join(
                    hidr[[HYDRO_CODE_COL, "volume_minimo"]], on=HYDRO_CODE_COL
                )
                initial_storage_data = initial_storage_data.with_columns(
                    (pl.col(value_column) + pl.col("volume_minimo")).alias(
                        value_column
                    )
                )

            initial_storage_data = initial_storage_data.filter(
                pl.col(HYDRO_CODE_COL).is_in(hydros)
            )
            entity_order = pl.DataFrame({
                "tmp": list(range(len(hydros))),
                HYDRO_CODE_COL: hydros,
            })
            initial_storage_data = initial_storage_data.join(
                entity_order, on=HYDRO_CODE_COL
            )
            initial_storage_values = initial_storage_data.sort("tmp")[
                value_column
            ].to_numpy()

            return initial_storage_values

        def _get_initial_stage_indices(entities: dict) -> np.ndarray:
            hydros = entities[HYDRO_CODE_COL]
            num_hydros = len(hydros)
            scenarios = [
                s for s in entities[SCENARIO_COL] if str(s).isnumeric()
            ]
            num_scenarios = len(scenarios)
            stages = entities[STAGE_COL]
            num_stages = len(stages)
            offsets_groups = [
                i * num_scenarios * num_stages for i in range(num_hydros)
            ]
            initial_stage_indices = np.tile(
                np.arange(num_scenarios), num_hydros
            )
            initial_stage_indices += np.repeat(offsets_groups, num_scenarios)
            return initial_stage_indices

        def _fill_initial_storage_df(
            df: pl.DataFrame,
            indices: np.ndarray,
            values: np.ndarray,
            entities: dict,
        ) -> pl.DataFrame:
            scenarios = [
                s for s in entities[SCENARIO_COL] if str(s).isnumeric()
            ]
            num_scenarios = len(scenarios)
            initial_storage_values_df = df[VALUE_COL].to_numpy()
            initial_storage_values_df[num_scenarios:] = (
                initial_storage_values_df[:-num_scenarios]
            )
            initial_storage_values_df[indices] = np.repeat(
                values, num_scenarios
            )
            df = df.with_columns(
                pl.Series(name=VALUE_COL, values=initial_storage_values_df)
            )
            df = df.with_columns(pl.col(VALUE_COL).fill_nan(0.0))
            return df

        varmi = Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL
        varmf = Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL
        varpi = Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL
        varpf = Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL
        variable_map = {
            varmi: varmf,
            varpi: varpf,
        }

        final_storage_df, entities = _get_final_storage_synthesis_data(
            synthesis
        )
        initial_storage_values = _get_initial_storage_values(
            synthesis, uow, entities
        )
        initial_stage_indices = _get_initial_stage_indices(entities)
        initial_storage_df = _fill_initial_storage_df(
            final_storage_df,
            initial_stage_indices,
            initial_storage_values,
            entities,
        )
        return initial_storage_df

    @classmethod
    def _calc_accumulated_productivity(
        cls, df: pl.DataFrame, entities: dict, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        hydro_df = Deck.hydros(uow).reset_index()
        # Monta a lista de arestas e constroi o grafo
        # direcionado das usinas (JUSANTE -> MONTANTE)
        hydro_codes = entities[HYDRO_CODE_COL]
        np_edges = list(
            hydro_df.loc[
                hydro_df[HYDRO_CODE_COL].isin(hydro_codes),
                ["codigo_usina_jusante", HYDRO_CODE_COL],
            ].to_numpy()
        )
        edges = [tuple(e) for e in np_edges]
        hydro_nodes_bfs = Graph(edges, directed=True).bfs(0)[1:]
        # Percorre todas as usinas a partir de um BFS, tendo
        # como nó de origem o 0 (MAR).
        for hydro_code in hydro_nodes_bfs:
            hydro_name = hydro_df.loc[
                hydro_df[HYDRO_CODE_COL] == hydro_code, HYDRO_CODE_COL
            ].iloc[0]
            cls._log(f"Calculando prodt. acumulada para {hydro_name}...")
            downstream_hydro_code = hydro_df.loc[
                hydro_df[HYDRO_CODE_COL] == hydro_code,
                "codigo_usina_jusante",
            ].iloc[0]
            if downstream_hydro_code == 0:
                continue
            downstream_hydro_name = hydro_df.loc[
                hydro_df[HYDRO_CODE_COL] == downstream_hydro_code,
                HYDRO_CODE_COL,
            ].iloc[0]
            hydro_productivity = df.loc[
                df[HYDRO_CODE_COL] == hydro_name, PRODUCTIVITY_TMP_COL
            ]
            downstream_productivity = df.loc[
                df[HYDRO_CODE_COL] == downstream_hydro_name,
                PRODUCTIVITY_TMP_COL,
            ].to_numpy()
            if (
                not hydro_productivity.empty
                and len(downstream_productivity) > 0
            ):
                hydro_productivity += downstream_productivity
        return df

    @classmethod
    def __stub_EARM_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o cálculo da energia armazenada em cada UHE a partir
        dos volumes armazenados e das quedas líquidas.
        """

        def _get_synthesis_data(
            synthesis: OperationSynthesis,
        ) -> Tuple[pd.DataFrame, dict]:
            df = cls._get_from_cache(synthesis)
            entities = cls._get_ordered_entities(synthesis)
            return df, entities

        def _add_productivity(
            net_drop_df: pd.DataFrame, net_drop_entities: dict
        ) -> pd.DataFrame:
            hidr = Deck.hidr(uow)
            hydro_codes = net_drop_entities[HYDRO_CODE_COL]
            num_entries_by_hydro = net_drop_df.loc[
                net_drop_df[HYDRO_CODE_COL] == hydro_codes[0]
            ].shape[0]
            specific_productivity = np.repeat(
                hidr.loc[hydro_codes, "produtibilidade_especifica"].to_numpy(),
                num_entries_by_hydro,
            )
            specific_productivity *= HM3_M3S_MONTHLY_FACTOR
            net_drop_df[PRODUCTIVITY_TMP_COL] = specific_productivity
            net_drop_df[PRODUCTIVITY_TMP_COL] *= net_drop_df[VALUE_COL]
            return net_drop_df

        def _cast_volume_to_energy(
            stored_volume_df: pd.DataFrame,
            net_drop_df: pd.DataFrame,
            stored_volume_entities: dict,
        ) -> pd.DataFrame:
            stored_volume_hydro_codes = stored_volume_entities[HYDRO_CODE_COL]
            stored_volume_hydro_blocks = stored_volume_entities[BLOCK_COL]
            net_drop_df = net_drop_df.loc[
                net_drop_df[HYDRO_CODE_COL].isin(stored_volume_hydro_codes)
                & net_drop_df[BLOCK_COL].isin(stored_volume_hydro_blocks)
            ].copy()

            net_drop_df = net_drop_df.sort_values([
                HYDRO_CODE_COL,
                STAGE_COL,
                BLOCK_COL,
            ])
            stored_volume_df = stored_volume_df.sort_values([
                HYDRO_CODE_COL,
                STAGE_COL,
                BLOCK_COL,
            ])

            stored_volume_df[VALUE_COL] = (
                stored_volume_df[VALUE_COL] - stored_volume_df[LOWER_BOUND_COL]
            ) * net_drop_df[PRODUCTIVITY_TMP_COL].to_numpy()
            stored_volume_df[LOWER_BOUND_COL] = 0.0
            stored_volume_df[UPPER_BOUND_COL] = (
                stored_volume_df[UPPER_BOUND_COL]
                - stored_volume_df[LOWER_BOUND_COL]
            ) * net_drop_df[PRODUCTIVITY_TMP_COL].to_numpy()
            return stored_volume_df

        with time_and_log(
            message_root="Tempo para conversao do VARM em EARM",
            logger=cls.logger,
        ):
            net_drop_synthesis = OperationSynthesis(
                Variable.QUEDA_LIQUIDA,
                synthesis.spatial_resolution,
            )

            earmi = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL
            earmf = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL
            varmi = Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL
            varmf = Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL

            energy_volume_map = {
                earmi: varmi,
                earmf: varmf,
            }
            stored_volume_synthesis = OperationSynthesis(
                energy_volume_map[synthesis.variable],
                synthesis.spatial_resolution,
            )

            stored_volume_df, stored_volume_entities = _get_synthesis_data(
                stored_volume_synthesis
            )
            net_drop_df, net_drop_entities = _get_synthesis_data(
                net_drop_synthesis
            )

            net_drop_df = _add_productivity(net_drop_df, net_drop_entities)
            net_drop_df = cls._calc_accumulated_productivity(
                net_drop_df, net_drop_entities, uow
            )
        return _cast_volume_to_energy(
            stored_volume_df, net_drop_df, stored_volume_entities
        )

    @classmethod
    def _generate_scenarios(
        cls, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Gera cenários para uma síntese que é feita apenas para um
        dos cenários obtidos no NWLISTOP.
        """
        num_scenarios = Deck.num_scenarios_final_simulation(uow)
        num_entries = df.shape[0]
        df = pl.concat([df] * num_scenarios)
        df = df.with_columns(
            pl.Series(
                name="serie",
                values=np.repeat(np.arange(1, num_scenarios + 1), num_entries),
            )
        )

        return df

    @classmethod
    def _resolve_SBM_entity_MER_MERL(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        sbm_index: int,
        sbm_name: str,
    ) -> Optional[pl.DataFrame]:
        """
        Obtem os dados da síntese de operação para um submercado
        a partir do arquivo de saída do NWLISTOP, especificamente
        para as variáveis MER e MERL.
        """

        logger_name = f"{synthesis.variable.value}_{sbm_name}"
        logger = Log.configure_process_logger(uow.queue, logger_name, sbm_index)
        with uow:
            logger.debug(
                f"Processando arquivo do submercado: {sbm_index} - {sbm_name}"
            )
            df = uow.files.get_nwlistop(
                synthesis.variable,
                synthesis.spatial_resolution,
                submercado=sbm_index,
            )
        df = cls._generate_scenarios(df, uow)
        return cls._post_resolve_entity(
            df,
            synthesis,
            {SUBMARKET_CODE_COL: sbm_index},
            uow,
        )

    @classmethod
    def __stub_MER_MERL(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza o processamento da síntese de mercado de energia,
        adequando o formato para ser compatível com as demais saídas
        do NWLISTOP.
        """

        def _resolve_SIN_MER_MERL(
            synthesis: OperationSynthesis, uow: AbstractUnitOfWork
        ) -> Optional[pl.DataFrame]:
            with time_and_log(
                message_root="Tempo para obter dados do SIN", logger=cls.logger
            ):
                with uow:
                    df = uow.files.get_nwlistop(
                        synthesis.variable,
                        synthesis.spatial_resolution,
                        "",
                    )
                df = cls._generate_scenarios(df, uow)
                df = cls._post_resolve_entity(df, synthesis, {}, uow)
            return cls._post_resolve({"SIN": df}, synthesis, uow)

        def _resolve_SBM_MER_MERL(
            synthesis: OperationSynthesis, uow: AbstractUnitOfWork
        ) -> Optional[pl.DataFrame]:
            submarkets = Deck.submarkets(uow).reset_index()
            real_submarkets = submarkets.loc[
                submarkets["ficticio"] == 0, :
            ].sort_values(SUBMARKET_CODE_COL)
            sbms_idx = real_submarkets[SUBMARKET_CODE_COL].unique()
            sbms_name = [
                real_submarkets.loc[
                    real_submarkets[SUBMARKET_CODE_COL] == s,
                    SUBMARKET_NAME_COL,
                ].iloc[0]
                for s in sbms_idx
            ]

            n_procs = int(Settings().processors)
            with time_and_log(
                message_root="Tempo para obter dados de SBM", logger=cls.logger
            ):
                with Pool(processes=n_procs) as pool:
                    async_res = {
                        idx: pool.apply_async(
                            cls._resolve_SBM_entity_MER_MERL,
                            (uow, synthesis, idx, name),
                        )
                        for idx, name in zip(sbms_idx, sbms_name)
                    }
                    dfs = {
                        ir: r.get(timeout=3600) for ir, r in async_res.items()
                    }

            df = cls._post_resolve(
                dfs,
                synthesis,
                uow,
            )
            return df

        RESOLUTION_FUNCTION_MAP: Dict[SpatialResolution, Callable] = {
            SpatialResolution.SISTEMA_INTERLIGADO: _resolve_SIN_MER_MERL,
            SpatialResolution.SUBMERCADO: _resolve_SBM_MER_MERL,
        }
        solver = RESOLUTION_FUNCTION_MAP[synthesis.spatial_resolution]
        res = solver(synthesis, uow)
        return res if res is not None else pl.DataFrame()

    @classmethod
    def __stub_EVMIN(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza o cálculo da energia de defluência mínima a partir
        dos valores da meta e da violação desta variável.
        """
        goal_synthesis = OperationSynthesis(
            Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            synthesis.spatial_resolution,
        )
        violation_synthesis = OperationSynthesis(
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            synthesis.spatial_resolution,
        )
        goal_df = cls._get_from_cache(goal_synthesis)
        violation_df = cls._get_from_cache(violation_synthesis)

        goal_df = goal_df.with_columns(
            (pl.col(VALUE_COL) + violation_df[VALUE_COL]).alias(VALUE_COL)
        )

        return goal_df

    @classmethod
    def _resolve_temporal_resolution_GTER_UTE(
        cls, df: Optional[pl.DataFrame], uow: AbstractUnitOfWork
    ) -> Optional[pl.DataFrame]:
        """
        Adiciona informação temporal a um DataFrame de síntese para a variável
        de Geração Térmica por UTE, utilizando
        as informações de duração dos patamares e datas de início dos estágios.
        """

        def _replace_scenario_info(
            df: pl.DataFrame,
            num_stages: int,
            num_scenarios: int,
            num_blocks: int,
            num_thermals: int,
        ) -> pl.DataFrame:
            """
            Substitui a informação de cenário por um intervalo fixo de `1`a
            `num_scenarios`, caso os dados fornecidos contenham informações
            de cenários de índices não regulares.
            """
            df = df.with_columns(
                pl.Series(
                    name=SCENARIO_COL,
                    values=np.tile(
                        np.tile(
                            np.repeat(
                                np.arange(1, num_scenarios + 1), num_blocks
                            ),
                            num_stages,
                        ),
                        num_thermals,
                    ),
                )
            )
            return df

        def _add_stage_info(
            df: pl.DataFrame,
            num_stages: int,
            num_scenarios: int,
            num_blocks: int,
            num_thermals: int,
            end_dates: list[datetime],
        ) -> pl.DataFrame:
            """
            Adiciona informações de estágio a um DataFrame, utilizando o
            número de valores de data distintos fornecidos para definir uma
            faixa ordenada de estágios de `1` a `num_stages`.
            """
            stages = np.arange(1, num_stages + 1)
            stages_to_df_column = np.tile(
                np.repeat(stages, num_scenarios * num_blocks), num_thermals
            )
            end_dates_to_df_column = np.tile(
                np.repeat(
                    np.array(end_dates, dtype="datetime64[ns]"),
                    num_scenarios * num_blocks,
                ),
                num_thermals,
            )
            df = df.with_columns(
                pl.Series(name=STAGE_COL, values=stages_to_df_column),
                pl.Series(name=END_DATE_COL, values=end_dates_to_df_column),
            )
            return df

        def _add_block_duration_info(
            df: pl.DataFrame,
            num_stages: int,
            num_scenarios: int,
            num_blocks: int,
            num_thermals: int,
            blocks: List[int],
            start_dates: List[datetime],
        ) -> pl.DataFrame:
            """
            Adiciona informações de duração de patamares a um DataFrame, utilizando
            as informações dos patamares e datas de início dos estágios.
            """
            df_block_lengths = Deck.block_lengths(uow)
            df_block_lengths = df_block_lengths.loc[
                df_block_lengths[BLOCK_COL].isin(blocks)
            ]
            block_durations = np.zeros(
                (num_scenarios * num_blocks * num_stages,), dtype=np.float64
            )
            data_block_size = num_scenarios * num_blocks
            for i, d in enumerate(start_dates):
                date_durations = df_block_lengths.loc[
                    df_block_lengths[START_DATE_COL] == d, VALUE_COL
                ].to_numpy()
                i_i = i * data_block_size
                i_f = i_i + data_block_size
                block_durations[i_i:i_f] = np.tile(
                    date_durations, num_scenarios
                )
            block_durations = np.tile(block_durations, num_thermals)
            df = df.with_columns(
                pl.Series(
                    name=BLOCK_DURATION_COL,
                    values=block_durations * STAGE_DURATION_HOURS,
                )
            )
            return df

        def _add_temporal_info(
            df: pl.DataFrame, uow: AbstractUnitOfWork
        ) -> pl.DataFrame:
            """
            Adiciona informação temporal a um DataFrame de síntese.
            """
            df = df.rename({
                "data": START_DATE_COL,
                "serie": SCENARIO_COL,
                "classe": THERMAL_CODE_COL,
            })
            df = df.sort(
                [THERMAL_CODE_COL, START_DATE_COL, SCENARIO_COL, BLOCK_COL],
            )
            num_stages = df[START_DATE_COL].unique().shape[0]
            num_scenarios = Deck.num_scenarios_final_simulation(uow)
            blocks = df[BLOCK_COL].unique().to_list()
            num_blocks = len(blocks)
            thermals = df[THERMAL_CODE_COL].unique().to_list()
            num_thermals = len(thermals)
            start_dates = Deck.internal_stages_starting_dates_final_simulation(
                uow
            )[:num_stages]
            end_dates = Deck.internal_stages_ending_dates_final_simulation(uow)[
                :num_stages
            ]

            df = _replace_scenario_info(
                df, num_stages, num_scenarios, num_blocks, num_thermals
            )
            df = _add_stage_info(
                df,
                num_stages,
                num_scenarios,
                num_blocks,
                num_thermals,
                end_dates,
            )
            df = _add_block_duration_info(
                df,
                num_stages,
                num_scenarios,
                num_blocks,
                num_thermals,
                blocks,
                start_dates,
            )
            return df

        if df is None:
            return None
        return _add_temporal_info(df, uow)

    @classmethod
    def _post_resolve_GTER_UTE_entity(
        cls, df: Optional[pl.DataFrame], uow: AbstractUnitOfWork
    ) -> Optional[pl.DataFrame]:
        """
        Realiza pós-processamento após a resolução da extração dados
        em um DataFrame de síntese da geração térmica por UTE.
        """
        if df is None:
            return df
        df = cls._resolve_temporal_resolution_GTER_UTE(df, uow)
        df = cls._resolve_starting_stage(df, uow)
        df_stats = calc_statistics(df)
        df = df.with_columns(
            pl.col(SCENARIO_COL).cast(str),
            pl.lit(False).alias(STATS_OR_SCENARIO_COL),
        )
        df_stats = df_stats.with_columns(
            pl.lit(True).alias(STATS_OR_SCENARIO_COL)
        )
        df_stats = df_stats[df.columns]
        df = pl.concat([df, df_stats])
        return df

    @classmethod
    def _resolve_GTER_UTE_entity(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        sbm_index: int,
        sbm_name: str,
    ) -> Optional[pl.DataFrame]:
        """
        Obtém os dados da síntese de operação para todas as UTE
        de um submercado a partir do arquivo de saída do NWLISTOP.
        """
        logger_name = f"{synthesis.variable.value}_{sbm_name}"
        logger = Log.configure_process_logger(uow.queue, logger_name, sbm_index)

        with uow:
            logger.debug(
                f"Processando arquivo do submercado: {sbm_index} - {sbm_name}"
            )
            df = uow.files.get_nwlistop(
                synthesis.variable,
                synthesis.spatial_resolution,
                submercado=sbm_index,
            )
        if df is not None:
            df = df.with_columns(pl.lit(sbm_index).alias(SUBMARKET_CODE_COL))
        return cls._post_resolve_GTER_UTE_entity(df, uow)

    @classmethod
    def _resolve_GTER_UTE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Obtem os dados da síntese de operação da geração térmica
        para as UTE de um submercado a partir dos arquivos
        de saída do NWLISTOP.
        """

        def _sort_thermals(
            s: OperationSynthesis, df: pl.DataFrame, uow: AbstractUnitOfWork
        ) -> pl.DataFrame:
            df = df.sort(s.spatial_resolution.sorting_synthesis_df_columns)
            return df

        submarkets = Deck.submarkets(uow).reset_index()
        real_submarkets = submarkets.loc[
            submarkets["ficticio"] == 0, :
        ].sort_values(SUBMARKET_CODE_COL)
        sbms_idx = real_submarkets[SUBMARKET_CODE_COL].unique()
        sbms_name = [
            real_submarkets.loc[
                real_submarkets[SUBMARKET_CODE_COL] == s, SUBMARKET_NAME_COL
            ].iloc[0]
            for s in sbms_idx
        ]

        n_procs = int(Settings().processors)
        synthesis = OperationSynthesis(
            variable=synthesis.variable,
            spatial_resolution=synthesis.spatial_resolution,
        )
        with time_and_log(
            message_root="Tempo para ler dados de UTE",
            logger=cls.logger,
        ):
            with Pool(processes=n_procs) as pool:
                async_res = {
                    idx: pool.apply_async(
                        cls._resolve_GTER_UTE_entity,
                        (uow, synthesis, idx, name),
                    )
                    for idx, name in zip(sbms_idx, sbms_name)
                }
                dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}

        df = cls._post_resolve(
            dfs,
            synthesis,
            uow,
            early_hooks=[_sort_thermals],
        )
        return df

    @classmethod
    def __resolve_UTE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pl.DataFrame]:
        """
        Resolve a síntese de operação para uma variável operativa
        de uma UTE a partir dos arquivos de saída do NWLISTOP.
        """
        if synthesis.variable == Variable.GERACAO_TERMICA:
            return cls._resolve_GTER_UTE(synthesis, uow)
        else:
            raise RuntimeError("Variável não suportada para UTEs")

    @classmethod
    def _resolve_spatial_resolution(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Despacha a função de resolução espacial para ler os dados a partir
        da saída do NWLISTOP, pós-processar e organizar em um DataFrame
        de acordo com a agregação desejada.
        """
        RESOLUTION_FUNCTION_MAP: Dict[SpatialResolution, Callable] = {
            SpatialResolution.SISTEMA_INTERLIGADO: cls.__resolve_SIN,
            SpatialResolution.SUBMERCADO: cls.__resolve_SBM,
            SpatialResolution.PAR_SUBMERCADOS: cls.__resolve_SBP,
            SpatialResolution.RESERVATORIO_EQUIVALENTE: cls.__resolve_REE,
            SpatialResolution.USINA_HIDROELETRICA: cls.__resolve_UHE,
            SpatialResolution.USINA_TERMELETRICA: cls.__resolve_UTE,
        }
        solver = RESOLUTION_FUNCTION_MAP[synthesis.spatial_resolution]
        res = solver(synthesis, uow)
        return res if res is not None else pl.DataFrame()

    @staticmethod
    def _resolve_starting_stage(
        df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Adiciona a informação do estágio inicial do caso aos dados,
        realizando um deslocamento da coluna "estagio" para que o
        estágio inicial do caso seja 1.

        Também elimina estágios incluídos como consequência do formato
        dos dados lidos, que pertencem ao período pré-estudo.
        """
        df = df.with_columns(
            pl.col(STAGE_COL) - (Deck.study_period_starting_month(uow) - 1)
        )
        df = df.filter(pl.col(STAGE_COL) > 0)
        return df

    @classmethod
    def _initial_stored_energy_df(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza a extração dos dados de energia armazenada inicial
        do `pmo.dat` em MWmes, agrupando para se obter
        os valores absoluto e percentual na agregação desejada.

        - nome_ree (`str`)
        - valor_MWmes (`float`)
        - valor_percentual (`float`)

        """

        max_column = "earmax"

        initial_stored_energy_df = Deck.initial_stored_energy(uow)
        initial_stored_energy_df[EER_CODE_COL] = Deck.eer_code_order(uow)
        if s.spatial_resolution == SpatialResolution.RESERVATORIO_EQUIVALENTE:
            return initial_stored_energy_df.rename(
                columns={EER_CODE_COL: GROUPING_TMP_COL}
            )

        initial_stored_energy_df[max_column] = (
            100
            * initial_stored_energy_df["valor_MWmes"]
            / initial_stored_energy_df["valor_percentual"]
        )
        if s.spatial_resolution == SpatialResolution.SUBMERCADO:
            eers_submarkets_map = Deck.eer_submarket_map(uow)
            initial_stored_energy_df.dropna(inplace=True)
            initial_stored_energy_df[GROUPING_TMP_COL] = (
                initial_stored_energy_df.apply(
                    lambda line: eers_submarkets_map.at[
                        line[EER_CODE_COL],
                        SUBMARKET_CODE_COL,
                    ],
                    axis=1,
                )
            )
        elif s.spatial_resolution == SpatialResolution.SISTEMA_INTERLIGADO:
            initial_stored_energy_df[GROUPING_TMP_COL] = 1
        initial_stored_energy_df = (
            initial_stored_energy_df.groupby(GROUPING_TMP_COL)
            .sum(numeric_only=True)
            .reset_index()
        )
        initial_stored_energy_df["valor_percentual"] = (
            100
            * initial_stored_energy_df["valor_MWmes"]
            / (initial_stored_energy_df[max_column])
        )
        return initial_stored_energy_df

    @classmethod
    def _get_from_cache(cls, s: OperationSynthesis) -> pl.DataFrame:
        """
        Extrai o resultado de uma síntese da cache caso exista, lançando
        um erro caso contrário.
        """
        if s in cls.CACHED_SYNTHESIS.keys():
            cls._log(f"Lendo do cache - {str(s)}", DEBUG)
            res = cls.CACHED_SYNTHESIS.get(s)
            if res is None:
                cls._log(f"Erro na leitura do cache - {str(s)}", ERROR)
                raise RuntimeError()
            return res
        else:
            cls._log(f"Erro na leitura do cache - {str(s)}", ERROR)
            raise RuntimeError()

    @classmethod
    def _stub_mappings(  # noqa
        cls, s: OperationSynthesis
    ) -> Optional[Callable]:
        """
        Obtem a função de resolução de cada síntese que foge ao
        fluxo de resolução padrão, por meio de um mapeamento de
        funções `stub` para cada variável e/ou resolução espacial.
        """
        f = None
        if s.variable == Variable.CUSTO_TOTAL:
            f = cls.__stub_CTO
        elif s.variable == Variable.ENERGIA_VERTIDA:
            f = cls.__stub_EVER
        elif all([
            s.variable
            in [
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            ],
            s.spatial_resolution
            in [
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                SpatialResolution.SUBMERCADO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ],
        ]):
            f = cls._stub_resolve_initial_stored_energy
        elif all([
            s.variable
            in [
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            ],
            s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
        ]):
            f = cls.__stub_resolve_initial_stored_volumes
        elif all([
            s.variable
            in [
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                Variable.VOLUME_AFLUENTE,
                Variable.VOLUME_INCREMENTAL,
                Variable.VOLUME_DEFLUENTE,
                Variable.VOLUME_VERTIDO,
                Variable.VOLUME_TURBINADO,
                Variable.VOLUME_RETIRADO,
                Variable.VOLUME_DESVIADO,
                Variable.VOLUME_EVAPORADO,
                Variable.VIOLACAO_EVAPORACAO,
                Variable.VIOLACAO_FPHA,
                Variable.VIOLACAO_POSITIVA_EVAPORACAO,
                Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
            ],
            s.spatial_resolution != SpatialResolution.USINA_HIDROELETRICA,
        ]):
            f = cls._hydro_resolution_variable_map
        elif all([
            s.variable
            in [
                Variable.VAZAO_AFLUENTE,
                Variable.VAZAO_INCREMENTAL,
                Variable.VAZAO_DEFLUENTE,
                Variable.VAZAO_VERTIDA,
                Variable.VAZAO_TURBINADA,
                Variable.VAZAO_RETIRADA,
                Variable.VAZAO_DESVIADA,
                Variable.VAZAO_EVAPORADA,
            ],
            s.spatial_resolution != SpatialResolution.USINA_HIDROELETRICA,
        ]):
            f = cls._flow_volume_hydro_variable_map
        elif all([
            s.variable
            in [
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            ],
            s.spatial_resolution != SpatialResolution.USINA_HIDROELETRICA,
        ]):
            f = cls._absolute_percent_volume_variable_map
        elif s.variable in [Variable.ENERGIA_DEFLUENCIA_MINIMA]:
            f = cls.__stub_EVMIN
        elif all([
            s.variable
            in [
                Variable.VAZAO_RETIRADA,
            ],
            s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
        ]):
            f = cls._convert_volume_to_flow
        elif all([
            s.variable
            in [
                Variable.VOLUME_AFLUENTE,
                Variable.VOLUME_INCREMENTAL,
                Variable.VOLUME_TURBINADO,
                Variable.VOLUME_VERTIDO,
                Variable.VOLUME_DESVIADO,
            ],
            s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
        ]):
            f = cls._convert_flow_to_volume
        elif all([
            s.variable == Variable.VAZAO_DEFLUENTE,
            s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
        ]):
            f = cls.__stub_QDEF
        elif all([
            s.variable == Variable.VOLUME_DEFLUENTE,
            s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
        ]):
            f = cls.__stub_VDEF
        elif all([
            s.variable == Variable.VIOLACAO_EVAPORACAO,
            s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
        ]):
            f = cls.__stub_VEVAP
        elif all([
            s.variable
            in [
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            ],
            s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
        ]):
            f = cls.__stub_EARM_UHE
        elif s.variable in [Variable.MERCADO, Variable.MERCADO_LIQUIDO]:
            f = cls.__stub_MER_MERL
        return f

    @classmethod
    def _resolve_stub(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Tuple[pl.DataFrame, bool]:
        """
        Realiza a resolução da síntese por meio de uma implementação
        alternativa ao fluxo natural de resolução (`stub`), caso esta seja
        uma variável que não possa ser resolvida diretamente a partir
        da extração de dados do NWLISTOP.
        """
        f = cls._stub_mappings(s)
        if f:
            df, is_stub = f(s, uow), True
        else:
            df, is_stub = pl.DataFrame(), False
        if is_stub:
            df = cls._post_resolve({"": df}, s, uow)
            df = cls._resolve_bounds(s, df, uow)
        return df, is_stub

    @classmethod
    def __get_from_cache_if_exists(cls, s: OperationSynthesis) -> pl.DataFrame:
        """
        Obtém uma síntese da operação a partir da cache, caso esta
        exista. Caso contrário, retorna um DataFrame vazio.
        """
        if s in cls.CACHED_SYNTHESIS.keys():
            return cls._get_from_cache(s)
        else:
            return pl.DataFrame()

    @classmethod
    def __store_in_cache_if_needed(
        cls, s: OperationSynthesis, df: pl.DataFrame
    ):
        """
        Adiciona um DataFrame com os dados de uma síntese à cache
        caso esta seja uma variável que deva ser armazenada.
        """
        if s in cls.SYNTHESIS_TO_CACHE:
            with time_and_log(
                message_root="Tempo para armazenamento na cache",
                logger=cls.logger,
            ):
                cls.CACHED_SYNTHESIS[s] = df

    @classmethod
    def _resolve_bounds(
        cls, s: OperationSynthesis, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza o cálculo dos limites superiores e inferiores para
        a síntese caso esta seja uma variável limitada.
        """
        with time_and_log(
            message_root="Tempo para calculo dos limites",
            logger=cls.logger,
        ):
            df = OperationVariableBounds.resolve_bounds(
                s,
                df,
                cls._get_ordered_entities(s),
                uow,
            )

        return df

    @classmethod
    def _resolve_synthesis(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Realiza a resolução de uma síntese, opcionalmente adicionando
        limites superiores e inferiores aos valores de cada linha.
        """
        df = cls._resolve_spatial_resolution(s, uow)
        if df is not None:
            df = cls._resolve_bounds(s, df, uow)
        return df

    @classmethod
    def _export_metadata(
        cls,
        success_synthesis: List[OperationSynthesis],
        uow: AbstractUnitOfWork,
    ):
        """
        Cria um DataFrame com os metadados das variáveis de síntese
        e realiza a exportação para um arquivo de metadados.
        """

        metadata_df = pl.DataFrame({
            "chave": [str(s) for s in success_synthesis],
            "nome_curto_variavel": [
                s.variable.short_name for s in success_synthesis
            ],
            "nome_longo_variavel": [
                s.variable.long_name for s in success_synthesis
            ],
            "nome_curto_agregacao": [
                s.spatial_resolution.value for s in success_synthesis
            ],
            "nome_longo_agregacao": [
                s.spatial_resolution.long_name for s in success_synthesis
            ],
            "unidade": [
                UNITS[s].value if s in UNITS else "" for s in success_synthesis
            ],
            "calculado": [
                s in SYNTHESIS_DEPENDENCIES for s in success_synthesis
            ],
            "limitado": [
                OperationVariableBounds.is_bounded(s) for s in success_synthesis
            ],
        })

        with uow:
            uow.export.synthetize_df(
                metadata_df, OPERATION_SYNTHESIS_METADATA_OUTPUT
            )

    @classmethod
    def _add_synthesis_stats(cls, s: OperationSynthesis, df: pl.DataFrame):
        """
        Adiciona um DataFrame com estatísticas de uma síntese ao
        DataFrame de estatísticas da agregação espacial em questão.
        """
        df = df.with_columns(pl.lit(s.variable.value).alias(VARIABLE_COL))

        if s.spatial_resolution not in cls.SYNTHESIS_STATS:
            cls.SYNTHESIS_STATS[s.spatial_resolution] = [df]
        else:
            cls.SYNTHESIS_STATS[s.spatial_resolution].append(df)

    @classmethod
    def _export_scenario_synthesis(
        cls, s: OperationSynthesis, df: pl.DataFrame, uow: AbstractUnitOfWork
    ):
        """
        Realiza a exportação dos dados para uma síntese da
        operação desejada. Opcionalmente, os dados são armazenados
        em cache para uso futuro e as estatísticas são adicionadas
        ao DataFrame de estatísticas da agregação espacial em questão.
        """
        filename = str(s)
        with time_and_log(
            message_root="Tempo para preparacao para exportacao",
            logger=cls.logger,
        ):
            scenarios_df = df.filter(~pl.col(STATS_OR_SCENARIO_COL))
            stats_df = df.filter(pl.col(STATS_OR_SCENARIO_COL))
            scenarios_df = scenarios_df.with_columns(
                pl.col(SCENARIO_COL).cast(int)
            )
            if stats_df.shape[0] == 0:
                scenarios_df = scenarios_df.sort(
                    s.spatial_resolution.sorting_synthesis_df_columns
                )
                stats_df = calc_statistics(scenarios_df)
            stats_df = stats_df.drop([STATS_OR_SCENARIO_COL])
            cls._add_synthesis_stats(s, stats_df)
            cls.__store_in_cache_if_needed(s, scenarios_df)
        with time_and_log(
            message_root="Tempo para exportacao dos dados", logger=cls.logger
        ):
            with uow:
                scenarios_df = scenarios_df.drop([STATS_OR_SCENARIO_COL])
                scenarios_df = scenarios_df[
                    s.spatial_resolution.all_synthesis_df_columns
                ]
                uow.export.synthetize_df(scenarios_df, filename)

    @classmethod
    def _export_stats(
        cls,
        uow: AbstractUnitOfWork,
    ):
        """
        Realiza a exportação dos dados de estatísticas de síntese
        da operação. As estatísticas são exportadas para um arquivo
        único por agregação espacial, de nome
        `OPERACAO_{agregacao}`.
        """
        for res, dfs in cls.SYNTHESIS_STATS.items():
            with uow:
                df_col_order = dfs[0].columns
                dfs = [df[df_col_order] for df in dfs]
                df = pl.concat(dfs)
                df_columns = df.columns
                columns_without_variable = [
                    c for c in df_columns if c != VARIABLE_COL
                ]
                df = df[[VARIABLE_COL] + columns_without_variable]
                df = df.sort(res.sorting_synthesis_df_columns)
                uow.export.synthetize_df(
                    df, f"{OPERATION_SYNTHESIS_STATS_ROOT}_{res.value}"
                )

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: List[str], uow: AbstractUnitOfWork
    ) -> List[OperationSynthesis]:
        """
        Realiza o pré-processamento das variáveis de síntese fornecidas,
        filtrando as válidas para o caso em questão e adicionando dependências
        caso a síntese de operação de uma variável dependa de outra.
        """
        try:
            if len(variables) == 0:
                synthesis_variables = cls._default_args()
            else:
                all_variables = cls._match_wildcards(variables)
                synthesis_variables = cls._process_variable_arguments(
                    all_variables
                )
            valid_synthesis = cls._filter_valid_variables(
                synthesis_variables, uow
            )
            synthesis_with_dependencies = cls._add_synthesis_dependencies(
                valid_synthesis
            )
        except Exception as e:
            print_exc()
            cls._log(str(e), ERROR)
            cls._log("Erro no pré-processamento das variáveis", ERROR)
            synthesis_with_dependencies = []
        return synthesis_with_dependencies

    @classmethod
    def _synthetize_single_variable(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[OperationSynthesis]:
        """
        Realiza a síntese de operação para uma variável
        fornecida.
        """
        filename = str(s)
        with time_and_log(
            message_root=f"Tempo para sintese de {filename}",
            logger=cls.logger,
        ):
            try:
                found_synthesis = False
                cls._log(f"Realizando sintese de {filename}")
                df = cls.__get_from_cache_if_exists(s)
                is_stub = cls._stub_mappings(s) is not None
                if df.shape[0] == 0:
                    df, is_stub = cls._resolve_stub(s, uow)
                    if not is_stub:
                        df = cls._resolve_synthesis(s, uow)
                if df is not None:
                    if df.shape[0] > 0:
                        found_synthesis = True
                        cls._export_scenario_synthesis(s, df, uow)
                        return s
                if not found_synthesis:
                    cls._log(
                        "Nao foram encontrados dados"
                        + f" para a sintese de {filename}",
                        WARNING,
                    )
                return None
            except Exception as e:
                print_exc()
                cls._log(str(e), ERROR)
                cls._log(
                    f"Nao foi possível realizar a sintese de: {filename}",
                    ERROR,
                )
                return None

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        """
        Realiza a síntese de operação para as variáveis operativas
        fornecidas, na agregação desejada. As variáveis são pré-processadas
        para filtrar as válidas para o caso em questão,
        e então são resolvidas de acordo com a síntese.
        """
        cls.logger = logging.getLogger("main")
        uow.subdir = OPERATION_SYNTHESIS_SUBDIR
        with time_and_log(
            message_root="Tempo para sintese da operacao",
            logger=cls.logger,
        ):
            synthesis_with_dependencies = cls._preprocess_synthesis_variables(
                variables, uow
            )
            success_synthesis: List[OperationSynthesis] = []
            for s in synthesis_with_dependencies:
                r = cls._synthetize_single_variable(s, uow)
                if r:
                    success_synthesis.append(r)

            cls._export_stats(uow)
            cls._export_metadata(success_synthesis, uow)
