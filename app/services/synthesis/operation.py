from typing import Callable, Dict, List, Tuple, Optional, TypeVar, Any
import pandas as pd  # type: ignore
import numpy as np
import logging
from datetime import datetime
from traceback import print_exc
from logging import INFO, WARNING, ERROR
from multiprocessing import Pool
from app.utils.graph import Graph
from app.utils.log import Log
from app.utils.timing import time_and_log
from app.utils.regex import match_variables_with_wildcards
from app.utils.operations import fast_group_df, quantile_scenario_labels
from app.model.settings import Settings
from app.services.deck.bounds import OperationVariableBounds
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.model.operation.variable import Variable
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.operationsynthesis import (
    OperationSynthesis,
    SUPPORTED_SYNTHESIS,
    SYNTHESIS_DEPENDENCIES,
    UNITS,
)
from app.internal.constants import (
    HM3_M3S_MONTHLY_FACTOR,
    STAGE_DURATION_HOURS,
    STAGE_COL,
    START_DATE_COL,
    END_DATE_COL,
    SCENARIO_COL,
    BLOCK_COL,
    BLOCK_DURATION_COL,
    VALUE_COL,
    OPERATION_SYNTHESIS_COMMON_COLUMNS,
    EER_CODE_COL,
    EER_NAME_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    THERMAL_CODE_COL,
    THERMAL_NAME_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_SOURCE_NAME_COL,
    EXCHANGE_TARGET_CODE_COL,
    EXCHANGE_TARGET_NAME_COL,
    VARIABLE_COL,
    GROUPING_TMP_COL,
    PRODUCTIVITY_TMP_COL,
    LOWER_BOUND_COL,
    UPPER_BOUND_COL,
    STRING_DF_TYPE,
    OPERATION_SYNTHESIS_METADATA_OUTPUT,
    OPERATION_SYNTHESIS_STATS_ROOT,
    OPERATION_SYNTHESIS_SUBDIR,
    QUANTILES_FOR_STATISTICS,
    STATS_OR_SCENARIO_COL,
)


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
    CACHED_SYNTHESIS: Dict[OperationSynthesis, pd.DataFrame] = {}
    ORDERED_SYNTHESIS_ENTITIES: Dict[OperationSynthesis, Dict[str, list]] = {}

    # Estatísticas das sínteses são armazenadas separadamente
    SYNTHESIS_STATS: Dict[SpatialResolution, List[pd.DataFrame]] = {}

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
        has_wind = Deck.considera_geracao_eolica(uow)
        simulation_with_hydro = Deck.agregacao_simulacao_final(uow)
        policy_with_hydro = Deck.politica_hibrida(uow)
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
            if all(
                [
                    v.variable == Variable.VALOR_AGUA,
                    v.spatial_resolution
                    == SpatialResolution.USINA_HIDROELETRICA,
                    not has_hydro,
                ]
            ):
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
        cls, df: pd.DataFrame, cols: List[str]
    ):
        """
        Extrai valores únicos na ordem em que aparecem para um
        conjunto de colunas de um DataFrame.
        """
        return {col: df[col].unique().tolist() for col in cols}

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
        df: Optional[pd.DataFrame],
        s: OperationSynthesis,
        entity_column_values: Dict[str, Any],
        uow: AbstractUnitOfWork,
        internal_stubs: Dict[Variable, Callable] = {},
    ) -> Optional[pd.DataFrame]:
        """
        Realiza pós-processamento após a resolução da extração dados
        em um DataFrame de síntese extraído do NWLISTOP.
        """
        if df is None:
            return df
        df = cls._resolve_temporal_resolution(df, uow)
        for col, val in entity_column_values.items():
            df[col] = val
            if col in [
                HYDRO_NAME_COL,
                THERMAL_NAME_COL,
                EER_NAME_COL,
                SUBMARKET_NAME_COL,
            ]:
                df = df.astype({col: STRING_DF_TYPE})
        df = cls._resolve_starting_stage(df, uow)
        if s.variable in internal_stubs:
            df = internal_stubs[s.variable](df, uow)
        df_stats = cls._calc_statistics(df)
        df[STATS_OR_SCENARIO_COL] = False
        df_stats[STATS_OR_SCENARIO_COL] = True
        df = pd.concat([df, df_stats], ignore_index=True)
        df = df.astype({SCENARIO_COL: STRING_DF_TYPE})
        return df

    @classmethod
    def _post_resolve(
        cls,
        resolve_responses: Dict[str, Optional[pd.DataFrame]],
        s: OperationSynthesis,
        uow: AbstractUnitOfWork,
        early_hooks: List[Callable] = [],
        late_hooks: List[Callable] = [],
    ) -> Optional[pd.DataFrame]:
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
                df = pd.concat(valid_dfs, ignore_index=True)
            else:
                return None

            spatial_resolution = s.spatial_resolution

            for c in early_hooks:
                df = c(s, df, uow)

            df = df.sort_values(
                spatial_resolution.sorting_synthesis_df_columns
            ).reset_index(drop=True)

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

    @classmethod
    def _resolve_temporal_resolution(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona informação temporal a um DataFrame de síntese, utilizando
        as informações de duração dos patamares e datas de início dos estágios.
        """

        def _replace_scenario_info(
            df: pd.DataFrame,
            num_stages: int,
            num_scenarios: int,
            num_blocks: int,
        ) -> pd.DataFrame:
            """
            Substitui a informação de cenário por um intervalo fixo de `1`a
            `num_scenarios`, caso os dados fornecidos contenham informações
            de cenários de índices não regulares.
            """
            df[SCENARIO_COL] = np.tile(
                np.repeat(np.arange(1, num_scenarios + 1), num_blocks),
                num_stages,
            )
            return df

        def _add_stage_info(
            df: pd.DataFrame,
            num_stages: int,
            num_scenarios: int,
            num_blocks: int,
            end_dates: List[datetime],
        ) -> pd.DataFrame:
            """
            Adiciona informações de estágio a um DataFrame, utilizando o
            número de valores de data distintos fornecidos para definir uma
            faixa ordenada de estágios de `1` a `num_stages`.
            """
            stages = np.arange(1, num_stages + 1)
            stages_to_df_column = np.repeat(stages, num_scenarios * num_blocks)
            end_dates_to_df_column: np.ndarray = np.repeat(
                np.array(end_dates), num_scenarios * num_blocks
            )
            df[STAGE_COL] = stages_to_df_column
            df[END_DATE_COL] = end_dates_to_df_column
            return df

        def _add_block_duration_info(
            df: pd.DataFrame,
            num_stages: int,
            num_scenarios: int,
            num_blocks: int,
            blocks: List[int],
            start_dates: List[datetime],
        ) -> pd.DataFrame:
            """
            Adiciona informações de duração de patamares a um DataFrame, utilizando
            as informações dos patamares e datas de início dos estágios.
            """
            df_block_lengths = Deck.duracao_mensal_patamares(uow)
            df_block_lengths = df_block_lengths.loc[
                df_block_lengths[BLOCK_COL].isin(blocks)
            ]
            block_durations = np.zeros(
                (num_scenarios * num_blocks * num_stages,), dtype=np.float64
            )
            data_block_size = num_scenarios * num_blocks
            for i, d in enumerate(start_dates):
                date_durations = df_block_lengths.loc[
                    df_block_lengths["data"] == d, "valor"
                ].to_numpy()
                i_i = i * data_block_size
                i_f = i_i + data_block_size
                block_durations[i_i:i_f] = np.tile(
                    date_durations, num_scenarios
                )
            df[BLOCK_DURATION_COL] = block_durations * STAGE_DURATION_HOURS
            return df

        def _add_temporal_info(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            """
            Adiciona informação temporal a um DataFrame de síntese.
            """
            df = df.rename(
                columns={"data": START_DATE_COL, "serie": SCENARIO_COL}
            ).copy()
            df.sort_values(
                [START_DATE_COL, SCENARIO_COL, BLOCK_COL], inplace=True
            )
            num_stages = df[START_DATE_COL].unique().shape[0]
            num_scenarios = Deck.numero_cenarios_simulacao_final(uow)
            blocks = df[BLOCK_COL].unique().tolist()
            num_blocks = len(blocks)
            start_dates = Deck.datas_inicio_estagios_internos_sim_final(uow)[
                :num_stages
            ]
            end_dates = Deck.datas_fim_estagios_internos_sim_final(uow)[
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
    ) -> Optional[pd.DataFrame]:
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
    ) -> Optional[pd.DataFrame]:
        """
        Obtem os dados da síntese de operação para um submercado
        a partir do arquivo de saída do NWLISTOP.
        """

        logger_name = f"{synthesis.variable.value}_{sbm_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, sbm_index
        )
        with uow:
            logger.info(
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
                SUBMARKET_NAME_COL: sbm_name,
            },
            uow,
        )

    @classmethod
    def __resolve_SBM(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pd.DataFrame]:
        """
        Resolve a síntese de operação para uma variável operativa
        de um submercado a partir dos arquivos de saída do NWLISTOP.
        """

        submarkets = Deck.submercados(uow)
        real_submarkets = submarkets.loc[
            submarkets["ficticio"] == 0, :
        ].sort_values("nome_submercado")
        sbms_idx = real_submarkets["codigo_submercado"].unique()
        sbms_name = [
            real_submarkets.loc[
                real_submarkets["codigo_submercado"] == s, "nome_submercado"
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
    ) -> Optional[pd.DataFrame]:
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
            logger.info(
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
                EXCHANGE_SOURCE_NAME_COL: sbm1_name,
                EXCHANGE_TARGET_CODE_COL: sbm2_index,
                EXCHANGE_TARGET_NAME_COL: sbm2_name,
            },
            uow,
        )

    @classmethod
    def __resolve_SBP(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pd.DataFrame]:
        """
        Resolve a síntese de operação para uma variável operativa
        de um par de submercados a partir dos arquivos de saída do NWLISTOP.
        """

        submarkets = Deck.submercados(uow)
        sbms_idx = submarkets["codigo_submercado"].unique()
        sbms_name = [
            submarkets.loc[
                submarkets["codigo_submercado"] == s, "nome_submercado"
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
    ) -> Optional[pd.DataFrame]:
        """
        Obtem os dados da síntese de operação para um REE
        a partir do arquivo de saída do NWLISTOP.
        """

        logger_name = f"{synthesis.variable.value}_{ree_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, ree_index
        )
        with uow:
            logger.info(
                f"Processando arquivo do REE: {ree_index} - {ree_name}"
            )
            df = uow.files.get_nwlistop(
                synthesis.variable,
                synthesis.spatial_resolution,
                ree=ree_index,
            )

        aux_df = Deck.hydro_eer_submarket_map(uow)

        return cls._post_resolve_entity(
            df,
            synthesis,
            {
                EER_CODE_COL: ree_index,
                EER_NAME_COL: ree_name,
                SUBMARKET_CODE_COL: aux_df.loc[
                    aux_df[EER_NAME_COL] == ree_name, SUBMARKET_CODE_COL
                ].iloc[0],
                SUBMARKET_NAME_COL: aux_df.loc[
                    aux_df[EER_NAME_COL] == ree_name, SUBMARKET_NAME_COL
                ].iloc[0],
            },
            uow,
        )

    @classmethod
    def __resolve_REE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pd.DataFrame]:
        """
        Resolve a síntese de operação para uma variável operativa
        de um REE a partir dos arquivos de saída do NWLISTOP.
        """

        def _add_submarket_to_eer_synthesis(
            s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            if SUBMARKET_NAME_COL in df.columns:
                return df
            else:
                with time_and_log(
                    message_root="Tempo para adicionar SBM dos REE",
                    logger=cls.logger,
                ):
                    eers = Deck.rees(uow).set_index("nome")
                    submarkets = Deck.submercados(uow)
                    submarkets = submarkets.drop_duplicates(
                        ["codigo_submercado", "nome_submercado"]
                    ).set_index("codigo_submercado")
                    entities = cls._get_ordered_entities(s)
                    eers_df = entities[EER_NAME_COL]
                    submarkets_codes_df = [
                        eers.at[r, "submercado"] for r in eers_df
                    ]
                    submarkets_names_df = [
                        submarkets.at[c, "nome_submercado"]
                        for c in submarkets_codes_df
                    ]
                    num_blocks = len(entities[BLOCK_COL])
                    num_stages = len(entities[STAGE_COL])
                    num_scenarios = len(entities[SCENARIO_COL])
                    df[SUBMARKET_NAME_COL] = np.repeat(
                        submarkets_names_df,
                        num_scenarios * num_stages * num_blocks,
                    )
                    df = df.astype({SUBMARKET_NAME_COL: STRING_DF_TYPE})
                    return df[
                        [EER_NAME_COL, SUBMARKET_NAME_COL]
                        + OPERATION_SYNTHESIS_COMMON_COLUMNS
                    ]

        rees = Deck.rees(uow).sort_values("nome")
        rees_idx = rees["codigo"]
        rees_name = rees["nome"]

        n_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para ler dados de REE", logger=cls.logger
        ):
            with Pool(processes=n_procs) as pool:
                async_res = {
                    idx: pool.apply_async(
                        cls._resolve_REE_entity, (uow, synthesis, idx, name)
                    )
                    for idx, name in zip(rees_idx, rees_name)
                }
                dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}

        df = cls._post_resolve(
            dfs,
            synthesis,
            uow,
            late_hooks=[_add_submarket_to_eer_synthesis],
        )
        return df

    @classmethod
    def _resolve_UHE_entity(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        uhe_index: int,
        uhe_name: str,
    ) -> Optional[pd.DataFrame]:
        """
        Obtem os dados da síntese de operação para uma UHE
        a partir do arquivo de saída do NWLISTOP.
        """

        def _calc_block_0_weighted_mean(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            """
            Calcula um valor médio ponderado para o estágio a partir
            de valores fornecidos por patamar de alguma variável operativa
            de uma UHE.
            """
            n_blocks = Deck.numero_patamares(uow)
            unique_cols_for_block_0 = [HYDRO_NAME_COL, STAGE_COL, SCENARIO_COL]
            df_block_0 = df.copy()
            df_block_0[VALUE_COL] = (
                df_block_0[VALUE_COL] * df_block_0[BLOCK_DURATION_COL]
            ) / STAGE_DURATION_HOURS
            df_base = df.iloc[::n_blocks].reset_index(drop=True).copy()
            df_base[BLOCK_COL] = 0
            df_base[BLOCK_DURATION_COL] = STAGE_DURATION_HOURS
            arr = df_block_0[VALUE_COL].to_numpy()
            n_linhas = arr.shape[0]
            n_elementos_distintos = n_linhas // n_blocks
            df_base[VALUE_COL] = arr.reshape((n_elementos_distintos, -1)).sum(
                axis=1
            )
            df_block_0 = pd.concat(
                [df_block_0, df_base], ignore_index=True, copy=True
            )
            df_block_0 = df_block_0.sort_values(
                unique_cols_for_block_0 + [BLOCK_COL]
            )
            return df_block_0

        logger_name = f"{synthesis.variable.value}_{uhe_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, uhe_index
        )

        with uow:
            logger.info(
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
        }
        aux_df = Deck.hydro_eer_submarket_map(uow)
        return cls._post_resolve_entity(
            df,
            synthesis,
            {
                HYDRO_CODE_COL: uhe_index,
                HYDRO_NAME_COL: uhe_name,
                EER_CODE_COL: aux_df.at[uhe_name, EER_CODE_COL],
                EER_NAME_COL: aux_df.at[uhe_name, EER_NAME_COL],
                SUBMARKET_CODE_COL: aux_df.at[uhe_name, SUBMARKET_CODE_COL],
                SUBMARKET_NAME_COL: aux_df.at[uhe_name, SUBMARKET_NAME_COL],
            },
            uow,
            internal_stubs=internal_stubs,
        )

    @classmethod
    def __resolve_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pd.DataFrame]:
        """
        Resolve a síntese de operação para uma variável operativa
        de uma UHE a partir dos arquivos de saída do NWLISTOP.
        """

        def _limit_stages_with_hydro(
            s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = df.loc[
                df[START_DATE_COL]
                < Deck.data_fim_estagios_individualizados_sim_final(uow),
            ].reset_index(drop=True)
            return df

        uhes = Deck.uhes(uow).sort_values("nome_usina")
        uhes_idx = uhes["codigo_usina"]
        uhes_name = uhes["nome_usina"]

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
                    for idx, name in zip(uhes_idx, uhes_name)
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
    ) -> pd.DataFrame:
        """
        Realiza a conversão de síntese de volume para vazão.
        """
        variable_map = {
            Variable.VAZAO_VERTIDA: Variable.VOLUME_VERTIDO,
            Variable.VAZAO_TURBINADA: Variable.VOLUME_TURBINADO,
            Variable.VAZAO_DESVIADA: Variable.VOLUME_DESVIADO,
            Variable.VAZAO_RETIRADA: Variable.VOLUME_RETIRADO,
        }
        volume_synthesis = OperationSynthesis(
            variable_map[synthesis.variable],
            synthesis.spatial_resolution,
        )
        df = cls._get_from_cache(volume_synthesis)
        df.loc[:, VALUE_COL] = (
            df[VALUE_COL]
            * HM3_M3S_MONTHLY_FACTOR
            * STAGE_DURATION_HOURS
            / df[BLOCK_DURATION_COL]
        )

        return df

    @classmethod
    def _convert_flow_to_volume(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza a conversão de síntese de vazão para volume.
        """
        variable_map = {
            Variable.VOLUME_AFLUENTE: Variable.VAZAO_AFLUENTE,
            Variable.VOLUME_INCREMENTAL: Variable.VAZAO_INCREMENTAL,
        }
        flow_synthesis = OperationSynthesis(
            variable_map[synthesis.variable],
            synthesis.spatial_resolution,
        )
        df = cls._get_from_cache(flow_synthesis)
        df.loc[:, VALUE_COL] = (
            df[VALUE_COL]
            * df[BLOCK_DURATION_COL]
            / (HM3_M3S_MONTHLY_FACTOR * STAGE_DURATION_HOURS)
        )
        return df

    @classmethod
    def __stub_QDEF(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
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

        spilled_df.loc[:, VALUE_COL] = (
            turbined_df[VALUE_COL].to_numpy()
            + spilled_df[VALUE_COL].to_numpy()
        )
        return spilled_df

    @classmethod
    def __stub_VDEF(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
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

        spilled_df.loc[:, VALUE_COL] = (
            turbined_df[VALUE_COL].to_numpy()
            + spilled_df[VALUE_COL].to_numpy()
        )
        return spilled_df

    @classmethod
    def __stub_VEVAP(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
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

        positive_df.loc[:, VALUE_COL] = (
            negative_df[VALUE_COL].to_numpy()
            + positive_df[VALUE_COL].to_numpy()
        )
        return positive_df

    @classmethod
    def __stub_EVER(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
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

        reservoir_df.loc[:, VALUE_COL] = (
            run_of_river_df[VALUE_COL].to_numpy()
            + reservoir_df[VALUE_COL].to_numpy()
        )

        return reservoir_df

    @classmethod
    def _hydro_resolution_variable_map(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
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
    ) -> pd.DataFrame:
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
    ) -> pd.DataFrame:
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
    ) -> pd.DataFrame:
        """
        Resolve a síntese de energias armazenadas iniciais para um REE,
        SBM ou para o SIN, processando informações existentes no `pmo.dat`
        e adequando o formato para serem utilizadas em conjunto com os
        resultados das energias armazenadas finais do NWLISTOP.
        """

        def _get_final_storage_synthesis_data(
            synthesis: OperationSynthesis,
        ) -> Tuple[pd.DataFrame, dict]:

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
            initial_storage_data = cls._initial_stored_energy_df(
                synthesis, uow
            )
            value_column = (
                "valor_MWmes"
                if synthesis.variable == earmi
                else "valor_percentual"
            )
            groups = entities.get(
                grouping_col_map[synthesis.spatial_resolution]
            ) or [1]
            initial_storage_values = (
                initial_storage_data.set_index(GROUPING_TMP_COL)
                .loc[groups, value_column]
                .to_numpy()
            )
            return initial_storage_values

        def _get_initial_stage_indices(entities: dict) -> np.ndarray:
            groups = entities.get(
                grouping_col_map[synthesis.spatial_resolution]
            ) or [1]
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
            df: pd.DataFrame,
            indices: np.ndarray,
            values: np.ndarray,
            entities: dict,
        ) -> pd.DataFrame:
            scenarios = [
                s for s in entities[SCENARIO_COL] if str(s).isnumeric()
            ]
            num_scenarios = len(scenarios)
            initial_storage_df = df.copy()
            initial_storage_values_df = initial_storage_df[
                VALUE_COL
            ].to_numpy()
            initial_storage_values_df[num_scenarios:] = (
                initial_storage_values_df[:-num_scenarios]
            )
            initial_storage_values_df[indices] = np.repeat(
                values, num_scenarios
            )
            initial_storage_df[VALUE_COL] = initial_storage_values_df
            initial_storage_df[VALUE_COL] = initial_storage_df[
                VALUE_COL
            ].fillna(0.0)
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
            SpatialResolution.RESERVATORIO_EQUIVALENTE: EER_NAME_COL,
            SpatialResolution.SUBMERCADO: SUBMARKET_NAME_COL,
            SpatialResolution.SISTEMA_INTERLIGADO: None,
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
    def __stub_resolve_initial_stored_volumes(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Resolve a síntese de volumes armazenados iniciais para uma UHE,
        processando informações existentes no `pmo.dat` e adequando
        o formato para serem utilizadas em conjunto com os resultados
        dos volumes armazenados finais do NWLISTOP.
        """

        def _get_final_storage_synthesis_data(
            synthesis: OperationSynthesis,
        ) -> Tuple[pd.DataFrame, dict]:

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
            initial_storage_data = Deck.volume_armazenado_inicial(uow)
            value_column = (
                "valor_hm3"
                if synthesis.variable == varmi
                else "valor_percentual"
            )
            if synthesis.variable == varmi:
                hidr = Deck.hidr(uow)
                initial_storage_data[value_column] += hidr.loc[
                    initial_storage_data[HYDRO_CODE_COL].to_numpy(),
                    "volume_minimo",
                ].to_numpy()
            initial_storage_data = initial_storage_data.loc[
                initial_storage_data[HYDRO_CODE_COL].isin(hydros)
            ]
            initial_storage_values = (
                initial_storage_data.set_index(HYDRO_CODE_COL)
                .loc[hydros, value_column]
                .to_numpy()
            )
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
            df: pd.DataFrame,
            indices: np.ndarray,
            values: np.ndarray,
            entities: dict,
        ) -> pd.DataFrame:
            scenarios = [
                s for s in entities[SCENARIO_COL] if str(s).isnumeric()
            ]
            num_scenarios = len(scenarios)
            initial_storage_df = df.copy()
            initial_storage_values_df = initial_storage_df[
                VALUE_COL
            ].to_numpy()
            initial_storage_values_df[num_scenarios:] = (
                initial_storage_values_df[:-num_scenarios]
            )
            initial_storage_values_df[indices] = np.repeat(
                values, num_scenarios
            )
            initial_storage_df[VALUE_COL] = initial_storage_values_df
            initial_storage_df[VALUE_COL] = initial_storage_df[
                VALUE_COL
            ].fillna(0.0)
            return initial_storage_df

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
        cls, df: pd.DataFrame, entities: dict, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        hydro_df = Deck.uhes(uow)
        # Monta a lista de arestas e constroi o grafo
        # direcionado das usinas (JUSANTE -> MONTANTE)
        uhes = entities[HYDRO_CODE_COL]
        np_edges = list(
            hydro_df.loc[
                hydro_df[HYDRO_CODE_COL].isin(uhes),
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

            net_drop_df = net_drop_df.sort_values(
                [HYDRO_CODE_COL, STAGE_COL, BLOCK_COL]
            )
            stored_volume_df = stored_volume_df.sort_values(
                [HYDRO_CODE_COL, STAGE_COL, BLOCK_COL]
            )

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
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Gera cenários para uma síntese que é feita apenas para um
        dos cenários obtidos no NWLISTOP.
        """
        num_scenarios = Deck.numero_cenarios_simulacao_final(uow)
        num_entries = df.shape[0]
        df = pd.concat([df] * num_scenarios, ignore_index=True)
        df["serie"] = np.repeat(np.arange(1, num_scenarios + 1), num_entries)
        return df

    @classmethod
    def _resolve_SBM_entity_MER_MERL(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        sbm_index: int,
        sbm_name: str,
    ) -> Optional[pd.DataFrame]:
        """
        Obtem os dados da síntese de operação para um submercado
        a partir do arquivo de saída do NWLISTOP, especificamente
        para as variáveis MER e MERL.
        """

        logger_name = f"{synthesis.variable.value}_{sbm_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, sbm_index
        )
        with uow:
            logger.info(
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
            {SUBMARKET_CODE_COL: sbm_index, SUBMARKET_NAME_COL: sbm_name},
            uow,
        )

    @classmethod
    def __stub_MER_MERL(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o processamento da síntese de mercado de energia,
        adequando o formato para ser compatível com as demais saídas
        do NWLISTOP.
        """

        def _resolve_SIN_MER_MERL(
            synthesis: OperationSynthesis, uow: AbstractUnitOfWork
        ) -> Optional[pd.DataFrame]:
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
        ) -> Optional[pd.DataFrame]:

            submarkets = Deck.submercados(uow)
            real_submarkets = submarkets.loc[
                submarkets["ficticio"] == 0, :
            ].sort_values("nome_submercado")
            sbms_idx = real_submarkets["codigo_submercado"].unique()
            sbms_name = [
                real_submarkets.loc[
                    real_submarkets["codigo_submercado"] == s,
                    "nome_submercado",
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
        return res if res is not None else pd.DataFrame()

    @classmethod
    def __stub_EVMIN(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
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

        goal_df.loc[:, VALUE_COL] = (
            goal_df[VALUE_COL].to_numpy() - violation_df[VALUE_COL].to_numpy()
        )
        return goal_df

    @classmethod
    def _resolve_temporal_resolution_GTER_UTE(
        cls, df: Optional[pd.DataFrame], uow: AbstractUnitOfWork
    ) -> Optional[pd.DataFrame]:
        """
        Adiciona informação temporal a um DataFrame de síntese para a variável
        de Geração Térmica por UTE, utilizando
        as informações de duração dos patamares e datas de início dos estágios.
        """

        def _replace_scenario_info(
            df: pd.DataFrame,
            num_stages: int,
            num_scenarios: int,
            num_blocks: int,
            num_thermals: int,
        ) -> pd.DataFrame:
            """
            Substitui a informação de cenário por um intervalo fixo de `1`a
            `num_scenarios`, caso os dados fornecidos contenham informações
            de cenários de índices não regulares.
            """
            df[SCENARIO_COL] = np.tile(
                np.tile(
                    np.repeat(np.arange(1, num_scenarios + 1), num_blocks),
                    num_stages,
                ),
                num_thermals,
            )
            return df

        def _add_stage_info(
            df: pd.DataFrame,
            num_stages: int,
            num_scenarios: int,
            num_blocks: int,
            num_thermals: int,
            end_dates: np.ndarray,
        ) -> pd.DataFrame:
            """
            Adiciona informações de estágio a um DataFrame, utilizando o
            número de valores de data distintos fornecidos para definir uma
            faixa ordenada de estágios de `1` a `num_stages`.
            """
            stages = np.arange(1, num_stages + 1)
            stages_to_df_column = np.tile(
                np.repeat(stages, num_scenarios * num_blocks), num_thermals
            )
            end_dates_to_df_column: np.ndarray = np.tile(
                np.repeat(end_dates, num_scenarios * num_blocks), num_thermals
            )
            df[STAGE_COL] = stages_to_df_column
            df[END_DATE_COL] = end_dates_to_df_column
            return df

        def _add_block_duration_info(
            df: pd.DataFrame,
            num_stages: int,
            num_scenarios: int,
            num_blocks: int,
            num_thermals: int,
            blocks: List[int],
            start_dates: List[datetime],
        ) -> pd.DataFrame:
            """
            Adiciona informações de duração de patamares a um DataFrame, utilizando
            as informações dos patamares e datas de início dos estágios.
            """
            df_block_lengths = Deck.duracao_mensal_patamares(uow)
            df_block_lengths = df_block_lengths.loc[
                df_block_lengths["patamar"].isin(blocks)
            ]
            block_durations = np.zeros(
                (num_scenarios * num_blocks * num_stages,), dtype=np.float64
            )
            data_block_size = num_scenarios * num_blocks
            for i, d in enumerate(start_dates):
                date_durations = df_block_lengths.loc[
                    df_block_lengths["data"] == d, "valor"
                ].to_numpy()
                i_i = i * data_block_size
                i_f = i_i + data_block_size
                block_durations[i_i:i_f] = np.tile(
                    date_durations, num_scenarios
                )
            block_durations = np.tile(block_durations, num_thermals)
            df[BLOCK_DURATION_COL] = STAGE_DURATION_HOURS * block_durations
            return df

        def _add_temporal_info(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            """
            Adiciona informação temporal a um DataFrame de síntese.
            """
            df = df.rename(
                columns={
                    "data": START_DATE_COL,
                    "serie": SCENARIO_COL,
                    "classe": THERMAL_CODE_COL,
                }
            ).copy()
            df = df.sort_values(
                [THERMAL_CODE_COL, START_DATE_COL, SCENARIO_COL, BLOCK_COL],
            ).reset_index(drop=True)
            num_stages = df[START_DATE_COL].unique().shape[0]
            num_scenarios = Deck.numero_cenarios_simulacao_final(uow)
            blocks = df[BLOCK_COL].unique().tolist()
            num_blocks = len(blocks)
            thermals = df[THERMAL_CODE_COL].unique().tolist()
            num_thermals = len(thermals)
            start_dates = Deck.datas_inicio_estagios_internos_sim_final(uow)[
                :num_stages
            ]
            end_dates = np.array(
                Deck.datas_fim_estagios_internos_sim_final(uow)[:num_stages]
            )
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
        cls, df: Optional[pd.DataFrame], uow: AbstractUnitOfWork
    ) -> Optional[pd.DataFrame]:
        """
        Realiza pós-processamento após a resolução da extração dados
        em um DataFrame de síntese da geração térmica por UTE.
        """
        if df is None:
            return df
        df = cls._resolve_temporal_resolution_GTER_UTE(df, uow)
        df = cls._resolve_starting_stage(df, uow)
        df_stats = cls._calc_statistics(df)
        df[STATS_OR_SCENARIO_COL] = False
        df_stats[STATS_OR_SCENARIO_COL] = True
        df = pd.concat([df, df_stats], ignore_index=True)
        df = df.astype({SCENARIO_COL: STRING_DF_TYPE})
        return df

    @classmethod
    def _resolve_GTER_UTE_entity(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        sbm_index: int,
        sbm_name: str,
    ) -> Optional[pd.DataFrame]:
        """
        Obtém os dados da síntese de operação para todas as UTE
        de um submercado a partir do arquivo de saída do NWLISTOP.
        """
        logger_name = f"{synthesis.variable.value}_{sbm_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, sbm_index
        )

        with uow:
            logger.info(
                f"Processando arquivo do submercado: {sbm_index} - {sbm_name}"
            )
            df = uow.files.get_nwlistop(
                synthesis.variable,
                synthesis.spatial_resolution,
                submercado=sbm_index,
            )
        if df is not None:
            df[SUBMARKET_CODE_COL] = sbm_index
            df[SUBMARKET_NAME_COL] = sbm_name
        return cls._post_resolve_GTER_UTE_entity(df, uow)

    @classmethod
    def _resolve_GTER_UTE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtem os dados da síntese de operação da geração térmica
        para as UTE de um submercado a partir dos arquivos
        de saída do NWLISTOP.
        """

        def _replace_thermal_code_by_name(
            s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = df.sort_values(
                s.spatial_resolution.sorting_synthesis_df_columns
            ).reset_index(drop=True)
            conft = Deck.utes(uow)
            thermals_in_data = df[THERMAL_CODE_COL].unique().tolist()
            thermals_names = [
                conft.loc[conft["codigo_usina"] == c, "nome_usina"].iloc[0]
                for c in thermals_in_data
            ]
            lines_by_thermal = df.loc[
                df[THERMAL_CODE_COL] == thermals_in_data[0]
            ].shape[0]
            df[THERMAL_NAME_COL] = np.repeat(thermals_names, lines_by_thermal)
            df = df.astype({THERMAL_NAME_COL: STRING_DF_TYPE})
            return df

        submarkets = Deck.submercados(uow)
        real_submarkets = submarkets.loc[
            submarkets["ficticio"] == 0, :
        ].sort_values("nome_submercado")
        sbms_idx = real_submarkets["codigo_submercado"].unique()
        sbms_name = [
            real_submarkets.loc[
                real_submarkets["codigo_submercado"] == s, "nome_submercado"
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
            early_hooks=[_replace_thermal_code_by_name],
        )
        return df

    @classmethod
    def __resolve_UTE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pd.DataFrame]:
        """
        Resolve a síntese de operação para uma variável operativa
        de uma UTE a partir dos arquivos de saída do NWLISTOP.
        """
        if synthesis.variable == Variable.GERACAO_TERMICA:
            return cls._resolve_GTER_UTE(synthesis, uow)
        else:
            raise RuntimeError("Variável não suportada para UTEs")

    @classmethod
    def __resolve_PEE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        raise NotImplementedError()
        # with uow:
        #     eolica = uow.files.get_eolica()
        #     if eolica is None:
        #         if cls.logger is not None:
        #             cls.logger.error(
        #                 "Erro no processamento do eolica-cadastro.csv para"
        #                 + " síntese da operação"
        #             )
        #         raise RuntimeError()
        #     uees_idx = []
        #     uees_name = []
        #     regs = eolica.pee_cad()
        #     df = pd.DataFrame()
        #     if regs is None:
        #         return df
        #     elif isinstance(regs, list):
        #         for r in regs:
        #             uees_idx.append(r.codigo_pee)
        #             uees_name.append(r.nome_pee)
        #     dfs_uees = []
        #     for s, n in zip(uees_idx, uees_name):
        #         if cls.logger is not None:
        #             cls.logger.info(f"Processando arquivo da UEE: {s} - {n}")
        #         df_uee = cls._resolve_temporal_resolution(
        #             uow.files.get_nwlistop(
        #                 synthesis.variable,
        #                 synthesis.spatial_resolution,
        #                 uee=s,
        #             ),
        #             uow,
        #         )
        #         if df_uee is None:
        #             continue
        #         cols = df_uee.columns.tolist()
        #         df_uee["pee"] = n
        #         dfs_uees.append(df_uee)
        #     df = pd.concat(
        #         dfs_uees,
        #         ignore_index=True,
        #     )
        #     df = df[["pee"] + cols]
        #     df = df.rename(columns={"serie": "cenario"})

        #     # Otimização: ordena as entidades para facilitar a busca
        #     usinas = cls._get_unique_column_values_in_order(df, ["pee"])
        #     outras_cols = cls._get_unique_column_values_in_order(
        #         df_uee[0],
        #         ["estagio", "dataInicio", "dataFim", "patamar", "cenario"],
        #     )
        #     cls._set_ordered_entities(synthesis, {**usinas, **outras_cols})

        #     return df

    @classmethod
    def _resolve_spatial_resolution(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
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
            SpatialResolution.PARQUE_EOLICO_EQUIVALENTE: cls.__resolve_PEE,
        }
        solver = RESOLUTION_FUNCTION_MAP[synthesis.spatial_resolution]
        res = solver(synthesis, uow)
        return res if res is not None else pd.DataFrame()

    @classmethod
    def _resolve_starting_stage(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona a informação do estágio inicial do caso aos dados,
        realizando um deslocamento da coluna "estagio" para que o
        estágio inicial do caso seja 1.

        Também elimina estágios incluídos como consequência do formato
        dos dados lidos, que pertencem ao período pré-estudo.
        """
        df.loc[:, STAGE_COL] -= Deck.mes_inicio_estudo(uow) - 1
        df = df.loc[df[STAGE_COL] > 0]
        return df

    @classmethod
    def _calc_mean_std(cls, df: pd.DataFrame) -> pd.DataFrame:
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

    @classmethod
    def _calc_quantiles(
        cls, df: pd.DataFrame, quantiles: List[float]
    ) -> pd.DataFrame:
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
            cls._log("Erro no cálculo dos quantis", ERROR)
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

    @classmethod
    def _calc_statistics(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza o pós-processamento de um DataFrame com dados da
        síntese da operação de uma determinada variável, calculando
        estatísticas como quantis e média para cada variável, em cada
        estágio e patamar.
        """
        df_q = cls._calc_quantiles(df, QUANTILES_FOR_STATISTICS)
        df_m = cls._calc_mean_std(df)
        df_stats = pd.concat([df_q, df_m], ignore_index=True)
        return df_stats

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

        initial_stored_energy_df = Deck.energia_armazenada_inicial(uow)
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
            eers_submarkets_map = Deck.hydro_eer_submarket_map(uow)
            initial_stored_energy_df.dropna(inplace=True)
            initial_stored_energy_df[GROUPING_TMP_COL] = (
                initial_stored_energy_df.apply(
                    lambda linha: eers_submarkets_map.loc[
                        eers_submarkets_map[EER_NAME_COL]
                        == linha["nome_ree"].strip(),
                        SUBMARKET_CODE_COL,
                    ].iloc[0],
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
    def _get_from_cache(cls, s: OperationSynthesis) -> pd.DataFrame:
        """
        Extrai o resultado de uma síntese da cache caso exista, lançando
        um erro caso contrário.
        """
        if s in cls.CACHED_SYNTHESIS.keys():
            cls._log(f"Lendo do cache - {str(s)}")
            res = cls.CACHED_SYNTHESIS.get(s)
            if res is None:
                cls._log(f"Erro na leitura do cache - {str(s)}", ERROR)
                raise RuntimeError()
            return res.copy()
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
        if s.variable == Variable.ENERGIA_VERTIDA:
            f = cls.__stub_EVER
        elif all(
            [
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
            ]
        ):
            f = cls._stub_resolve_initial_stored_energy
        elif all(
            [
                s.variable
                in [
                    Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                    Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
                ],
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_resolve_initial_stored_volumes
        elif all(
            [
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
            ]
        ):
            f = cls._hydro_resolution_variable_map
        elif all(
            [
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
            ]
        ):
            f = cls._flow_volume_hydro_variable_map
        elif all(
            [
                s.variable
                in [
                    Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
                    Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                ],
                s.spatial_resolution != SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls._absolute_percent_volume_variable_map
        elif s.variable in [Variable.ENERGIA_DEFLUENCIA_MINIMA]:
            f = cls.__stub_EVMIN
        elif all(
            [
                s.variable
                in [
                    Variable.VAZAO_TURBINADA,
                    Variable.VAZAO_VERTIDA,
                    Variable.VAZAO_RETIRADA,
                    Variable.VAZAO_DESVIADA,
                ],
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls._convert_volume_to_flow
        elif all(
            [
                s.variable
                in [
                    Variable.VOLUME_AFLUENTE,
                    Variable.VOLUME_INCREMENTAL,
                ],
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls._convert_flow_to_volume
        elif all(
            [
                s.variable == Variable.VAZAO_DEFLUENTE,
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_QDEF
        elif all(
            [
                s.variable == Variable.VOLUME_DEFLUENTE,
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_VDEF
        elif all(
            [
                s.variable == Variable.VIOLACAO_EVAPORACAO,
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_VEVAP
        elif all(
            [
                s.variable
                in [
                    Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                    Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                ],
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_EARM_UHE
        elif s.variable in [Variable.MERCADO, Variable.MERCADO_LIQUIDO]:
            f = cls.__stub_MER_MERL
        return f

    @classmethod
    def _resolve_stub(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Tuple[pd.DataFrame, bool]:
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
            df, is_stub = pd.DataFrame(), False
        if is_stub:
            df = cls._post_resolve({"": df}, s, uow)
            df = cls._resolve_bounds(s, df, uow)
        return df, is_stub

    @classmethod
    def __get_from_cache_if_exists(cls, s: OperationSynthesis) -> pd.DataFrame:
        """
        Obtém uma síntese da operação a partir da cache, caso esta
        exista. Caso contrário, retorna um DataFrame vazio.
        """
        if s in cls.CACHED_SYNTHESIS.keys():
            return cls._get_from_cache(s)
        else:
            return pd.DataFrame()

    @classmethod
    def __store_in_cache_if_needed(
        cls, s: OperationSynthesis, df: pd.DataFrame
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
                cls.CACHED_SYNTHESIS[s] = df.copy()

    @classmethod
    def _resolve_bounds(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
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
    ) -> pd.DataFrame:
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
            uow.export.synthetize_df(
                metadata_df, OPERATION_SYNTHESIS_METADATA_OUTPUT
            )

    @classmethod
    def _add_synthesis_stats(cls, s: OperationSynthesis, df: pd.DataFrame):
        """
        Adiciona um DataFrame com estatísticas de uma síntese ao
        DataFrame de estatísticas da agregação espacial em questão.
        """
        df[VARIABLE_COL] = s.variable.value

        if s.spatial_resolution not in cls.SYNTHESIS_STATS:
            cls.SYNTHESIS_STATS[s.spatial_resolution] = [df]
        else:
            cls.SYNTHESIS_STATS[s.spatial_resolution].append(df)

    @classmethod
    def _export_scenario_synthesis(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
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
            scenarios_df = df.loc[~df[STATS_OR_SCENARIO_COL]]
            stats_df = df.loc[df[STATS_OR_SCENARIO_COL]]
            scenarios_df = scenarios_df.astype({SCENARIO_COL: int})
            stats_df = stats_df.reset_index(drop=True)
            if stats_df.empty:
                scenarios_df = scenarios_df.sort_values(
                    s.spatial_resolution.sorting_synthesis_df_columns
                ).reset_index(drop=True)
                stats_df = cls._calc_statistics(scenarios_df)
            stats_df = stats_df.drop(columns=[STATS_OR_SCENARIO_COL])
            cls._add_synthesis_stats(s, stats_df)
            cls.__store_in_cache_if_needed(s, scenarios_df)
        with time_and_log(
            message_root="Tempo para exportacao dos dados", logger=cls.logger
        ):
            with uow:
                scenarios_df = scenarios_df.drop(
                    columns=[STATS_OR_SCENARIO_COL]
                )
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
                df = pd.concat(dfs, ignore_index=True)
                df_columns = df.columns.tolist()
                columns_without_variable = [
                    c for c in df_columns if c != VARIABLE_COL
                ]
                df = df[[VARIABLE_COL] + columns_without_variable]
                df = df.astype({VARIABLE_COL: STRING_DF_TYPE})
                df = df.sort_values(
                    res.sorting_synthesis_df_columns
                ).reset_index(drop=True)
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
                if df.empty:
                    df, is_stub = cls._resolve_stub(s, uow)
                    if not is_stub:
                        df = cls._resolve_synthesis(s, uow)
                if df is not None:
                    if not df.empty:
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
