from typing import Callable, Dict, List, Tuple, Optional, Type, TypeVar, Any
import pandas as pd  # type: ignore
import numpy as np
import logging
from time import time
from datetime import datetime
import traceback
from multiprocessing import Pool
from dateutil.relativedelta import relativedelta  # type: ignore
from app.utils.graph import Graph
from app.utils.log import Log
from app.utils.regex import match_variables_with_wildcards
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
    EER_COL,
    SUBMARKET_COL,
    HYDRO_COL,
    THERMAL_COL,
)


class OperationSynthetizer:

    T = TypeVar("T")
    logger: Optional[logging.Logger] = None

    # By default, all synthesis are calculated
    DEFAULT_OPERATION_SYNTHESIS_ARGS: List[str] = SUPPORTED_SYNTHESIS

    # All synthesis that are in the dependencies of any
    # other must be cached
    SYNTHESIS_TO_CACHE: List[OperationSynthesis] = list(
        set([p for pr in SYNTHESIS_DEPENDENCIES.values() for p in pr])
    )

    # Caching strategies for reducing synthesis time
    CACHED_SYNTHESIS: Dict[OperationSynthesis, pd.DataFrame] = {}

    ORDERED_SYNTHESIS_ENTITIES: Dict[OperationSynthesis, Dict[str, list]] = {}

    UHE_REE_SBM_MAP: Optional[pd.DataFrame] = None

    SYNTHESIS_STATS: Dict[SpatialResolution, List[pd.DataFrame]] = {}

    @classmethod
    def _log(cls, msg: str, level: str = "info"):
        if cls.logger is not None:
            level_map = {
                "info": cls.logger.info,
                "warning": cls.logger.warning,
                "error": cls.logger.error,
            }
            level_map[level](msg)

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
        wind_generation_simulation = Deck.considera_geracao_eolica(uow)
        has_wind = wind_generation_simulation != 0
        simulation_with_hydro = Deck.agregacao_simulacao_final(uow)
        eers = Deck.rees(uow)
        policy_with_hydro = eers["ano_fim_individualizado"].isna().sum() == 0
        has_hydro = simulation_with_hydro or policy_with_hydro
        valid_variables: List[OperationSynthesis] = []
        if cls.logger is not None:
            cls.logger.info(
                f"Caso com geração de cenários de eólica: {has_wind}"
            )
            cls.logger.info(f"Caso com modelagem híbrida: {has_hydro}")
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
        if cls.logger is not None:
            cls.logger.info(f"Sinteses: {valid_variables}")
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
            end_dates_to_df_column = np.repeat(
                end_dates, num_scenarios * num_blocks
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
        with uow:
            cls._log("Processando arquivo do SIN")
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
                synthesis.spatial_resolution.entity_synthesis_df_columns[
                    0
                ]: sbm_name
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
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
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
                synthesis.spatial_resolution.entity_synthesis_df_columns[
                    0
                ]: sbm1_name,
                synthesis.spatial_resolution.entity_synthesis_df_columns[
                    1
                ]: sbm2_name,
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
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                cls._log("Paralelizando...")
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

        return cls._post_resolve_entity(
            df,
            synthesis,
            {
                synthesis.spatial_resolution.entity_synthesis_df_columns[
                    0
                ]: ree_name
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
            if SUBMARKET_COL in df.columns:
                return df
            else:
                ti = time()
                eers = Deck.rees(uow).set_index("nome")
                submarkets = Deck.submercados(uow)
                submarkets = submarkets.drop_duplicates(
                    ["codigo_submercado", "nome_submercado"]
                ).set_index("codigo_submercado")
                entities = cls._get_ordered_entities(s)
                eers_df = entities[EER_COL]
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
                df[SUBMARKET_COL] = np.repeat(
                    submarkets_names_df,
                    num_scenarios * num_stages * num_blocks,
                )
                tf = time()
                cls._log(f"Tempo para adicionar SBM dos REE: {tf - ti:.2f} s")
                return df[
                    [EER_COL, SUBMARKET_COL]
                    + OPERATION_SYNTHESIS_COMMON_COLUMNS
                ]

        rees = Deck.rees(uow).sort_values("nome")
        rees_idx = rees["codigo"]
        rees_name = rees["nome"]

        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
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
    def _post_resolve_entity(
        cls,
        df: Optional[pd.DataFrame],
        s: OperationSynthesis,
        entity_column_values: Dict[str, str],
        uow: AbstractUnitOfWork,
        internal_stubs: Dict[Variable, Callable] = {},
    ) -> Optional[pd.DataFrame]:
        """
        Realiza pós-processamento após a resolução da extração dados
        em um DataFrame de síntese extraído do NWLISTOP.
        """
        if df is None:
            return df
        spatial_res = s.spatial_resolution
        df = cls._resolve_temporal_resolution(df, uow)
        for col, val in entity_column_values.items():
            df[col] = val
        df = df[spatial_res.all_synthesis_df_columns]
        df = cls._resolve_starting_stage(df, uow)
        if s.variable in internal_stubs:
            df = internal_stubs[s.variable](df, uow)
        df_stats = cls._calc_statistics(df)
        return pd.concat([df, df_stats], ignore_index=True)

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
        cls._log("Compactando dados...")
        ti = time()
        valid_dfs = [df for df in resolve_responses.values() if df is not None]
        if len(valid_dfs) > 0:
            df = pd.concat(valid_dfs, ignore_index=True)
        else:
            return None

        for c in early_hooks:
            df = c(s, df, uow)

        spatial_resolution = s.spatial_resolution

        df = df.sort_values(
            spatial_resolution.sorting_synthesis_df_columns
        ).reset_index(drop=True)

        entity_columns_order = cls._get_unique_column_values_in_order(
            df,
            spatial_resolution.entity_synthesis_df_columns,
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

        tf = time()
        if cls.logger is not None:
            cls._log(f"Tempo para compactação dos dados: {tf - ti:.2f} s")
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
            Variable.COTA_JUSANTE: cls._internal_stub_calc_block_0_weighted_mean,  # noqa
            Variable.QUEDA_LIQUIDA: cls._internal_stub_calc_block_0_weighted_mean,  # noqa
        }
        return cls._post_resolve_entity(
            df,
            synthesis,
            {
                synthesis.spatial_resolution.entity_synthesis_df_columns[
                    0
                ]: uhe_name
            },
            uow,
            internal_stubs=internal_stubs,
        )

    @classmethod
    def _post_resolve_gtert(
        cls, df: Optional[pd.DataFrame], uow: AbstractUnitOfWork
    ) -> Optional[pd.DataFrame]:
        """
        Realiza pós-processamento após a resolução da extração dados
        em um DataFrame de síntese da geração térmica por UTE.
        """
        if df is None:
            return df
        df = df.rename(
            columns={
                "data": START_DATE_COL,
                "serie": SCENARIO_COL,
                "classe": THERMAL_COL,
            }
        ).copy()
        df.sort_values(
            [THERMAL_COL, START_DATE_COL, SCENARIO_COL, BLOCK_COL],
            inplace=True,
        )
        num_stages = df[START_DATE_COL].unique().shape[0]
        stages = np.arange(1, num_stages + 1)
        num_scenarios = Deck.numero_cenarios_simulacao_final(uow)
        blocks = df[BLOCK_COL].unique().tolist()
        num_blocks = len(blocks)
        thermals = df[THERMAL_COL].unique().tolist()
        num_thermals = len(thermals)
        start_dates = Deck.datas_inicio_estagios_internos_sim_final(uow)[
            :num_stages
        ]
        end_dates = Deck.datas_fim_estagios_internos_sim_final(uow)[
            :num_stages
        ]
        stages_to_df_column = np.tile(
            np.repeat(stages, num_scenarios * num_blocks), num_thermals
        )
        end_dates_to_df_column = np.tile(
            np.repeat(end_dates, num_scenarios * num_blocks), num_thermals
        )
        df = df.copy()
        df[STAGE_COL] = stages_to_df_column
        df[END_DATE_COL] = end_dates_to_df_column
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
            block_durations[i_i:i_f] = np.tile(date_durations, num_scenarios)
        block_durations = np.tile(block_durations, num_thermals)
        df[BLOCK_DURATION_COL] = STAGE_DURATION_HOURS * block_durations
        return df

    @classmethod
    def _resolve_gtert(
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
        return cls._post_resolve_gtert(df, uow)

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
        sintese_pos = OperationSynthesis(
            Variable.VIOLACAO_POSITIVA_EVAPORACAO,
            synthesis.spatial_resolution,
        )
        sintese_neg = OperationSynthesis(
            Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
            synthesis.spatial_resolution,
        )
        df_pos = cls._get_from_cache(sintese_pos)
        df_neg = cls._get_from_cache(sintese_neg)

        df_pos.loc[:, "valor"] = (
            df_neg["valor"].to_numpy() + df_pos["valor"].to_numpy()
        )
        return df_pos

    @classmethod
    def __stub_EVER(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sintese_reserv = OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            synthesis.spatial_resolution,
        )
        sintese_fio = OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            synthesis.spatial_resolution,
        )
        cache_reserv = cls._get_from_cache(sintese_reserv)
        cache_fio = cls._get_from_cache(sintese_fio)

        cache_reserv.loc[:, "valor"] = (
            cache_fio["valor"].to_numpy() + cache_reserv["valor"].to_numpy()
        )
        return cache_reserv

    @classmethod
    def __stub_mapa_variaveis_agregacao_simples_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o mapeamento de síntese de uma variável calculada
        a partir de uma agregação simples de variáveis de UHE.
        """

        s = OperationSynthesis(
            variable=synthesis.variable,
            spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
        )
        df_uhe = cls._get_from_cache(s)

        return df_uhe

    @classmethod
    def __stub_mapa_variaveis_vazao_UHE(
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

        s = OperationSynthesis(
            variable=variable_map[synthesis.variable],
            spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
        )
        df_uhe = cls._get_from_cache(s)

        return df_uhe

    @classmethod
    def __stub_mapa_variaveis_volumes_percentuais_UHE(
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

        variable_map = {
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL: Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,  # noqa
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL: Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,  # noqa
        }

        s = OperationSynthesis(
            variable=variable_map[synthesis.variable],
            spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
        )
        df_uhe = cls._get_from_cache(s)

        return df_uhe

    @classmethod
    def __stub_resolve_energias_iniciais_ree(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        earmi = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL
        earmf = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL
        earpi = Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL
        earpf = Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL
        variable_map = {
            earmi: earmf,
            earpi: earpf,
        }
        sintese_final = OperationSynthesis(
            variable=variable_map[synthesis.variable],
            spatial_resolution=synthesis.spatial_resolution,
        )
        df_final = cls._get_from_cache(sintese_final)
        # Contém as duas colunas: absoluta e percentual
        earmi_pmo = cls._earmi_percentual(synthesis, uow)
        col_earmi_pmo = (
            "valor_MWmes"
            if synthesis.variable
            == Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL
            else "valor_percentual"
        )
        df_inicial = df_final.copy()
        col_group_map = {
            SpatialResolution.RESERVATORIO_EQUIVALENTE: "ree",
            SpatialResolution.SUBMERCADO: "submercado",
            SpatialResolution.SISTEMA_INTERLIGADO: None,
        }
        col_group = col_group_map[synthesis.spatial_resolution]
        if col_group is not None:
            groups = df_inicial[col_group].unique().tolist()
        else:
            groups = [1]
        n_groups = len(groups)
        cenarios = df_inicial["cenario"].unique().tolist()
        n_cenarios = len(cenarios)
        estagios = df_inicial["estagio"].unique().tolist()
        n_estagios = len(estagios)
        # Faz uma atribuição posicional. A maneira mais pythonica é lenta.
        offset_meses = Deck.mes_inicio_estudo(uow) - 1
        offsets_groups = [i * n_cenarios * n_estagios for i in range(n_groups)]
        indices_primeiros_estagios = offset_meses * n_cenarios + np.tile(
            np.arange(n_cenarios), n_groups
        )
        indices_primeiros_estagios += np.repeat(offsets_groups, n_cenarios)
        earmi_pmo = earmi_pmo.loc[earmi_pmo["group"].isin(groups)]
        valores_earmi = (
            earmi_pmo.set_index("group").loc[groups, col_earmi_pmo].to_numpy()
        )
        valores_iniciais = df_inicial["valor"].to_numpy()
        valores_iniciais[n_cenarios:] = valores_iniciais[:-n_cenarios]
        valores_iniciais[indices_primeiros_estagios] = np.repeat(
            valores_earmi, n_cenarios
        )
        df_inicial["valor"] = valores_iniciais
        df_inicial["valor"] = df_inicial["valor"].fillna(0.0)
        return df_inicial

    @classmethod
    def __stub_resolve_volumes_iniciais_uhe(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        varmi = Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL
        varmf = Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL
        varpi = Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL
        varpf = Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL
        variable_map = {
            varmi: varmf,
            varpi: varpf,
        }
        sintese_final = OperationSynthesis(
            variable=variable_map[synthesis.variable],
            spatial_resolution=synthesis.spatial_resolution,
        )
        df_final = cls._get_from_cache(sintese_final)
        varmi_pmo = Deck.volume_armazenado_inicial(uow)
        col_varmi_pmo = (
            "valor_hm3"
            if synthesis.variable
            == Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL
            else "valor_percentual"
        )
        entities = cls._get_ordered_entities(sintese_final)
        df_inicial = df_final.copy()
        uhes = entities["usina"]
        n_uhes = len(uhes)
        cenarios = entities["cenario"]
        n_cenarios = len(cenarios)
        estagios = entities["estagio"]
        n_estagios = len(estagios)
        # Faz uma atribuição posicional. A maneira mais pythonica é lenta.
        offset_meses = Deck.mes_inicio_estudo(uow) - 1
        offsets_uhes = [i * n_cenarios * n_estagios for i in range(n_uhes)]
        indices_primeiros_estagios = offset_meses * n_cenarios + np.tile(
            np.arange(n_cenarios), n_uhes
        )
        indices_primeiros_estagios += np.repeat(offsets_uhes, n_cenarios)
        varmi_pmo = varmi_pmo.loc[varmi_pmo["nome_usina"].isin(uhes)]
        valores_varmi = (
            varmi_pmo.set_index("nome_usina")
            .loc[uhes, col_varmi_pmo]
            .to_numpy()
        )
        valores_iniciais = df_inicial["valor"].to_numpy()
        valores_iniciais[n_cenarios:] = valores_iniciais[:-n_cenarios]
        valores_iniciais[indices_primeiros_estagios] = np.repeat(
            valores_varmi, n_cenarios
        )
        df_inicial["valor"] = valores_iniciais
        df_inicial["valor"] = df_inicial["valor"].fillna(0.0)
        return df_inicial

    @classmethod
    def _obtem_usinas_jusante(
        cls, nome_usina: str, df_uhes: pd.DataFrame
    ) -> List[str]:
        uhes_jusante: List[str] = []
        codigo_uhe_jusante = df_uhes.loc[
            df_uhes["nome_usina"] == nome_usina, "codigo_usina_jusante"
        ].iloc[0]
        while codigo_uhe_jusante != 0:
            nome_uhe_jusante = df_uhes.loc[
                df_uhes["codigo_usina"] == codigo_uhe_jusante, "nome_usina"
            ].iloc[0]
            uhes_jusante.append(nome_uhe_jusante)
            codigo_uhe_jusante = df_uhes.loc[
                df_uhes["codigo_usina"] == codigo_uhe_jusante,
                "codigo_usina_jusante",
            ].iloc[0]

        return uhes_jusante

    @classmethod
    def _acumula_produtibilidades_reservatorios(
        cls, df: pd.DataFrame, uhes: List[str], uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        df_usinas = Deck.uhes(uow)
        # Monta a lista de arestas e constroi o grafo
        # direcionado das usinas (JUSANTE -> MONTANTE)
        np_edges = list(
            df_usinas.loc[
                df_usinas["nome_usina"].isin(uhes),
                ["codigo_usina_jusante", "codigo_usina"],
            ].to_numpy()
        )
        edges = [tuple(e) for e in np_edges]
        g = Graph(edges, directed=True)
        bfs_usinas = g.bfs(0)[1:]
        # Percorre todas as usinas a partir de um BFS, tendo
        # como nó de origem o 0 (MAR).
        for codigo_usina in bfs_usinas:
            nome_usina = df_usinas.loc[
                df_usinas["codigo_usina"] == codigo_usina, "nome_usina"
            ].iloc[0]
            if cls.logger:
                cls.logger.info(
                    f"Calculando prodt. acumulada para {nome_usina}..."
                )
            codigo_usina_jusante = df_usinas.loc[
                df_usinas["codigo_usina"] == codigo_usina,
                "codigo_usina_jusante",
            ].iloc[0]
            if codigo_usina_jusante == 0:
                continue
            nome_usina_jusante = df_usinas.loc[
                df_usinas["codigo_usina"] == codigo_usina_jusante, "nome_usina"
            ].iloc[0]
            prod_usina = df.loc[df["usina"] == nome_usina, "prod"]
            prod_jusante = df.loc[
                df["usina"] == nome_usina_jusante, "prod"
            ].to_numpy()
            if not prod_usina.empty and len(prod_jusante) > 0:
                prod_usina += prod_jusante
        return df

    @classmethod
    def __stub_EARM_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        ti = time()
        sintese_hliq = OperationSynthesis(
            Variable.QUEDA_LIQUIDA,
            synthesis.spatial_resolution,
        )
        mapa_earm_varm = {
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL: Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,  # noqa
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL: Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,  # noqa
        }
        sintese_varm = OperationSynthesis(
            mapa_earm_varm[synthesis.variable],
            synthesis.spatial_resolution,
        )
        df_hliq = cls._get_from_cache(sintese_hliq)
        df_hliq = df_hliq.loc[df_hliq["patamar"] == 0].copy()
        df_varm = cls._get_from_cache(sintese_varm)
        # Converte queda líquida em produtividade usando a
        # produtibilidade específica
        hidr = Deck.hidr(uow)
        nomes_uhes_hliq = cls._get_ordered_entities(sintese_hliq)["usina"]
        n_entradas_uhe = df_hliq.loc[
            df_hliq["usina"] == nomes_uhes_hliq[0]
        ].shape[0]
        hidr_uhes = hidr.loc[
            hidr["nome_usina"].isin(nomes_uhes_hliq)
        ].set_index("nome_usina")
        # Produtibilidades em MW / ( (m3/s) / m)
        prod_esp = np.repeat(
            hidr_uhes.loc[
                nomes_uhes_hliq, "produtibilidade_especifica"
            ].to_numpy(),
            n_entradas_uhe,
        )
        # Converte para MW / ( (hm3 / mes) / m )
        prod_esp /= 2.63

        # Adiciona ao df e acumula as produtibilidades nos reservatórios
        df_hliq["prod_esp"] = prod_esp
        df_hliq["prod"] = df_hliq["prod_esp"] * df_hliq["valor"]
        df_hliq = cls._acumula_produtibilidades_reservatorios(
            df_hliq, nomes_uhes_hliq, uow
        )

        nomes_uhes_varm = cls._get_ordered_entities(sintese_varm)["usina"]
        df_hliq = df_hliq.loc[df_hliq["usina"].isin(nomes_uhes_varm)].copy()

        # Multiplica o volume (útil) armazenado em cada UHE pela
        # produtibilidade acumulada nos pontos de operação.
        df_hliq = df_hliq.sort_values(["usina", "estagio", "patamar"])
        df_varm = df_varm.sort_values(["usina", "estagio", "patamar"])

        df_varm["valor"] = (
            df_varm["valor"] - df_varm["limiteInferior"]
        ) * df_hliq["prod"].to_numpy()
        df_varm["limiteInferior"] = 0.0
        df_varm["limiteSuperior"] = (
            df_varm["limiteSuperior"] - df_varm["limiteInferior"]
        ) * df_hliq["prod"].to_numpy()

        tf = time()
        if cls.logger:
            cls.logger.info(
                f"Tempo para conversão do VARM em EARM: {tf - ti:.2f} s"
            )

        return df_varm

    @classmethod
    def __stub_energia_defluencia_minima(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sintese_meta = OperationSynthesis(
            Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            synthesis.spatial_resolution,
        )
        sintese_violacao = OperationSynthesis(
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            synthesis.spatial_resolution,
        )
        df_meta = cls._get_from_cache(sintese_meta)
        df_violacao = cls._get_from_cache(sintese_violacao)

        df_meta.loc[:, "valor"] = (
            df_meta["valor"].to_numpy() - df_violacao["valor"].to_numpy()
        )
        return df_meta

    @classmethod
    def _internal_stub_calc_block_0_weighted_mean(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        n_blocks = Deck.numero_patamares(uow)
        unique_cols_for_block_0 = ["usina", "estagio", "cenario"]
        df_block_0 = df.copy()
        df_block_0["valor"] = (
            df_block_0["valor"] * df_block_0["duracaoPatamar"]
        ) / STAGE_DURATION_HOURS
        df_base = df.iloc[::n_blocks].reset_index(drop=True).copy()
        df_base["patamar"] = 0
        df_base["duracaoPatamar"] = STAGE_DURATION_HOURS
        arr = df_block_0["valor"].to_numpy()
        n_linhas = arr.shape[0]
        n_elementos_distintos = n_linhas // n_blocks
        df_base["valor"] = arr.reshape((n_elementos_distintos, -1)).mean(
            axis=1
        )
        df_block_0 = pd.concat(
            [df_block_0, df_base], ignore_index=True, copy=True
        )
        df_block_0 = df_block_0.sort_values(
            unique_cols_for_block_0 + ["patamar"]
        )
        return df_block_0

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
            ]
            return df

        def _add_eer_submarket_to_hydro_synthesis(
            s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            if EER_COL in df.columns and SUBMARKET_COL in df.columns:
                return df
            else:
                ti = time()
                entities = cls._get_ordered_entities(s)
                hydro_df = entities[HYDRO_COL]
                aux_df = Deck.uhes_rees_submercados_map(uow)
                aux_df = aux_df.loc[hydro_df]
                num_blocks = len(entities[BLOCK_COL])
                num_stages = len(entities[STAGE_COL])
                num_scenarios = len(entities[SCENARIO_COL])
                df[EER_COL] = np.repeat(
                    aux_df[EER_COL].tolist(),
                    num_scenarios * num_stages * num_blocks,
                )
                df[SUBMARKET_COL] = np.repeat(
                    aux_df[SUBMARKET_COL].tolist(),
                    num_scenarios * num_stages * num_blocks,
                )
                tf = time()
                cls._log(
                    f"Tempo para adicionar REE e SBM das UHE: {tf - ti:.2f} s"
                )
                return df[
                    [HYDRO_COL, EER_COL, SUBMARKET_COL]
                    + OPERATION_SYNTHESIS_COMMON_COLUMNS
                ]

        uhes = Deck.uhes(uow).sort_values("nome_usina")
        uhes_idx = uhes["codigo_usina"]
        uhes_name = uhes["nome_usina"]

        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                cls._log("Paralelizando...")
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
            late_hooks=[_add_eer_submarket_to_hydro_synthesis],
        )
        return df

    @classmethod
    def __stub_GTER_UTE_patamar(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """ """

        def _replace_thermal_code_by_name(
            s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            conft = Deck.utes(uow)
            thermals_in_data = df[THERMAL_COL].unique().tolist()
            thermals_names = [
                conft.loc[conft["codigo_usina"] == c, "nome_usina"].iloc[0]
                for c in thermals_in_data
            ]
            lines_by_thermal = df.loc[
                df[THERMAL_COL] == thermals_in_data[0]
            ].shape[0]
            df[THERMAL_COL] = np.repeat(thermals_names, lines_by_thermal)
            return df

        def _add_submarket_to_thermal_synthesis(
            s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            if SUBMARKET_COL in df.columns:
                return df
            else:
                ti = time()
                thermals = Deck.utes(uow).set_index("nome_usina")
                submarkets = Deck.submercados(uow)
                submarkets = submarkets.drop_duplicates(
                    ["codigo_submercado", "nome_submercado"]
                ).set_index("codigo_submercado")
                # Obtem os nomes dos SBMs na mesma ordem em que aparecem as UTEs
                entities = cls._get_ordered_entities(s)
                thermals_df = entities[THERMAL_COL]
                codigos_sbms_df = [
                    thermals.at[r, "submercado"] for r in thermals_df
                ]
                nomes_sbms_df = [
                    submarkets.at[c, "nome_submercado"]
                    for c in codigos_sbms_df
                ]
                # Aplica de modo posicional por desempenho
                num_blocks = len(entities[BLOCK_COL])
                num_stages = len(entities[STAGE_COL])
                num_scenarios = len(entities[SCENARIO_COL])
                df[SUBMARKET_COL] = np.repeat(
                    nomes_sbms_df, num_scenarios * num_stages * num_blocks
                )
                tf = time()
                if cls.logger:
                    cls.logger.info(
                        f"Tempo para adicionar SBM das UTE: {tf - ti:.2f} s"
                    )
                # Reordena as colunas e retorna
                return df[
                    [THERMAL_COL, SUBMARKET_COL]
                    + OPERATION_SYNTHESIS_COMMON_COLUMNS
                ]

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
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                cls._log("Paralelizando...")
            async_res = {
                idx: pool.apply_async(
                    cls._resolve_gtert, (uow, synthesis, idx, name)
                )
                for idx, name in zip(sbms_idx, sbms_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}

        df = cls._post_resolve(
            dfs,
            synthesis,
            uow,
            early_hooks=[_replace_thermal_code_by_name],
            late_hooks=[_add_submarket_to_thermal_synthesis],
        )
        return df

    @classmethod
    def __resolve_UTE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pd.DataFrame]:
        if synthesis.variable == Variable.GERACAO_TERMICA:
            return cls.__stub_GTER_UTE_patamar(synthesis, uow)
        else:
            raise RuntimeError("Variável não suportada para UTEs")

    @classmethod
    def __resolve_PEE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            eolica = uow.files.get_eolica()
            if eolica is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do eolica-cadastro.csv para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            uees_idx = []
            uees_name = []
            regs = eolica.pee_cad()
            df = pd.DataFrame()
            if regs is None:
                return df
            elif isinstance(regs, list):
                for r in regs:
                    uees_idx.append(r.codigo_pee)
                    uees_name.append(r.nome_pee)
            dfs_uees = []
            for s, n in zip(uees_idx, uees_name):
                if cls.logger is not None:
                    cls.logger.info(f"Processando arquivo da UEE: {s} - {n}")
                df_uee = cls._resolve_temporal_resolution(
                    uow.files.get_nwlistop(
                        synthesis.variable,
                        synthesis.spatial_resolution,
                        uee=s,
                    ),
                    uow,
                )
                if df_uee is None:
                    continue
                cols = df_uee.columns.tolist()
                df_uee["pee"] = n
                dfs_uees.append(df_uee)
            df = pd.concat(
                dfs_uees,
                ignore_index=True,
            )
            df = df[["pee"] + cols]
            df = df.rename(columns={"serie": "cenario"})

            # Otimização: ordena as entidades para facilitar a busca
            usinas = cls._get_unique_column_values_in_order(df, ["pee"])
            outras_cols = cls._get_unique_column_values_in_order(
                df_uee[0],
                ["estagio", "dataInicio", "dataFim", "patamar", "cenario"],
            )
            cls._set_ordered_entities(synthesis, {**usinas, **outras_cols})

            return df

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
        df.loc[:, "estagio"] -= Deck.mes_inicio_estudo(uow) - 1
        df = df.loc[df["estagio"] > 0]
        return df

    @classmethod
    def _calc_mean(cls, df: pd.DataFrame) -> pd.DataFrame:
        cols_valores = ["cenario", "valor"]
        cols_agrupamento = [c for c in df.columns if c not in cols_valores]
        try:
            df_mean = (
                df.groupby(cols_agrupamento, sort=False)[["valor"]]
                .mean(engine="numba")
                .reset_index()
            )
        except ZeroDivisionError:
            df_mean = (
                df.groupby(cols_agrupamento, sort=False)[["valor"]]
                .mean(engine="cython")
                .reset_index()
            )
        df_mean["cenario"] = "mean"
        try:
            df_std = (
                df.groupby(cols_agrupamento, sort=False)[["valor"]]
                .std(engine="numba")
                .reset_index()
            )
        except ZeroDivisionError:
            df_std = (
                df.groupby(cols_agrupamento, sort=False)[["valor"]]
                .std(engine="cython")
                .reset_index()
            )
        df_std["cenario"] = "std"
        return pd.concat([df_mean, df_std], ignore_index=True)

    @classmethod
    def _calc_quantiles(
        cls, df: pd.DataFrame, quantiles: List[float]
    ) -> pd.DataFrame:
        cols_valores = ["cenario", "valor"]
        cols_agrupamento = [c for c in df.columns if c not in cols_valores]
        df_q = (
            df.groupby(cols_agrupamento, sort=False)[["cenario", "valor"]]
            .quantile(quantiles)
            .reset_index()
        )

        def quantile_map(q: float) -> str:
            if q == 0:
                label = "min"
            elif q == 1:
                label = "max"
            elif q == 0.5:
                label = "median"
            else:
                label = f"p{int(100 * q)}"
            return label

        level_column = [c for c in df_q.columns if "level_" in c]
        if len(level_column) != 1:
            if cls.logger is not None:
                cls.logger.error("Erro no cálculo dos quantis")
                raise RuntimeError()

        df_q = df_q.drop(columns=["cenario"]).rename(
            columns={level_column[0]: "cenario"}
        )
        df_q["cenario"] = df_q["cenario"].apply(quantile_map)
        return df_q

    @classmethod
    def _calc_statistics(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza o pós-processamento de um DataFrame com dados da
        síntese da operação de uma determinada variável, calculando
        estatísticas como quantis e média para cada variável, em cada
        estágio, cenário e patamar.
        """
        df_q = cls._calc_quantiles(df, [0.05 * i for i in range(21)])
        df_m = cls._calc_mean(df)
        df_stats = pd.concat([df_q, df_m], ignore_index=True)
        return df_stats

    @classmethod
    def _earmi_percentual(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        earmi_pmo = Deck.energia_armazenada_inicial(uow)
        if s.spatial_resolution == SpatialResolution.RESERVATORIO_EQUIVALENTE:
            return earmi_pmo.rename(columns={"nome_ree": "group"})
        earmi_pmo["earmax"] = (
            100 * earmi_pmo["valor_MWmes"] / earmi_pmo["valor_percentual"]
        )
        if s.spatial_resolution == SpatialResolution.SUBMERCADO:
            sistema = Deck.submercados(uow)
            rees = Deck.rees(uow)
            nomes_rees = rees["nome"].unique().tolist()
            rees_submercados = {
                r: str(
                    sistema.loc[
                        sistema["codigo_submercado"]
                        == int(
                            rees.loc[rees["nome"] == r, "submercado"].iloc[0]
                        ),
                        "nome_submercado",
                    ].tolist()[0]
                )
                for r in nomes_rees
            }
            earmi_pmo.dropna(inplace=True)
            earmi_pmo["group"] = earmi_pmo.apply(
                lambda linha: rees_submercados[linha["nome_ree"].strip()],
                axis=1,
            )
        else:
            earmi_pmo["group"] = 1
        earmi_pmo = (
            earmi_pmo.groupby("group").sum(numeric_only=True).reset_index()
        )
        earmi_pmo["valor_percentual"] = (
            100 * earmi_pmo["valor_MWmes"] / (earmi_pmo["earmax"])
        )
        return earmi_pmo

    @classmethod
    def _get_from_cache(cls, s: OperationSynthesis) -> pd.DataFrame:
        if s in cls.CACHED_SYNTHESIS.keys():
            if cls.logger:
                cls.logger.info(f"Lendo do cache - {str(s)}")
            val = cls.CACHED_SYNTHESIS.get(s)
            if val is None:
                if cls.logger:
                    cls.logger.error(f"Erro na leitura do cache - {str(s)}")
                raise RuntimeError()
            return val.copy()
        else:
            if cls.logger:
                cls.logger.error(f"Erro na leitura do cache - {str(s)}")
            raise RuntimeError()

    @classmethod
    def _stub_mappings(  # noqa
        cls, s: OperationSynthesis
    ) -> Optional[Callable]:
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
            f = cls.__stub_resolve_energias_iniciais_ree
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
            f = cls.__stub_resolve_volumes_iniciais_uhe
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
            f = cls.__stub_mapa_variaveis_agregacao_simples_UHE
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
                ],
                s.spatial_resolution != SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_mapa_variaveis_vazao_UHE
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
            f = cls.__stub_mapa_variaveis_volumes_percentuais_UHE
        elif s.variable in [Variable.ENERGIA_DEFLUENCIA_MINIMA]:
            f = cls.__stub_energia_defluencia_minima
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
        return f

    @classmethod
    def _resolve_stub(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Tuple[pd.DataFrame, bool]:
        f = cls._stub_mappings(s)
        if f:
            df, is_stub = f(s, uow), True
        else:
            df, is_stub = pd.DataFrame(), False
        if is_stub:
            df = cls._resolve_bounds(s, df, uow)
        return df, is_stub

    @classmethod
    def __get_from_cache_if_exists(cls, s: OperationSynthesis) -> pd.DataFrame:
        if s in cls.CACHED_SYNTHESIS.keys():
            return cls._get_from_cache(s)
        else:
            return pd.DataFrame()

    @classmethod
    def __store_in_cache_if_needed(
        cls, s: OperationSynthesis, df: pd.DataFrame
    ):
        if s in cls.SYNTHESIS_TO_CACHE:
            ti = time()
            cls.CACHED_SYNTHESIS[s] = df.copy()
            tf = time()
            if cls.logger:
                cls.logger.info(
                    f"Tempo para armazenamento na cache: {tf - ti:.2f} s"
                )

    @classmethod
    def _resolve_bounds(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        ti = time()
        df = OperationVariableBounds.resolve_bounds(s, df, uow)
        tf = time()
        if cls.logger:
            cls.logger.info(f"Tempo para cálculo dos limites: {tf - ti:.2f} s")
        return df

    @classmethod
    def _resolve_synthesis(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
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
            uow.export.synthetize_df(metadata_df, "METADADOS_OPERACAO")

    @classmethod
    def _add_synthesis_stats(cls, s: OperationSynthesis, df: pd.DataFrame):
        df["variavel"] = s.variable.value
        if s.spatial_resolution not in cls.SYNTHESIS_STATS:
            cls.SYNTHESIS_STATS[s.spatial_resolution] = [df]
        else:
            cls.SYNTHESIS_STATS[s.spatial_resolution].append(df)

    @classmethod
    def _export_scenario_synthesis(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ):
        filename = str(s)
        ti_e = time()
        n_cenarios = Deck.numero_cenarios_simulacao_final(uow)
        cenarios = list(range(1, n_cenarios + 1))
        df_cenarios = df.loc[df["cenario"].isin(cenarios)].reset_index(
            drop=True
        )
        df_cenarios = df_cenarios.astype({"cenario": int})
        df_stats = df.loc[~df["cenario"].isin(cenarios)].reset_index(drop=True)
        if df_stats.empty:
            df_stats = cls._calc_statistics(df_cenarios)
        cls._add_synthesis_stats(s, df_stats)
        cls.__store_in_cache_if_needed(s, df_cenarios)
        with uow:
            uow.export.synthetize_df(df_cenarios, filename)
        tf_e = time()
        cls.logger.info(
            f"Tempo para exportação dos dados: {tf_e - ti_e:.2f} s"
        )

    @classmethod
    def _export_stats(
        cls,
        uow: AbstractUnitOfWork,
    ):
        for res, dfs in cls.SYNTHESIS_STATS.items():
            with uow:
                df = pd.concat(dfs, ignore_index=True)
                cols_df = df.columns.tolist()
                cols_sem_variavel = [c for c in cols_df if c != "variavel"]
                df = df[["variavel"] + cols_sem_variavel]
                uow.export.synthetize_df(
                    df, f"ESTATISTICAS_OPERACAO_{res.value}"
                )

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: List[str], uow: AbstractUnitOfWork
    ) -> List[OperationSynthesis]:
        if len(variables) == 0:
            synthesis_variables = cls._default_args()
        else:
            all_variables = cls._match_wildcards(variables)
            synthesis_variables = cls._process_variable_arguments(
                all_variables
            )
        valid_synthesis = cls._filter_valid_variables(synthesis_variables, uow)
        synthesis_with_dependencies = cls._add_synthesis_dependencies(
            valid_synthesis
        )
        return synthesis_with_dependencies

    @classmethod
    def _synthetize_single_variable(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[OperationSynthesis]:
        ti_s = time()
        try:
            filename = str(s)
            found_synthesis = False
            cls.logger.info(f"Realizando sintese de {filename}")
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
                    tf_s = time()
                    cls.logger.info(
                        f"Tempo para síntese de {filename}: {tf_s - ti_s:.2f} s"
                    )
                    return s
            if not found_synthesis:
                cls.logger.warning(
                    "Nao foram encontrados dados"
                    + f" para a sintese de {filename}"
                )
        except Exception as e:
            traceback.print_exc()
            cls.logger.error(str(e))
            cls.logger.error(
                f"Nao foi possível realizar a sintese de: {filename}"
            )

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
        ti = time()
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
        tf = time()
        cls.logger.info(f"Tempo para síntese da operação: {tf - ti:.2f} s")
