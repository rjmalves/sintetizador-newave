from typing import Callable, Dict, List, Tuple, Optional, TypeVar
import pandas as pd  # type: ignore
import numpy as np  # type: ignore
import logging
from traceback import print_exc
from multiprocessing import Pool
from logging import INFO, ERROR
from datetime import datetime
from dateutil.relativedelta import relativedelta  # type: ignore
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.log import Log
from app.utils.timing import time_and_log
from app.utils.regex import match_variables_with_wildcards
from app.utils.operations import fast_group_df
from app.model.settings import Settings
from app.services.deck.deck import Deck
from app.model.scenario.variable import Variable
from app.model.scenario.spatialresolution import SpatialResolution
from app.model.scenario.step import Step
from app.model.scenario.scenariosynthesis import (
    ScenarioSynthesis,
    SUPPORTED_SYNTHESIS,
    UNITS,
)
from app.internal.constants import (
    STAGE_COL,
    MONTH_COL,
    VALUE_COL,
    DATE_COL,
    START_DATE_COL,
    END_DATE_COL,
    CONFIG_COL,
    LTA_COL,
    LTA_VALUE_COL,
    ITERATION_COL,
    SPAN_COL,
    SCENARIO_COL,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    EER_CODE_COL,
    EER_NAME_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    NULL_INFLOW_STATION,
    STRING_DF_TYPE,
    VARIABLE_COL,
    SCENARIO_SYNTHESIS_STATS_ROOT,
    SCENARIO_SYNTHESIS_SUBDIR,
    QUANTILES_FOR_STATISTICS,
)


class ScenarioSynthetizer:

    # Por padrão, todas as sínteses suportadas são consideradas
    DEFAULT_OPERATION_SYNTHESIS_ARGS: List[str] = SUPPORTED_SYNTHESIS

    COMMON_COLUMNS: List[str] = [
        ITERATION_COL,
        STAGE_COL,
        START_DATE_COL,
        END_DATE_COL,
        SCENARIO_COL,
        SPAN_COL,
    ]

    CACHED_SYNTHESIS: Dict[Tuple[Variable, Step], pd.DataFrame] = {}

    CACHED_MLT_VALUES: Dict[
        Tuple[Variable, SpatialResolution], pd.DataFrame
    ] = {}

    T = TypeVar("T")

    logger: Optional[logging.Logger] = None

    SYNTHESIS_STATS: Dict[
        Tuple[SpatialResolution, Step], List[pd.DataFrame]
    ] = {}

    @classmethod
    def clear_cache(cls):
        """
        Limpa o cache de síntese de cenários.
        """
        cls.CACHED_SYNTHESIS.clear()
        cls.CACHED_MLT_VALUES.clear()
        cls.SYNTHESIS_STATS.clear()

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _default_args(cls) -> List[ScenarioSynthesis]:
        """
        Uma lista com os argumentos padrão para a
        síntese de cenários, utilizados caso não seja
        especificada nenhuma lista de sínteses desejadas.

        :return: A lista de objetos de síntese de cenários
        :rtype: List[ScenarioSynthesis]
        """
        args = [
            ScenarioSynthesis.factory(a)
            for a in cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
            # if "_BKW" not in a
        ]
        return [arg for arg in args if arg is not None]

    @classmethod
    def _match_wildcards(cls, variables: List[str]) -> List[str]:
        """
        Busca dentre as sínteses suportadas aquelas que
        atendem às especificadas, utilizando
        wildcards para se referir a mais de uma síntese.

        :return: A lista de strings associadas às sínteses
        :rtype: List[str]
        """
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[ScenarioSynthesis]:
        """
        Processa as strings fornecidas para construir os objetos
        de sínteses de cenários correspondentes.

        :return: A lista de objetos de síntese de cenários
        :rtype: List[ScenarioSynthesis]
        """
        args_data = [ScenarioSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        return valid_args

    @classmethod
    def filter_valid_variables(
        cls, variables: List[ScenarioSynthesis], uow: AbstractUnitOfWork
    ) -> List[ScenarioSynthesis]:
        """
        Filtra as variáveis de síntese de cenários para que sejam somente
        sintetizadas as válidas para o caso em questão. Para esta tarefa,
        são lidos campos de configuração do caso, como o uso de períodos
        individualizados e a consideração de geração eólica.

        :return: A lista de objetos de síntese válidos
        :rtype: List[ScenarioSynthesis]
        """
        valid_variables: List[ScenarioSynthesis] = []
        simulation_with_hydro = Deck.agregacao_simulacao_final(uow)
        policy_with_hydro = Deck.politica_hibrida(uow)
        has_hydro = simulation_with_hydro or policy_with_hydro
        for v in variables:
            if v.variable == Variable.VAZAO_INCREMENTAL and not has_hydro:
                continue
            valid_variables.append(v)
        cls._log(f"Sinteses: {valid_variables}")
        return valid_variables

    @classmethod
    def _generate_hydro_incremental_inflow_dataframe(
        cls, hydro_code: int, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtém a série histórica de vazões incrementais para uma UHE,
        considerando os códigos e postos cadastrados no arquivo `confhd.dat`.

        - data (`datetime`)
        - vazao (`float`)

        :return: A tabela como um DataFrame
        :rtype: pd.DataFrame | None
        """
        uhes = Deck.uhes(uow)
        vazoes = Deck.vazoes(uow)
        inflow_station = uhes.loc[
            uhes["codigo_usina"] == hydro_code, "posto"
        ].iloc[0]
        natural_inflow = vazoes[inflow_station].to_numpy()
        null_station = inflow_station == NULL_INFLOW_STATION
        if not null_station:
            upstream_hydro_codes = uhes.loc[
                uhes["codigo_usina_jusante"] == hydro_code, "codigo_usina"
            ].tolist()
            upstream_hydro_codes = [u for u in upstream_hydro_codes if u != 0]
            upstream_inflow_stations = list(
                set(
                    [
                        uhes.loc[
                            uhes["codigo_usina"] == uhe_montante, "posto"
                        ].iloc[0]
                        for uhe_montante in upstream_hydro_codes
                    ]
                )
            )
            for upstream_station in upstream_inflow_stations:
                natural_inflow = (
                    natural_inflow - vazoes[upstream_station].to_numpy()
                )
        history_starting_year = int(
            uhes.loc[
                uhes["codigo_usina"] == hydro_code, "ano_inicio_historico"
            ].iloc[0]
        )
        history_ending_year = int(
            uhes.loc[
                uhes["codigo_usina"] == hydro_code, "ano_fim_historico"
            ].iloc[0]
        )
        dates = pd.date_range(
            datetime(year=history_starting_year, month=1, day=1),
            datetime(year=history_ending_year, month=12, day=1),
            freq="MS",
        )
        return pd.DataFrame(
            data={
                DATE_COL: dates,
                VALUE_COL: natural_inflow[: len(dates)],
            }
        )

    @classmethod
    def _eval_monthly_lta(cls, history: pd.DataFrame) -> pd.DataFrame:
        """
        Extrai a MLT de uma série histórica de vazões de
        uma UHE, agrupando por mês.

        - mes (`int`)
        - valor (`float`)

        :return: A tabela como um DataFrame
        :rtype: pd.DataFrame | None
        """
        history[MONTH_COL] = history.apply(
            lambda linha: linha[DATE_COL].month, axis=1
        )
        return (
            history.groupby([MONTH_COL]).mean(numeric_only=True).reset_index()
        )

    @classmethod
    def _model_dataframe_for_hydro_lta(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Gera um DataFrame para ser utilizado para preenchimento dos
        valores da MLT de cada UHE em cada estágio do modelo.

        Os valores de estágio e mês são ajustados para que sejam
        coerentes com o modelo, substituindo a visão blocada de
        estagio = 1 para janeiro do primeiro ano do período de estudo
        para estagio = 1 para o mês de início do estudo. Adicionalmente,
        são filtrados estágios do período pré-estudo.

        - estagio (`int`)
        - mes (`int`)

        :return: A tabela como um DataFrame
        :rtype: pd.DataFrame | None

        """

        months_column = cls._generate_model_dataframe_month_column(uow)
        stages_column = cls._generate_model_dataframe_stage_column(
            uow, len(months_column)
        )

        lta_df = pd.DataFrame(
            data={
                STAGE_COL: stages_column,
                MONTH_COL: months_column,
            }
        )
        lta_df = cls._resolve_starting_stage(lta_df, uow)
        return lta_df.copy()

    @classmethod
    def _generate_lta_hydro_inflow_series(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Extrai a MLT para todas as UHEs.

        - codigo_usina (`int`)
        - usina (`str`)
        - codigo_ree (`int`)
        - ree (`str`)
        - codigo_submercado (`int`)
        - submercado (`str`)
        - estagio (`int`)
        - mes (`int`)
        - mlt (`float`)

        :return: A tabela como um DataFrame
        :rtype: pd.DataFrame | None
        """

        def _calc_hydro_lta_df(
            hydro_code: int, lta_model_df: pd.DataFrame, map_line: pd.Series
        ) -> pd.DataFrame:
            # TODO - substituir pelo uso do bfs com grafo
            inflow = cls._generate_hydro_incremental_inflow_dataframe(
                hydro_code, uow
            )
            lta_inflow = cls._eval_monthly_lta(inflow)
            lta_hydro_df = lta_model_df.merge(
                pd.DataFrame(
                    data={
                        HYDRO_CODE_COL: [hydro_code] * len(lta_inflow),
                        HYDRO_NAME_COL: [
                            hydro_eer_submarket_map.at[
                                hydro_code, HYDRO_NAME_COL
                            ]
                        ]
                        * len(lta_inflow),
                        MONTH_COL: lta_inflow[MONTH_COL],
                        LTA_COL: lta_inflow[VALUE_COL].to_numpy(),
                    }
                ),
                on=MONTH_COL,
            )
            for col in [
                EER_CODE_COL,
                EER_NAME_COL,
                SUBMARKET_CODE_COL,
                SUBMARKET_NAME_COL,
            ]:
                lta_hydro_df[col] = map_line[col]
            for col in [HYDRO_NAME_COL, EER_NAME_COL, SUBMARKET_NAME_COL]:
                lta_hydro_df = lta_hydro_df.astype({col: STRING_DF_TYPE})
            return lta_hydro_df

        with time_and_log(
            "Tempo para calculo da MLT por UHE", logger=cls.logger
        ):
            hydro_eer_submarket_map = (
                Deck.hydro_eer_submarket_map(uow)
                .reset_index()
                .set_index(HYDRO_CODE_COL)
            )
            lta_model_df = cls._model_dataframe_for_hydro_lta(uow)
            lta_hydro_dfs: List[pd.DataFrame] = []
            for hydro_code, map_line in hydro_eer_submarket_map.iterrows():
                lta_hydro_df = _calc_hydro_lta_df(
                    hydro_code, lta_model_df, map_line
                )
                lta_hydro_dfs.append(lta_hydro_df)

            return pd.concat(lta_hydro_dfs, ignore_index=True).sort_values(
                [STAGE_COL, HYDRO_CODE_COL]
            )

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
        df = df.loc[df[STAGE_COL] > 0].reset_index(drop=True)
        return df

    @classmethod
    def _generate_model_dataframe_month_column(
        cls, uow: AbstractUnitOfWork
    ) -> np.ndarray:
        """
        Gera uma coluna com os meses de cada estágio do caso a ser
        utilizada nos DataFrames com valores da MLT para cada síntese,
        em formato previamente conhecido.
        """
        starting_date_with_tendency = (
            Deck.data_inicio_com_tendencia_hidrologica(uow)
        )
        ending_date_with_post_study_years = Deck.data_fim_com_pos_estudo(uow)
        dates = pd.date_range(
            starting_date_with_tendency,
            ending_date_with_post_study_years,
            freq="MS",
        )
        return np.array([d.month for d in dates], dtype=np.int64)

    @classmethod
    def _generate_model_dataframe_stage_column(
        cls, uow: AbstractUnitOfWork, num_stages: int
    ) -> np.ndarray:
        """
        Gera uma coluna com os estágios do caso a ser
        utilizada nos DataFrames com valores da MLT para cada síntese,
        em formato previamente conhecido.
        """
        past_stages = Deck.num_estagios_tendencia_hidrologica(uow)
        stages_with_past_tendency = np.arange(
            -past_stages + 1, num_stages - past_stages + 1, dtype=np.int64
        )
        return stages_with_past_tendency

    @classmethod
    def _model_dataframe_for_eer_lta(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Gera um DataFrame para ser utilizado para preenchimento dos
        valores da MLT de cada REE em cada estágio do modelo.

        Os valores de estágio e mês são ajustados para que sejam
        coerentes com o modelo, substituindo a visão blocada de
        estagio = 1 para janeiro do primeiro ano do período de estudo
        para estagio = 1 para o mês de início do estudo. Adicionalmente,
        são filtrados estágios do período pré-estudo.

        - estagio (`int`)
        - configuracao (`int`)
        - mes (`int`)

        :return: A tabela como um DataFrame
        :rtype: pd.DataFrame | None
        """

        def __generate_configuration_column(
            uow: AbstractUnitOfWork,
        ) -> np.ndarray:
            configurations_df = Deck.configuracoes(uow)
            starting_date_with_tendency = (
                Deck.data_inicio_com_tendencia_hidrologica(uow)
            )
            ending_date_with_post_study_years = Deck.data_fim_com_pos_estudo(
                uow
            )
            past_stages = Deck.num_estagios_tendencia_hidrologica(uow)
            additional_tendency_configurations = np.array([1] * past_stages)
            configurations = (
                configurations_df.loc[
                    (configurations_df["data"] >= starting_date_with_tendency)
                    & (
                        configurations_df["data"]
                        <= ending_date_with_post_study_years
                    ),
                    "valor",
                ]
                .to_numpy()
                .flatten()
            )
            return np.concatenate(
                [additional_tendency_configurations, configurations]
            )

        months_column = cls._generate_model_dataframe_month_column(uow)
        stages_column = cls._generate_model_dataframe_stage_column(
            uow, len(months_column)
        )
        configuration_column = __generate_configuration_column(uow)

        lta_df = pd.DataFrame(
            data={
                STAGE_COL: stages_column,
                CONFIG_COL: configuration_column,
                MONTH_COL: months_column,
            }
        )
        lta_df = cls._resolve_starting_stage(lta_df, uow)
        return lta_df.copy()

    @classmethod
    def _generate_lta_eer_energy_series(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Extrai a MLT em energia para todos os REEs.

        - codigo_ree (`int`)
        - ree (`str`)
        - codigo_submercado (`int`)
        - submercado (`str`)
        - estagio (`int`)
        - configuracao (`int`)
        - mes (`int`)
        - mlt (`float`)

        :return: A tabela como um DataFrame
        :rtype: pd.DataFrame | None
        """

        def _energy_history_df(uow: AbstractUnitOfWork) -> pd.DataFrame:
            energy_history = Deck.engnat(uow)
            # Para cada REE, obtem a série de MLT para os estágios do modelo
            starting_year = Deck.ano_inicio_estudo(uow)
            history_final_year = starting_year - 1
            energy_history = energy_history.loc[
                energy_history["data"].dt.year < history_final_year
            ]
            return energy_history.copy()

        def _calc_eer_lta_df(
            file_eer_index: int,
            lta_model_df: pd.DataFrame,
            energy_history_df: pd.DataFrame,
            map_line: pd.Series,
        ) -> pd.DataFrame:
            eer_lta = np.zeros((lta_model_df.shape[0],))
            # TODO - substituir por ordenacao e repeticao posicional
            for eer_idx, lta_line in lta_model_df.iterrows():
                eer_lta[eer_idx] = energy_history_df.loc[
                    (energy_history_df["configuracao"] == lta_line[CONFIG_COL])
                    & (energy_history_df["ree"] == file_eer_index)
                    & (
                        energy_history_df["data"].dt.month
                        == lta_line[MONTH_COL]
                    ),
                    "valor",
                ].mean()
            lta_eer_df = lta_model_df.copy()
            lta_eer_df[LTA_COL] = eer_lta
            for col in [
                EER_CODE_COL,
                EER_NAME_COL,
                SUBMARKET_CODE_COL,
                SUBMARKET_NAME_COL,
            ]:
                lta_eer_df[col] = map_line[col]
            for col in [EER_NAME_COL, SUBMARKET_NAME_COL]:
                lta_eer_df = lta_eer_df.astype({col: STRING_DF_TYPE})
            return lta_eer_df

        with time_and_log(
            "Tempo para calculo da MLT por REE", logger=cls.logger
        ):
            energy_history = _energy_history_df(uow)
            eer_submarket_map = (
                Deck.hydro_eer_submarket_map(uow)
                .drop_duplicates(subset=[EER_CODE_COL])
                .set_index(EER_CODE_COL)
            )
            eer_order = Deck.eer_code_order(uow)
            eer_submarket_map = eer_submarket_map.loc[eer_order].reset_index()
            lta_model_df = cls._model_dataframe_for_eer_lta(uow)
            lta_eer_dfs: List[pd.DataFrame] = []
            for idx, map_line in eer_submarket_map.iterrows():
                lta_eer_df = _calc_eer_lta_df(
                    idx + 1, lta_model_df, energy_history, map_line
                )
                lta_eer_dfs.append(lta_eer_df)
            return pd.concat(lta_eer_dfs, ignore_index=True)

    @classmethod
    def _agg_lta_hydro_inflow_series(
        cls, variable: Variable, col: Optional[str], uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza a agregaçao da MLT de vazões incrementais para
        todas as UHEs segundo uma coluna desejada.
        """
        hydro_lta = cls._get_lta_df(
            variable,
            SpatialResolution.USINA_HIDROELETRICA,
            uow,
        )
        col_list = [col] if col is not None else []
        df = (
            hydro_lta.groupby(col_list + [STAGE_COL])
            .sum(numeric_only=True)
            .reset_index()
        )
        return df[col_list + [STAGE_COL, LTA_COL]]

    @classmethod
    def _agg_lta_eer_energy_series(
        cls, col: Optional[str], uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza a agregaçao da MLT de energias para
        todos os REEs segundo uma coluna desejada.
        """
        eer_lta = cls._get_lta_df(
            Variable.ENA_ABSOLUTA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            uow,
        )
        col_list = [col] if col is not None else []
        df = (
            eer_lta.groupby(col_list + [STAGE_COL])
            .sum(numeric_only=True)
            .reset_index()
        )
        return df[col_list + [STAGE_COL, LTA_COL]]

    @classmethod
    def _resolve_lta_submarket_energy_series(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with time_and_log(
            "Tempo para agregação da MLT de ENAA - SBM", logger=cls.logger
        ):
            return cls._agg_lta_eer_energy_series(SUBMARKET_NAME_COL, uow)

    @classmethod
    def _resolve_lta_sin_energy_series(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with time_and_log(
            "Tempo para agregação da MLT de ENAA - SIN", logger=cls.logger
        ):
            return cls._agg_lta_eer_energy_series(None, uow)

    @classmethod
    def _resolve_lta_eer_inflow_series(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with time_and_log(
            "Tempo para agregação da MLT de QINC - REE", logger=cls.logger
        ):
            return cls._agg_lta_hydro_inflow_series(
                Variable.VAZAO_INCREMENTAL, EER_NAME_COL, uow
            )

    @classmethod
    def _resolve_lta_submarket_inflow_series(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with time_and_log(
            "Tempo para agregação da MLT de QINC - SBM", logger=cls.logger
        ):
            return cls._agg_lta_hydro_inflow_series(
                Variable.VAZAO_INCREMENTAL, SUBMARKET_NAME_COL, uow
            )

    @classmethod
    def _resolve_lta_sin_inflow_series(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with time_and_log(
            "Tempo para agregação da MLT de QINC - SIN", logger=cls.logger
        ):
            return cls._agg_lta_hydro_inflow_series(
                Variable.VAZAO_INCREMENTAL, None, uow
            )

    @classmethod
    def _get_lta_df(
        cls,
        variable: Variable,
        spatial_resolution: SpatialResolution,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        """
        Obtém um DataFrame com dados de MLT para uma variável
        desejada, aproveitando informações da cache ou calculando
        se for necessário.
        """
        CACHING_FUNCTION_MAP: Dict[
            Tuple[Variable, SpatialResolution], Callable
        ] = {
            (
                Variable.ENA_ABSOLUTA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): cls._generate_lta_eer_energy_series,
            (
                Variable.ENA_ABSOLUTA,
                SpatialResolution.SUBMERCADO,
            ): cls._resolve_lta_submarket_energy_series,
            (
                Variable.ENA_ABSOLUTA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): cls._resolve_lta_sin_energy_series,
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): cls._generate_lta_hydro_inflow_series,
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): cls._resolve_lta_eer_inflow_series,
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.SUBMERCADO,
            ): cls._resolve_lta_submarket_inflow_series,
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): cls._resolve_lta_sin_inflow_series,
        }
        if cls.CACHED_MLT_VALUES.get((variable, spatial_resolution)) is None:
            cls.CACHED_MLT_VALUES[(variable, spatial_resolution)] = (
                CACHING_FUNCTION_MAP[(variable, spatial_resolution)](uow)
            )
        return cls.CACHED_MLT_VALUES.get(
            (variable, spatial_resolution), pd.DataFrame()
        ).copy()

    @classmethod
    def _format_scenario_data(
        cls, data: np.ndarray, num_scenarios: int, num_stages: int
    ) -> np.ndarray:
        """
        Formata um conjunto de dados com a repetição adequada
        para ser adicionado a uma coluna de um DataFrame com formato
        previamente conhecido.
        """
        return np.tile(np.repeat(data, num_scenarios), (num_stages,))

    @classmethod
    def _add_energy_eer_data(
        cls,
        uow: AbstractUnitOfWork,
        energy_df: pd.DataFrame,
        dates: List[datetime],
    ) -> pd.DataFrame:
        """
        Adiciona dados de do REE aos dados de energia
        lidos do arquivo binário `energiab.dat`.

        - estagio (`int`)
        - data (`datetime`)
        - data_fim (`datetime`)
        - serie (`int`)
        - abertura (`int`)
        - codigo_usina (`int`)
        - nome_usina (`str`)
        - ree (`int`)
        - submercado (`int`)
        - valor (`float`)

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """

        def _add_entities(
            energy_df: pd.DataFrame,
            num_scenarios: int,
            num_stages: int,
            uow: AbstractUnitOfWork,
        ) -> pd.DataFrame:
            eer_submarket_map = (
                Deck.hydro_eer_submarket_map(uow)
                .drop_duplicates(subset=[EER_CODE_COL])
                .set_index(EER_CODE_COL)
            )
            eer_order = Deck.eer_code_order(uow)
            eer_submarket_map = eer_submarket_map.loc[eer_order].reset_index()
            for col in [
                EER_CODE_COL,
                EER_NAME_COL,
                SUBMARKET_CODE_COL,
                SUBMARKET_NAME_COL,
            ]:
                energy_df[col] = cls._format_scenario_data(
                    eer_submarket_map[col].to_numpy(),
                    num_scenarios,
                    num_stages,
                )
            for col in [EER_NAME_COL, SUBMARKET_NAME_COL]:
                energy_df = energy_df.astype({col: STRING_DF_TYPE})
            return energy_df

        def _add_dates(
            energy_df: pd.DataFrame,
            dates: List[datetime],
            num_scenarios: int,
            num_eers: int,
        ) -> pd.DataFrame:
            end_dates = [d + relativedelta(months=1) for d in dates]
            sorted_start_dates: np.ndarray = np.repeat(
                np.array(dates), num_scenarios * num_eers
            )
            sorted_end_dates: np.ndarray = np.repeat(
                np.array(end_dates), num_scenarios * num_eers
            )
            energy_df[START_DATE_COL] = sorted_start_dates
            energy_df[END_DATE_COL] = sorted_end_dates
            return energy_df

        # Extrai dimensões para repetir vetores
        energy_df = energy_df.copy()
        num_scenarios = len(energy_df[SCENARIO_COL].unique())
        num_eers = len(energy_df[EER_CODE_COL].unique())
        num_stages = len(energy_df[STAGE_COL].unique())
        num_spans = (
            len(energy_df[SPAN_COL].unique()) if SPAN_COL in energy_df else 1
        )

        # Edita o DF e retorna
        energy_df = _add_entities(
            energy_df, num_scenarios * num_spans, num_stages, uow
        )
        energy_df = _add_dates(
            energy_df, dates, num_scenarios * num_spans, num_eers
        )
        energy_df = cls._resolve_starting_stage(energy_df, uow)
        energy_df_columns = [
            EER_CODE_COL,
            EER_NAME_COL,
            SUBMARKET_CODE_COL,
            SUBMARKET_NAME_COL,
            STAGE_COL,
            START_DATE_COL,
            END_DATE_COL,
            SCENARIO_COL,
            VALUE_COL,
        ]
        energy_df_columns += (
            [SPAN_COL] if SPAN_COL in energy_df.columns else []
        )
        return energy_df[energy_df_columns]

    @classmethod
    def _add_inflow_hydro_data(
        cls,
        uow: AbstractUnitOfWork,
        inflow_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Adiciona dados de código da UHE, nome da UHE, ree da UHE e
        submercado da UHE aos dados de vazão lidos do arquivo
        binário `vazaob.dat`.

        - estagio (`int`)
        - data (`datetime`)
        - data_fim (`datetime`)
        - serie (`int`)
        - abertura (`int`)
        - codigo_usina (`int`)
        - usina (`str`)
        - codigo_ree (`int`)
        - ree (`str`)
        - codigo_submercado (`int`)
        - submercado (`str`)
        - valor (`float`)

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """

        def _add_entities(
            inflow_df: pd.DataFrame,
            num_scenarios: int,
            num_stages: int,
            uow: AbstractUnitOfWork,
        ) -> pd.DataFrame:
            hydro_eer_submarket_map = (
                Deck.hydro_eer_submarket_map(uow)
                .reset_index()
                .set_index(HYDRO_CODE_COL)
            )
            hydro_order = Deck.hydro_code_order(uow)
            hydro_eer_submarket_map = hydro_eer_submarket_map.loc[
                hydro_order
            ].reset_index()
            for col in [
                HYDRO_CODE_COL,
                HYDRO_NAME_COL,
                EER_CODE_COL,
                EER_NAME_COL,
                SUBMARKET_CODE_COL,
                SUBMARKET_NAME_COL,
            ]:
                inflow_df[col] = cls._format_scenario_data(
                    hydro_eer_submarket_map[col].to_numpy(),
                    num_scenarios,
                    num_stages,
                )
            for col in [HYDRO_NAME_COL, EER_NAME_COL, SUBMARKET_NAME_COL]:
                inflow_df = inflow_df.astype({col: STRING_DF_TYPE})
            return inflow_df

        def _add_dates(
            inflow_df: pd.DataFrame,
            num_scenarios: int,
            num_hydros: int,
            num_stages: int,
            uow: AbstractUnitOfWork,
        ) -> pd.DataFrame:
            starting_date = Deck.data_inicio_com_tendencia_hidrologica(uow)
            ending_date = starting_date + relativedelta(months=num_stages - 1)
            dates = pd.date_range(
                starting_date,
                ending_date,
                freq="MS",
            )
            end_dates = [d + relativedelta(months=1) for d in dates]
            sorted_start_dates = np.repeat(dates, num_scenarios * num_hydros)
            sorted_end_dates = np.repeat(end_dates, num_scenarios * num_hydros)
            inflow_df[START_DATE_COL] = sorted_start_dates
            inflow_df[END_DATE_COL] = sorted_end_dates
            return inflow_df

        # Extrai dimensões para repetir vetores
        inflow_df = inflow_df.copy()
        num_scenarios = len(inflow_df[SCENARIO_COL].unique())
        num_hydros = len(inflow_df[HYDRO_CODE_COL].unique())
        num_stages = len(inflow_df[STAGE_COL].unique())
        num_spans = (
            len(inflow_df[SPAN_COL].unique()) if SPAN_COL in inflow_df else 1
        )

        # Edita o DF e retorna
        inflow_df = _add_entities(
            inflow_df, num_scenarios * num_spans, num_stages, uow
        )
        inflow_df = _add_dates(
            inflow_df, num_scenarios * num_spans, num_hydros, num_stages, uow
        )
        inflow_df = cls._resolve_starting_stage(inflow_df, uow)
        inflow_df.drop(columns=[HYDRO_CODE_COL], inplace=True)
        inflow_df_columns = [
            STAGE_COL,
            START_DATE_COL,
            END_DATE_COL,
            SCENARIO_COL,
            HYDRO_NAME_COL,
            EER_CODE_COL,
            EER_NAME_COL,
            SUBMARKET_CODE_COL,
            SUBMARKET_NAME_COL,
            VALUE_COL,
        ]
        inflow_df_columns += (
            [SPAN_COL] if SPAN_COL in inflow_df.columns else []
        )
        return inflow_df[inflow_df_columns]

    @classmethod
    def _post_resolve_energy_iteration(
        cls,
        generated_energy_df: pd.DataFrame,
        converted_energy_df: pd.DataFrame,
        uow: AbstractUnitOfWork,
        hydro_simulation_stages: int,
        dates: List[datetime],
        it: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Realiza o pós-processamento para cálculo de estatísticas e adição
        de dados de submercado aos dados de energias lidos.
        """
        if not converted_energy_df.empty and not generated_energy_df.empty:
            energy_df = pd.concat(
                [
                    converted_energy_df.loc[
                        converted_energy_df[STAGE_COL]
                        <= hydro_simulation_stages
                    ],
                    generated_energy_df.loc[
                        generated_energy_df[STAGE_COL]
                        > hydro_simulation_stages
                    ],
                ],
                ignore_index=True,
            )
        else:
            energy_df = generated_energy_df
        if not energy_df.empty:
            energy_df = energy_df.rename(
                columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
            )
            energy_df = cls._add_energy_eer_data(uow, energy_df, dates)
            if it is not None:
                energy_df[ITERATION_COL] = it
            df_stats = cls._calc_statistics(energy_df)
            energy_df = pd.concat([energy_df, df_stats], ignore_index=True)
            energy_df = energy_df.astype({SCENARIO_COL: STRING_DF_TYPE})
        return energy_df

    @classmethod
    def _post_resolve_inflow_iteration(
        cls,
        inflow_df: pd.DataFrame,
        uow: AbstractUnitOfWork,
        it: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Realiza o pós-processamento para cálculo de estatísticas e adição
        de dados de REE e submercado aos dados de vazão lidos.
        """
        if not inflow_df.empty:
            inflow_df = inflow_df.rename(
                columns={"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL}
            )
            inflow_df = cls._add_inflow_hydro_data(uow, inflow_df)
            if it is not None:
                inflow_df[ITERATION_COL] = it
            df_stats = cls._calc_statistics(inflow_df)
            inflow_df = pd.concat([inflow_df, df_stats], ignore_index=True)
            inflow_df = inflow_df.astype({SCENARIO_COL: STRING_DF_TYPE})
        return inflow_df

    @classmethod
    def _resolve_forward_energy_iteration(
        cls, uow: AbstractUnitOfWork, it: int
    ) -> pd.DataFrame:
        """
        Obtem os dados de ENA para a etapa forward em uma determinada
        iteração de interesse, considerando já os estágios individualizados
        e agregados, nos quais a energia é lida do arquivo binário
        `enavazf.dat` e `energiaf.dat`, respectivamente. É adicionada uma
        coluna `iteracao` ao DataFrame resultante.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        logger = Log.configure_process_logger(
            uow._queue, Variable.ENA_ABSOLUTA.value, it
        )
        logger.info(f"Obtendo energias forward da it. {it}")
        generated_energy_df = Deck.energiaf(it, uow)
        converted_energy_df = Deck.enavazf(it, uow)
        hydro_simulation_stages = Deck.num_estagios_individualizados_politica(
            uow
        )
        dates = Deck.datas_inicio_estagios_internos_politica_com_tendencia(uow)

        return cls._post_resolve_energy_iteration(
            generated_energy_df,
            converted_energy_df,
            uow,
            hydro_simulation_stages,
            dates,
            it,
        )

    @classmethod
    def _post_resolve(
        cls, resolve_responses: Dict[int, pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Realiza o pós-processamento para agregação dos dados de todos os
        DataFrames lidos de um conjunto de arquivos.
        """
        with time_and_log("Tempo para compactacao dos dados", cls.logger):
            valid_dfs = [
                df for df in resolve_responses.values() if df is not None
            ]
            if len(valid_dfs) > 0:
                df = pd.concat(valid_dfs, ignore_index=True)
            else:
                df = pd.DataFrame()
            return df

    @classmethod
    def _resolve_forward_energy(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        """
        Obtem os dados de ENA para a etapa forward em todas as iterações feitas
        pelo modelo.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        num_iterations = Deck.num_iteracoes(uow)
        num_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para obter energias forward",
            logger=cls.logger,
        ):
            with Pool(processes=num_procs) as pool:
                async_res = {
                    it: pool.apply_async(
                        cls._resolve_forward_energy_iteration, (uow, it)
                    )
                    for it in range(1, num_iterations + 1)
                }
                dfs = {it: r.get(timeout=3600) for it, r in async_res.items()}

        return cls._post_resolve(dfs)

    @classmethod
    def _resolve_forward_inflow_iteration(
        cls, uow: AbstractUnitOfWork, it: int
    ) -> pd.DataFrame:
        """
        Obtem os dados de QINC para a etapa forward em uma determinada
        iteração de interesse, considerando apenas os estágios individualizados,
        nos quais a vazão é lida do arquivo binário `vazaof.dat`. É adicionada
        uma coluna `iteracao` ao DataFrame resultante.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        logger = Log.configure_process_logger(
            uow.queue, Variable.VAZAO_INCREMENTAL.value, it
        )
        logger.info(f"Obtendo vazões forward da it. {it}")
        inflow_df = Deck.vazaof(it, uow)
        return cls._post_resolve_inflow_iteration(inflow_df, uow, it)

    @classmethod
    def _resolve_forward_inflow(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        """
        Obtem os dados de QINC para a etapa forward em todas as iterações
        feitas pelo modelo.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        num_iterations = Deck.num_iteracoes(uow)
        num_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para obter vazoes forward",
            logger=cls.logger,
        ):
            with Pool(processes=num_procs) as pool:
                async_res = {
                    it: pool.apply_async(
                        cls._resolve_forward_inflow_iteration, (uow, it)
                    )
                    for it in range(1, num_iterations + 1)
                }
                dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        return cls._post_resolve(dfs)

    @classmethod
    def _resolve_backward_energy_iteration(
        cls, uow: AbstractUnitOfWork, it: int
    ) -> pd.DataFrame:
        """
        Obtem os dados de ENA para a etapa backward em uma determinada
        iteração de interesse, considerando já os estágios individualizados
        e agregados, nos quais a energia é lida do arquivo binário
        `enavazb.dat` e `energiab.dat`, respectivamente. É adicionada uma
        coluna `iteracao` ao DataFrame resultante.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        logger = Log.configure_process_logger(
            uow._queue, Variable.ENA_ABSOLUTA.value, it
        )
        logger.info(f"Obtendo energias backward da it. {it}")
        generated_energy_df = Deck.energiab(it, uow)
        converted_energy_df = Deck.enavazb(it, uow)
        hydro_simulation_stages = Deck.num_estagios_individualizados_politica(
            uow
        )
        dates = Deck.datas_inicio_estagios_internos_politica(uow)

        return cls._post_resolve_energy_iteration(
            generated_energy_df,
            converted_energy_df,
            uow,
            hydro_simulation_stages,
            dates,
            it,
        )

    @classmethod
    def _resolve_backward_energy(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        """
        Obtem os dados de ENA para a etapa backward em todas as iterações
        feitas pelo modelo.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        num_iterations = Deck.num_iteracoes(uow)
        num_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para obter energias backward",
            logger=cls.logger,
        ):
            with Pool(processes=num_procs) as pool:
                async_res = {
                    it: pool.apply_async(
                        cls._resolve_backward_energy_iteration, (uow, it)
                    )
                    for it in range(1, num_iterations + 1)
                }
                dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}

        return cls._post_resolve(dfs)

    @classmethod
    def _resolve_backward_inflow_iteration(
        cls, uow: AbstractUnitOfWork, it: int
    ) -> pd.DataFrame:
        """
        Obtem os dados de QINC para a etapa backward em uma determinada
        iteração de interesse, considerando apenas os estágios individualizados,
        nos quais a vazão é lida do arquivo binário `vazaob.dat`. É adicionada
        uma coluna `iteracao` ao DataFrame resultante.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        logger = Log.configure_process_logger(
            uow.queue, Variable.VAZAO_INCREMENTAL.value, it
        )
        logger.info(f"Obtendo vazões backward da it. {it}")
        inflow_df = Deck.vazaob(it, uow)

        return cls._post_resolve_inflow_iteration(inflow_df, uow, it)

    @classmethod
    def _resolve_backward_inflow(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        """
        Obtem os dados de QINC para a etapa backward em todas as iterações
        feitas pelo modelo.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        num_iterations = Deck.num_iteracoes(uow)
        num_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para obter vazoes backward",
            logger=cls.logger,
        ):
            with Pool(processes=num_procs) as pool:
                async_res = {
                    it: pool.apply_async(
                        cls._resolve_backward_inflow_iteration, (uow, it)
                    )
                    for it in range(1, num_iterations + 1)
                }
                dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        return cls._post_resolve(dfs)

    @classmethod
    def _resolve_final_simulation_energy(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtem os dados de ENA para a etapa de simulação final.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        cls._log("Obtendo energias da simulação final")
        with time_and_log(
            message_root="Tempo para obter energias da simulacao final",
            logger=cls.logger,
        ):
            generated_energy_df = Deck.energias(uow)
            converted_energy_df = Deck.enavazs(uow)
        hydro_simulation_stages = Deck.num_estagios_individualizados_sf(uow)
        dates = Deck.datas_inicio_estagios_internos_politica_com_tendencia(uow)

        df = cls._post_resolve_energy_iteration(
            generated_energy_df,
            converted_energy_df,
            uow,
            hydro_simulation_stages,
            dates,
            it=None,
        )
        return cls._post_resolve({0: df})

    @classmethod
    def _resolve_final_simulation_inflow(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtem os dados de QINC para a etapa de simulação final.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        cls._log("Obtendo vazões da simulação final")
        with time_and_log(
            message_root="Tempo para obter vazoes da simulacao final",
            logger=cls.logger,
        ):
            inflow_df = Deck.vazaos(uow)

        df = cls._post_resolve_inflow_iteration(
            inflow_df,
            uow,
            it=None,
        )
        return cls._post_resolve({0: df})

    @classmethod
    def _get_cached_variable(
        cls,
        variable: Variable,
        step: Step,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        """
        Obtem um DataFrame com os dados de uma variável sintetizada,
        em uma determinada etapa, a partir do cache. Caso este dado
        não exista, ele é calculado a partir da função de resolução
        adequada, armazenado no cache e retornado.

        :return: Os dados da variável, para a etapa, como um DataFrame.
        :rtype: pd.DataFrame
        """
        CACHING_FUNCTION_MAP: Dict[Tuple[Variable, Step], Callable] = {
            (Variable.ENA_ABSOLUTA, Step.FORWARD): cls._resolve_forward_energy,
            (
                Variable.ENA_ABSOLUTA,
                Step.BACKWARD,
            ): cls._resolve_backward_energy,
            (
                Variable.ENA_ABSOLUTA,
                Step.FINAL_SIMULATION,
            ): cls._resolve_final_simulation_energy,
            (
                Variable.VAZAO_INCREMENTAL,
                Step.FORWARD,
            ): cls._resolve_forward_inflow,
            (
                Variable.VAZAO_INCREMENTAL,
                Step.BACKWARD,
            ): cls._resolve_backward_inflow,
            (
                Variable.VAZAO_INCREMENTAL,
                Step.FINAL_SIMULATION,
            ): cls._resolve_final_simulation_inflow,
        }

        if cls.CACHED_SYNTHESIS.get((variable, step)) is None:
            cls.CACHED_SYNTHESIS[(variable, step)] = CACHING_FUNCTION_MAP[
                (variable, step)
            ](uow)
        return cls.CACHED_SYNTHESIS.get((variable, step), pd.DataFrame())

    @classmethod
    def _resolve_group(
        cls, group_col: List[str], df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Realiza o agrupamento dos dados por meio de uma soma, cosiderando
        uma lista de colunas para agrupamento e excluindo a coluna "valor",
        que será sempre agregada.

        :return: Os dados agrupados como um DataFrame.
        :rtype: pd.DataFrame
        """
        if not df.empty:
            cols = group_col + [
                c for c in cls.COMMON_COLUMNS if c in df.columns
            ]
            grouped_df = df.groupby(cols).sum(numeric_only=True).reset_index()
            return grouped_df[cols + ["valor"]]
        else:
            return df

    @classmethod
    def _calc_lta(
        cls,
        df: pd.DataFrame,
        lta_df: pd.DataFrame,
        filter_col: Optional[str],
    ) -> pd.DataFrame:
        """
        Adiciona uma informação da MLT (Média de Longo
        Termo) para cada cenário sintetizado em dados pertencentes
        à etapa backward.

        :return: Os dados com MLT como um DataFrame.
        :rtype: pd.DataFrame
        """

        def _df_sorting_columns(
            df: pd.DataFrame, filter_col: Optional[str]
        ) -> List[str]:
            iteration_col = (
                [ITERATION_COL] if ITERATION_COL in df.columns else []
            )
            filter_col_list = [filter_col] if filter_col is not None else []
            span_col = [SPAN_COL] if SPAN_COL in df.columns else []
            return (
                iteration_col
                + [STAGE_COL]
                + filter_col_list
                + [SCENARIO_COL]
                + span_col
            )

        def _lta_df_sorting_columns(filter_col: Optional[str]) -> List[str]:
            filter_col_list = [filter_col] if filter_col is not None else []
            return [STAGE_COL] + filter_col_list

        df = df.sort_values(_df_sorting_columns(df, filter_col))
        lta_df = lta_df.sort_values(_lta_df_sorting_columns(filter_col))
        num_scenarios = len(df[SCENARIO_COL].unique())
        stages = df[STAGE_COL].unique()
        num_iterations = (
            len(df[ITERATION_COL].unique())
            if ITERATION_COL in df.columns
            else 1
        )
        num_spans = len(df[SPAN_COL].unique()) if SPAN_COL in df.columns else 1
        elements = df[filter_col].unique() if filter_col is not None else []

        lta_df = lta_df.loc[lta_df[STAGE_COL].isin(stages)].copy()
        if len(elements) > 0:
            lta_df = lta_df.loc[lta_df[filter_col].isin(elements)].copy()
        sorted_ltas = np.repeat(
            lta_df[LTA_COL].to_numpy(), num_scenarios * num_spans
        )

        df[LTA_COL] = np.tile(sorted_ltas, num_iterations)
        df[LTA_VALUE_COL] = df[VALUE_COL] / df[LTA_COL]
        df.replace([np.inf, -np.inf], 0, inplace=True)
        return df

    @classmethod
    def _resolve_lta(
        cls,
        synthesis: ScenarioSynthesis,
        df: pd.DataFrame,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        """
        Adiciona uma informação da MLT (Média de Longo
        Termo) para cada cenário sintetizado na forma das colunas:

        - mlt (`float`)
        - valorMlt (`float`)

        Para este processamento, é considerada a resolução espacial
        e também a etapa a qual pertencem os cenários da síntese.

        :return: Os dados com MLT como um DataFrame.
        :rtype: pd.DataFrame
        """
        # Descobre o valor em MLT
        df = df.copy()
        lta_df = cls._get_lta_df(
            synthesis.variable,
            synthesis.spatial_resolution,
            uow,
        )

        FILTER_MAP = {
            SpatialResolution.USINA_HIDROELETRICA: HYDRO_NAME_COL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE: EER_NAME_COL,
            SpatialResolution.SUBMERCADO: SUBMARKET_NAME_COL,
            SpatialResolution.SISTEMA_INTERLIGADO: None,
        }
        filter_col = FILTER_MAP[synthesis.spatial_resolution]
        return cls._calc_lta(df, lta_df, filter_col)

    @classmethod
    def _resolve_spatial_resolution(
        cls, synthesis: ScenarioSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza a resolução da agregação espacial dos dados,
        considerando a síntese desejada e os dados disponíveis.

        Caso a variável não exista diretamente como uma saída do modelo,
        é realizada uma agregação na resolução espacial desejada.

        Adicionalmente, é adicionada uma informação da MLT (Média de Longo
        Termo) para cada cenário sintetizado na forma das colunas:

        - mlt (`float`)
        - valorMlt (`float`)

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        RESOLUTION_MAP: Dict[SpatialResolution, List[str]] = {
            SpatialResolution.SISTEMA_INTERLIGADO: [],
            SpatialResolution.SUBMERCADO: [SUBMARKET_NAME_COL],
            SpatialResolution.RESERVATORIO_EQUIVALENTE: [
                EER_NAME_COL,
                SUBMARKET_NAME_COL,
            ],
            SpatialResolution.USINA_HIDROELETRICA: [
                HYDRO_NAME_COL,
                EER_NAME_COL,
                SUBMARKET_NAME_COL,
            ],
        }
        df = cls._get_cached_variable(synthesis.variable, synthesis.step, uow)
        df = cls._resolve_group(
            RESOLUTION_MAP[synthesis.spatial_resolution], df
        )
        return cls._resolve_lta(synthesis, df, uow)

    @classmethod
    def _calc_mean_std(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza o pós-processamento para calcular o valor médio e o desvio
        padrão de uma variável dentre todos os estágios e patamares,
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
        de uma variável dentre todos os estágios e patamares,
        agrupando de acordo com as demais colunas.
        """
        value_columns = [SCENARIO_COL, VALUE_COL]
        grouping_columns = [c for c in df.columns if c not in value_columns]
        quantile_df = (
            df.groupby(grouping_columns, sort=False)[[SCENARIO_COL, VALUE_COL]]
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

        level_column = [c for c in quantile_df.columns if "level_" in c]
        if len(level_column) != 1:
            cls._log("Erro no cálculo dos quantis", ERROR)
            raise RuntimeError()

        quantile_df = quantile_df.drop(columns=[SCENARIO_COL]).rename(
            columns={level_column[0]: SCENARIO_COL}
        )
        num_entities = quantile_df.shape[0] // len(quantiles)
        quantile_labels = np.tile(
            np.array([quantile_map(q) for q in quantiles]), num_entities
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
    def _export_metadata(
        cls,
        success_synthesis: List[ScenarioSynthesis],
        uow: AbstractUnitOfWork,
    ):
        """
        Realiza a exportação dos metadados dos cenários sintetizados, com
        a descrição de quais sínteses foram realizadas e algumas
        características de cada uma.

        - chave (`str`)
        - nome_curto_variavel (`str`)
        - nome_longo_variavel (`str`)
        - nome_curto_agregacao (`str`)
        - nome_longo_agregacao (`str`)
        - nome_curto_etapa (`str`)
        - nome_longo_etapa (`str`)
        - unidade (`str`)

        """
        metadata_df = pd.DataFrame(
            columns=[
                "chave",
                "nome_curto_variavel",
                "nome_longo_variavel",
                "nome_curto_agregacao",
                "nome_longo_agregacao",
                "nome_curto_etapa",
                "nome_longo_etapa",
                "unidade",
            ]
        )
        for s in success_synthesis:
            metadata_df.loc[metadata_df.shape[0]] = [
                str(s),
                s.variable.short_name,
                s.variable.long_name,
                s.spatial_resolution.short_name,
                s.spatial_resolution.long_name,
                s.step.short_name,
                s.step.long_name,
                UNITS[s].value if s in UNITS else "",
            ]
        with uow:
            uow.export.synthetize_df(metadata_df, "METADADOS_CENARIOS")

    @classmethod
    def _add_synthesis_stats(cls, s: ScenarioSynthesis, df: pd.DataFrame):
        """
        Adiciona um DataFrame com estatísticas de uma síntese ao
        DataFrame de estatísticas da agregação espacial e etapa em questão.
        """
        df[VARIABLE_COL] = s.variable.value

        key = (s.spatial_resolution, s.step)

        if key not in cls.SYNTHESIS_STATS:
            cls.SYNTHESIS_STATS[key] = [df]
        else:
            cls.SYNTHESIS_STATS[key].append(df)

    @classmethod
    def _export_scenario_synthesis(
        cls, s: ScenarioSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ):
        """
        Realiza a exportação dos dados para uma síntese dos
        cenários desejada. Opcionalmente, os dados são armazenados
        em cache para uso futuro e as estatísticas são adicionadas
        ao DataFrame de estatísticas da agregação espacial e etapa em questão.
        """
        filename = str(s)
        with time_and_log(
            message_root="Tempo para exportacao dos dados", logger=cls.logger
        ):
            num_scenarios = Deck.numero_cenarios_simulacao_final(uow)
            scenarios = pd.Series(
                [str(i) for i in np.arange(1, num_scenarios + 1)],
                dtype=STRING_DF_TYPE,
            )
            df = df.astype({SCENARIO_COL: STRING_DF_TYPE})
            # TODO - garantir tipo de dados das colunas iteracao e estagio como int
            scenarios_df = df.loc[df[SCENARIO_COL].isin(scenarios)]
            scenarios_df = scenarios_df.astype({SCENARIO_COL: int})
            stats_df = df.drop(index=scenarios_df.index).reset_index(drop=True)
            scenarios_df = scenarios_df.sort_values(
                s.sorting_synthesis_df_columns
            ).reset_index(drop=True)

            if stats_df.empty:
                stats_df = cls._calc_statistics(scenarios_df)
            cls._add_synthesis_stats(s, stats_df)
            with uow:
                uow.export.synthetize_df(scenarios_df, filename)

    @classmethod
    def _export_stats(
        cls,
        uow: AbstractUnitOfWork,
    ):
        """
        Realiza a exportação dos dados de estatísticas de síntese
        da operação. As estatísticas são exportadas para um arquivo
        único por agregação espacial e etapa, de nome
        `CENARIOS_{agregacao}_{etapa}`.
        """
        for (res, step), dfs in cls.SYNTHESIS_STATS.items():
            with uow:
                df = pd.concat(dfs, ignore_index=True)
                df_columns = df.columns.tolist()
                columns_without_variable = [
                    c for c in df_columns if c != VARIABLE_COL
                ]
                df = df[[VARIABLE_COL] + columns_without_variable]
                df = df.astype({VARIABLE_COL: STRING_DF_TYPE})
                filename = (
                    f"{SCENARIO_SYNTHESIS_STATS_ROOT}_{res.value}_{step.value}"
                )
                uow.export.synthetize_df(df, filename)

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: List[str], uow: AbstractUnitOfWork
    ) -> List[ScenarioSynthesis]:
        """
        Realiza o pré-processamento das variáveis de síntese fornecidas,
        filtrando as válidas para o caso em questão.
        """
        try:
            if len(variables) == 0:
                synthesis_variables = cls._default_args()
            else:
                all_variables = cls._match_wildcards(variables)
                synthesis_variables = cls._process_variable_arguments(
                    all_variables
                )
            valid_synthesis = cls.filter_valid_variables(
                synthesis_variables, uow
            )
        except Exception as e:
            print_exc()
            cls._log(str(e), level=ERROR)
            cls._log("Erro no pré-processamento das variáveis", ERROR)
            valid_synthesis = []
        return valid_synthesis

    @classmethod
    def _synthetize_single_variable(
        cls, s: ScenarioSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[ScenarioSynthesis]:
        """
        Realiza a síntese de cenários para uma variável
        fornecida.
        """
        filename = str(s)
        with time_and_log(
            message_root=f"Tempo para sintese de {filename}",
            logger=cls.logger,
        ):
            try:
                filename = str(s)
                cls._log(f"Realizando síntese de {filename}")
                df = cls._resolve_spatial_resolution(s, uow)
                if df is None:
                    return None
                elif isinstance(df, pd.DataFrame):
                    if df.empty:
                        cls._log("Erro ao realizar a síntese")
                        return None
                cls._export_scenario_synthesis(s, df, uow)
                return s
            except Exception as e:
                print_exc()
                cls._log(str(e), level=ERROR)
                return None

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        """
        Realiza a síntese dos cenários para as variáveis fornecidas,
        na agregação desejada e para a etapa escolhida. As variáveis são
        pré-processadas para filtrar as válidas para o caso em questão,
        e então são resolvidas de acordo com a síntese.
        """
        cls.logger = logging.getLogger("main")
        uow.subdir = SCENARIO_SYNTHESIS_SUBDIR
        with time_and_log(
            message_root="Tempo para síntese dos cenários", logger=cls.logger
        ):
            valid_synthesis = cls._preprocess_synthesis_variables(
                variables, uow
            )
            success_synthesis: List[ScenarioSynthesis] = []
            for s in valid_synthesis:
                r = cls._synthetize_single_variable(s, uow)
                if r:
                    success_synthesis.append(r)

            cls._export_stats(uow)
            cls._export_metadata(success_synthesis, uow)
