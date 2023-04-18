from typing import Callable, Dict, List, Tuple, Optional
import pandas as pd  # type: ignore
import numpy as np  # type: ignore

from datetime import datetime
from dateutil.relativedelta import relativedelta
from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.scenario.variable import Variable
from sintetizador.model.scenario.spatialresolution import SpatialResolution
from sintetizador.model.scenario.step import Step
from sintetizador.model.scenario.scenariosynthesis import ScenarioSynthesis


class ScenarioSynthetizer:
    DEFAULT_SCENARIO_SYNTHESIS_ARGS: List[str] = [
        "ENAA_REE_FOR",
        "ENAA_REE_BKW",
        "ENAA_REE_SF",
        "ENAA_SBM_FOR",
        "ENAA_SBM_BKW",
        "ENAA_SBM_SF",
        "ENAA_SIN_FOR",
        "ENAA_SIN_BKW",
        "ENAA_SIN_SF",
        "QINC_UHE_FOR",
        "QINC_UHE_BKW",
        "QINC_UHE_SF",
        "QINC_REE_FOR",
        "QINC_REE_BKW",
        "QINC_REE_SF",
        "QINC_SBM_FOR",
        "QINC_SBM_BKW",
        "QINC_SBM_SF",
        "QINC_SIN_FOR",
        "QINC_SIN_BKW",
        "QINC_SIN_SF",
        "ENAM_REE_FOR",
        "ENAM_REE_BKW",
        "ENAM_REE_SF",
        "ENAM_SBM_FOR",
        "ENAM_SBM_BKW",
        "ENAM_SBM_SF",
        "ENAM_SIN_FOR",
        "ENAM_SIN_BKW",
        "ENAM_SIN_SF",
        "QINCM_UHE_FOR",
        "QINCM_UHE_BKW",
        "QINCM_UHE_SF",
        "QINCM_REE_FOR",
        "QINCM_REE_BKW",
        "QINCM_REE_SF",
        "QINCM_SBM_FOR",
        "QINCM_SBM_BKW",
        "QINCM_SBM_SF",
        "QINCM_SIN_FOR",
        "QINCM_SIN_BKW",
        "QINCM_SIN_SF",
    ]

    COMMON_COLUMNS: List[str] = [
        "iteracao",
        "estagio",
        "data",
        "data_fim",
        "serie",
        "abertura",
    ]

    CACHED_SYNTHESIS: Dict[Tuple[Variable, Step], pd.DataFrame] = {}

    CACHED_MLT_VALUES: Dict[
        Tuple[Variable, SpatialResolution], pd.DataFrame
    ] = {}

    @classmethod
    def _default_args(cls) -> List[ScenarioSynthesis]:
        return [
            ScenarioSynthesis.factory(a)
            for a in cls.DEFAULT_SCENARIO_SYNTHESIS_ARGS
        ]

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[ScenarioSynthesis]:
        args_data = [ScenarioSynthesis.factory(c) for c in args]
        for i, a in enumerate(args_data):
            if a is None:
                Log.log(f"Erro no argumento fornecido: {args[i]}")
                return []
        return args_data

    @classmethod
    def _validate_spatial_resolution_request(
        cls, spatial_resolution: SpatialResolution, *args, **kwargs
    ) -> bool:
        RESOLUTION_ARGS_MAP: Dict[SpatialResolution, List[str]] = {
            SpatialResolution.SISTEMA_INTERLIGADO: [],
            SpatialResolution.SUBMERCADO: ["submercado"],
            SpatialResolution.RESERVATORIO_EQUIVALENTE: ["ree"],
            SpatialResolution.USINA_HIDROELETRICA: ["uhe"],
            SpatialResolution.PARQUE_EOLICO_EQUIVALENTE: ["pee"],
        }

        mandatory = RESOLUTION_ARGS_MAP[spatial_resolution]
        valid = all([a in kwargs.keys() for a in mandatory])
        if not valid:
            Log.log().error(
                f"Erro no processamento da informação por {spatial_resolution}"
            )
        return valid

    @classmethod
    def filter_valid_variables(
        cls, variables: List[ScenarioSynthesis], uow: AbstractUnitOfWork
    ) -> List[ScenarioSynthesis]:
        with uow:
            dger = uow.files.get_dger()
            ree = uow.files.get_ree()
        valid_variables: List[ScenarioSynthesis] = []
        sf_indiv = dger.agregacao_simulacao_final == 1
        politica_indiv = ree.rees["Mês Fim Individualizado"].isna().sum() == 0
        indiv = sf_indiv or politica_indiv
        eolica = dger.considera_geracao_eolica != 0
        Log.log().info(f"Caso com geração de cenários de eólica: {eolica}")
        Log.log().info(f"Caso com modelagem híbrida: {indiv}")
        for v in variables:
            if v.variable == Variable.VAZAO_INCREMENTAL_ABSOLUTA and not indiv:
                continue
            valid_variables.append(v)
        Log.log().info(f"Variáveis: {valid_variables}")
        return valid_variables

    @classmethod
    def _gera_serie_incremental_uhe(
        cls, uhe: int, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtém a série histórica de vazões incrementais para uma UHE.

        - data (`datetime`)
        - vazao (`float`)

        :return: A tabela como um DataFrame
        :rtype: pd.DataFrame | None
        """
        uhes = uow.files.get_confhd().usinas
        hidr = uow.files.get_hidr().cadastro
        vazoes = uow.files.get_vazoes().vazoes
        posto = uhes.loc[uhes["Número"] == uhe, "Posto"].tolist()[0]
        vazao_natural = vazoes[posto].to_numpy()
        posto_nulo = posto == 300
        if not posto_nulo:
            uhes_montante = uhes.loc[uhes["Jusante"] == uhe, "Número"].tolist()
            uhes_montante = [u for u in uhes_montante if u != 0]
            postos_montante = list(
                set(
                    [
                        hidr.at[int(uhe_montante), "Posto"]
                        for uhe_montante in uhes_montante
                    ]
                )
            )
            for posto_montante in postos_montante:
                vazao_natural = (
                    vazao_natural - vazoes[posto_montante].to_numpy()
                )
        ano_inicio_historico = int(
            uhes.loc[uhes["Número"] == uhe, "Início do Histórico"].iloc[0]
        )
        ano_fim_historico = int(
            uhes.loc[uhes["Número"] == uhe, "Fim do Histórico"].iloc[0]
        )
        datas = pd.date_range(
            datetime(year=ano_inicio_historico, month=1, day=1),
            datetime(year=ano_fim_historico, month=12, day=1),
            freq="MS",
        )
        return pd.DataFrame(
            data={
                "data": datas,
                "vazao": vazao_natural[: len(datas)],
            }
        )

    @classmethod
    def _mlt(cls, serie_historica: pd.DataFrame) -> pd.DataFrame:
        """
        Extrai a MLT de uma série histórica de vazões de
        uma UHE, agrupando por mês.

        - mes (`int`)
        - vazao (`float`)

        :return: A tabela como um DataFrame
        :rtype: pd.DataFrame | None
        """
        serie_historica["mes"] = serie_historica.apply(
            lambda linha: linha["data"].month, axis=1
        )
        return (
            serie_historica.groupby(["mes"])
            .mean(numeric_only=True)
            .reset_index()
        )

    @classmethod
    def _gera_series_vazao_mlt_uhes(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Extrai a MLT para todas as UHEs.

        - codigo_usina (`int`)
        - nome_usina (`str`)
        - codigo_ree (`int`)
        - nome_ree (`str`)
        - codigo_submercado (`int`)
        - nome_submercado (`str`)
        - mes (`int`)
        - vazao (`float`)

        :return: A tabela como um DataFrame
        :rtype: pd.DataFrame | None
        """
        Log.log().info("Calculando séries de MLT para QINC - UHE")
        uhes = uow.files.get_confhd().usinas
        hidr = uow.files.get_hidr().cadastro
        df_completo_mlt = pd.DataFrame(
            columns=["codigo_usina", "nome_usina", "mes", "vazao"]
        )
        for uhe in uhes["Número"]:
            vazao = cls._gera_serie_incremental_uhe(uhe, uow)
            vazao_mlt = cls._mlt(vazao)
            df_mlt = pd.DataFrame(
                data={
                    "codigo_usina": [uhe] * len(vazao_mlt),
                    "nome_usina": [hidr.at[uhe, "Nome"]] * len(vazao_mlt),
                    "mes": vazao_mlt["mes"],
                    "vazao": vazao_mlt["vazao"],
                }
            )
            df_completo_mlt = pd.concat(
                [df_completo_mlt, df_mlt], ignore_index=True
            )
        # Adiciona dados de ree e submercado à série
        confhd = uow.files.get_confhd().usinas
        rees = uow.files.get_ree().rees
        sistema = uow.files.get_sistema().custo_deficit
        df_completo_mlt["codigo_ree"] = df_completo_mlt.apply(
            lambda linha: confhd.loc[
                confhd["Número"] == linha["codigo_usina"], "REE"
            ].tolist()[0],
            axis=1,
        )
        df_completo_mlt["nome_ree"] = df_completo_mlt.apply(
            lambda linha: rees.loc[
                rees["Número"] == linha["codigo_ree"], "Nome"
            ].tolist()[0],
            axis=1,
        )
        df_completo_mlt["codigo_submercado"] = df_completo_mlt.apply(
            lambda linha: rees.loc[
                rees["Número"] == linha["codigo_ree"], "Submercado"
            ].tolist()[0],
            axis=1,
        )
        df_completo_mlt["nome_submercado"] = df_completo_mlt.apply(
            lambda linha: sistema.loc[
                sistema["Num. Subsistema"] == linha["codigo_submercado"],
                "Nome",
            ].tolist()[0],
            axis=1,
        )
        return df_completo_mlt

    @classmethod
    def _gera_series_energia_mlt_uhes(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Extrai a MLT para todas as UHEs.

        Premissa: a conversão para a energia é
        feita com base na primeira configuração por
        enquanto.

        - codigo_usina (`int`)
        - nome_usina (`str`)
        - codigo_ree (`int`)
        - nome_ree (`str`)
        - codigo_submercado (`int`)
        - nome_submercado (`str`)
        - mes (`int`)
        - vazao (`float`)

        :return: A tabela como um DataFrame
        :rtype: pd.DataFrame | None
        """
        Log.log().info("Calculando séries de MLT para ENAA - UHE")
        mlt_uhe = cls._get_cached_mlt(
            Variable.VAZAO_INCREMENTAL_ABSOLUTA,
            SpatialResolution.USINA_HIDROELETRICA,
            uow,
        ).copy()
        pmo = uow.files.get_pmo()
        prodts = pmo.produtibilidades_equivalentes
        prodts = prodts.loc[prodts["configuracao"] == 1]
        col_reserv = "produtibilidade_acumulada_calculo_altura_65"
        col_fio = "produtibilidade_equivalente_volmin_volmax"
        reservatorios = (
            prodts[["nome_usina", col_reserv]].dropna()["nome_usina"].tolist()
        )
        mlt_uhe.loc[
            mlt_uhe["nome_usina"].isin(reservatorios), "prodt"
        ] = mlt_uhe.loc[mlt_uhe["nome_usina"].isin(reservatorios)].apply(
            lambda linha: prodts.loc[
                prodts["nome_usina"] == linha["nome_usina"], col_reserv
            ].tolist()[0],
            axis=1,
        )
        mlt_uhe.loc[
            ~mlt_uhe["nome_usina"].isin(reservatorios), "prodt"
        ] = mlt_uhe.loc[~mlt_uhe["nome_usina"].isin(reservatorios)].apply(
            lambda linha: prodts.loc[
                prodts["nome_usina"] == linha["nome_usina"], col_fio
            ].tolist()[0],
            axis=1,
        )
        # Limita as afluências das fio d'água ao engolimento
        engolimentos = cls._engolimento_maximo_uhes(uow)
        mlt_uhe.loc[
            ~mlt_uhe["nome_usina"].isin(reservatorios), "vazao_max"
        ] = mlt_uhe.loc[~mlt_uhe["nome_usina"].isin(reservatorios)].apply(
            lambda linha: engolimentos[linha["codigo_usina"]],
            axis=1,
        )
        mlt_uhe.loc[
            ~mlt_uhe["nome_usina"].isin(reservatorios), "vazao"
        ] = mlt_uhe.loc[
            ~mlt_uhe["nome_usina"].isin(reservatorios), ["vazao", "vazao_max"]
        ].min(
            axis=1
        )
        # Multiplica todas pelas produtibilidades
        mlt_uhe["vazao"] = mlt_uhe["vazao"] * mlt_uhe["prodt"]
        return mlt_uhe.drop(columns=["prodt", "vazao_max"])

    @classmethod
    def _engolimento_maximo_uhes(
        cls, uow: AbstractUnitOfWork
    ) -> Dict[int, float]:
        hidr = uow.files.get_hidr().cadastro
        engolimentos: Dict[int, float] = {}
        for idx, linha in hidr.iterrows():
            engol_max = 0.0
            n_conj = linha["Num Conjunto Máquinas"]
            for i in range(1, n_conj + 1):
                n_maq = linha[f"Num Máquinas Conjunto {i}"]
                qef_maq = linha[f"QEf Conjunto {i}"]
                engol_max += n_maq * qef_maq
            engolimentos[idx] = engol_max
        return engolimentos

    @classmethod
    def _agrega_serie_mlt(
        cls, variavel: Variable, col: str, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        mlt_uhe = cls._get_cached_mlt(
            variavel,
            SpatialResolution.USINA_HIDROELETRICA,
            uow,
        )
        col_list = [col] if col is not None else []
        df = mlt_uhe.groupby(col_list + ["mes"]).sum().reset_index()
        return df[col_list + ["mes", "vazao"]]

    @classmethod
    def _resolve_enaa_mlt_ree(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        Log.log().info("Calculando séries de MLT para ENAA - REE")
        return cls._agrega_serie_mlt(Variable.ENA_ABSOLUTA, "nome_ree", uow)

    @classmethod
    def _resolve_enaa_mlt_submercado(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        Log.log().info("Calculando séries de MLT para ENAA - SBM")
        return cls._agrega_serie_mlt(
            Variable.ENA_ABSOLUTA, "nome_submercado", uow
        )

    @classmethod
    def _resolve_enaa_mlt_sin(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        Log.log().info("Calculando séries de MLT para ENAA - SIN")
        return cls._agrega_serie_mlt(Variable.ENA_ABSOLUTA, None, uow)

    @classmethod
    def _resolve_qinc_mlt_ree(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        Log.log().info("Calculando séries de MLT para QINC - REE")
        return cls._agrega_serie_mlt(
            Variable.VAZAO_INCREMENTAL_ABSOLUTA, "nome_ree", uow
        )

    @classmethod
    def _resolve_qinc_mlt_submercado(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        Log.log().info("Calculando séries de MLT para QINC - SBM")
        return cls._agrega_serie_mlt(
            Variable.VAZAO_INCREMENTAL_ABSOLUTA, "nome_submercado", uow
        )

    @classmethod
    def _resolve_qinc_mlt_sin(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        Log.log().info("Calculando séries de MLT para QINC - SIN")
        return cls._agrega_serie_mlt(
            Variable.VAZAO_INCREMENTAL_ABSOLUTA, None, uow
        )

    @classmethod
    def _get_cached_mlt(
        cls,
        variable: Variable,
        spatial_resolution: SpatialResolution,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        CACHING_FUNCTION_MAP: Dict[
            Tuple[Variable, SpatialResolution], Callable
        ] = {
            (
                Variable.ENA_ABSOLUTA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): cls._gera_series_energia_mlt_uhes,
            (
                Variable.ENA_ABSOLUTA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): cls._resolve_enaa_mlt_ree,
            (
                Variable.ENA_ABSOLUTA,
                SpatialResolution.SUBMERCADO,
            ): cls._resolve_enaa_mlt_submercado,
            (
                Variable.ENA_ABSOLUTA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): cls._resolve_enaa_mlt_sin,
            (
                Variable.VAZAO_INCREMENTAL_ABSOLUTA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): cls._gera_series_vazao_mlt_uhes,
            (
                Variable.VAZAO_INCREMENTAL_ABSOLUTA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): cls._resolve_qinc_mlt_ree,
            (
                Variable.VAZAO_INCREMENTAL_ABSOLUTA,
                SpatialResolution.SUBMERCADO,
            ): cls._resolve_qinc_mlt_submercado,
            (
                Variable.VAZAO_INCREMENTAL_ABSOLUTA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): cls._resolve_qinc_mlt_sin,
        }
        if cls.CACHED_MLT_VALUES.get((variable, spatial_resolution)) is None:
            cls.CACHED_MLT_VALUES[
                (variable, spatial_resolution)
            ] = CACHING_FUNCTION_MAP[(variable, spatial_resolution)](uow)
        return cls.CACHED_MLT_VALUES.get(
            (variable, spatial_resolution), pd.DataFrame()
        )

    @classmethod
    def _formata_dados_series(
        cls, dados: np.ndarray, n_series: int, n_estagios: int
    ) -> np.ndarray:
        return np.tile(np.repeat(dados, n_series), (n_estagios,))

    @classmethod
    def _adiciona_dados_rees_forward(
        cls, uow: AbstractUnitOfWork, energiaf: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Adiciona dados do REE aos dados de energia lidos do arquivo
        binário `energiaf.dat`.

        - estagio (`int`)
        - data (`datetime`)
        - data_fim (`datetime`)
        - serie (`int`)
        - codigo_ree (`int`)
        - nome_ree (`str`)
        - codigo_submercado (`int`)
        - nome_submercado (`str`)
        - valor (`float`)

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        energiaf_dados = energiaf.copy()
        series = energiaf_dados["serie"].unique()
        num_series = len(series)
        rees = energiaf_dados["ree"].unique()
        num_rees = len(rees)
        estagios = energiaf_dados["estagio"].unique()
        num_estagios = len(estagios)
        # Obtem os dados de cada usina
        dger = uow.files.get_dger()
        sistema = uow.files.get_sistema().custo_deficit
        dados_rees = uow.files.get_ree().rees
        dados_rees["Nome Submercado"] = dados_rees.apply(
            lambda linha: sistema.loc[
                sistema["Num. Subsistema"] == linha["Submercado"], "Nome"
            ].tolist()[0],
            axis=1,
        )
        # Gera os vetores da dimensão do DF extraído do arquivo vazaof
        codigos_ordenados = cls._formata_dados_series(
            dados_rees["Número"].to_numpy(), num_series, num_estagios
        )
        nomes_ordenados = cls._formata_dados_series(
            dados_rees["Nome"].to_numpy(), num_series, num_estagios
        )
        submercados_ordenados = cls._formata_dados_series(
            dados_rees["Submercado"].to_numpy(), num_series, num_estagios
        )
        nomes_submercados_ordenados = cls._formata_dados_series(
            dados_rees["Nome Submercado"].to_numpy(), num_series, num_estagios
        )
        datas = pd.date_range(
            datetime(
                year=dger.ano_inicio_estudo - 1,
                month=1,
                day=1,
            ),
            datetime(
                year=dger.ano_inicio_estudo + dger.num_anos_estudo - 1,
                month=12,
                day=1,
            ),
            freq="MS",
        )
        datas_ordenadas = np.repeat(datas, num_series * num_rees)
        # Edita o DF e retorna
        energiaf_dados["codigo_ree"] = codigos_ordenados
        energiaf_dados["nome_ree"] = nomes_ordenados
        energiaf_dados["codigo_submercado"] = submercados_ordenados
        energiaf_dados["nome_submercado"] = nomes_submercados_ordenados
        energiaf_dados["data"] = datas_ordenadas
        energiaf_dados["data_fim"] = [
            d + relativedelta(months=1) for d in datas_ordenadas
        ]
        energiaf_dados["estagio"] -= dger.mes_inicio_estudo - 1
        energiaf_dados = energiaf_dados.loc[energiaf_dados["estagio"] > 0]
        return energiaf_dados[
            [
                "estagio",
                "data",
                "data_fim",
                "serie",
                "codigo_ree",
                "nome_ree",
                "codigo_submercado",
                "nome_submercado",
                "valor",
            ]
        ]

    @classmethod
    def _adiciona_dados_uhes_forward(
        cls, uow: AbstractUnitOfWork, vazaof: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Adiciona dados de código da UHE, nome da UHE, ree da UHE e
        submercado da UHE aos dados de vazão lidos do arquivo
        binário `vazaof.dat`.

        - estagio (`int`)
        - data (`datetime`)
        - data_fim (`datetime`)
        - serie (`int`)
        - codigo_usina (`int`)
        - nome_usina (`str`)
        - codigo_ree (`int`)
        - nome_ree (`str`)
        - codigo_submercado (`int`)
        - nome_submercado (`str`)
        - valor (`float`)

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        # Extrai dimensões para repetir vetores
        vazaof_dados = vazaof.copy()
        series = vazaof_dados["serie"].unique()
        num_series = len(series)
        uhes = vazaof_dados["uhe"].unique()
        num_uhes = len(uhes)
        estagios = vazaof_dados["estagio"].unique()
        num_estagios = len(estagios)
        # Obtem os dados de cada usina
        dger = uow.files.get_dger()
        confhd = uow.files.get_confhd().usinas
        rees = uow.files.get_ree().rees
        sistema = uow.files.get_sistema().custo_deficit
        dados_uhes = pd.DataFrame(uhes).apply(
            lambda linha: confhd.loc[
                linha[0] - 1, ["Número", "Nome", "REE"]
            ].tolist(),
            axis=1,
            result_type="expand",
        )
        dados_uhes[3] = dados_uhes.apply(
            lambda linha: rees.loc[
                rees["Número"] == linha[2], "Nome"
            ].tolist()[0],
            axis=1,
        )
        dados_uhes[4] = dados_uhes.apply(
            lambda linha: rees.loc[
                rees["Número"] == linha[2], "Submercado"
            ].tolist()[0],
            axis=1,
        )
        dados_uhes[5] = dados_uhes.apply(
            lambda linha: sistema.loc[
                sistema["Num. Subsistema"] == linha[4], "Nome"
            ].tolist()[0],
            axis=1,
        )
        # Gera os vetores da dimensão do DF extraído do arquivo vazaof
        codigos_ordenados = cls._formata_dados_series(
            dados_uhes[0].to_numpy(), num_series, num_estagios
        )
        nomes_ordenados = cls._formata_dados_series(
            dados_uhes[1].to_numpy(), num_series, num_estagios
        )
        codigos_rees_ordenados = cls._formata_dados_series(
            dados_uhes[2].to_numpy(), num_series, num_estagios
        )
        nomes_rees_ordenados = cls._formata_dados_series(
            dados_uhes[3].to_numpy(), num_series, num_estagios
        )
        codigos_submercados_ordenados = cls._formata_dados_series(
            dados_uhes[4].to_numpy(), num_series, num_estagios
        )
        nomes_submercados_ordenados = cls._formata_dados_series(
            dados_uhes[5].to_numpy(), num_series, num_estagios
        )
        datas = pd.date_range(
            datetime(
                year=dger.ano_inicio_estudo - 1,
                month=1,
                day=1,
            ),
            datetime(
                year=dger.ano_inicio_estudo - 1,
                month=1,
                day=1,
            )
            + relativedelta(months=num_estagios - 1),
            freq="MS",
        )
        datas_ordenadas = np.repeat(datas, num_series * num_uhes)
        # Edita o DF e retorna
        vazaof_dados["codigo_usina"] = codigos_ordenados
        vazaof_dados["nome_usina"] = nomes_ordenados
        vazaof_dados["codigo_ree"] = codigos_rees_ordenados
        vazaof_dados["nome_ree"] = nomes_rees_ordenados
        vazaof_dados["codigo_submercado"] = codigos_submercados_ordenados
        vazaof_dados["nome_submercado"] = nomes_submercados_ordenados
        vazaof_dados["data"] = datas_ordenadas
        vazaof_dados["data_fim"] = [
            d + relativedelta(months=1) for d in datas_ordenadas
        ]
        vazaof_dados["estagio"] -= dger.mes_inicio_estudo - 1
        vazaof_dados = vazaof_dados.loc[vazaof_dados["estagio"] > 0]
        vazaof_dados.drop(columns=["uhe"], inplace=True)
        return vazaof_dados[
            [
                "estagio",
                "data",
                "data_fim",
                "serie",
                "codigo_usina",
                "nome_usina",
                "codigo_ree",
                "nome_ree",
                "codigo_submercado",
                "nome_submercado",
                "valor",
            ]
        ]

    @classmethod
    def _adiciona_dados_rees_backward(
        cls, uow: AbstractUnitOfWork, energiab: pd.DataFrame
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
        # Extrai dimensões para repetir vetores
        energiab_dados = energiab.copy()
        series = energiab_dados["serie"].unique()
        num_series = len(series)
        rees = energiab_dados["ree"].unique()
        num_rees = len(rees)
        estagios = energiab_dados["estagio"].unique()
        num_estagios = len(estagios)
        aberturas = energiab_dados["abertura"].unique()
        num_aberturas = len(aberturas)
        # Obtem os dados de cada usina
        dger = uow.files.get_dger()
        rees = uow.files.get_ree().rees
        sistema = uow.files.get_sistema().custo_deficit
        dados_rees = uow.files.get_ree().rees
        dados_rees["Nome Submercado"] = dados_rees.apply(
            lambda linha: sistema.loc[
                sistema["Num. Subsistema"] == linha["Submercado"], "Nome"
            ].tolist()[0],
            axis=1,
        )
        # Gera os vetores da dimensão do DF extraído do arquivo vazaob
        codigos_ordenados = cls._formata_dados_series(
            dados_rees["Número"].to_numpy(),
            num_series * num_aberturas,
            num_estagios,
        )
        nomes_ordenados = cls._formata_dados_series(
            dados_rees["Nome"].to_numpy(),
            num_series * num_aberturas,
            num_estagios,
        )
        codigos_submercados_ordenados = cls._formata_dados_series(
            dados_rees["Submercado"].to_numpy(),
            num_series * num_aberturas,
            num_estagios,
        )
        nomes_submercados_ordenados = cls._formata_dados_series(
            dados_rees["Nome Submercado"].to_numpy(),
            num_series * num_aberturas,
            num_estagios,
        )
        datas = pd.date_range(
            datetime(
                year=dger.ano_inicio_estudo,
                month=1,
                day=1,
            ),
            datetime(
                year=dger.ano_inicio_estudo + dger.num_anos_estudo - 1,
                month=12,
                day=1,
            ),
            freq="MS",
        )
        datas_ordenadas = np.repeat(
            datas, num_series * num_aberturas * num_rees
        )
        # Edita o DF e retorna
        energiab_dados["codigo_ree"] = codigos_ordenados
        energiab_dados["nome_ree"] = nomes_ordenados
        energiab_dados["codigo_submercado"] = codigos_submercados_ordenados
        energiab_dados["nome_submercado"] = nomes_submercados_ordenados
        energiab_dados["data"] = datas_ordenadas
        energiab_dados["data_fim"] = [
            d + relativedelta(months=1) for d in datas_ordenadas
        ]
        energiab_dados["estagio"] -= dger.mes_inicio_estudo - 1
        energiab_dados = energiab_dados.loc[energiab_dados["estagio"] > 0]
        return energiab_dados[
            [
                "estagio",
                "data",
                "data_fim",
                "serie",
                "abertura",
                "codigo_ree",
                "nome_ree",
                "codigo_submercado",
                "nome_submercado",
                "valor",
            ]
        ]

    @classmethod
    def _adiciona_dados_uhes_backward(
        cls, uow: AbstractUnitOfWork, vazaob: pd.DataFrame
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
        - nome_usina (`str`)
        - codigo_ree (`int`)
        - nome_ree (`str`)
        - codigo_submercado (`int`)
        - nome_submercado (`str`)
        - valor (`float`)

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        # Extrai dimensões para repetir vetores
        vazaob_dados = vazaob.copy()
        series = vazaob_dados["serie"].unique()
        num_series = len(series)
        uhes = vazaob_dados["uhe"].unique()
        num_uhes = len(uhes)
        estagios = vazaob_dados["estagio"].unique()
        num_estagios = len(estagios)
        aberturas = vazaob_dados["abertura"].unique()
        num_aberturas = len(aberturas)
        # Obtem os dados de cada usina
        dger = uow.files.get_dger()
        confhd = uow.files.get_confhd().usinas
        rees = uow.files.get_ree().rees
        sistema = uow.files.get_sistema().custo_deficit
        dados_uhes = pd.DataFrame(uhes).apply(
            lambda linha: confhd.loc[
                linha[0] - 1, ["Número", "Nome", "REE"]
            ].tolist(),
            axis=1,
            result_type="expand",
        )
        dados_uhes[3] = dados_uhes.apply(
            lambda linha: rees.loc[
                rees["Número"] == linha[2], "Nome"
            ].tolist()[0],
            axis=1,
        )
        dados_uhes[4] = dados_uhes.apply(
            lambda linha: rees.loc[
                rees["Número"] == linha[2], "Submercado"
            ].tolist()[0],
            axis=1,
        )
        dados_uhes[5] = dados_uhes.apply(
            lambda linha: sistema.loc[
                sistema["Num. Subsistema"] == linha[4], "Nome"
            ].tolist()[0],
            axis=1,
        )
        # Gera os vetores da dimensão do DF extraído do arquivo vazaob
        codigos_ordenados = cls._formata_dados_series(
            dados_uhes[0].to_numpy(), num_series * num_aberturas, num_estagios
        )
        nomes_ordenados = cls._formata_dados_series(
            dados_uhes[1].to_numpy(), num_series * num_aberturas, num_estagios
        )
        codigos_rees_ordenados = cls._formata_dados_series(
            dados_uhes[2].to_numpy(), num_series * num_aberturas, num_estagios
        )
        nomes_rees_ordenados = cls._formata_dados_series(
            dados_uhes[3].to_numpy(), num_series * num_aberturas, num_estagios
        )
        codigos_submercados_ordenados = cls._formata_dados_series(
            dados_uhes[4].to_numpy(), num_series * num_aberturas, num_estagios
        )
        nomes_submercados_ordenados = cls._formata_dados_series(
            dados_uhes[5].to_numpy(), num_series * num_aberturas, num_estagios
        )
        datas = pd.date_range(
            datetime(
                year=dger.ano_inicio_estudo - 1,
                month=1,
                day=1,
            ),
            datetime(
                year=dger.ano_inicio_estudo - 1,
                month=1,
                day=1,
            )
            + relativedelta(months=num_estagios - 1),
            freq="MS",
        )
        datas_ordenadas = np.repeat(
            datas, num_series * num_aberturas * num_uhes
        )
        # Edita o DF e retorna
        vazaob_dados["codigo_usina"] = codigos_ordenados
        vazaob_dados["nome_usina"] = nomes_ordenados
        vazaob_dados["codigo_ree"] = codigos_rees_ordenados
        vazaob_dados["nome_ree"] = nomes_rees_ordenados
        vazaob_dados["codigo_submercado"] = codigos_submercados_ordenados
        vazaob_dados["nome_submercado"] = nomes_submercados_ordenados
        vazaob_dados["data"] = datas_ordenadas
        vazaob_dados["data_fim"] = [
            d + relativedelta(months=1) for d in datas_ordenadas
        ]
        vazaob_dados["estagio"] -= dger.mes_inicio_estudo - 1
        vazaob_dados = vazaob_dados.loc[vazaob_dados["estagio"] > 0]
        vazaob_dados.drop(columns=["uhe"], inplace=True)
        return vazaob_dados[
            [
                "estagio",
                "data",
                "data_fim",
                "serie",
                "abertura",
                "codigo_usina",
                "nome_usina",
                "codigo_ree",
                "nome_ree",
                "codigo_submercado",
                "nome_submercado",
                "valor",
            ]
        ]

    @classmethod
    def _adiciona_dados_rees_sf(
        cls, uow: AbstractUnitOfWork, energias: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Adiciona dados do REE aos dados de energia lidos do arquivo
        binário `energias.dat`.

        - codigo_ree (`int`)
        - ree (`str`)
        - estagio (`int`)
        - data (`datetime`)
        - data_fim (`datetime`)
        - serie (`int`)
        - valor (`float`)

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        energias_dados = energias.copy()
        series = energias_dados["serie"].unique()
        num_series = len(series)
        rees = energias_dados["ree"].unique()
        num_rees = len(rees)
        estagios = energias_dados["estagio"].unique()
        num_estagios = len(estagios)
        # Obtem os dados de cada usina
        dger = uow.files.get_dger()
        sistema = uow.files.get_sistema().custo_deficit
        dados_rees = uow.files.get_ree().rees
        dados_rees["Nome Submercado"] = dados_rees.apply(
            lambda linha: sistema.loc[
                sistema["Num. Subsistema"] == linha["Submercado"], "Nome"
            ].tolist()[0],
            axis=1,
        )
        # Gera os vetores da dimensão do DF extraído do arquivo vazaof
        codigos_ordenados = cls._formata_dados_series(
            dados_rees["Número"].to_numpy(), num_series, num_estagios
        )
        nomes_ordenados = cls._formata_dados_series(
            dados_rees["Nome"].to_numpy(), num_series, num_estagios
        )
        submercados_ordenados = cls._formata_dados_series(
            dados_rees["Submercado"].to_numpy(), num_series, num_estagios
        )
        nomes_submercados_ordenados = cls._formata_dados_series(
            dados_rees["Nome Submercado"].to_numpy(), num_series, num_estagios
        )
        datas = pd.date_range(
            datetime(
                year=dger.ano_inicio_estudo - 1,
                month=1,
                day=1,
            ),
            datetime(
                year=dger.ano_inicio_estudo + dger.num_anos_estudo - 1,
                month=12,
                day=1,
            ),
            freq="MS",
        )
        datas_ordenadas = np.repeat(datas, num_series * num_rees)
        # Edita o DF e retorna
        energias_dados["codigo_ree"] = codigos_ordenados
        energias_dados["nome_ree"] = nomes_ordenados
        energias_dados["codigo_submercado"] = submercados_ordenados
        energias_dados["nome_submercado"] = nomes_submercados_ordenados
        energias_dados["data"] = datas_ordenadas
        energias_dados["data_fim"] = [
            d + relativedelta(months=1) for d in datas_ordenadas
        ]
        energias_dados["estagio"] -= dger.mes_inicio_estudo - 1
        energias_dados = energias_dados.loc[energias_dados["estagio"] > 0]
        return energias_dados[
            [
                "estagio",
                "data",
                "data_fim",
                "serie",
                "codigo_ree",
                "nome_ree",
                "codigo_submercado",
                "nome_submercado",
                "valor",
            ]
        ]

    @classmethod
    def _adiciona_dados_uhes_sf(
        cls, uow: AbstractUnitOfWork, vazaos: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Adiciona dados de código da UHE, nome da UHE, ree da UHE e
        submercado da UHE aos dados de vazão lidos do arquivo
        binário `vazaos.dat`.

        - estagio (`int`)
        - data (`datetime`)
        - data_fim (`datetime`)
        - serie (`int`)
        - codigo_uhe (`float`)
        - nome_uhe (`float`)
        - ree (`float`)
        - submercado (`float`)
        - valor (`float`)

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        # Extrai dimensões para repetir vetores
        vazaos_dados = vazaos.copy()
        series = vazaos_dados["serie"].unique()
        num_series = len(series)
        uhes = vazaos_dados["uhe"].unique()
        num_uhes = len(uhes)
        estagios = vazaos_dados["estagio"].unique()
        num_estagios = len(estagios)
        # Obtem os dados de cada usina
        dger = uow.files.get_dger()
        confhd = uow.files.get_confhd().usinas
        rees = uow.files.get_ree().rees
        sistema = uow.files.get_sistema().custo_deficit
        dados_uhes = pd.DataFrame(uhes).apply(
            lambda linha: confhd.loc[
                linha[0] - 1, ["Número", "Nome", "REE"]
            ].tolist(),
            axis=1,
            result_type="expand",
        )
        dados_uhes[3] = dados_uhes.apply(
            lambda linha: rees.loc[
                rees["Número"] == linha[2], "Nome"
            ].tolist()[0],
            axis=1,
        )
        dados_uhes[4] = dados_uhes.apply(
            lambda linha: rees.loc[
                rees["Número"] == linha[2], "Submercado"
            ].tolist()[0],
            axis=1,
        )
        dados_uhes[5] = dados_uhes.apply(
            lambda linha: sistema.loc[
                sistema["Num. Subsistema"] == linha[4], "Nome"
            ].tolist()[0],
            axis=1,
        )
        # Gera os vetores da dimensão do DF extraído do arquivo vazaof
        codigos_ordenados = cls._formata_dados_series(
            dados_uhes[0].to_numpy(), num_series, num_estagios
        )
        nomes_ordenados = cls._formata_dados_series(
            dados_uhes[1].to_numpy(), num_series, num_estagios
        )
        codigos_rees_ordenados = cls._formata_dados_series(
            dados_uhes[2].to_numpy(), num_series, num_estagios
        )
        nomes_rees_ordenados = cls._formata_dados_series(
            dados_uhes[3].to_numpy(), num_series, num_estagios
        )
        codigos_submercados_ordenados = cls._formata_dados_series(
            dados_uhes[4].to_numpy(), num_series, num_estagios
        )
        nomes_submercados_ordenados = cls._formata_dados_series(
            dados_uhes[5].to_numpy(), num_series, num_estagios
        )
        datas = pd.date_range(
            datetime(
                year=dger.ano_inicio_estudo - 1,
                month=1,
                day=1,
            ),
            datetime(
                year=dger.ano_inicio_estudo - 1,
                month=1,
                day=1,
            )
            + relativedelta(months=num_estagios - 1),
            freq="MS",
        )
        datas_ordenadas = np.repeat(datas, num_series * num_uhes)
        # Edita o DF e retorna
        vazaos_dados["codigo_usina"] = codigos_ordenados
        vazaos_dados["nome_usina"] = nomes_ordenados
        vazaos_dados["codigo_ree"] = codigos_rees_ordenados
        vazaos_dados["nome_ree"] = nomes_rees_ordenados
        vazaos_dados["codigo_submercado"] = codigos_submercados_ordenados
        vazaos_dados["nome_submercado"] = nomes_submercados_ordenados
        vazaos_dados["data"] = datas_ordenadas
        vazaos_dados["data_fim"] = [
            d + relativedelta(months=1) for d in datas_ordenadas
        ]
        vazaos_dados["estagio"] -= dger.mes_inicio_estudo - 1
        vazaos_dados = vazaos_dados.loc[vazaos_dados["estagio"] > 0]
        vazaos_dados.drop(columns=["uhe"], inplace=True)
        return vazaos_dados[
            [
                "estagio",
                "data",
                "data_fim",
                "serie",
                "codigo_usina",
                "nome_usina",
                "codigo_ree",
                "nome_ree",
                "codigo_submercado",
                "nome_submercado",
                "valor",
            ]
        ]

    @classmethod
    def _resolve_enaa_forward(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            pmo = uow.files.get_pmo()
            n_iters = pmo.convergencia["Iteração"].max()
            df_completo = pd.DataFrame()
            for it in range(1, n_iters + 1):
                enavaz = uow.files.get_enavazf(it)
                energiaf = uow.files.get_energiaf(it)
                df_enavaz = (
                    enavaz.series if enavaz is not None else pd.DataFrame()
                )
                df_energia = (
                    energiaf.series if energiaf is not None else pd.DataFrame()
                )
                if not df_enavaz.empty and not df_energia.empty:
                    n_indiv = uow.files._numero_estagios_individualizados()
                    df_ena = pd.concat(
                        [
                            df_enavaz.loc[df_enavaz["estagio"] <= n_indiv],
                            df_energia.loc[df_energia["estagio"] > n_indiv],
                        ],
                        ignore_index=True,
                    )
                else:
                    df_ena = df_energia
                if not df_ena.empty:
                    ena_it = cls._adiciona_dados_rees_forward(uow, df_ena)
                    ena_it["iteracao"] = it
                    df_completo = pd.concat(
                        [df_completo, ena_it], ignore_index=True
                    )
        return df_completo

    @classmethod
    def _resolve_qinc_forward(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            pmo = uow.files.get_pmo()
            n_iters = pmo.convergencia["Iteração"].max()
            df_completo = pd.DataFrame()
            for it in range(1, n_iters + 1):
                arq = uow.files.get_vazaof(it)
                vaz_it = (
                    cls._adiciona_dados_uhes_forward(uow, arq.series)
                    if arq is not None
                    else pd.DataFrame()
                )
                if not vaz_it.empty:
                    vaz_it["iteracao"] = it
                    df_completo = pd.concat(
                        [df_completo, vaz_it], ignore_index=True
                    )
        return df_completo

    @classmethod
    def _resolve_enaa_backward(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            enavazb = uow.files.get_enavazb()
            energiab = uow.files.get_energiab()
            df_enavaz = (
                enavazb.series if enavazb is not None else pd.DataFrame()
            )
            df_energia = (
                energiab.series if energiab is not None else pd.DataFrame()
            )
            if not df_enavaz.empty and not df_energia.empty:
                n_indiv = uow.files._numero_estagios_individualizados()
                df_ena = pd.concat(
                    [
                        df_enavaz.loc[df_enavaz["estagio"] <= n_indiv],
                        df_energia.loc[df_energia["estagio"] > n_indiv],
                    ],
                    ignore_index=True,
                )
            else:
                df_ena = df_energia
            enas = (
                cls._adiciona_dados_rees_backward(uow, df_ena)
                if not df_ena.empty
                else pd.DataFrame()
            )
        return enas

    @classmethod
    def _resolve_qinc_backward(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            arq = uow.files.get_vazaob()
            df = (
                cls._adiciona_dados_uhes_backward(uow, arq.series)
                if arq is not None
                else pd.DataFrame()
            )
        return df

    @classmethod
    def _resolve_enaa_sf(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            enavaz = uow.files.get_enavazs()
            energias = uow.files.get_energias()
            df_enavaz = enavaz.series if enavaz is not None else pd.DataFrame()
            df_energia = (
                energias.series if energias is not None else pd.DataFrame()
            )
            if not df_enavaz.empty and not df_energia.empty:
                n_indiv = uow.files._numero_estagios_individualizados()
                df_ena = pd.concat(
                    [
                        df_enavaz.loc[df_enavaz["estagio"] <= n_indiv],
                        df_energia.loc[df_energia["estagio"] > n_indiv],
                    ],
                    ignore_index=True,
                )
            else:
                df_ena = df_energia
            enas = (
                cls._adiciona_dados_rees_sf(uow, df_ena)
                if df_ena is not None
                else pd.DataFrame()
            )
        return enas

    @classmethod
    def _resolve_qinc_sf(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            arq = uow.files.get_vazaos()
            df = (
                cls._adiciona_dados_uhes_sf(uow, arq.series)
                if arq is not None
                else pd.DataFrame()
            )
        return df

    @classmethod
    def _get_cached_variable(
        cls, variable: Variable, step: Step, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        CACHING_FUNCTION_MAP: Dict[Tuple[Variable, Step], Callable] = {
            (Variable.ENA_ABSOLUTA, Step.FORWARD): cls._resolve_enaa_forward,
            (Variable.ENA_ABSOLUTA, Step.BACKWARD): cls._resolve_enaa_backward,
            (
                Variable.ENA_ABSOLUTA,
                Step.FINAL_SIMULATION,
            ): cls._resolve_enaa_sf,
            (
                Variable.VAZAO_INCREMENTAL_ABSOLUTA,
                Step.FORWARD,
            ): cls._resolve_qinc_forward,
            (
                Variable.VAZAO_INCREMENTAL_ABSOLUTA,
                Step.BACKWARD,
            ): cls._resolve_qinc_backward,
            (
                Variable.VAZAO_INCREMENTAL_ABSOLUTA,
                Step.FINAL_SIMULATION,
            ): cls._resolve_qinc_sf,
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
        if not df.empty:
            cols = group_col + [
                c for c in cls.COMMON_COLUMNS if c in df.columns
            ]
            df_agrupado = df.groupby(cols).sum().reset_index()
            return df_agrupado[cols + ["valor"]]
        else:
            return df

    @classmethod
    def _mlt_absolute_variable_map(cls, variable: Variable) -> Variable:
        MAP = {
            Variable.ENA_MLT: Variable.ENA_ABSOLUTA,
            Variable.VAZAO_INCREMENTAL_MLT: Variable.VAZAO_INCREMENTAL_ABSOLUTA,
        }
        return MAP[variable]

    @classmethod
    def _apply_mlt_forward_sf(
        cls,
        df: pd.DataFrame,
        df_mlt: pd.DataFrame,
        filter_col: Optional[str],
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        series = df["serie"].unique()
        num_series = len(series)
        elements = df[filter_col].unique() if filter_col is not None else []

        df_mlts_elements = pd.DataFrame()
        for mes in range(1, 13):
            if len(elements) > 0:
                for element in elements:
                    df_mlts_elements = pd.concat(
                        [
                            df_mlts_elements,
                            df_mlt.loc[
                                (df_mlt[filter_col] == element)
                                & (df_mlt["mes"] == mes),
                                "vazao",
                            ],
                        ],
                        ignore_index=True,
                    )
            else:
                df_mlts_elements = pd.concat(
                    [
                        df_mlts_elements,
                        df_mlt.loc[
                            (df_mlt["mes"] == mes),
                            "vazao",
                        ],
                    ],
                    ignore_index=True,
                )

        mlts_ordenadas = np.tile(
            np.repeat(df_mlts_elements.to_numpy(), num_series),
            uow.files.get_dger().num_anos_estudo + 1,
        )
        df["mlt"] = mlts_ordenadas
        df["valor_mlt"] = df["valor"] / df["mlt"]
        df.replace([np.inf, -np.inf], 0, inplace=True)
        return df

    @classmethod
    def _apply_mlt_backward(
        cls,
        df: pd.DataFrame,
        df_mlt: pd.DataFrame,
        filter_col: Optional[str],
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        series = df["serie"].unique()
        num_series = len(series)
        aberturas = df["abertura"].unique()
        num_aberturas = len(aberturas)
        elements = df[filter_col].unique() if filter_col is not None else []

        df_mlts_elements = pd.DataFrame()
        for mes in range(1, 13):
            if len(elements) > 0:
                for element in elements:
                    df_mlts_elements = pd.concat(
                        [
                            df_mlts_elements,
                            df_mlt.loc[
                                (df_mlt[filter_col] == element)
                                & (df_mlt["mes"] == mes),
                                "vazao",
                            ],
                        ],
                        ignore_index=True,
                    )
            else:
                df_mlts_elements = pd.concat(
                    [
                        df_mlts_elements,
                        df_mlt.loc[
                            (df_mlt["mes"] == mes),
                            "vazao",
                        ],
                    ],
                    ignore_index=True,
                )

        mlts_ordenadas = np.tile(
            np.repeat(df_mlts_elements.to_numpy(), num_series * num_aberturas),
            uow.files.get_dger().num_anos_estudo,
        )
        df["mlt"] = mlts_ordenadas
        df["valor_mlt"] = df["valor"] / df["mlt"]
        df.replace([np.inf, -np.inf], 0, inplace=True)
        return df

    @classmethod
    def _apply_mlt(
        cls,
        synthesis: ScenarioSynthesis,
        df: pd.DataFrame,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        with uow:
            # Descobre o valor em MLT
            df = df.copy()
            df_mlt = cls._get_cached_mlt(
                cls._mlt_absolute_variable_map(synthesis.variable),
                synthesis.spatial_resolution,
                uow,
            )
            filter_cols = [
                c for c in df_mlt.columns if c not in ["mes", "vazao"]
            ]
            filter_col = None if len(filter_cols) == 0 else filter_cols[0]
            # Aplica a conversão
            APPLY_MAP: Dict[Step, Callable] = {
                Step.FORWARD: cls._apply_mlt_forward_sf,
                Step.FINAL_SIMULATION: cls._apply_mlt_forward_sf,
                Step.BACKWARD: cls._apply_mlt_backward,
            }
            return APPLY_MAP[synthesis.step](df, df_mlt, filter_col, uow)

    @classmethod
    def _resolve_spatial_resolution(
        cls, synthesis: ScenarioSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        RESOLUTION_MAP: Dict[SpatialResolution, List[str]] = {
            SpatialResolution.SISTEMA_INTERLIGADO: [],
            SpatialResolution.SUBMERCADO: ["nome_submercado"],
            SpatialResolution.RESERVATORIO_EQUIVALENTE: ["nome_ree"],
            SpatialResolution.USINA_HIDROELETRICA: ["nome_usina"],
        }
        if synthesis.variable in [
            Variable.ENA_ABSOLUTA,
            Variable.VAZAO_INCREMENTAL_ABSOLUTA,
        ]:
            # Variáveis absolutas - agregação mais simples
            df = cls._get_cached_variable(
                synthesis.variable, synthesis.step, uow
            )
            return cls._resolve_group(
                RESOLUTION_MAP[synthesis.spatial_resolution], df
            )
        elif synthesis.variable in [
            Variable.ENA_MLT,
            Variable.VAZAO_INCREMENTAL_MLT,
        ]:
            # Variáveis normalizadas pela MLT - agregação deve ser feita na
            # variável equivalente absoluta
            df = cls._get_cached_variable(
                cls._mlt_absolute_variable_map(synthesis.variable),
                synthesis.step,
                uow,
            )
            df = cls._resolve_group(
                RESOLUTION_MAP[synthesis.spatial_resolution], df
            )
            df = cls._apply_mlt(synthesis, df, uow)
            pass
        else:
            return pd.DataFrame()

    @classmethod
    def _postprocess(cls, df: pd.DataFrame):
        column_names = {
            "data": "dataInicio",
            "data_fim": "dataFim",
            "nome_usina": "usina",
            "nome_ree": "ree",
            "nome_submercado": "submercado",
        }
        return df.rename(
            columns={k: v for k, v in column_names.items() if k in df.columns}
        )

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        if len(variables) == 0:
            variables = ScenarioSynthetizer._default_args()
        else:
            variables = ScenarioSynthetizer._process_variable_arguments(
                variables
            )
        valid_synthesis = ScenarioSynthetizer.filter_valid_variables(
            variables, uow
        )
        for s in valid_synthesis:
            filename = str(s)
            Log.log().info(f"Realizando síntese de {filename}")
            df = cls._resolve_spatial_resolution(s, uow)
            if df is None:
                continue
            elif isinstance(df, pd.DataFrame):
                if df.empty:
                    Log.log().info("Erro ao realizar a síntese")
                    continue
            # TODO - adicionar estatísticas ao postprocess
            df = cls._postprocess(df)
            with uow:
                uow.export.synthetize_df(df, filename)
