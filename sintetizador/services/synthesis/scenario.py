from typing import Callable, Dict, List, Tuple
import pandas as pd  # type: ignore
import numpy as np  # type: ignore

from datetime import datetime
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
    ]

    CACHED_SYNTHESIS: Dict[Tuple[Variable, Step], pd.DataFrame] = {}

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

        - codigo_ree (`int`)
        - ree (`str`)
        - estagio (`int`)
        - data (`datetime`)
        - serie (`int`)
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
        dados_rees["Submercado"] = dados_rees.apply(
            lambda linha: sistema.loc[
                sistema["Núm. Subsistema"] == linha["Submercado"], "Nome"
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
        energiaf_dados["ree"] = nomes_ordenados
        energiaf_dados["submercado"] = submercados_ordenados
        energiaf_dados["data"] = datas_ordenadas
        energiaf_dados["estagio"] -= dger.mes_inicio_estudo - 1
        return energiaf_dados[
            [
                "estagio",
                "data",
                "serie",
                "codigo_ree",
                "ree",
                "submercado",
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
        dados_uhes = pd.DataFrame(uhes).apply(
            lambda linha: confhd.loc[
                linha[0] - 1, ["Número", "Nome", "REE"]
            ].tolist(),
            axis=1,
            result_type="expand",
        )
        dados_uhes[3] = dados_uhes.apply(
            lambda linha: rees.loc[
                rees["Número"] == linha[2], "Submercado"
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
        rees_ordenados = cls._formata_dados_series(
            dados_uhes[2].to_numpy(), num_series, num_estagios
        )
        submercados_ordenados = cls._formata_dados_series(
            dados_uhes[3].to_numpy(), num_series, num_estagios
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
        datas_ordenadas = np.repeat(datas, num_series * num_uhes)
        # Edita o DF e retorna
        vazaof_dados["codigo_usina"] = codigos_ordenados
        vazaof_dados["nome_usina"] = nomes_ordenados
        vazaof_dados["ree"] = rees_ordenados
        vazaof_dados["submercado"] = submercados_ordenados
        vazaof_dados["data"] = datas_ordenadas
        vazaof_dados["estagio"] -= dger.mes_inicio_estudo - 1
        vazaof_dados.drop(columns=["uhe"], inplace=True)
        return vazaof_dados[
            [
                "estagio",
                "data",
                "serie",
                "codigo_usina",
                "nome_usina",
                "ree",
                "submercado",
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
        - serie (`int`)
        - abertura (`int`)
        - codigo_uhe (`float`)
        - nome_uhe (`float`)
        - ree (`float`)
        - submercado (`float`)
        - valor (`float`)

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        # Extrai dimensões para repetir vetores
        energiab_dados = energiab.copy()
        series = energiab_dados["serie"].unique()
        num_series = len(series)
        uhes = energiab_dados["uhe"].unique()
        num_uhes = len(uhes)
        estagios = energiab_dados["estagio"].unique()
        num_estagios = len(estagios)
        aberturas = energiab_dados["abertura"].unique()
        num_aberturas = len(aberturas)
        # Obtem os dados de cada usina
        dger = uow.files.get_dger()
        confhd = uow.files.get_confhd().usinas
        rees = uow.files.get_ree().rees
        sistema = uow.files.get_sistema().custo_deficit
        dados_rees = uow.files.get_ree().rees
        dados_rees["Submercado"] = dados_rees.apply(
            lambda linha: sistema.loc[
                sistema["Núm. Subsistema"] == linha["Submercado"], "Nome"
            ].tolist()[0],
            axis=1,
        )
        # Gera os vetores da dimensão do DF extraído do arquivo vazaob
        codigos_ordenados = cls._formata_dados_series(
            dados_rees["Número"].to_numpy(), num_series, num_estagios
        )
        nomes_ordenados = cls._formata_dados_series(
            dados_rees["Nome"].to_numpy(), num_series, num_estagios
        )
        submercados_ordenados = cls._formata_dados_series(
            dados_rees["Submercado"].to_numpy(), num_series, num_estagios
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
            datas, num_series * num_aberturas * num_uhes
        )
        # Edita o DF e retorna
        energiab_dados["codigo_ree"] = codigos_ordenados
        energiab_dados["ree"] = nomes_ordenados
        energiab_dados["submercado"] = submercados_ordenados
        energiab_dados["data"] = datas_ordenadas
        energiab_dados["estagio"] -= dger.mes_inicio_estudo - 1
        return energiab_dados[
            [
                "estagio",
                "data",
                "serie",
                "abertura",
                "codigo_ree",
                "ree",
                "submercado",
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
        - serie (`int`)
        - abertura (`int`)
        - codigo_uhe (`float`)
        - nome_uhe (`float`)
        - ree (`float`)
        - submercado (`float`)
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
        dados_uhes = pd.DataFrame(uhes).apply(
            lambda linha: confhd.loc[
                linha[0] - 1, ["Número", "Nome", "REE"]
            ].tolist(),
            axis=1,
            result_type="expand",
        )
        dados_uhes[3] = dados_uhes.apply(
            lambda linha: rees.loc[
                rees["Número"] == linha[2], "Submercado"
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
        rees_ordenados = cls._formata_dados_series(
            dados_uhes[2].to_numpy(), num_series * num_aberturas, num_estagios
        )
        submercados_ordenados = cls._formata_dados_series(
            dados_uhes[3].to_numpy(), num_series * num_aberturas, num_estagios
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
            datas, num_series * num_aberturas * num_uhes
        )
        # Edita o DF e retorna
        vazaob_dados["codigo_usina"] = codigos_ordenados
        vazaob_dados["nome_usina"] = nomes_ordenados
        vazaob_dados["ree"] = rees_ordenados
        vazaob_dados["submercado"] = submercados_ordenados
        vazaob_dados["data"] = datas_ordenadas
        vazaob_dados["estagio"] -= dger.mes_inicio_estudo - 1
        vazaob_dados.drop(columns=["uhe"], inplace=True)
        return vazaob_dados[
            [
                "estagio",
                "data",
                "serie",
                "abertura",
                "codigo_usina",
                "nome_usina",
                "ree",
                "submercado",
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
        dados_rees["Submercado"] = dados_rees.apply(
            lambda linha: sistema.loc[
                sistema["Núm. Subsistema"] == linha["Submercado"], "Nome"
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
        energias_dados["ree"] = nomes_ordenados
        energias_dados["submercado"] = submercados_ordenados
        energias_dados["data"] = datas_ordenadas
        energias_dados["estagio"] -= dger.mes_inicio_estudo - 1
        return energias_dados[
            [
                "estagio",
                "data",
                "serie",
                "codigo_ree",
                "ree",
                "submercado",
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
        dados_uhes = pd.DataFrame(uhes).apply(
            lambda linha: confhd.loc[
                linha[0] - 1, ["Número", "Nome", "REE"]
            ].tolist(),
            axis=1,
            result_type="expand",
        )
        dados_uhes[3] = dados_uhes.apply(
            lambda linha: rees.loc[
                rees["Número"] == linha[2], "Submercado"
            ].tolist()[0],
            axis=1,
        )
        # Gera os vetores da dimensão do DF extraído do arquivo vazaos
        codigos_ordenados = cls._formata_dados_series(
            dados_uhes[0].to_numpy(), num_series, num_estagios
        )
        nomes_ordenados = cls._formata_dados_series(
            dados_uhes[1].to_numpy(), num_series, num_estagios
        )
        rees_ordenados = cls._formata_dados_series(
            dados_uhes[2].to_numpy(), num_series, num_estagios
        )
        submercados_ordenados = cls._formata_dados_series(
            dados_uhes[3].to_numpy(), num_series, num_estagios
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
        datas_ordenadas = np.repeat(datas, num_series * num_uhes)
        # Edita o DF e retorna
        vazaos_dados["codigo_usina"] = codigos_ordenados
        vazaos_dados["nome_usina"] = nomes_ordenados
        vazaos_dados["ree"] = rees_ordenados
        vazaos_dados["submercado"] = submercados_ordenados
        vazaos_dados["data"] = datas_ordenadas
        vazaos_dados["estagio"] -= dger.mes_inicio_estudo - 1
        vazaos_dados.drop(columns=["uhe"], inplace=True)
        return vazaos_dados[
            [
                "estagio",
                "data",
                "serie",
                "codigo_usina",
                "nome_usina",
                "ree",
                "submercado",
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
                df_enavaz = uow.files.get_enavazf(it).series
                df_energia = uow.files.get_energiaf(it).series
                if not df_enavaz.empty:
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
                vaz_it = cls._adiciona_dados_uhes_forward(uow, arq.series)
                vaz_it["iteracao"] = it
                df_completo = pd.concat(
                    [df_completo, vaz_it], ignore_index=True
                )
        return df_completo

    @classmethod
    def _resolve_enaa_backward(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            df_enavaz = uow.files.get_enavazb().series
            df_energia = uow.files.get_energiab().series
            if not df_enavaz.empty:
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
            enas = cls._adiciona_dados_rees_backward(uow, df_ena)
        return enas

    @classmethod
    def _resolve_qinc_backward(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            arq = uow.files.get_vazaob()
            df = cls._adiciona_dados_uhes_backward(uow, arq.series)
        return df

    @classmethod
    def _resolve_enaa_sf(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            df_enavaz = uow.files.get_enavazs().series
            df_energia = uow.files.get_energias().series
            if not df_enavaz.empty:
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
            enas = cls._adiciona_dados_rees_sf(uow, df_ena)
        return enas

    @classmethod
    def _resolve_qinc_sf(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            arq = uow.files.get_vazaos()
            df = cls._adiciona_dados_uhes_sf(uow, arq.series)
        return df

    @classmethod
    def _resolve_spatial_resolution(
        cls, synthesis: ScenarioSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        # TODO - refatorar para melhorar a resolução existente
        # Agora o que tem que ser feito na resolução espacial
        # Depende da variável: QINC não faz nada se for UHE e
        # ENA não faz nada se for REE..
        RESOLUTION_FUNCTION_MAP: Dict[SpatialResolution, Callable] = {
            SpatialResolution.SISTEMA_INTERLIGADO: cls.__resolve_SIN,
            SpatialResolution.SUBMERCADO: cls.__resolve_SBM,
            SpatialResolution.RESERVATORIO_EQUIVALENTE: cls.__resolve_REE,
            SpatialResolution.USINA_HIDROELETRICA: cls.__resolve_UHE,
            SpatialResolution.PARQUE_EOLICO_EQUIVALENTE: cls.__resolve_PEE,
        }

        solver = RESOLUTION_FUNCTION_MAP[synthesis.spatial_resolution]
        return solver(synthesis, uow)

    @classmethod
    def _resolve_starting_stage(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ):
        with uow:
            dger = uow.files.get_dger()
        starting_date = datetime(
            year=dger.ano_inicio_estudo, month=dger.mes_inicio_estudo, day=1
        )
        starting_df = df.loc[df["dataInicio"] >= starting_date].copy()
        starting_df.loc[:, "estagio"] -= starting_date.month - 1
        return starting_df.copy()

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

            df = cls._resolve_starting_stage(df, uow)
            with uow:
                uow.export.synthetize_df(df, filename)
