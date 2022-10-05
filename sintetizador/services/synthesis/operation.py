from typing import Callable, Dict, List, Tuple
import pandas as pd
from inewave.config import MESES_DF
from datetime import datetime
from dateutil.relativedelta import relativedelta

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.temporalresolution import TemporalResolution
from sintetizador.model.operation.operationsynthesis import OperationSynthesis


class OperationSynthetizer:

    DEFAULT_OPERATION_SYNTHESIS_ARGS: List[str] = [
        "CMO_SBM_EST",
        "CMO_SBM_PAT",
        "VAGUA_REE_EST",
        "CTER_SBM_EST",
        "CTER_SIN_EST",
        "COP_SIN_EST",
        "ENAA_REE_EST",
        "ENAA_SBM_EST",
        "ENAA_SIN_EST",
        "EARPF_REE_EST",
        "EARPF_SBM_EST",
        "EARPF_SIN_EST",
        "EARMF_REE_EST",
        "EARMF_SBM_EST",
        "EARMF_SIN_EST",
        "GHID_REE_EST",
        "GHID_SBM_EST",
        "GHID_SIN_EST",
        "GTER_SBM_EST",
        "GTER_SIN_EST",
        "GHID_REE_PAT",
        "GHID_SBM_PAT",
        "GHID_SIN_PAT",
        "GTER_SBM_PAT",
        "GTER_SIN_PAT",
        "EVERT_REE_EST",
        "EVERT_SBM_EST",
        "EVERT_SIN_EST",
        "QAFL_UHE_EST",
        "QINC_UHE_EST",
        "VTUR_UHE_EST",
        "VVER_UHE_EST",
        "VARMF_UHE_EST",
        "VARPF_UHE_EST",
        "GHID_UHE_PAT",
        "VENTO_UEE_EST",
        "GEOL_UEE_EST",
        "GEOL_SBM_EST",
        "GEOL_SIN_EST",
        "GEOL_UEE_PAT",
        "GEOL_SBM_PAT",
        "GEOL_SIN_PAT",
        "INT_SBP_EST",
        "INT_SBP_PAT",
    ]

    @classmethod
    def _default_args(cls) -> List[OperationSynthesis]:
        return [
            OperationSynthesis.factory(a)
            for a in cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        ]

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[OperationSynthesis]:
        args_data = [OperationSynthesis.factory(c) for c in args]
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
            SpatialResolution.PAR_SUBMERCADOS: ["submercados"],
            SpatialResolution.RESERVATORIO_EQUIVALENTE: ["ree"],
            SpatialResolution.USINA_HIDROELETRICA: ["uhe"],
            SpatialResolution.USINA_TERMELETRICA: ["ute"],
            SpatialResolution.USINA_EOLICA: ["uee"],
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
        cls, variables: List[OperationSynthesis], uow: AbstractUnitOfWork
    ) -> List[OperationSynthesis]:
        with uow:
            dger = uow.files.get_dger()
            ree = uow.files.get_ree()
        valid_variables: List[OperationSynthesis] = []
        indiv = ree.rees["Mês Fim Individualizado"].isna().sum() == 0
        eolica = dger.considera_geracao_eolica != 0
        Log.log().info(f"Caso com geração de cenários de eólica: {eolica}")
        Log.log().info(f"Caso com modelagem híbrida: {indiv}")
        for v in variables:
            if (
                v.variable
                in [Variable.VELOCIDADE_VENTO, Variable.GERACAO_EOLICA]
                and not eolica
            ):
                continue
            if (
                v.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA
                and not indiv
            ):
                continue
            valid_variables.append(v)
        Log.log().info(f"Variáveis: {valid_variables}")
        return valid_variables

    @classmethod
    def __resolve_EST(cls, df: pd.DataFrame) -> pd.DataFrame:
        anos = df["Ano"].unique().tolist()
        labels = pd.date_range(
            datetime(year=anos[0], month=1, day=1),
            datetime(year=anos[-1], month=12, day=1),
            freq="MS",
        )
        df_series = pd.DataFrame()
        for a in anos:
            df_ano = df.loc[df["Ano"] == a, MESES_DF].T
            df_ano.columns = [
                str(s) for s in list(range(1, df_ano.shape[1] + 1))
            ]
            df_series = pd.concat([df_series, df_ano], ignore_index=True)
        cols = df_series.columns.tolist()
        df_series["Estagio"] = list(range(1, len(labels) + 1))
        df_series["Data Inicio"] = labels
        f = lambda x: x["Data Inicio"] + relativedelta(months=1)
        df_series["Data Fim"] = df_series.apply(f, axis=1)
        return df_series[["Estagio", "Data Inicio", "Data Fim"] + cols]

    @classmethod
    def __resolve_PAT(cls, df: pd.DataFrame) -> pd.DataFrame:
        anos = df["Ano"].unique().tolist()
        patamares = df["Patamar"].unique().tolist()
        labels = pd.date_range(
            datetime(year=anos[0], month=1, day=1),
            datetime(year=anos[-1], month=12, day=1),
            freq="MS",
        ).tolist()
        df_series = pd.DataFrame()
        for a in anos:
            for p in patamares:
                df_ano_patamar = df.loc[
                    (df["Ano"] == a) & (df["Patamar"] == p),
                    MESES_DF,
                ].T
                cols = [
                    str(s) for s in list(range(1, df_ano_patamar.shape[1] + 1))
                ]
                df_ano_patamar.columns = cols
                df_ano_patamar["Patamar"] = p
                df_ano_patamar = df_ano_patamar[["Patamar"] + cols]
                df_series = pd.concat(
                    [df_series, df_ano_patamar], ignore_index=True
                )
        cols = df_series.columns.tolist()
        df_series["Estagio"] = list(range(1, len(labels) + 1)) * len(patamares)
        df_series["Data Inicio"] = labels * len(patamares)
        f = lambda x: x["Data Inicio"] + relativedelta(months=1)
        df_series["Data Fim"] = df_series.apply(f, axis=1)
        return df_series[["Estagio", "Data Inicio", "Data Fim"] + cols]

    @classmethod
    def _resolve_temporal_resolution(
        cls, synthesis: OperationSynthesis, df: pd.DataFrame
    ) -> pd.DataFrame:

        RESOLUTION_FUNCTION_MAP: Dict[TemporalResolution, Callable] = {
            TemporalResolution.ESTAGIO: cls.__resolve_EST,
            TemporalResolution.PATAMAR: cls.__resolve_PAT,
        }

        solver = RESOLUTION_FUNCTION_MAP[synthesis.temporal_resolution]
        return solver(df)

    @classmethod
    def __resolve_SIN(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            Log.log().info(f"Processando arquivo do SIN")
            df = uow.files.get_nwlistop(
                synthesis.variable,
                synthesis.spatial_resolution,
                synthesis.temporal_resolution,
                "",
            )
            if df is not None:
                return cls._resolve_temporal_resolution(synthesis, df)
            else:
                return pd.DataFrame()

    @classmethod
    def __resolve_SBM(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            sistema = uow.files.get_sistema()
            sistemas_reais = sistema.custo_deficit.loc[
                sistema.custo_deficit["Fictício"] == 0, :
            ]
            sbms_idx = sistemas_reais["Num. Subsistema"]
            sbms_name = sistemas_reais["Nome"]
            df = pd.DataFrame()
            for s, n in zip(sbms_idx, sbms_name):
                Log.log().info(f"Processando arquivo do submercado: {s} - {n}")
                df_sbm = cls._resolve_temporal_resolution(
                    synthesis,
                    uow.files.get_nwlistop(
                        synthesis.variable,
                        synthesis.spatial_resolution,
                        synthesis.temporal_resolution,
                        submercado=s,
                    ),
                )
                if df_sbm is None:
                    continue
                cols = df_sbm.columns.tolist()
                df_sbm["Submercado"] = n
                df_sbm = df_sbm[["Submercado"] + cols]
                df = pd.concat(
                    [df, df_sbm],
                    ignore_index=True,
                )
            return df

    @classmethod
    def __resolve_SBP(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            sistema = uow.files.get_sistema()
            sistemas_reais = sistema.custo_deficit.loc[
                sistema.custo_deficit["Fictício"] == 0, :
            ]
            sbms_idx = sistemas_reais["Num. Subsistema"]
            sbms_name = sistemas_reais["Nome"]
            df = pd.DataFrame()
            for s1, n1 in zip(sbms_idx, sbms_name):
                for s2, n2 in zip(sbms_idx, sbms_name):
                    # Ignora o mesmo SBM
                    if s1 >= s2:
                        continue
                    Log.log().info(
                        "Processando arquivo do par de "
                        + f"submercados: {s1} - {n1} | {s2} - {n2}"
                    )
                    df_sbm = cls._resolve_temporal_resolution(
                        synthesis,
                        uow.files.get_nwlistop(
                            synthesis.variable,
                            synthesis.spatial_resolution,
                            synthesis.temporal_resolution,
                            submercados=(s1, s2),
                        ),
                    )
                    if df_sbm is None:
                        continue
                    cols = df_sbm.columns.tolist()
                    df_sbm["Submercado De"] = n1
                    df_sbm["Submercado Para"] = n2
                    df_sbm = df_sbm[
                        ["Submercado De", "Submercado Para"] + cols
                    ]
                    df = pd.concat(
                        [df, df_sbm],
                        ignore_index=True,
                    )
            return df

    @classmethod
    def __resolve_REE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            ree = uow.files.get_ree()
            rees_idx = ree.rees["Número"]
            rees_name = ree.rees["Nome"]
            df = pd.DataFrame()
            for s, n in zip(rees_idx, rees_name):
                Log.log().info(f"Processando arquivo do REE: {s} - {n}")
                df_ree = cls._resolve_temporal_resolution(
                    synthesis,
                    uow.files.get_nwlistop(
                        synthesis.variable,
                        synthesis.spatial_resolution,
                        synthesis.temporal_resolution,
                        ree=s,
                    ),
                )
                if df_ree is None:
                    continue
                cols = df_ree.columns.tolist()
                df_ree["REE"] = n
                df_ree = df_ree[["REE"] + cols]
                df = pd.concat(
                    [df, df_ree],
                    ignore_index=True,
                )
            return df

    @classmethod
    def __resolve_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            confhd = uow.files.get_confhd()
            uhes_idx = confhd.usinas["Número"]
            uhes_name = confhd.usinas["Nome"]
            df = pd.DataFrame()
            for s, n in zip(uhes_idx, uhes_name):
                Log.log().info(f"Processando arquivo da UHE: {s} - {n}")
                df_uhe = cls._resolve_temporal_resolution(
                    synthesis,
                    uow.files.get_nwlistop(
                        synthesis.variable,
                        synthesis.spatial_resolution,
                        synthesis.temporal_resolution,
                        uhe=s,
                    ),
                )
                if df_uhe is None:
                    continue
                cols = df_uhe.columns.tolist()
                df_uhe["Usina"] = n
                df_uhe = df_uhe[["Usina"] + cols]
                df = pd.concat(
                    [df, df_uhe],
                    ignore_index=True,
                )
            return df

    @classmethod
    def __resolve_UTE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            conft = uow.files.get_conft()
            utes_idx = conft.usinas["Número"]
            utes_name = conft.usinas["Nome"]
            df = pd.DataFrame()
            for s, n in zip(utes_idx, utes_name):
                Log.log().info(f"Processando arquivo da UTE: {s} - {n}")
                df_ute = cls._resolve_temporal_resolution(
                    synthesis,
                    uow.files.get_nwlistop(
                        synthesis.variable,
                        synthesis.spatial_resolution,
                        synthesis.temporal_resolution,
                        ute=s,
                    ),
                )
                if df_ute is None:
                    continue
                cols = df_ute.columns.tolist()
                df_ute["Usina"] = n
                df_ute = df_ute[["Usina"] + cols]
                df = pd.concat(
                    [df, df_ute],
                    ignore_index=True,
                )
            return df

    @classmethod
    def __resolve_UEE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            eolica_cadastro = uow.files.get_eolicacadastro()
            uees_idx = []
            uees_name = []
            regs = eolica_cadastro.eolica_cadastro()
            df = pd.DataFrame()
            if regs is None:
                return df
            elif isinstance(regs, list):
                for r in regs:
                    uees_idx.append(r.codigo_eolica)
                    uees_name.append(r.nome_eolica)
            for s, n in zip(uees_idx, uees_name):
                Log.log().info(f"Processando arquivo da UEE: {s} - {n}")
                df_uee = cls._resolve_temporal_resolution(
                    synthesis,
                    uow.files.get_nwlistop(
                        synthesis.variable,
                        synthesis.spatial_resolution,
                        synthesis.temporal_resolution,
                        uee=s,
                    ),
                )
                if df_uee is None:
                    continue
                cols = df_uee.columns.tolist()
                df_uee["Usina"] = n
                df_uee = df_uee[["Usina"] + cols]
                df = pd.concat(
                    [df, df_uee],
                    ignore_index=True,
                )
            return df

    @classmethod
    def _resolve_spatial_resolution(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:

        RESOLUTION_FUNCTION_MAP: Dict[SpatialResolution, Callable] = {
            SpatialResolution.SISTEMA_INTERLIGADO: cls.__resolve_SIN,
            SpatialResolution.SUBMERCADO: cls.__resolve_SBM,
            SpatialResolution.PAR_SUBMERCADOS: cls.__resolve_SBP,
            SpatialResolution.RESERVATORIO_EQUIVALENTE: cls.__resolve_REE,
            SpatialResolution.USINA_HIDROELETRICA: cls.__resolve_UHE,
            SpatialResolution.USINA_TERMELETRICA: cls.__resolve_UTE,
            SpatialResolution.USINA_EOLICA: cls.__resolve_UEE,
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
        starting_df = df.loc[df["Data Inicio"] >= starting_date].copy()
        starting_df.loc[:, "Estagio"] -= starting_date.month - 1
        return starting_df.copy()

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        if len(variables) == 0:
            variables = OperationSynthetizer._default_args()
        else:
            variables = OperationSynthetizer._process_variable_arguments(
                variables
            )
        valid_synthesis = OperationSynthetizer.filter_valid_variables(
            variables, uow
        )
        for s in valid_synthesis:
            filename = str(s)
            Log.log().info(f"Realizando síntese de {filename}")
            df = cls._resolve_spatial_resolution(s, uow)
            df = cls._resolve_starting_stage(df, uow)
            with uow:
                uow.export.synthetize_df(df, filename)
