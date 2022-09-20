from typing import Callable, Dict, List, Tuple
import pandas as pd
from inewave.config import MESES_DF
from datetime import datetime

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.temporalresolution import TemporalResolution
from sintetizador.model.operation.operationsynthesis import OperationSynthesis


class OperationSynthetizer:

    DEFAULT_OPERATION_SYNTHESIS_ARGS: List[str] = [
        "CMO_SBM_MES",
        "VAGUA_REE_MES",
        "CTER_SBM_MES",
        "CTER_SIN_MES",
        "COP_SIN_MES",
        "ENA_REE_MES",
        "ENA_SBM_MES",
        "ENA_SIN_MES",
        "EARP_REE_MES",
        "EARP_SBM_MES",
        "EARP_SIN_MES",
        "EARM_REE_MES",
        "EARM_SBM_MES",
        "EARM_SIN_MES",
        "GHID_REE_MES",
        "GHID_SBM_MES",
        "GHID_SIN_MES",
        "GTER_SBM_MES",
        "GTER_SIN_MES",
        "GHID_REE_PAT",
        "GHID_SBM_PAT",
        "GHID_SIN_PAT",
        "GTER_SBM_PAT",
        "GTER_SIN_PAT",
        "EVER_REE_MES",
        "EVER_SBM_MES",
        "EVER_SIN_MES",
        "QAFL_UHE_MES",
        "QINC_UHE_MES",
        "VTUR_UHE_MES",
        "VVER_UHE_MES",
        "VARM_UHE_MES",
        "VARP_UHE_MES",
        "GHID_UHE_PAT",
        "VENTO_UEE_MES",
        "GEOL_UEE_MES",
        "GEOL_SBM_MES",
        "GEOL_SIN_MES",
        "GEOL_UEE_PAT",
        "GEOL_SBM_PAT",
        "GEOL_SIN_PAT",
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
    def __resolve_MES(cls, df: pd.DataFrame) -> pd.DataFrame:
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
        df_series["Data"] = labels
        return df_series[["Data"] + cols]

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
        df_series["Data"] = labels * len(patamares)
        return df_series[["Data"] + cols]

    @classmethod
    def _resolve_temporal_resolution(
        cls, synthesis: OperationSynthesis, df: pd.DataFrame
    ) -> pd.DataFrame:

        RESOLUTION_FUNCTION_MAP: Dict[TemporalResolution, Callable] = {
            TemporalResolution.MES: cls.__resolve_MES,
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
                df_uhe["UHE"] = n
                df_uhe = df_uhe[["UHE"] + cols]
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
                df_ute["UTE"] = n
                df_ute = df_ute[["UTE"] + cols]
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
                df_uee["UEE"] = n
                df_uee = df_uee[["UEE"] + cols]
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
            SpatialResolution.RESERVATORIO_EQUIVALENTE: cls.__resolve_REE,
            SpatialResolution.USINA_HIDROELETRICA: cls.__resolve_UHE,
            SpatialResolution.USINA_TERMELETRICA: cls.__resolve_UTE,
            SpatialResolution.USINA_EOLICA: cls.__resolve_UEE,
        }

        solver = RESOLUTION_FUNCTION_MAP[synthesis.spatial_resolution]
        return solver(synthesis, uow)

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
            with uow:
                uow.export.synthetize_df(df, filename)
