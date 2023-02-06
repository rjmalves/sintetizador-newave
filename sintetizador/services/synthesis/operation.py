from typing import Callable, Dict, List, Optional
import pandas as pd
import numpy as np
from inewave.config import MESES_DF
from datetime import datetime
from dateutil.relativedelta import relativedelta

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.temporalresolution import TemporalResolution
from sintetizador.model.operation.operationsynthesis import OperationSynthesis


FATOR_HM3_M3S = 1.0 / 2.63


class OperationSynthetizer:

    IDENTIFICATION_COLUMNS = [
        "dataInicio",
        "dataFim",
        "estagio",
        "submercado",
        "submercadoDe",
        "submercadoPara",
        "ree",
        "pee",
        "usina",
        "patamar",
    ]

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
        "EVER_REE_EST",
        "EVER_SBM_EST",
        "EVER_SIN_EST",
        "EVERR_REE_EST",
        "EVERR_SBM_EST",
        "EVERR_SIN_EST",
        "EVERF_REE_EST",
        "EVERF_SBM_EST",
        "EVERF_SIN_EST",
        "EVERFT_REE_EST",
        "EVERFT_SBM_EST",
        "EVERFT_SIN_EST",
        "QAFL_UHE_EST",
        "QINC_UHE_EST",
        "QDEF_UHE_EST",
        "QDEF_UHE_PAT",
        "VTUR_UHE_EST",
        "VTUR_UHE_PAT",
        "QTUR_UHE_EST",
        "QTUR_UHE_PAT",
        "VVER_UHE_EST",
        "VVER_UHE_PAT",
        "QVER_UHE_EST",
        "QVER_UHE_PAT",
        "VARMF_UHE_EST",
        "VARMF_REE_EST",
        "VARMF_SBM_EST",
        "VARMF_SIN_EST",
        "VARPF_UHE_EST",
        "GHID_UHE_PAT",
        "GHID_UHE_EST",
        "VENTO_PEE_EST",
        "GEOL_PEE_EST",
        "GEOL_SBM_EST",
        "GEOL_SIN_EST",
        "GEOL_PEE_PAT",
        "GEOL_SBM_PAT",
        "GEOL_SIN_PAT",
        "INT_SBP_EST",
        "INT_SBP_PAT",
        "DEF_SBM_EST",
        "DEF_SBM_PAT",
        "DEF_SIN_EST",
        "DEF_SIN_PAT",
    ]

    SYNTHESIS_TO_CACHE: List[OperationSynthesis] = [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.ESTAGIO,
        ),
    ]

    CACHED_SYNTHESIS: Dict[OperationSynthesis, pd.DataFrame] = {}

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
        df_series["estagio"] = list(range(1, len(labels) + 1))
        df_series["dataInicio"] = labels
        f = lambda x: x["dataInicio"] + relativedelta(months=1)
        df_series["dataFim"] = df_series.apply(f, axis=1)
        return df_series[["estagio", "dataInicio", "dataFim"] + cols]

    @classmethod
    def __resolve_PAT(cls, df: pd.DataFrame) -> pd.DataFrame:
        anos = df["Ano"].unique().tolist()
        patamares = df["Patamar"].unique().tolist()
        labels = []
        for a in anos:
            for p in patamares:
                labels += pd.date_range(
                    datetime(year=a, month=1, day=1),
                    datetime(year=a, month=12, day=1),
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
                df_ano_patamar["patamar"] = str(p)
                df_ano_patamar = df_ano_patamar[["patamar"] + cols]
                df_series = pd.concat(
                    [df_series, df_ano_patamar], ignore_index=True
                )
        cols = df_series.columns.tolist()
        labels_estagios = []
        for i in range(len(anos)):
            labels_estagios += list(range(12 * i + 1, 12 * (i + 1) + 1)) * len(
                patamares
            )

        df_series["estagio"] = labels_estagios
        df_series["dataInicio"] = labels
        f = lambda x: x["dataInicio"] + relativedelta(months=1)
        df_series["dataFim"] = df_series.apply(f, axis=1)
        return df_series[["estagio", "dataInicio", "dataFim"] + cols]

    @classmethod
    def _resolve_temporal_resolution(
        cls, synthesis: OperationSynthesis, df: pd.DataFrame
    ) -> pd.DataFrame:

        if df is None:
            return None

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
                df_sbm["submercado"] = n
                df_sbm = df_sbm[["submercado"] + cols]
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
                    df_sbm["submercadoDe"] = n1
                    df_sbm["submercadoPara"] = n2
                    df_sbm = df_sbm[["submercadoDe", "submercadoPara"] + cols]
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
                df_ree["ree"] = n
                df_ree = df_ree[["ree"] + cols]
                df = pd.concat(
                    [df, df_ree],
                    ignore_index=True,
                )
            return df

    @classmethod
    def __stub_GHID_VTUR_VVER(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            confhd = uow.files.get_confhd()
            ree = uow.files.get_ree()
            # Obtem o fim do peroodo individualizado
            fim = datetime(
                year=int(ree.rees["Ano Fim Individualizado"].tolist()[0]),
                month=int(ree.rees["Mês Fim Individualizado"].tolist()[0]),
                day=1,
            )
            uhes_idx = confhd.usinas["Número"]
            uhes_name = confhd.usinas["Nome"]
            df = pd.DataFrame()
            for s, n in zip(uhes_idx, uhes_name):
                Log.log().info(f"Processando arquivo da UHE: {s} - {n}")
                df_uhe = cls._resolve_temporal_resolution(
                    OperationSynthesis(
                        variable=synthesis.variable,
                        spatial_resolution=synthesis.spatial_resolution,
                        temporal_resolution=TemporalResolution.PATAMAR,
                    ),
                    uow.files.get_nwlistop(
                        synthesis.variable,
                        synthesis.spatial_resolution,
                        TemporalResolution.PATAMAR,
                        uhe=s,
                    ),
                )
                if df_uhe is None:
                    continue
                cols = df_uhe.columns.tolist()
                df_uhe["usina"] = n
                df_uhe = df_uhe[["usina"] + cols]
                df = pd.concat(
                    [df, df_uhe],
                    ignore_index=True,
                )
            cols_nao_cenarios = [
                "estagio",
                "dataInicio",
                "dataFim",
                "patamar",
                "usina",
            ]
            cols_cenarios = [
                c for c in df.columns.tolist() if c not in cols_nao_cenarios
            ]
            if synthesis.temporal_resolution == TemporalResolution.ESTAGIO:
                patamares = df["patamar"].unique().tolist()
                cenarios_patamares: List[np.ndarray] = []
                p0 = patamares[0]
                for p in patamares:
                    cenarios_patamares.append(
                        df.loc[df["patamar"] == p, cols_cenarios].to_numpy()
                    )
                df.loc[df["patamar"] == p0, cols_cenarios] = 0.0
                for c in cenarios_patamares:
                    df.loc[df["patamar"] == p0, cols_cenarios] += c
                df = df.loc[df["patamar"] == p0, :]

            df = df.loc[df["dataInicio"] < fim, :]
            return df

    @classmethod
    def __stub_QTUR_QVER(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        variable_map = {
            Variable.VAZAO_VERTIDA: Variable.VOLUME_VERTIDO,
            Variable.VAZAO_TURBINADA: Variable.VOLUME_TURBINADO,
        }
        with uow:
            confhd = uow.files.get_confhd()
            ree = uow.files.get_ree()
            # Obtem o fim do peroodo individualizado
            fim = datetime(
                year=int(ree.rees["Ano Fim Individualizado"].tolist()[0]),
                month=int(ree.rees["Mês Fim Individualizado"].tolist()[0]),
                day=1,
            )
            uhes_idx = confhd.usinas["Número"]
            uhes_name = confhd.usinas["Nome"]
            df = pd.DataFrame()
            for s, n in zip(uhes_idx, uhes_name):
                Log.log().info(f"Processando arquivo da UHE: {s} - {n}")
                df_uhe = cls._resolve_temporal_resolution(
                    OperationSynthesis(
                        variable=synthesis.variable,
                        spatial_resolution=synthesis.spatial_resolution,
                        temporal_resolution=TemporalResolution.PATAMAR,
                    ),
                    uow.files.get_nwlistop(
                        variable_map[synthesis.variable],
                        synthesis.spatial_resolution,
                        TemporalResolution.PATAMAR,
                        uhe=s,
                    ),
                )
                if df_uhe is None:
                    continue
                cols = df_uhe.columns.tolist()
                df_uhe["usina"] = n
                df_uhe = df_uhe[["usina"] + cols]
                df = pd.concat(
                    [df, df_uhe],
                    ignore_index=True,
                )
            cols_nao_cenarios = [
                "estagio",
                "dataInicio",
                "dataFim",
                "patamar",
                "usina",
            ]
            cols_cenarios = [
                c for c in df.columns.tolist() if c not in cols_nao_cenarios
            ]
            if synthesis.temporal_resolution == TemporalResolution.ESTAGIO:
                patamares = df["patamar"].unique().tolist()
                cenarios_patamares: List[np.ndarray] = []
                p0 = patamares[0]
                for p in patamares:
                    cenarios_patamares.append(
                        df.loc[df["patamar"] == p, cols_cenarios].to_numpy()
                    )
                df.loc[df["patamar"] == p0, cols_cenarios] = 0.0
                for c in cenarios_patamares:
                    df.loc[df["patamar"] == p0, cols_cenarios] += c
                df = df.loc[df["patamar"] == p0, :]

            df.loc[:, cols_cenarios] *= FATOR_HM3_M3S
            df = df.loc[df["dataInicio"] < fim, :]
            return df

    @classmethod
    def __stub_QDEF(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sintese_tur = OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            synthesis.spatial_resolution,
            synthesis.temporal_resolution,
        )
        sintese_ver = OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            synthesis.spatial_resolution,
            synthesis.temporal_resolution,
        )
        cache_tur = cls.CACHED_SYNTHESIS.get(sintese_tur)
        cache_ver = cls.CACHED_SYNTHESIS.get(sintese_ver)
        df_tur = (
            cache_tur
            if cache_tur is not None
            else cls.__stub_QTUR_QVER(
                OperationSynthesis(
                    Variable.VAZAO_TURBINADA,
                    synthesis.spatial_resolution,
                    synthesis.temporal_resolution,
                ),
                uow,
            )
        )
        df_ver = (
            cache_ver
            if cache_ver is not None
            else cls.__stub_QTUR_QVER(
                OperationSynthesis(
                    Variable.VAZAO_VERTIDA,
                    synthesis.spatial_resolution,
                    synthesis.temporal_resolution,
                ),
                uow,
            )
        )
        cols_nao_cenarios = [
            "estagio",
            "dataInicio",
            "dataFim",
            "patamar",
            "usina",
        ]
        cols_cenarios = [
            c for c in df_ver.columns.tolist() if c not in cols_nao_cenarios
        ]
        df_ver.loc[:, cols_cenarios] = (
            df_tur[cols_cenarios].to_numpy() + df_ver[cols_cenarios].to_numpy()
        )
        return df_ver

    @classmethod
    def __stub_EVER(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sintese_reserv = OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            synthesis.spatial_resolution,
            synthesis.temporal_resolution,
        )
        sintese_fio = OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            synthesis.spatial_resolution,
            synthesis.temporal_resolution,
        )
        cache_reserv = cls.CACHED_SYNTHESIS.get(sintese_reserv)
        cache_fio = cls.CACHED_SYNTHESIS.get(sintese_fio)

        df_reserv = (
            cache_reserv
            if cache_reserv is not None
            else cls._resolve_spatial_resolution(
                OperationSynthesis(
                    Variable.ENERGIA_VERTIDA_RESERV,
                    synthesis.spatial_resolution,
                    synthesis.temporal_resolution,
                ),
                uow,
            )
        )
        df_fio = (
            cache_fio
            if cache_fio is not None
            else cls._resolve_spatial_resolution(
                OperationSynthesis(
                    Variable.ENERGIA_VERTIDA_FIO,
                    synthesis.spatial_resolution,
                    synthesis.temporal_resolution,
                ),
                uow,
            )
        )
        cols_nao_cenarios = [
            "estagio",
            "dataInicio",
            "dataFim",
            "patamar",
            "usina",
            "ree",
            "submercado",
        ]
        cols_cenarios = [
            c for c in df_reserv.columns.tolist() if c not in cols_nao_cenarios
        ]
        df_reserv.loc[:, cols_cenarios] = (
            df_fio[cols_cenarios].to_numpy()
            + df_reserv[cols_cenarios].to_numpy()
        )
        return df_reserv

    @classmethod
    def __stub_VARMF_REE_SBM_SIN(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            confhd = uow.files.get_confhd()
            ree = uow.files.get_ree()
            sistema = uow.files.get_sistema()

            rees_usinas = confhd.usinas["REE"].unique().tolist()
            nomes_rees = {
                r: str(ree.rees.loc[ree.rees["Número"] == r, "Nome"])
                for r in rees_usinas
            }
            rees_submercados = {
                r: str(
                    sistema.custo_deficit.loc[
                        sistema.custo_deficit["Num. Subsistema"]
                        == int(
                            ree.rees.loc[ree.rees["Número"] == r, "Submercado"]
                        ),
                        "Nome",
                    ]
                )
                for r in rees_usinas
            }
            s = OperationSynthesis(
                variable=synthesis.variable,
                spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
                temporal_resolution=synthesis.temporal_resolution,
            )
            cache_uhe = cls.CACHED_SYNTHESIS.get(s)
            if cache_uhe is None:
                df_uhe = cls._resolve_spatial_resolution(s, uow).copy()
                cls.CACHED_SYNTHESIS[s] = df_uhe
            else:
                df_uhe = cache_uhe.copy()

            df_uhe["group"] = df_uhe.apply(
                lambda linha: int(
                    confhd.usinas.loc[
                        confhd.usinas["Nome"] == linha["usina"], "REE"
                    ]
                ),
                axis=1,
            )
            if (
                synthesis.spatial_resolution
                == SpatialResolution.RESERVATORIO_EQUIVALENTE
            ):
                df_uhe["group"] = df_uhe.apply(
                    lambda linha: nomes_rees[linha["group"]], axis=1
                )
            elif synthesis.spatial_resolution == SpatialResolution.SUBMERCADO:
                df_uhe["group"] = df_uhe.apply(
                    lambda linha: rees_submercados[linha["group"]], axis=1
                )
            elif (
                synthesis.spatial_resolution
                == SpatialResolution.SISTEMA_INTERLIGADO
            ):
                df_uhe["group"] = 1

            cols_group = ["group"] + [
                c
                for c in df_uhe.columns()
                if c in cls.IDENTIFICATION_COLUMNS and c != "usina"
            ]
            df_group = df_uhe.groupby(cols_group).sum().reset_index()

            group_name = {
                SpatialResolution.RESERVATORIO_EQUIVALENTE: "ree",
                SpatialResolution.SUBMERCADO: "submercado",
            }
            if (
                synthesis.spatial_resolution
                == SpatialResolution.SISTEMA_INTERLIGADO
            ):
                df_group = df_group.drop(columns=["group"])
            else:
                df_group = df_group.rename(
                    {"group": group_name[synthesis.spatial_resolution]}
                )
            return df_group

    @classmethod
    def __resolve_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        if synthesis.variable in [
            Variable.VAZAO_TURBINADA,
            Variable.VAZAO_VERTIDA,
        ]:
            return cls.__stub_QTUR_QVER(synthesis, uow)
        elif synthesis.variable == Variable.VAZAO_DEFLUENTE:
            return cls.__stub_QDEF(synthesis, uow)
        elif synthesis.variable in [
            Variable.GERACAO_HIDRAULICA,
            Variable.VOLUME_TURBINADO,
            Variable.VOLUME_VERTIDO,
        ]:
            return cls.__stub_GHID_VTUR_VVER(synthesis, uow)
        with uow:
            confhd = uow.files.get_confhd()
            ree = uow.files.get_ree()
            # Obtem o fim do peroodo individualizado
            fim = datetime(
                year=int(ree.rees["Ano Fim Individualizado"].tolist()[0]),
                month=int(ree.rees["Mês Fim Individualizado"].tolist()[0]),
                day=1,
            )
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
                df_uhe["usina"] = n
                df_uhe = df_uhe[["usina"] + cols]
                df = pd.concat(
                    [df, df_uhe],
                    ignore_index=True,
                )
            df = df.loc[df["dataInicio"] < fim, :]
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
                df_ute["usina"] = n
                df_ute = df_ute[["usina"] + cols]
                df = pd.concat(
                    [df, df_ute],
                    ignore_index=True,
                )
            return df

    @classmethod
    def __resolve_PEE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            eolica_cadastro = uow.files.get_eolicacadastro()
            uees_idx = []
            uees_name = []
            regs = eolica_cadastro.pee_cad()
            df = pd.DataFrame()
            if regs is None:
                return df
            elif isinstance(regs, list):
                for r in regs:
                    uees_idx.append(r.codigo_pee)
                    uees_name.append(r.nome_pee)
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
                df_uee["pee"] = n
                df_uee = df_uee[["pee"] + cols]
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
    def _processa_media(
        cls, df: pd.DataFrame, probabilities: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        cols_cenarios = [
            col
            for col in df.columns.tolist()
            if col not in cls.IDENTIFICATION_COLUMNS
        ]
        cols_cenarios = [
            c for c in cols_cenarios if c not in ["min", "max", "median"]
        ]
        cols_cenarios = [c for c in cols_cenarios if "p" not in c]
        estagios = [int(e) for e in df["estagio"].unique()]
        if probabilities is not None:
            df["mean"] = 0.0
            for e in estagios:
                df_estagio = probabilities.loc[
                    probabilities["estagio"] == e, :
                ]
                probabilidades = {
                    str(int(linha["cenario"])): linha["probabilidade"]
                    for _, linha in df_estagio.iterrows()
                }
                probabilidades = {
                    **probabilidades,
                    **{
                        c: 0.0
                        for c in cols_cenarios
                        if c not in probabilidades.keys()
                    },
                }
                df_cenarios_estagio = df.loc[
                    df["estagio"] == e, cols_cenarios
                ].mul(probabilidades, fill_value=0.0)
                df.loc[df["estagio"] == e, "mean"] = df_cenarios_estagio[
                    list(probabilidades.keys())
                ].sum(axis=1)
        else:
            df["mean"] = df[cols_cenarios].mean(axis=1)
        return df

    @classmethod
    def _processa_quantis(
        cls, df: pd.DataFrame, quantiles: List[float]
    ) -> pd.DataFrame:
        cols_cenarios = [
            col
            for col in df.columns.tolist()
            if col not in cls.IDENTIFICATION_COLUMNS
        ]
        for q in quantiles:
            if q == 0:
                label = "min"
            elif q == 1:
                label = "max"
            elif q == 0.5:
                label = "median"
            else:
                label = f"p{int(100 * q)}"
            df[label] = df[cols_cenarios].quantile(q, axis=1)
        return df

    @classmethod
    def _postprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = cls._processa_quantis(df, [0.05 * i for i in range(21)])
        df = cls._processa_media(df, None)
        cols_not_scenarios = [
            c for c in df.columns if c in cls.IDENTIFICATION_COLUMNS
        ]
        cols_scenarios = [
            c for c in df.columns if c not in cls.IDENTIFICATION_COLUMNS
        ]
        df = pd.melt(
            df,
            id_vars=cols_not_scenarios,
            value_vars=cols_scenarios,
            var_name="cenario",
            value_name="valor",
        )
        return df

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
            if s.variable == Variable.ENERGIA_VERTIDA:
                df = cls.__stub_EVER(s, uow)
            elif all(
                [
                    s.variable == Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                    s.spatial_resolution
                    != SpatialResolution.USINA_HIDROELETRICA,
                ]
            ):
                df = cls.__stub_VARMF_REE_SBM_SIN(s, uow)
            else:
                df = cls._resolve_spatial_resolution(s, uow)
                if s in cls.SYNTHESIS_TO_CACHE:
                    cls.CACHED_SYNTHESIS[s] = df.copy()
            df = cls._resolve_starting_stage(df, uow)
            with uow:
                df = cls._postprocess(df)
                uow.export.synthetize_df(df, filename)
