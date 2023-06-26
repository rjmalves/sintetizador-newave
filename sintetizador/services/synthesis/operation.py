from typing import Callable, Dict, List, Optional, Type, TypeVar
import pandas as pd  # type: ignore
import numpy as np
import logging
import traceback
from multiprocessing import Pool
from inewave.config import MESES_DF
from datetime import datetime
from dateutil.relativedelta import relativedelta  # type: ignore
from sintetizador.utils.log import Log
from sintetizador.model.settings import Settings
from sintetizador.services.unitofwork import AbstractUnitOfWork
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
        "CDEF_SBM_EST",
        "CDEF_SIN_EST",
        "VEOL_SBM_PAT",
        "VEOL_SBM_EST",
        "VDEFMIN_UHE_PAT",
        "VDEFMAX_UHE_PAT",
        "VTURMIN_UHE_PAT",
        "VTURMAX_UHE_PAT",
        "VFPHA_UHE_PAT",
        "VDEFMIN_UHE_EST",
        "VDEFMAX_UHE_EST",
        "VTURMIN_UHE_EST",
        "VTURMAX_UHE_EST",
        "VFPHA_UHE_EST",
        "VDEFMIN_REE_PAT",
        "VDEFMAX_REE_PAT",
        "VTURMIN_REE_PAT",
        "VTURMAX_REE_PAT",
        "VFPHA_REE_PAT",
        "VDEFMIN_REE_EST",
        "VDEFMAX_REE_EST",
        "VTURMIN_REE_EST",
        "VTURMAX_REE_EST",
        "VEVMIN_REE_EST",
        "VFPHA_REE_EST",
        "VDEFMIN_SBM_PAT",
        "VDEFMAX_SBM_PAT",
        "VTURMIN_SBM_PAT",
        "VTURMAX_SBM_PAT",
        "VFPHA_SBM_PAT",
        "VDEFMIN_SBM_EST",
        "VDEFMAX_SBM_EST",
        "VTURMIN_SBM_EST",
        "VTURMAX_SBM_EST",
        "VEVMIN_SBM_EST",
        "VFPHA_SBM_EST",
        "VDEFMIN_SIN_PAT",
        "VDEFMAX_SIN_PAT",
        "VTURMIN_SIN_PAT",
        "VTURMAX_SIN_PAT",
        "VFPHA_SIN_PAT",
        "VDEFMIN_SIN_EST",
        "VDEFMAX_SIN_EST",
        "VTURMIN_SIN_EST",
        "VTURMAX_SIN_EST",
        "VEVMIN_SIN_EST",
        "VFPHA_SIN_EST",
        "VVMINOP_REE_EST",
        "VVMINOP_SBM_EST",
        "VVMINOP_SIN_EST",
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
        OperationSynthesis(
            Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_DEFLUENCIA_MINIMA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_TURBINAMENTO_MINIMO,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_DEFLUENCIA_MINIMA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_TURBINAMENTO_MINIMO,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_DEFLUENCIA_MINIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_TURBINAMENTO_MINIMO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_DEFLUENCIA_MINIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_TURBINAMENTO_MINIMO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_DEFLUENCIA_MINIMA,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_TURBINAMENTO_MINIMO,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_DEFLUENCIA_MINIMA,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_TURBINAMENTO_MINIMO,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_VMINOP,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.ESTAGIO,
        ),
    ]

    CACHED_SYNTHESIS: Dict[OperationSynthesis, pd.DataFrame] = {}

    T = TypeVar("T")

    logger: Optional[logging.Logger] = None

    @classmethod
    def _validate_data(cls, data, type: Type[T], msg: str = "dados") -> T:
        if not isinstance(data, type):
            if cls.logger is not None:
                cls.logger.error(f"Erro na leitura de {msg}")
            raise RuntimeError()
        return data

    @classmethod
    def _default_args(cls) -> List[OperationSynthesis]:
        args = [
            OperationSynthesis.factory(a)
            for a in cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        ]
        return [arg for arg in args if arg is not None]

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[OperationSynthesis]:
        args_data = [OperationSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        return valid_args

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
            if cls.logger is not None:
                cls.logger.error(
                    f"Erro no processamento da informação por {spatial_resolution}"
                )
        return valid

    @classmethod
    def filter_valid_variables(
        cls, variables: List[OperationSynthesis], uow: AbstractUnitOfWork
    ) -> List[OperationSynthesis]:
        with uow:
            dger = uow.files.get_dger()
            rees = cls._validate_data(
                uow.files.get_ree().rees, pd.DataFrame, "REE"
            )
        valid_variables: List[OperationSynthesis] = []
        sf_indiv = dger.agregacao_simulacao_final == 1
        politica_indiv = rees["Mês Fim Individualizado"].isna().sum() == 0
        indiv = sf_indiv or politica_indiv
        geracao_eolica = cls._validate_data(
            dger.considera_geracao_eolica, int, "dger"
        )
        eolica = geracao_eolica != 0
        if cls.logger is not None:
            cls.logger.info(
                f"Caso com geração de cenários de eólica: {eolica}"
            )
            cls.logger.info(f"Caso com modelagem híbrida: {indiv}")
        for v in variables:
            if (
                v.variable
                in [
                    Variable.VELOCIDADE_VENTO,
                    Variable.GERACAO_EOLICA,
                    Variable.CORTE_GERACAO_EOLICA,
                ]
                and not eolica
            ):
                continue
            if (
                v.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA
                and not indiv
            ):
                continue
            if (
                v.variable
                in [
                    Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
                    Variable.VIOLACAO_DEFLUENCIA_MINIMA,
                    Variable.VIOLACAO_FPHA,
                    Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
                    Variable.VIOLACAO_TURBINAMENTO_MINIMO,
                ]
                and not indiv
            ):
                continue
            valid_variables.append(v)
        if cls.logger is not None:
            cls.logger.info(f"Variáveis: {valid_variables}")
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
        df_series["dataFim"] = df_series.apply(
            lambda x: x["dataInicio"] + relativedelta(months=1), axis=1
        )
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
        df_series["dataFim"] = df_series.apply(
            lambda x: x["dataInicio"] + relativedelta(months=1), axis=1
        )
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
            if cls.logger is not None:
                cls.logger.info("Processando arquivo do SIN")
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
            sistema = cls._validate_data(
                uow.files.get_sistema().custo_deficit,
                pd.DataFrame,
                "submercados",
            )
            sistemas_reais = sistema.loc[sistema["Fictício"] == 0, :]
            sbms_idx = sistemas_reais["Num. Subsistema"]
            sbms_name = sistemas_reais["Nome"]
            df = pd.DataFrame()
            for s, n in zip(sbms_idx, sbms_name):
                if cls.logger is not None:
                    cls.logger.info(
                        f"Processando arquivo do submercado: {s} - {n}"
                    )
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
            sistema = cls._validate_data(
                uow.files.get_sistema().custo_deficit,
                pd.DataFrame,
                "submercados",
            )
            sbms_idx = sistema["Num. Subsistema"]
            sbms_name = sistema["Nome"]
            df = pd.DataFrame()
            for s1, n1 in zip(sbms_idx, sbms_name):
                for s2, n2 in zip(sbms_idx, sbms_name):
                    # Ignora o mesmo SBM
                    if s1 >= s2:
                        continue
                    if cls.logger is not None:
                        cls.logger.info(
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
            rees = cls._validate_data(
                uow.files.get_ree().rees, pd.DataFrame, "REEs"
            )
            rees_idx = rees["Número"]
            rees_name = rees["Nome"]
            df = pd.DataFrame()
            for s, n in zip(rees_idx, rees_name):
                if cls.logger is not None:
                    cls.logger.info(f"Processando arquivo do REE: {s} - {n}")
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
    def _resolve_UHE_usina(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        uhe_index: int,
        uhe_name: str,
    ) -> pd.DataFrame:
        logger_name = f"{synthesis.variable.value}_{uhe_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, uhe_index
        )
        with uow:
            logger.info(
                f"Processando arquivo da UHE: {uhe_index} - {uhe_name}"
            )
            df_uhe = cls._resolve_temporal_resolution(
                synthesis,
                uow.files.get_nwlistop(
                    synthesis.variable,
                    synthesis.spatial_resolution,
                    synthesis.temporal_resolution,
                    uhe=uhe_index,
                ),
            )
            if df_uhe is None:
                return None
            cols = df_uhe.columns.tolist()
            df_uhe["usina"] = uhe_name
            df_uhe = df_uhe[["usina"] + cols]
            return df_uhe

    @classmethod
    def __stub_agrega_estagio_variaveis_por_patamar(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            confhd = cls._validate_data(
                uow.files.get_confhd().usinas, pd.DataFrame, "UHEs"
            )
            rees = cls._validate_data(
                uow.files.get_ree().rees, pd.DataFrame, "REEs"
            )
            dger = uow.files.get_dger()
            ano_inicio = cls._validate_data(
                dger.ano_inicio_estudo, int, "dger"
            )
            anos_estudo = cls._validate_data(dger.num_anos_estudo, int, "dger")
            # Obtem o fim do periodo individualizado
            if rees["Ano Fim Individualizado"].isna().sum() > 0:
                fim = datetime(
                    year=ano_inicio + anos_estudo,
                    month=1,
                    day=1,
                )
            else:
                fim = datetime(
                    year=int(rees["Ano Fim Individualizado"].iloc[0]),
                    month=int(rees["Mês Fim Individualizado"].iloc[0]),
                    day=1,
                )
            uhes_idx = confhd["Número"]
            uhes_name = confhd["Nome"]

        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                idx: pool.apply_async(
                    cls._resolve_UHE_usina,
                    (
                        uow,
                        OperationSynthesis(
                            variable=synthesis.variable,
                            spatial_resolution=synthesis.spatial_resolution,
                            temporal_resolution=TemporalResolution.PATAMAR,
                        ),
                        idx,
                        name,
                    ),
                )
                for idx, name in zip(uhes_idx, uhes_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        for _, df in dfs.items():
            df_completo = pd.concat([df_completo, df], ignore_index=True)

        cols_nao_cenarios = [
            "estagio",
            "dataInicio",
            "dataFim",
            "patamar",
            "usina",
        ]
        cols_cenarios = [
            c
            for c in df_completo.columns.tolist()
            if c not in cols_nao_cenarios
        ]
        if synthesis.temporal_resolution == TemporalResolution.ESTAGIO:
            patamares = df_completo["patamar"].unique().tolist()
            cenarios_patamares: List[np.ndarray] = []
            p0 = patamares[0]
            for p in patamares:
                cenarios_patamares.append(
                    df_completo.loc[
                        df_completo["patamar"] == p, cols_cenarios
                    ].to_numpy()
                )
            df_completo.loc[df_completo["patamar"] == p0, cols_cenarios] = 0.0
            for c in cenarios_patamares:
                df_completo.loc[
                    df_completo["patamar"] == p0, cols_cenarios
                ] += c
            df_completo = df_completo.loc[df_completo["patamar"] == p0, :]
            df_completo = df_completo.drop(columns="patamar")

        df_completo = df_completo.loc[df_completo["dataInicio"] < fim, :]
        return df_completo

    @classmethod
    def __stub_QTUR_QVER(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        variable_map = {
            Variable.VAZAO_VERTIDA: Variable.VOLUME_VERTIDO,
            Variable.VAZAO_TURBINADA: Variable.VOLUME_TURBINADO,
        }
        with uow:
            confhd = cls._validate_data(
                uow.files.get_confhd().usinas, pd.DataFrame, "UHEs"
            )
            rees = cls._validate_data(
                uow.files.get_ree().rees, pd.DataFrame, "REEs"
            )
            dger = uow.files.get_dger()
            ano_inicio = cls._validate_data(
                dger.ano_inicio_estudo, int, "dger"
            )
            anos_estudo = cls._validate_data(dger.num_anos_estudo, int, "dger")

            # Obtem o fim do periodo individualizado
            if rees["Ano Fim Individualizado"].isna().sum() > 0:
                fim = datetime(
                    year=ano_inicio + anos_estudo,
                    month=1,
                    day=1,
                )
            else:
                fim = datetime(
                    year=int(rees["Ano Fim Individualizado"].iloc[0]),
                    month=int(rees["Mês Fim Individualizado"].iloc[0]),
                    day=1,
                )
            uhes_idx = confhd["Número"]
            uhes_name = confhd["Nome"]

        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                idx: pool.apply_async(
                    cls._resolve_UHE_usina,
                    (
                        uow,
                        OperationSynthesis(
                            variable=variable_map[synthesis.variable],
                            spatial_resolution=synthesis.spatial_resolution,
                            temporal_resolution=TemporalResolution.PATAMAR,
                        ),
                        idx,
                        name,
                    ),
                )
                for idx, name in zip(uhes_idx, uhes_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        for _, df in dfs.items():
            df_completo = pd.concat([df_completo, df], ignore_index=True)

        cols_nao_cenarios = [
            "estagio",
            "dataInicio",
            "dataFim",
            "patamar",
            "usina",
        ]
        cols_cenarios = [
            c
            for c in df_completo.columns.tolist()
            if c not in cols_nao_cenarios
        ]
        if synthesis.temporal_resolution == TemporalResolution.ESTAGIO:
            patamares = df_completo["patamar"].unique().tolist()
            cenarios_patamares: List[np.ndarray] = []
            p0 = patamares[0]
            for p in patamares:
                cenarios_patamares.append(
                    df_completo.loc[
                        df_completo["patamar"] == p, cols_cenarios
                    ].to_numpy()
                )
            df_completo.loc[df_completo["patamar"] == p0, cols_cenarios] = 0.0
            for c in cenarios_patamares:
                df_completo.loc[
                    df_completo["patamar"] == p0, cols_cenarios
                ] += c
            df_completo = df_completo.loc[df_completo["patamar"] == p0, :]
            df_completo = df_completo.drop(columns="patamar")

        df_completo.loc[:, cols_cenarios] *= FATOR_HM3_M3S
        df_completo = df_completo.loc[df_completo["dataInicio"] < fim, :]
        return df_completo

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
    def __stub_agrega_variaveis_indiv_REE_SBM_SIN(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            sistema = cls._validate_data(
                uow.files.get_sistema().custo_deficit,
                pd.DataFrame,
                "submercados",
            )
            confhd = cls._validate_data(
                uow.files.get_confhd().usinas, pd.DataFrame, "UHEs"
            )
            rees = cls._validate_data(
                uow.files.get_ree().rees, pd.DataFrame, "REEs"
            )

            rees_usinas = confhd["REE"].unique().tolist()
            nomes_rees = {
                r: str(rees.loc[rees["Número"] == r, "Nome"].tolist()[0])
                for r in rees_usinas
            }
            rees_submercados = {
                r: str(
                    sistema.loc[
                        sistema["Num. Subsistema"]
                        == int(
                            rees.loc[rees["Número"] == r, "Submercado"].iloc[0]
                        ),
                        "Nome",
                    ].tolist()[0]
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
                df_uhe = cls._resolve_spatial_resolution(s, uow)
                cls.CACHED_SYNTHESIS[s] = df_uhe
            else:
                df_uhe = cache_uhe

            if df_uhe is None:
                return None
            if df_uhe.empty:
                return None

            df_uhe = df_uhe.copy()

            df_uhe["group"] = df_uhe.apply(
                lambda linha: int(
                    confhd.loc[confhd["Nome"] == linha["usina"], "REE"].iloc[0]
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
                for c in df_uhe.columns
                if c in cls.IDENTIFICATION_COLUMNS and c != "usina"
            ]
            df_group = (
                df_uhe.groupby(cols_group).sum(numeric_only=True).reset_index()
            )

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
                    columns={"group": group_name[synthesis.spatial_resolution]}
                )
            return df_group

    @classmethod
    def __resolve_stub_vminop_sin(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sintese_sbm = OperationSynthesis(
            variable=synthesis.variable,
            spatial_resolution=SpatialResolution.SUBMERCADO,
            temporal_resolution=synthesis.temporal_resolution,
        )
        cache_vminop = cls.CACHED_SYNTHESIS.get(sintese_sbm)
        df_vminop = (
            cache_vminop
            if cache_vminop is not None
            else cls.__resolve_SBM(sintese_sbm, uow)
        )
        cols_group = [
            c
            for c in df_vminop.columns
            if c in cls.IDENTIFICATION_COLUMNS and c != "submercado"
        ]
        df_sin = (
            df_vminop.groupby(cols_group).sum(numeric_only=True).reset_index()
        )
        return df_sin

    @classmethod
    def __postprocess_violacoes_UHE_estagio(
        cls, df_completo: pd.DataFrame, cols_cenarios: List[str]
    ):
        patamares = df_completo["patamar"].unique().tolist()
        cenarios_patamares: List[np.ndarray] = []
        p0 = patamares[0]
        for p in patamares:
            cenarios_patamares.append(
                df_completo.loc[
                    df_completo["patamar"] == p, cols_cenarios
                ].to_numpy()
            )
        df_completo.loc[df_completo["patamar"] == p0, cols_cenarios] = 0.0
        for c in cenarios_patamares:
            df_completo.loc[df_completo["patamar"] == p0, cols_cenarios] += c
        df_completo = df_completo.loc[df_completo["patamar"] == p0, :]
        return df_completo.drop(columns=["patamar"])

    @classmethod
    def __stub_violacoes_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ):
        with uow:
            confhd = cls._validate_data(
                uow.files.get_confhd().usinas, pd.DataFrame, "UHEs"
            )
            rees = cls._validate_data(
                uow.files.get_ree().rees, pd.DataFrame, "REEs"
            )
            dger = uow.files.get_dger()
            ano_inicio = cls._validate_data(
                dger.ano_inicio_estudo, int, "dger"
            )
            anos_estudo = cls._validate_data(dger.num_anos_estudo, int, "dger")

            # Obtem o fim do periodo individualizado
            if rees["Ano Fim Individualizado"].isna().sum() > 0:
                fim = datetime(
                    year=ano_inicio + anos_estudo,
                    month=1,
                    day=1,
                )
            else:
                fim = datetime(
                    year=int(rees["Ano Fim Individualizado"].iloc[0]),
                    month=int(rees["Mês Fim Individualizado"].iloc[0]),
                    day=1,
                )
            uhes_idx = confhd["Número"]
            uhes_name = confhd["Nome"]

        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                idx: pool.apply_async(
                    cls._resolve_UHE_usina,
                    (
                        uow,
                        OperationSynthesis(
                            variable=synthesis.variable,
                            spatial_resolution=synthesis.spatial_resolution,
                            temporal_resolution=TemporalResolution.PATAMAR,
                        ),
                        idx,
                        name,
                    ),
                )
                for idx, name in zip(uhes_idx, uhes_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        for _, df in dfs.items():
            df_completo = pd.concat([df_completo, df], ignore_index=True)

        cols_nao_cenarios = [
            "estagio",
            "dataInicio",
            "dataFim",
            "patamar",
            "usina",
        ]
        cols_cenarios = [
            c
            for c in df_completo.columns.tolist()
            if c not in cols_nao_cenarios
        ]
        if synthesis.temporal_resolution == TemporalResolution.ESTAGIO:
            df_completo = cls.__postprocess_violacoes_UHE_estagio(
                df_completo, cols_cenarios
            )

        df_completo.loc[:, cols_cenarios] *= FATOR_HM3_M3S
        if df_completo is not None:
            if not df_completo.empty:
                df_completo = df_completo.loc[
                    df_completo["dataInicio"] < fim, :
                ]
        return df_completo

    @classmethod
    def __resolve_stubs_UHE(
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
            return cls.__stub_agrega_estagio_variaveis_por_patamar(
                synthesis, uow
            )
        elif synthesis.variable in [
            Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
            Variable.VIOLACAO_DEFLUENCIA_MINIMA,
            Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
            Variable.VIOLACAO_TURBINAMENTO_MINIMO,
            Variable.VIOLACAO_FPHA,
        ]:
            return cls.__stub_violacoes_UHE(synthesis, uow)

    @classmethod
    def __resolve_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        if synthesis.variable in [
            Variable.VAZAO_TURBINADA,
            Variable.VAZAO_VERTIDA,
            Variable.VAZAO_DEFLUENTE,
            Variable.GERACAO_HIDRAULICA,
            Variable.VOLUME_TURBINADO,
            Variable.VOLUME_VERTIDO,
            Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
            Variable.VIOLACAO_DEFLUENCIA_MINIMA,
            Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
            Variable.VIOLACAO_TURBINAMENTO_MINIMO,
            Variable.VIOLACAO_FPHA,
        ]:
            return cls.__resolve_stubs_UHE(synthesis, uow)
        with uow:
            confhd = cls._validate_data(
                uow.files.get_confhd().usinas, pd.DataFrame, "UHEs"
            )
            rees = cls._validate_data(
                uow.files.get_ree().rees, pd.DataFrame, "REEs"
            )
            dger = uow.files.get_dger()
            ano_inicio = cls._validate_data(
                dger.ano_inicio_estudo, int, "dger"
            )
            anos_estudo = cls._validate_data(dger.num_anos_estudo, int, "dger")

            # Obtem o fim do periodo individualizado
            if rees["Ano Fim Individualizado"].isna().sum() > 0:
                fim = datetime(
                    year=ano_inicio + anos_estudo,
                    month=1,
                    day=1,
                )
            else:
                fim = datetime(
                    year=int(rees["Ano Fim Individualizado"].iloc[0]),
                    month=int(rees["Mês Fim Individualizado"].iloc[0]),
                    day=1,
                )
            uhes_idx = confhd["Número"]
            uhes_name = confhd["Nome"]

        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                idx: pool.apply_async(
                    cls._resolve_UHE_usina, (uow, synthesis, idx, name)
                )
                for idx, name in zip(uhes_idx, uhes_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        for _, df in dfs.items():
            df_completo = pd.concat([df_completo, df], ignore_index=True)

        if not df_completo.empty:
            df_completo = df_completo.loc[df_completo["dataInicio"] < fim, :]
        return df_completo

    @classmethod
    def __resolve_UTE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            conft = cls._validate_data(
                uow.files.get_conft().usinas, pd.DataFrame, "UTEs"
            )
            utes_idx = conft["Número"]
            utes_name = conft["Nome"]
            df = pd.DataFrame()
            for s, n in zip(utes_idx, utes_name):
                if cls.logger is not None:
                    cls.logger.info(f"Processando arquivo da UTE: {s} - {n}")
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
                if cls.logger is not None:
                    cls.logger.info(f"Processando arquivo da UEE: {s} - {n}")
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
            ano_inicio = cls._validate_data(
                dger.ano_inicio_estudo, int, "dger"
            )
            mes_inicio = cls._validate_data(
                dger.mes_inicio_estudo, int, "dger"
            )
        starting_date = datetime(year=ano_inicio, month=mes_inicio, day=1)
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
            df["std"] = df[cols_cenarios].std(axis=1)
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
        cls.logger = logging.getLogger("main")
        try:
            if len(variables) == 0:
                synthesis_variables = OperationSynthetizer._default_args()
            else:
                synthesis_variables = (
                    OperationSynthetizer._process_variable_arguments(variables)
                )
            valid_synthesis = OperationSynthetizer.filter_valid_variables(
                synthesis_variables, uow
            )

            for s in valid_synthesis:
                filename = str(s)
                cls.logger.info(f"Realizando síntese de {filename}")
                if s.variable == Variable.ENERGIA_VERTIDA:
                    df = cls.__stub_EVER(s, uow)
                elif all(
                    [
                        s.variable == Variable.VIOLACAO_VMINOP,
                        s.spatial_resolution
                        == SpatialResolution.SISTEMA_INTERLIGADO,
                    ]
                ):
                    df = cls.__resolve_stub_vminop_sin(s, uow)
                elif all(
                    [
                        s.variable
                        in [
                            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                            Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
                            Variable.VIOLACAO_DEFLUENCIA_MINIMA,
                            Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
                            Variable.VIOLACAO_TURBINAMENTO_MINIMO,
                            Variable.VIOLACAO_FPHA,
                        ],
                        s.spatial_resolution
                        != SpatialResolution.USINA_HIDROELETRICA,
                    ]
                ):
                    df = cls.__stub_agrega_variaveis_indiv_REE_SBM_SIN(s, uow)
                else:
                    df = cls._resolve_spatial_resolution(s, uow)
                    if s in cls.SYNTHESIS_TO_CACHE:
                        cls.CACHED_SYNTHESIS[s] = df.copy()
                if df is not None:
                    if not df.empty:
                        df = cls._resolve_starting_stage(df, uow)
                        with uow:
                            df = cls._postprocess(df)
                            uow.export.synthetize_df(df, filename)
        except Exception as e:
            traceback.print_exc()
            cls.logger.error(str(e))
