from typing import Callable, Dict, List, Tuple, Optional, Type, TypeVar
import pandas as pd  # type: ignore
import numpy as np
import logging
import traceback
from multiprocessing import Pool
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta  # type: ignore
from inewave.newave import Dger, Ree, Confhd, Conft, Sistema
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
        "MERL_SBM_EST",
        "MERL_SIN_EST",
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
    def _get_dger(cls, uow: AbstractUnitOfWork) -> Dger:
        with uow:
            dger = uow.files.get_dger()
            if dger is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do dger.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return dger

    @classmethod
    def _get_ree(cls, uow: AbstractUnitOfWork) -> Ree:
        with uow:
            ree = uow.files.get_ree()
            if ree is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do ree.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return ree

    @classmethod
    def _get_confhd(cls, uow: AbstractUnitOfWork) -> Confhd:
        with uow:
            confhd = uow.files.get_confhd()
            if confhd is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do confhd.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return confhd

    @classmethod
    def _get_conft(cls, uow: AbstractUnitOfWork) -> Conft:
        with uow:
            conft = uow.files.get_conft()
            if conft is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do conft.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return conft

    @classmethod
    def _get_sistema(cls, uow: AbstractUnitOfWork) -> Sistema:
        with uow:
            sist = uow.files.get_sistema()
            if sist is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do sistema.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return sist

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
        dger = cls._get_dger(uow)
        rees = cls._validate_data(cls._get_ree(uow).rees, pd.DataFrame, "REE")
        valid_variables: List[OperationSynthesis] = []
        sf_indiv = dger.agregacao_simulacao_final == 1
        politica_indiv = rees["ano_fim_individualizado"].isna().sum() == 0
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
        cols = df.columns.tolist()
        datas = df["data"].unique().tolist()
        datas.sort()
        df = df.copy()
        df["estagio"] = df.apply(lambda x: datas.index(x["data"]) + 1, axis=1)
        df["dataFim"] = df.apply(
            lambda x: x["data"] + relativedelta(months=1), axis=1
        )
        df = df.rename(columns={"data": "dataInicio"})
        return df[
            ["estagio", "dataInicio", "dataFim"]
            + [c for c in cols if c != "data"]
        ]

    @classmethod
    def __resolve_PAT(cls, df: pd.DataFrame) -> pd.DataFrame:
        cols = df.columns.tolist()
        datas = df["data"].unique().tolist()
        datas.sort()
        df = df.copy()
        df["estagio"] = df.apply(lambda x: datas.index(x["data"]) + 1, axis=1)
        df["dataFim"] = df.apply(
            lambda x: x["data"] + relativedelta(months=1), axis=1
        )
        df = df.rename(columns={"data": "dataInicio"})
        return df[
            ["estagio", "dataInicio", "dataFim", "patamar"]
            + [c for c in cols if c not in ["patamar", "data"]]
        ]

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
        sistema = cls._validate_data(
            cls._get_sistema(uow).custo_deficit,
            pd.DataFrame,
            "submercados",
        )
        sistemas_reais = sistema.loc[sistema["ficticio"] == 0, :]
        sbms_idx = sistemas_reais["codigo_submercado"].unique()
        df = pd.DataFrame()
        with uow:
            for s in sbms_idx:
                n = sistemas_reais.loc[
                    sistemas_reais["codigo_submercado"] == s, "nome_submercado"
                ].iloc[0]
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
        sistema = cls._validate_data(
            cls._get_sistema(uow).custo_deficit,
            pd.DataFrame,
            "submercados",
        )
        sbms_idx = sistema["codigo_submercado"].unique()
        sbms_name = sistema["nome_submercado"]
        df = pd.DataFrame()
        with uow:
            for s1 in sbms_idx:
                n1 = sistema.loc[
                    sistema["codigo_submercado"] == s1, "nome_submercado"
                ].iloc[0]
                for s2 in sbms_idx:
                    n2 = sistema.loc[
                        sistema["codigo_submercado"] == s2, "nome_submercado"
                    ].iloc[0]
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
        rees = cls._validate_data(cls._get_ree(uow).rees, pd.DataFrame, "REEs")
        rees_idx = rees["codigo"]
        rees_name = rees["nome"]
        df = pd.DataFrame()
        with uow:
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
        confhd = cls._validate_data(
            cls._get_confhd(uow).usinas, pd.DataFrame, "UHEs"
        )
        rees = cls._validate_data(cls._get_ree(uow).rees, pd.DataFrame, "REEs")
        dger = cls._get_dger(uow)
        ano_inicio = cls._validate_data(dger.ano_inicio_estudo, int, "dger")
        anos_estudo = cls._validate_data(dger.num_anos_estudo, int, "dger")
        agregacao_sim_final = dger.agregacao_simulacao_final
        anos_pos_sim_final = cls._validate_data(
            dger.num_anos_pos_sim_final, int, "dger"
        )

        # Obtem o fim do periodo individualizado
        if agregacao_sim_final == 1:
            fim = datetime(
                year=ano_inicio + anos_estudo + anos_pos_sim_final,
                month=1,
                day=1,
            )
        elif rees["ano_fim_individualizado"].isna().sum() > 0:
            fim = datetime(
                year=ano_inicio + anos_estudo + anos_pos_sim_final,
                month=1,
                day=1,
            )
        else:
            fim = datetime(
                year=int(rees["ano_fim_individualizado"].iloc[0]),
                month=int(rees["mes_fim_individualizado"].iloc[0]),
                day=1,
            )
        uhes_idx = confhd["codigo_usina"]
        uhes_name = confhd["nome_usina"]

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

        if synthesis.temporal_resolution == TemporalResolution.ESTAGIO:
            patamares = df_completo["patamar"].unique().tolist()
            cenarios_patamares: List[np.ndarray] = []
            p0 = patamares[0]
            for p in patamares:
                cenarios_patamares.append(
                    df_completo.loc[
                        df_completo["patamar"] == p, "valor"
                    ].to_numpy()
                )
            df_completo.loc[df_completo["patamar"] == p0, "valor"] = 0.0
            for c in cenarios_patamares:
                df_completo.loc[df_completo["patamar"] == p0, "valor"] += c
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
        confhd = cls._validate_data(
            cls._get_confhd(uow).usinas, pd.DataFrame, "UHEs"
        )
        rees = cls._validate_data(cls._get_ree(uow).rees, pd.DataFrame, "REEs")
        dger = cls._get_dger(uow)
        ano_inicio = cls._validate_data(dger.ano_inicio_estudo, int, "dger")
        anos_estudo = cls._validate_data(dger.num_anos_estudo, int, "dger")
        anos_pos_sim_final = cls._validate_data(
            dger.num_anos_pos_sim_final, int, "dger"
        )

        agregacao_sim_final = dger.agregacao_simulacao_final
        # Obtem o fim do periodo individualizado
        if agregacao_sim_final == 1:
            fim = datetime(
                year=ano_inicio + anos_estudo + anos_pos_sim_final,
                month=1,
                day=1,
            )
        elif rees["ano_fim_individualizado"].isna().sum() > 0:
            fim = datetime(
                year=ano_inicio + anos_estudo + anos_pos_sim_final,
                month=1,
                day=1,
            )
        else:
            fim = datetime(
                year=int(rees["ano_fim_individualizado"].iloc[0]),
                month=int(rees["mes_fim_individualizado"].iloc[0]),
                day=1,
            )
        uhes_idx = confhd["codigo_usina"]
        uhes_name = confhd["nome_usina"]

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

        if synthesis.temporal_resolution == TemporalResolution.ESTAGIO:
            patamares = df_completo["patamar"].unique().tolist()
            cenarios_patamares: List[np.ndarray] = []
            p0 = patamares[0]
            for p in patamares:
                cenarios_patamares.append(
                    df_completo.loc[
                        df_completo["patamar"] == p, "valor"
                    ].to_numpy()
                )
            df_completo.loc[df_completo["patamar"] == p0, "valor"] = 0.0
            for c in cenarios_patamares:
                df_completo.loc[df_completo["patamar"] == p0, "valor"] += c
            df_completo = df_completo.loc[df_completo["patamar"] == p0, :]
            df_completo = df_completo.drop(columns="patamar")

        df_completo.loc[:, "valor"] *= FATOR_HM3_M3S
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
        df_ver.loc[:, "valor"] = (
            df_tur["valor"].to_numpy() + df_ver["valor"].to_numpy()
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

        df_reserv.loc[:, "valor"] = (
            df_fio["valor"].to_numpy() + df_reserv["valor"].to_numpy()
        )
        return df_reserv

    @classmethod
    def __stub_agrega_variaveis_indiv_REE_SBM_SIN(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sistema = cls._validate_data(
            cls._get_sistema(uow).custo_deficit,
            pd.DataFrame,
            "submercados",
        )
        confhd = cls._validate_data(
            cls._get_confhd(uow).usinas, pd.DataFrame, "UHEs"
        )
        rees = cls._validate_data(cls._get_ree(uow).rees, pd.DataFrame, "REEs")

        rees_usinas = confhd["ree"].unique().tolist()
        nomes_rees = {
            r: str(rees.loc[rees["codigo"] == r, "nome"].tolist()[0])
            for r in rees_usinas
        }
        rees_submercados = {
            r: str(
                sistema.loc[
                    sistema["codigo_submercado"]
                    == int(
                        rees.loc[rees["codigo"] == r, "submercado"].iloc[0]
                    ),
                    "nome_submercado",
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
                confhd.loc[confhd["nome_usina"] == linha["usina"], "ree"].iloc[
                    0
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
    def __postprocess_violacoes_UHE_estagio(cls, df_completo: pd.DataFrame):
        patamares = df_completo["patamar"].unique().tolist()
        cenarios_patamares: List[np.ndarray] = []
        p0 = patamares[0]
        for p in patamares:
            cenarios_patamares.append(
                df_completo.loc[
                    df_completo["patamar"] == p, "valor"
                ].to_numpy()
            )
        df_completo.loc[df_completo["patamar"] == p0, "valor"] = 0.0
        for c in cenarios_patamares:
            df_completo.loc[df_completo["patamar"] == p0, "valor"] += c
        df_completo = df_completo.loc[df_completo["patamar"] == p0, :]
        return df_completo.drop(columns=["patamar"])

    @classmethod
    def __stub_violacoes_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ):
        confhd = cls._validate_data(
            cls._get_confhd(uow).usinas, pd.DataFrame, "UHEs"
        )
        rees = cls._validate_data(cls._get_ree(uow).rees, pd.DataFrame, "REEs")
        dger = cls._get_dger(uow)
        agregacao_sim_final = dger.agregacao_simulacao_final
        ano_inicio = cls._validate_data(dger.ano_inicio_estudo, int, "dger")
        anos_estudo = cls._validate_data(dger.num_anos_estudo, int, "dger")

        anos_pos_sim_final = cls._validate_data(
            dger.num_anos_pos_sim_final, int, "dger"
        )

        # Obtem o fim do periodo individualizado
        if agregacao_sim_final == 1:
            fim = datetime(
                year=ano_inicio + anos_estudo + anos_pos_sim_final,
                month=1,
                day=1,
            )
        elif rees["ano_fim_individualizado"].isna().sum() > 0:
            fim = datetime(
                year=ano_inicio + anos_estudo + anos_pos_sim_final,
                month=1,
                day=1,
            )
        else:
            fim = datetime(
                year=int(rees["ano_fim_individualizado"].iloc[0]),
                month=int(rees["mes_fim_individualizado"].iloc[0]),
                day=1,
            )
        uhes_idx = confhd["codigo_usina"]
        uhes_name = confhd["nome_usina"]

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

        if synthesis.temporal_resolution == TemporalResolution.ESTAGIO:
            df_completo = cls.__postprocess_violacoes_UHE_estagio(df_completo)

        df_completo.loc[:, "valor"] *= FATOR_HM3_M3S
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
        confhd = cls._validate_data(
            cls._get_confhd(uow).usinas, pd.DataFrame, "UHEs"
        )
        rees = cls._validate_data(cls._get_ree(uow).rees, pd.DataFrame, "REEs")
        dger = cls._get_dger(uow)
        ano_inicio = cls._validate_data(dger.ano_inicio_estudo, int, "dger")
        agregacao_sim_final = dger.agregacao_simulacao_final
        anos_estudo = cls._validate_data(dger.num_anos_estudo, int, "dger")
        anos_pos_sim_final = cls._validate_data(
            dger.num_anos_pos_sim_final, int, "dger"
        )

        # Obtem o fim do periodo individualizado
        if agregacao_sim_final == 1:
            fim = datetime(
                year=ano_inicio + anos_estudo + anos_pos_sim_final,
                month=1,
                day=1,
            )
        elif rees["ano_fim_individualizado"].isna().sum() > 0:
            fim = datetime(
                year=ano_inicio + anos_estudo + anos_pos_sim_final,
                month=1,
                day=1,
            )
        else:
            fim = datetime(
                year=int(rees["ano_fim_individualizado"].iloc[0]),
                month=int(rees["mes_fim_individualizado"].iloc[0]),
                day=1,
            )
        uhes_idx = confhd["codigo_usina"]
        uhes_name = confhd["nome_usina"]

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
        conft = cls._validate_data(
            cls._get_conft(uow).usinas, pd.DataFrame, "UTEs"
        )
        utes_idx = conft["codigo_usina"]
        utes_name = conft["nome_usina"]
        df = pd.DataFrame()
        with uow:
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
            if eolica_cadastro is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do eolica-cadastro.csv para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
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
        dger = cls._get_dger(uow)
        ano_inicio = cls._validate_data(dger.ano_inicio_estudo, int, "dger")
        mes_inicio = cls._validate_data(dger.mes_inicio_estudo, int, "dger")
        starting_date = datetime(ano_inicio, mes_inicio, 1)
        data_starting_date = df["dataInicio"].min().to_pydatetime()
        cls.logger.info("DEBUG STARTING STAGE")
        cls.logger.info(starting_date)
        cls.logger.info(data_starting_date)
        month_difference = int(
            (starting_date - data_starting_date) / timedelta(days=30)
        )
        cls.logger.info(month_difference)
        starting_df = df.copy()
        starting_df.loc[:, "estagio"] -= month_difference
        starting_df = starting_df.rename(columns={"serie": "cenario"})
        cls.logger.info(starting_df)
        return starting_df.copy()

    @classmethod
    def _processa_media(cls, df: pd.DataFrame) -> pd.DataFrame:
        cols_valores = ["cenario", "valor"]
        cols_agrupamento = [c for c in df.columns if c not in cols_valores]
        df_mean = (
            df.groupby(cols_agrupamento).mean(numeric_only=True).reset_index()
        )
        df_mean["cenario"] = "mean"
        df_std = (
            df.groupby(cols_agrupamento).std(numeric_only=True).reset_index()
        )
        df_std["cenario"] = "std"
        df = pd.concat([df, df_mean, df_std], ignore_index=True)
        return df

    @classmethod
    def _processa_quantis(
        cls, df: pd.DataFrame, quantiles: List[float]
    ) -> pd.DataFrame:
        cols_valores = ["cenario", "valor"]
        cols_agrupamento = [c for c in df.columns if c not in cols_valores]
        for q in quantiles:
            if q == 0:
                label = "min"
            elif q == 1:
                label = "max"
            elif q == 0.5:
                label = "median"
            else:
                label = f"p{int(100 * q)}"
            df_q = (
                df.groupby(cols_agrupamento)
                .quantile(q, numeric_only=True)
                .reset_index()
            )
            df_q["cenario"] = label
            df = pd.concat([df, df_q], ignore_index=True)
        return df

    @classmethod
    def _postprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = cls._processa_quantis(df, [0.05 * i for i in range(21)])
        df = cls._processa_media(df)
        return df

    @classmethod
    def _resolve_stub(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Tuple[pd.DataFrame, bool]:
        if s.variable == Variable.ENERGIA_VERTIDA:
            df = cls.__stub_EVER(s, uow)
            return df, True
        elif all(
            [
                s.variable == Variable.VIOLACAO_VMINOP,
                s.spatial_resolution == SpatialResolution.SISTEMA_INTERLIGADO,
            ]
        ):
            df = cls.__resolve_stub_vminop_sin(s, uow)
            return df, True
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
                s.spatial_resolution != SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            df = cls.__stub_agrega_variaveis_indiv_REE_SBM_SIN(s, uow)
            return df, True
        else:
            return pd.DataFrame(), False

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
                df, is_stub = cls._resolve_stub(s, uow)
                if not is_stub:
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
