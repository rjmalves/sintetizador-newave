from typing import Callable, Dict, List, Tuple, Optional, Type, TypeVar
import pandas as pd  # type: ignore
import numpy as np
import logging

# import traceback
from multiprocessing import Pool
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta  # type: ignore
from inewave.newave import Dger, Ree, Confhd, Conft, Sistema, Clast
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
        "serie",
    ]

    DEFAULT_OPERATION_SYNTHESIS_ARGS: List[str] = [
        "CMO_SBM_EST",
        "CMO_SBM_PAT",
        "VAGUA_REE_EST",
        "VAGUA_UHE_EST",
        "CTER_SBM_EST",
        "CTER_SIN_EST",
        "COP_SIN_EST",
        "ENAA_REE_EST",
        "ENAA_SBM_EST",
        "ENAA_SIN_EST",
        "ENAAR_REE_EST",
        "ENAAR_SBM_EST",
        "ENAAR_SIN_EST",
        "ENAAF_REE_EST",
        "ENAAF_SBM_EST",
        "ENAAF_SIN_EST",
        "EARPI_REE_EST",
        "EARPI_SBM_EST",
        "EARPI_SIN_EST",
        "EARPF_REE_EST",
        "EARPF_SBM_EST",
        "EARPF_SIN_EST",
        "EARMI_REE_EST",
        "EARMI_SBM_EST",
        "EARMI_SIN_EST",
        "EARMF_REE_EST",
        "EARMF_SBM_EST",
        "EARMF_SIN_EST",
        "GHIDR_REE_EST",
        "GHIDR_SBM_EST",
        "GHIDR_SIN_EST",
        "GHID_REE_EST",
        "GHID_SBM_EST",
        "GHID_SIN_EST",
        "GHIDF_REE_EST",
        "GHIDF_SBM_EST",
        "GHIDF_SIN_EST",
        "GTER_SBM_EST",
        "GTER_SIN_EST",
        "GHIDR_REE_PAT",
        "GHIDR_SBM_PAT",
        "GHIDR_SIN_PAT",
        "GHID_REE_PAT",
        "GHID_SBM_PAT",
        "GHID_SIN_PAT",
        "GHIDF_REE_PAT",
        "GHIDF_SBM_PAT",
        "GHIDF_SIN_PAT",
        "GTER_UTE_PAT",
        "GTER_UTE_EST",
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
        "EDESR_REE_EST",
        "EDESR_SBM_EST",
        "EDESR_SIN_EST",
        "EDESF_REE_EST",
        "EDESF_SBM_EST",
        "EDESF_SIN_EST",
        "EVMIN_REE_EST",
        "EVMIN_SBM_EST",
        "EVMIN_SIN_EST",
        "EVMOR_REE_EST",
        "EVMOR_SBM_EST",
        "EVMOR_SIN_EST",
        "EEVAP_REE_EST",
        "EEVAP_SBM_EST",
        "EEVAP_SIN_EST",
        "VTUR_UHE_PAT",
        "VTUR_UHE_EST",
        "VVER_UHE_PAT",
        "VVER_UHE_EST",
        "QTUR_UHE_PAT",
        "QTUR_UHE_EST",
        "QVER_UHE_PAT",
        "QVER_UHE_EST",
        "QAFL_UHE_EST",
        "QINC_UHE_EST",
        "VAFL_UHE_EST",
        "VINC_UHE_EST",
        "QDEF_UHE_PAT",
        "QDEF_UHE_EST",
        "VDEF_UHE_PAT",
        "VDEF_UHE_EST",
        "VRET_UHE_EST",
        "VDES_UHE_PAT",
        "VDES_UHE_EST",
        "QRET_UHE_EST",
        "QDES_UHE_PAT",
        "QDES_UHE_EST",
        "VTUR_REE_PAT",
        "VTUR_REE_EST",
        "VVER_REE_PAT",
        "VVER_REE_EST",
        "QTUR_REE_PAT",
        "QTUR_REE_EST",
        "QVER_REE_PAT",
        "QVER_REE_EST",
        "QAFL_REE_EST",
        "QINC_REE_EST",
        "VAFL_REE_EST",
        "VINC_REE_EST",
        "QDEF_REE_PAT",
        "QDEF_REE_EST",
        "VDEF_REE_PAT",
        "VDEF_REE_EST",
        "VRET_REE_EST",
        "VDES_REE_PAT",
        "VDES_REE_EST",
        "QRET_REE_EST",
        "QDES_REE_PAT",
        "QDES_REE_EST",
        "VTUR_SBM_PAT",
        "VTUR_SBM_EST",
        "VVER_SBM_PAT",
        "VVER_SBM_EST",
        "QTUR_SBM_PAT",
        "QTUR_SBM_EST",
        "QVER_SBM_PAT",
        "QVER_SBM_EST",
        "QAFL_SBM_EST",
        "QINC_SBM_EST",
        "VAFL_SBM_EST",
        "VINC_SBM_EST",
        "QDEF_SBM_PAT",
        "QDEF_SBM_EST",
        "VDEF_SBM_PAT",
        "VDEF_SBM_EST",
        "VRET_SBM_EST",
        "VDES_SBM_PAT",
        "VDES_SBM_EST",
        "QRET_SBM_EST",
        "QDES_SBM_PAT",
        "QDES_SBM_EST",
        "VTUR_SIN_PAT",
        "VTUR_SIN_EST",
        "VVER_SIN_PAT",
        "VVER_SIN_EST",
        "QTUR_SIN_PAT",
        "QTUR_SIN_EST",
        "QVER_SIN_PAT",
        "QVER_SIN_EST",
        "QAFL_SIN_EST",
        "QINC_SIN_EST",
        "VAFL_SIN_EST",
        "VINC_SIN_EST",
        "QDEF_SIN_PAT",
        "QDEF_SIN_EST",
        "VDEF_SIN_PAT",
        "VDEF_SIN_EST",
        "VRET_SIN_EST",
        "VDES_SIN_PAT",
        "VDES_SIN_EST",
        "QRET_SIN_EST",
        "QDES_SIN_PAT",
        "QDES_SIN_EST",
        "VARMI_UHE_EST",
        "VARMI_REE_EST",
        "VARMI_SBM_EST",
        "VARMI_SIN_EST",
        "VARMF_UHE_EST",
        "VARMF_REE_EST",
        "VARMF_SBM_EST",
        "VARMF_SIN_EST",
        "VARPI_UHE_EST",
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
        "HMON_UHE_EST",
        "HJUS_UHE_PAT",
        "HLIQ_UHE_PAT",
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
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.PATAMAR,
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
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
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
            Variable.VOLUME_DESVIADO,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
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
            Variable.VOLUME_RETIRADO,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
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
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
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
            Variable.VAZAO_RETIRADA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
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
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.USINA_TERMELETRICA,
            TemporalResolution.PATAMAR,
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.ESTAGIO,
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
            TemporalResolution.PATAMAR,
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
    def _get_clast(cls, uow: AbstractUnitOfWork) -> Clast:
        with uow:
            sist = uow.files.get_clast()
            if sist is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do clast.dat para"
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
                    Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                    Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                ]
                and not indiv
            ):
                continue
            if all(
                [
                    v.variable == Variable.VALOR_AGUA,
                    v.spatial_resolution
                    == SpatialResolution.USINA_HIDROELETRICA,
                    not indiv,
                ]
            ):
                continue
            valid_variables.append(v)
        if cls.logger is not None:
            cls.logger.info(f"Variáveis: {valid_variables}")
        return valid_variables

    @classmethod
    def __resolve_EST(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.sort_values(["data", "serie"], inplace=True)
        cols = df.columns.tolist()
        datas = df["data"].unique().tolist()
        datas.sort()
        n_datas = len(datas)
        series = df["serie"].unique().tolist()
        n_series = len(series)
        # Atribui estagio e dataFim de forma posicional
        estagios = list(range(1, n_datas + 1))
        estagios_df = np.repeat(estagios, n_series)
        datasFim = [d + relativedelta(months=1) for d in datas]
        datasFim_df = np.repeat(datasFim, n_series)
        df = df.rename(columns={"data": "dataInicio"})
        df["estagio"] = estagios_df
        df["dataFim"] = datasFim_df
        return df[
            ["estagio", "dataInicio", "dataFim"]
            + [c for c in cols if c not in ["data", "patamar"]]
        ]

    @classmethod
    def __resolve_PAT(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.sort_values(["data", "serie", "patamar"], inplace=True)
        cols = df.columns.tolist()
        datas = df["data"].unique().tolist()
        datas.sort()
        n_datas = len(datas)
        series = df["serie"].unique().tolist()
        n_series = len(series)
        patamares = df["patamar"].unique().tolist()
        n_patamares = len(patamares)
        # Atribui estagio e dataFim de forma posicional
        estagios = list(range(1, n_datas + 1))
        estagios_df = np.repeat(estagios, n_series * n_patamares)
        datasFim = [d + relativedelta(months=1) for d in datas]
        datasFim_df = np.repeat(datasFim, n_series * n_patamares)
        df = df.rename(columns={"data": "dataInicio"})
        df["estagio"] = estagios_df
        df["dataFim"] = datasFim_df
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
    def _resolve_SBM_submercado(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        sbm_index: int,
        sbm_name: str,
    ) -> pd.DataFrame:
        logger_name = f"{synthesis.variable.value}_{sbm_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, sbm_index
        )
        with uow:
            logger.info(
                f"Processando arquivo do submercado: {sbm_index} - {sbm_name}"
            )
            df_sbm = cls._resolve_temporal_resolution(
                synthesis,
                uow.files.get_nwlistop(
                    synthesis.variable,
                    synthesis.spatial_resolution,
                    synthesis.temporal_resolution,
                    submercado=sbm_index,
                ),
            )
            if df_sbm is None:
                return None
            cols = df_sbm.columns.tolist()
            df_sbm["submercado"] = sbm_name
            df_sbm = df_sbm[["submercado"] + cols]
            return df_sbm

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
        sbms_name = [
            sistemas_reais.loc[
                sistemas_reais["codigo_submercado"] == s, "nome_submercado"
            ].iloc[0]
            for s in sbms_idx
        ]
        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                idx: pool.apply_async(
                    cls._resolve_SBM_submercado, (uow, synthesis, idx, name)
                )
                for idx, name in zip(sbms_idx, sbms_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        dfs_validos = [d for d in dfs.values() if d is not None]
        if len(dfs_validos) > 0:
            df_completo = pd.concat(dfs_validos, ignore_index=True)

        return df_completo

    @classmethod
    def _resolve_SBP_par_submercados(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        sbm1_index: int,
        sbm1_name: str,
        sbm2_index: int,
        sbm2_name: str,
    ) -> pd.DataFrame:
        if sbm1_index >= sbm2_index:
            return pd.DataFrame()
        logger_name = f"{synthesis.variable.value}_{sbm1_name}_{sbm2_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, sbm1_index + 10 * sbm2_index
        )
        with uow:
            logger.info(
                "Processando arquivo do par de submercados:"
                + f" {sbm1_index}[{sbm1_name}] - {sbm2_index}[{sbm2_name}]"
            )
            df_sbp = cls._resolve_temporal_resolution(
                synthesis,
                uow.files.get_nwlistop(
                    synthesis.variable,
                    synthesis.spatial_resolution,
                    synthesis.temporal_resolution,
                    submercados=(sbm1_index, sbm2_index),
                ),
            )
            if df_sbp is None:
                return None
            cols = df_sbp.columns.tolist()
            df_sbp["submercadoDe"] = sbm1_name
            df_sbp["submercadoPara"] = sbm2_name
            df_sbp = df_sbp[["submercadoDe", "submercadoPara"] + cols]
            return df_sbp

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
        sbms_name = [
            sistema.loc[
                sistema["codigo_submercado"] == s, "nome_submercado"
            ].iloc[0]
            for s in sbms_idx
        ]
        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                f"{idx1}-{idx2}": pool.apply_async(
                    cls._resolve_SBP_par_submercados,
                    (uow, synthesis, idx1, name1, idx2, name2),
                )
                for idx1, name1 in zip(sbms_idx, sbms_name)
                for idx2, name2 in zip(sbms_idx, sbms_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        dfs_validos = [d for d in dfs.values() if d is not None]
        if len(dfs_validos) > 0:
            df_completo = pd.concat(dfs_validos, ignore_index=True)

        return df_completo

    @classmethod
    def _resolve_REE_ree(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        ree_index: int,
        ree_name: str,
    ) -> pd.DataFrame:
        logger_name = f"{synthesis.variable.value}_{ree_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, ree_index
        )
        with uow:
            logger.info(
                f"Processando arquivo do REE: {ree_index} - {ree_name}"
            )
            df_sbm = cls._resolve_temporal_resolution(
                synthesis,
                uow.files.get_nwlistop(
                    synthesis.variable,
                    synthesis.spatial_resolution,
                    synthesis.temporal_resolution,
                    ree=ree_index,
                ),
            )
            if df_sbm is None:
                return None
            cols = df_sbm.columns.tolist()
            df_sbm["ree"] = ree_name
            df_sbm = df_sbm[["ree"] + cols]
            return df_sbm

    @classmethod
    def __resolve_REE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        rees = cls._validate_data(cls._get_ree(uow).rees, pd.DataFrame, "REEs")
        rees_idx = rees["codigo"]
        rees_name = rees["nome"]
        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                idx: pool.apply_async(
                    cls._resolve_REE_ree, (uow, synthesis, idx, name)
                )
                for idx, name in zip(rees_idx, rees_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        dfs_validos = [d for d in dfs.values() if d is not None]
        if len(dfs_validos) > 0:
            df_completo = pd.concat(dfs_validos, ignore_index=True)

        return df_completo

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
    def _resolve_gtert_temporal(
        cls, df: Optional[pd.DataFrame]
    ) -> pd.DataFrame:
        if df is None:
            return df
        df.sort_values(["classe", "data", "serie", "patamar"], inplace=True)
        classes = df["classe"].unique().tolist()
        n_classes = len(classes)
        datas = df["data"].unique().tolist()
        datas.sort()
        n_datas = len(datas)
        series = df["serie"].unique().tolist()
        n_series = len(series)
        patamares = df["patamar"].unique().tolist()
        n_patamares = len(patamares)
        # Atribui estagio e dataFim de forma posicional
        estagios = list(range(1, n_datas + 1))
        estagios_df = np.tile(
            np.repeat(estagios, n_series * n_patamares), n_classes
        )
        datasFim = [d + relativedelta(months=1) for d in datas]
        datasFim_df = np.tile(
            np.repeat(datasFim, n_series * n_patamares), n_classes
        )
        df = df.copy()
        df["estagio"] = estagios_df
        df["dataFim"] = datasFim_df
        df = df.rename(columns={"data": "dataInicio"})
        return df

    @classmethod
    def _resolve_gtert(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        sbm_index: int,
        sbm_name: str,
    ) -> pd.DataFrame:
        logger_name = f"{synthesis.variable.value}_{sbm_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, sbm_index
        )
        with uow:
            logger.info(
                f"Processando arquivo do submercado: {sbm_index} - {sbm_name}"
            )
            df_gtert = cls._resolve_gtert_temporal(
                uow.files.get_nwlistop(
                    synthesis.variable,
                    synthesis.spatial_resolution,
                    synthesis.temporal_resolution,
                    submercado=sbm_index,
                )
            )
            return df_gtert

    @classmethod
    def __stub_agrega_estagio_variaveis_por_patamar(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        synt_pat = OperationSynthesis(
            synthesis.variable,
            synthesis.spatial_resolution,
            TemporalResolution.PATAMAR,
        )
        cache = cls.CACHED_SYNTHESIS.get(synt_pat)
        resolve_func = {
            SpatialResolution.USINA_HIDROELETRICA: cls.__resolve_UHE,
            SpatialResolution.USINA_TERMELETRICA: cls.__stub_GTER_UTE_patamar,
        }[synthesis.spatial_resolution]
        df_completo = (
            cache
            if cache is not None
            else resolve_func(
                synt_pat,
                uow,
            )
        )
        if cache is None:
            cls.CACHED_SYNTHESIS[synt_pat] = df_completo.copy()

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

        cls.CACHED_SYNTHESIS[synthesis] = df_completo.copy()

        return df_completo

    @classmethod
    def __stub_converte_volume_em_vazao(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        variable_map = {
            Variable.VAZAO_VERTIDA: Variable.VOLUME_VERTIDO,
            Variable.VAZAO_TURBINADA: Variable.VOLUME_TURBINADO,
            Variable.VAZAO_DESVIADA: Variable.VOLUME_DESVIADO,
            Variable.VAZAO_RETIRADA: Variable.VOLUME_RETIRADO,
        }
        synt_vol = OperationSynthesis(
            variable_map[synthesis.variable],
            synthesis.spatial_resolution,
            synthesis.temporal_resolution,
        )
        cache = cls.CACHED_SYNTHESIS.get(synt_vol)
        df_completo = (
            cache
            if cache is not None
            else cls.__resolve_UHE(
                synt_vol,
                uow,
            )
        )
        if cache is None:
            cls.CACHED_SYNTHESIS[synt_vol] = df_completo.copy()

        df_completo.loc[:, "valor"] *= FATOR_HM3_M3S
        cls.CACHED_SYNTHESIS[synthesis] = df_completo.copy()

        return df_completo

    @classmethod
    def __stub_converte_vazao_em_volume(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        variable_map = {
            Variable.VOLUME_AFLUENTE: Variable.VAZAO_AFLUENTE,
            Variable.VOLUME_INCREMENTAL: Variable.VAZAO_INCREMENTAL,
        }
        synt_vol = OperationSynthesis(
            variable_map[synthesis.variable],
            synthesis.spatial_resolution,
            synthesis.temporal_resolution,
        )
        cache = cls.CACHED_SYNTHESIS.get(synt_vol)
        df_completo = (
            cache
            if cache is not None
            else cls.__resolve_UHE(
                synt_vol,
                uow,
            )
        )
        if cache is None:
            cls.CACHED_SYNTHESIS[synt_vol] = df_completo.copy()

        df_completo.loc[:, "valor"] /= FATOR_HM3_M3S
        cls.CACHED_SYNTHESIS[synthesis] = df_completo.copy()

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
            else cls.__stub_converte_volume_em_vazao(
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
            else cls.__stub_converte_volume_em_vazao(
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
    def __stub_VDEF(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sintese_tur = OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            synthesis.spatial_resolution,
            synthesis.temporal_resolution,
        )
        sintese_ver = OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            synthesis.spatial_resolution,
            synthesis.temporal_resolution,
        )
        cache_tur = cls.CACHED_SYNTHESIS.get(sintese_tur)
        cache_ver = cls.CACHED_SYNTHESIS.get(sintese_ver)
        df_tur = (
            cache_tur
            if cache_tur is not None
            else cls.__stub_converte_volume_em_vazao(
                OperationSynthesis(
                    Variable.VOLUME_TURBINADO,
                    synthesis.spatial_resolution,
                    synthesis.temporal_resolution,
                ),
                uow,
            )
        )
        df_ver = (
            cache_ver
            if cache_ver is not None
            else cls.__stub_converte_volume_em_vazao(
                OperationSynthesis(
                    Variable.VOLUME_VERTIDO,
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
            if s.variable == Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL:
                df_uhe = cls.__stub_resolve_volumes_iniciais_uhe(s, uow)
            else:
                df_uhe = cls._resolve_spatial_resolution(s, uow)
            cls.CACHED_SYNTHESIS[s] = df_uhe
        else:
            df_uhe = cache_uhe

        if df_uhe is None:
            return None
        if df_uhe.empty:
            return None

        df_uhe = df_uhe.copy().reset_index(drop=True)

        # Extrai a lista de usinas e quantas linhas existem para cada
        usinas = df_uhe["usina"].drop_duplicates()
        if usinas.shape[0] > 1:
            n_linhas_usina = usinas.index[1] - usinas.index[0]
        else:
            n_linhas_usina = df_uhe.shape[0]
        df_usina_group = pd.DataFrame(data={"usina": usinas.tolist()})

        df_usina_group["group"] = df_usina_group.apply(
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
            df_usina_group["group"] = df_usina_group.apply(
                lambda linha: nomes_rees[linha["group"]], axis=1
            )
        elif synthesis.spatial_resolution == SpatialResolution.SUBMERCADO:
            df_usina_group["group"] = df_usina_group.apply(
                lambda linha: rees_submercados[linha["group"]], axis=1
            )
        elif (
            synthesis.spatial_resolution
            == SpatialResolution.SISTEMA_INTERLIGADO
        ):
            df_usina_group["group"] = 1

        cols_group = ["group"] + [
            c
            for c in df_uhe.columns
            if c in cls.IDENTIFICATION_COLUMNS and c != "usina"
        ]
        # Replica o grupo pelo número de linhas de cada usina e cria a coluna no df final
        df_uhe["group"] = np.repeat(
            df_usina_group["group"].to_numpy(), n_linhas_usina
        )

        df_uhe = df_uhe.astype({"serie": int})
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
            temporal_resolution=synthesis.temporal_resolution,
        )
        resolve_func = {
            SpatialResolution.RESERVATORIO_EQUIVALENTE: cls.__resolve_REE,
            SpatialResolution.SUBMERCADO: cls.__resolve_SBM,
            SpatialResolution.SISTEMA_INTERLIGADO: cls.__resolve_SIN,
        }[synthesis.spatial_resolution]
        cache_earm = cls.CACHED_SYNTHESIS.get(sintese_final)
        df_final = (
            cache_earm
            if cache_earm is not None
            else resolve_func(sintese_final, uow)
        )
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
        series = df_inicial["serie"].unique().tolist()
        n_series = len(series)
        estagios = df_inicial["estagio"].unique().tolist()
        n_estagios = len(estagios)
        # Faz uma atribuição posicional. A maneira mais pythonica é lenta.
        offset_meses = cls._offset_meses_inicio(df_inicial, uow)
        offsets_groups = [i * n_series * n_estagios for i in range(n_groups)]
        indices_primeiros_estagios = offset_meses * n_series + np.tile(
            np.arange(n_series), n_groups
        )
        indices_primeiros_estagios += np.repeat(offsets_groups, n_series)
        earmi_pmo = earmi_pmo.loc[earmi_pmo["group"].isin(groups)]
        valores_earmi = (
            earmi_pmo.set_index("group").loc[groups, col_earmi_pmo].to_numpy()
        )
        valores_iniciais = df_inicial["valor"].to_numpy()
        valores_iniciais[n_series:] = valores_iniciais[:-n_series]
        valores_iniciais[indices_primeiros_estagios] = np.repeat(
            valores_earmi, n_series
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
            temporal_resolution=synthesis.temporal_resolution,
        )
        cache_varm = cls.CACHED_SYNTHESIS.get(sintese_final)
        df_final = (
            cache_varm
            if cache_varm is not None
            else cls.__resolve_UHE(sintese_final, uow)
        )
        with uow:
            arq_pmo = uow.files.get_pmo()
            if arq_pmo is not None:
                varmi_pmo = cls._validate_data(
                    arq_pmo.volume_armazenado_inicial, pd.DataFrame, "VARMI"
                )
            else:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro na leitura do VARM inicial do pmo.dat"
                    )
                raise RuntimeError()
        col_varmi_pmo = (
            "valor_hm3"
            if synthesis.variable
            == Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL
            else "valor_percentual"
        )
        df_inicial = df_final.copy()
        uhes = df_inicial["usina"].unique().tolist()
        n_uhes = len(uhes)
        series = df_inicial["serie"].unique().tolist()
        n_series = len(series)
        estagios = df_inicial["estagio"].unique().tolist()
        n_estagios = len(estagios)
        # Faz uma atribuição posicional. A maneira mais pythonica é lenta.
        offset_meses = cls._offset_meses_inicio(df_inicial, uow)
        offsets_uhes = [i * n_series * n_estagios for i in range(n_uhes)]
        indices_primeiros_estagios = offset_meses * n_series + np.tile(
            np.arange(n_series), n_uhes
        )
        indices_primeiros_estagios += np.repeat(offsets_uhes, n_series)
        varmi_pmo = varmi_pmo.loc[varmi_pmo["nome_usina"].isin(uhes)]
        valores_varmi = (
            varmi_pmo.set_index("nome_usina")
            .loc[uhes, col_varmi_pmo]
            .to_numpy()
        )
        valores_iniciais = df_inicial["valor"].to_numpy()
        valores_iniciais[n_series:] = valores_iniciais[:-n_series]
        valores_iniciais[indices_primeiros_estagios] = np.repeat(
            valores_varmi, n_series
        )
        df_inicial["valor"] = valores_iniciais
        df_inicial["valor"] = df_inicial["valor"].fillna(0.0)
        return df_inicial

    @classmethod
    def __stub_energia_defluencia_minima(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sintese_meta = OperationSynthesis(
            Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            synthesis.spatial_resolution,
            synthesis.temporal_resolution,
        )
        sintese_violacao = OperationSynthesis(
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            synthesis.spatial_resolution,
            synthesis.temporal_resolution,
        )
        cache_meta = cls.CACHED_SYNTHESIS.get(sintese_meta)
        cache_violacao = cls.CACHED_SYNTHESIS.get(sintese_violacao)

        df_meta = (
            cache_meta
            if cache_meta is not None
            else cls._resolve_spatial_resolution(
                sintese_meta,
                uow,
            )
        )
        df_violacao = (
            cache_violacao
            if cache_violacao is not None
            else cls._resolve_spatial_resolution(
                sintese_violacao,
                uow,
            )
        )

        df_meta.loc[:, "valor"] = (
            df_meta["valor"].to_numpy() - df_violacao["valor"].to_numpy()
        )
        return df_meta

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
        synt_pat = OperationSynthesis(
            synthesis.variable,
            spatial_resolution=synthesis.spatial_resolution,
            temporal_resolution=TemporalResolution.PATAMAR,
        )
        cache = cls.CACHED_SYNTHESIS.get(synt_pat)
        df_completo = (
            cache
            if cache is not None
            else cls.__resolve_UHE_normal(synt_pat, uow)
        )
        df_completo.loc[:, "valor"] *= FATOR_HM3_M3S
        if not df_completo.empty:
            cls.CACHED_SYNTHESIS[synt_pat] = df_completo

        if synthesis.temporal_resolution == TemporalResolution.ESTAGIO:
            df_completo = cls.__postprocess_violacoes_UHE_estagio(df_completo)

        cls.CACHED_SYNTHESIS[synthesis] = df_completo
        return df_completo

    @classmethod
    def __resolve_UHE_normal(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
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
        dfs_validos = [d for d in dfs.values() if d is not None]
        if len(dfs_validos) > 0:
            df_completo = pd.concat(dfs_validos, ignore_index=True)

        if not df_completo.empty:
            df_completo = df_completo.loc[df_completo["dataInicio"] < fim, :]
        return df_completo

    @classmethod
    def __resolve_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        if (
            synthesis.variable
            in [
                Variable.GERACAO_HIDRAULICA,
                Variable.VOLUME_TURBINADO,
                Variable.VOLUME_VERTIDO,
                Variable.VOLUME_DESVIADO,
            ]
        ) and (synthesis.temporal_resolution == TemporalResolution.ESTAGIO):
            return cls.__stub_agrega_estagio_variaveis_por_patamar(
                synthesis, uow
            )
        elif synthesis.variable in [
            Variable.VAZAO_TURBINADA,
            Variable.VAZAO_VERTIDA,
            Variable.VAZAO_RETIRADA,
            Variable.VAZAO_DESVIADA,
        ]:
            return cls.__stub_converte_volume_em_vazao(synthesis, uow)
        elif synthesis.variable in [
            Variable.VOLUME_AFLUENTE,
            Variable.VOLUME_INCREMENTAL,
        ]:
            return cls.__stub_converte_vazao_em_volume(synthesis, uow)
        elif synthesis.variable == Variable.VAZAO_DEFLUENTE:
            return cls.__stub_QDEF(synthesis, uow)
        elif synthesis.variable == Variable.VOLUME_DEFLUENTE:
            return cls.__stub_VDEF(synthesis, uow)
        elif synthesis.variable in [
            Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
            Variable.VIOLACAO_DEFLUENCIA_MINIMA,
            Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
            Variable.VIOLACAO_TURBINAMENTO_MINIMO,
            Variable.VIOLACAO_FPHA,
        ]:
            return cls.__stub_violacoes_UHE(synthesis, uow)
        else:
            return cls.__resolve_UHE_normal(synthesis, uow)

    @classmethod
    def __resolve_UTE_normal(
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
    def __stub_GTER_UTE_patamar(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sistema = cls._validate_data(
            cls._get_sistema(uow).custo_deficit,
            pd.DataFrame,
            "submercados",
        )
        sistemas_reais = sistema.loc[sistema["ficticio"] == 0, :]
        sbms_idx = sistemas_reais["codigo_submercado"].unique()
        sbms_name = [
            sistemas_reais.loc[
                sistemas_reais["codigo_submercado"] == s, "nome_submercado"
            ].iloc[0]
            for s in sbms_idx
        ]
        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        synthesis = OperationSynthesis(
            variable=synthesis.variable,
            spatial_resolution=synthesis.spatial_resolution,
            temporal_resolution=TemporalResolution.PATAMAR,
        )
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                idx: pool.apply_async(
                    cls._resolve_gtert, (uow, synthesis, idx, name)
                )
                for idx, name in zip(sbms_idx, sbms_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        dfs_validos = [d for d in dfs.values() if d is not None]
        if len(dfs_validos) > 0:
            df_completo = pd.concat(dfs_validos, ignore_index=True)

        clast = cls._validate_data(
            cls._get_clast(uow).usinas,
            pd.DataFrame,
            "usinas termelétricas",
        )[["codigo_usina", "nome_usina"]].drop_duplicates()
        usinas_arquivo = df_completo["classe"].unique().tolist()
        nomes_usinas_arquivo = [
            clast.loc[clast["codigo_usina"] == c, "nome_usina"].iloc[0]
            for c in usinas_arquivo
        ]
        linhas_por_usina = df_completo.loc[
            df_completo["classe"] == usinas_arquivo[0]
        ].shape[0]
        df_completo["classe"] = np.repeat(
            nomes_usinas_arquivo, linhas_por_usina
        )
        return df_completo.rename(columns={"classe": "usina"})[
            [
                "usina",
                "estagio",
                "dataInicio",
                "dataFim",
                "patamar",
                "serie",
                "valor",
            ]
        ]

    @classmethod
    def __resolve_UTE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        if (
            synthesis.variable
            in [
                Variable.GERACAO_TERMICA,
            ]
        ) and (synthesis.temporal_resolution == TemporalResolution.PATAMAR):
            return cls.__stub_GTER_UTE_patamar(synthesis, uow)
        elif (
            synthesis.variable
            in [
                Variable.GERACAO_TERMICA,
            ]
        ) and (synthesis.temporal_resolution == TemporalResolution.ESTAGIO):
            return cls.__stub_agrega_estagio_variaveis_por_patamar(
                synthesis, uow
            )
        else:
            return cls.__resolve_UTE_normal(synthesis, uow)

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
    def _offset_meses_inicio(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> int:
        dger = cls._get_dger(uow)
        ano_inicio = cls._validate_data(dger.ano_inicio_estudo, int, "dger")
        mes_inicio = cls._validate_data(dger.mes_inicio_estudo, int, "dger")
        starting_date = datetime(ano_inicio, mes_inicio, 1)
        data_starting_date = df["dataInicio"].min().to_pydatetime()
        month_difference = int(
            (starting_date - data_starting_date) / timedelta(days=30)
        )
        return month_difference

    @classmethod
    def _resolve_starting_stage(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ):
        month_difference = cls._offset_meses_inicio(df, uow)
        starting_df = df.copy()
        starting_df.loc[:, "estagio"] -= month_difference
        # Considera somente estágios do período de estudo em diante
        starting_df = starting_df.loc[starting_df["estagio"] > 0]
        starting_df = starting_df.rename(columns={"serie": "cenario"})
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
        df_q = (
            df.groupby(cols_agrupamento)
            .quantile(quantiles, numeric_only=True)
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
        return pd.concat([df, df_q], ignore_index=True)

    @classmethod
    def _postprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = cls._processa_quantis(df, [0.05 * i for i in range(21)])
        df = cls._processa_media(df)
        df = df.astype({"cenario": str})
        return df

    @classmethod
    def _earmi_percentual(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            arq_pmo = uow.files.get_pmo()
            if arq_pmo is not None:
                earmi_pmo = cls._validate_data(
                    arq_pmo.energia_armazenada_inicial, pd.DataFrame, "EARMI"
                )
            else:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro na leitura do EARM inicial do pmo.dat"
                    )
                raise RuntimeError()
        if s.spatial_resolution == SpatialResolution.RESERVATORIO_EQUIVALENTE:
            return earmi_pmo.rename(columns={"nome_ree": "group"})
        earmi_pmo["earmax"] = (
            100 * earmi_pmo["valor_MWmes"] / earmi_pmo["valor_percentual"]
        )
        if s.spatial_resolution == SpatialResolution.SUBMERCADO:
            sistema = cls._validate_data(
                cls._get_sistema(uow).custo_deficit,
                pd.DataFrame,
                "submercados",
            )
            rees = cls._validate_data(
                cls._get_ree(uow).rees, pd.DataFrame, "REEs"
            )
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
            df = cls.__stub_resolve_energias_iniciais_ree(s, uow)
            return df, True
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
            df = cls.__stub_resolve_volumes_iniciais_uhe(s, uow)
            return df, True
        elif all(
            [
                s.variable
                in [
                    Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                    Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                    Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
                    Variable.VIOLACAO_DEFLUENCIA_MINIMA,
                    Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
                    Variable.VIOLACAO_TURBINAMENTO_MINIMO,
                    Variable.VIOLACAO_FPHA,
                    Variable.VOLUME_AFLUENTE,
                    Variable.VOLUME_INCREMENTAL,
                    Variable.VOLUME_DEFLUENTE,
                    Variable.VOLUME_VERTIDO,
                    Variable.VOLUME_TURBINADO,
                    Variable.VOLUME_RETIRADO,
                    Variable.VOLUME_DESVIADO,
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
            df = cls.__stub_agrega_variaveis_indiv_REE_SBM_SIN(s, uow)
            return df, True
        elif s.variable in [Variable.ENERGIA_DEFLUENCIA_MINIMA]:
            df = cls.__stub_energia_defluencia_minima(s, uow)
            return df, True
        else:
            return pd.DataFrame(), False

    @classmethod
    def __get_from_cache_if_exists(cls, s: OperationSynthesis) -> pd.DataFrame:
        if s in cls.CACHED_SYNTHESIS.keys():
            return cls.CACHED_SYNTHESIS.get(s)
        else:
            return pd.DataFrame()

    @classmethod
    def __store_in_cache_if_needed(
        cls, s: OperationSynthesis, df: pd.DataFrame
    ):
        if s in cls.SYNTHESIS_TO_CACHE:
            cls.CACHED_SYNTHESIS[s] = df.copy()

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
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
            try:
                filename = str(s)
                found_synthesis = False
                cls.logger.info(f"Realizando síntese de {filename}")
                df = cls.__get_from_cache_if_exists(s)
                if df.empty:
                    df, is_stub = cls._resolve_stub(s, uow)
                    if not is_stub:
                        df = cls._resolve_spatial_resolution(s, uow)
                        cls.__store_in_cache_if_needed(s, df)
                if df is not None:
                    if not df.empty:
                        found_synthesis = True
                        df = cls._resolve_starting_stage(df, uow)
                        df = cls._postprocess(df)
                        with uow:
                            uow.export.synthetize_df(df, filename)
                if not found_synthesis:
                    cls.logger.warning(
                        "Não foram encontrados dados"
                        + f" para a síntese de {str(s)}"
                    )
            except Exception as e:
                # traceback.print_exc()
                cls.logger.error(str(e))
                cls.logger.error(
                    f"Não foi possível realizar a síntese de: {str(s)}"
                )
