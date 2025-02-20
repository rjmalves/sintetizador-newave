from datetime import datetime, timedelta
from functools import partial
from logging import ERROR, INFO, Logger
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar, Union

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
from cfinterface.components.register import Register
from dateutil.relativedelta import relativedelta  # type: ignore
from inewave.newave import (
    Clast,
    Confhd,
    Conft,
    Curva,
    Dger,
    Dsvagua,
    Enavazb,
    Enavazf,
    Energiab,
    Energiaf,
    Energias,
    Engnat,
    Expt,
    Hidr,
    Manutt,
    Modif,
    Newavetim,
    Patamar,
    Pmo,
    Ree,
    Shist,
    Sistema,
    Term,
    Vazaob,
    Vazaof,
    # Enavazs,
    Vazaos,
    Vazoes,
)
from inewave.newave.modelos.modif import (
    CFUGA,
    CMONT,
    NUMCNJ,
    NUMMAQ,
    TURBMAXT,
    TURBMINT,
    VAZMAXT,
    VAZMIN,
    VAZMINT,
    VMAXT,
    VMINT,
    VOLMAX,
    VOLMIN,
)
from inewave.nwlistcf import Estados, Nwlistcfrel

from app.internal.constants import (
    BLOCK_COL,
    COEF_TYPE_COL,
    COEF_VALUE_COL,
    CONFIG_COL,
    CUT_INDEX_COL,
    EARM_COEF_CODE,
    EER_CODE_COL,
    EER_NAME_COL,
    ENA_COEF_CODE,
    ENTITY_INDEX_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    FOLLOWING_HYDRO_COL,
    GTER_COEF_CODE,
    HEIGHT_POLY_COLS,
    HM3_M3S_MONTHLY_FACTOR,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    ITERATION_COL,
    LAG_COL,
    LOSS_COL,
    LOSS_KIND_COL,
    LOWER_BOUND_COL,
    LOWER_BOUND_UNIT_COL,
    LOWER_DROP_COL,
    MAX_THERMAL_DISPATCH_LAG,
    MAXVIOL_COEF_CODE,
    NET_DROP_COL,
    PRODUCTIVITY_TMP_COL,
    QINC_COEF_CODE,
    RHS_COEF_CODE,
    RUN_OF_RIVER_REFERENCE_VOLUME_COL,
    SCENARIO_COL,
    SPEC_PRODUCTIVITY_COL,
    STAGE_COL,
    START_DATE_COL,
    STATE_VALUE_COL,
    STRING_DF_TYPE,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    THERMAL_CODE_COL,
    THERMAL_NAME_COL,
    UPPER_BOUND_COL,
    UPPER_BOUND_UNIT_COL,
    UPPER_DROP_COL,
    VALUE_COL,
    VARM_COEF_CODE,
    VOLUME_FOR_PRODUCTIVITY_TMP_COL,
    VOLUME_REGULATION_COL,
)
from app.model.operation.unit import Unit
from app.model.policy.unit import Unit as PolicyUnit
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.graph import Graph


class Deck:
    """
    Armazena as informações dos principais arquivos que
    são utilizados para o processo de síntese.
    """

    T = TypeVar("T")
    logger: Optional[Logger] = None

    DECK_DATA_CACHING: Dict[str, Any] = {}

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _get_dger(cls, uow: AbstractUnitOfWork) -> Dger:
        with uow:
            dger = uow.files.get_dger()
            if dger is None:
                msg = "Erro no processamento do dger.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return dger

    @classmethod
    def _get_shist(cls, uow: AbstractUnitOfWork) -> Shist:
        with uow:
            shist = uow.files.get_shist()
            if shist is None:
                msg = "Erro no processamento do shist.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return shist

    @classmethod
    def _get_curva(cls, uow: AbstractUnitOfWork) -> Curva:
        with uow:
            curva = uow.files.get_curva()
            if curva is None:
                msg = "Erro no processamento do curva.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return curva

    @classmethod
    def _get_ree(cls, uow: AbstractUnitOfWork) -> Ree:
        with uow:
            ree = uow.files.get_ree()
            if ree is None:
                msg = "Erro no processamento do ree.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return ree

    @classmethod
    def _get_confhd(cls, uow: AbstractUnitOfWork) -> Confhd:
        with uow:
            confhd = uow.files.get_confhd()
            if confhd is None:
                msg = "Erro no processamento do confhd.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return confhd

    @classmethod
    def _get_dsvagua(cls, uow: AbstractUnitOfWork) -> Dsvagua:
        with uow:
            dsvagua = uow.files.get_dsvagua()
            if dsvagua is None:
                msg = "Erro no processamento do dsvagua.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return dsvagua

    @classmethod
    def _get_modif(cls, uow: AbstractUnitOfWork) -> Modif:
        with uow:
            modif = uow.files.get_modif()
            if modif is None:
                msg = "Erro no processamento do modif.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return modif

    @classmethod
    def _get_hidr(cls, uow: AbstractUnitOfWork) -> Hidr:
        with uow:
            hidr = uow.files.get_hidr()
            if hidr is None:
                msg = "Erro no processamento do hidr.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return hidr

    @classmethod
    def _get_vazoes(cls, uow: AbstractUnitOfWork) -> Vazoes:
        with uow:
            vazoes = uow.files.get_vazoes()
            if vazoes is None:
                msg = "Erro no processamento do vazoes.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return vazoes

    @classmethod
    def _get_conft(cls, uow: AbstractUnitOfWork) -> Conft:
        with uow:
            conft = uow.files.get_conft()
            if conft is None:
                msg = "Erro no processamento do conft.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return conft

    @classmethod
    def _get_sistema(cls, uow: AbstractUnitOfWork) -> Sistema:
        with uow:
            sist = uow.files.get_sistema()
            if sist is None:
                msg = "Erro no processamento do sistema.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return sist

    @classmethod
    def _get_clast(cls, uow: AbstractUnitOfWork) -> Clast:
        with uow:
            clast = uow.files.get_clast()
            if clast is None:
                msg = "Erro no processamento do clast.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return clast

    @classmethod
    def _get_term(cls, uow: AbstractUnitOfWork) -> Term:
        with uow:
            term = uow.files.get_term()
            if term is None:
                msg = "Erro no processamento do term.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return term

    @classmethod
    def _get_manutt(cls, uow: AbstractUnitOfWork) -> Manutt:
        with uow:
            manutt = uow.files.get_manutt()
            if manutt is None:
                msg = "Erro no processamento do manutt.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return manutt

    @classmethod
    def _get_expt(cls, uow: AbstractUnitOfWork) -> Expt:
        with uow:
            expt = uow.files.get_expt()
            if expt is None:
                msg = "Erro no processamento do expt.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return expt

    @classmethod
    def _get_patamar(cls, uow: AbstractUnitOfWork) -> Patamar:
        with uow:
            pat = uow.files.get_patamar()
            if pat is None:
                msg = "Erro no processamento do patamar.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return pat

    @classmethod
    def _get_pmo(cls, uow: AbstractUnitOfWork) -> Pmo:
        with uow:
            pmo = uow.files.get_pmo()
            if pmo is None:
                msg = "Erro no processamento do pmo.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return pmo

    @classmethod
    def _get_newavetim(cls, uow: AbstractUnitOfWork) -> Newavetim:
        with uow:
            newavetim = uow.files.get_newavetim()
            if newavetim is None:
                msg = "Erro no processamento do newave.tim para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return newavetim

    @classmethod
    def _get_engnat(cls, uow: AbstractUnitOfWork) -> Engnat:
        with uow:
            engnat = uow.files.get_engnat()
            if engnat is None:
                msg = "Erro no processamento do engnat.dat para sintese"
                cls._log(
                    msg,
                    ERROR,
                )
                raise RuntimeError(msg)
            return engnat

    @classmethod
    def _get_energiaf(
        cls, iteracao: int, uow: AbstractUnitOfWork
    ) -> Optional[Energiaf]:
        with uow:
            energiaf = uow.files.get_energiaf(iteracao)
            return energiaf

    @classmethod
    def _get_enavazf(
        cls, iteracao: int, uow: AbstractUnitOfWork
    ) -> Optional[Enavazf]:
        with uow:
            enavazf = uow.files.get_enavazf(iteracao)
            return enavazf

    @classmethod
    def _get_vazaof(
        cls, iteracao: int, uow: AbstractUnitOfWork
    ) -> Optional[Vazaof]:
        with uow:
            vazaof = uow.files.get_vazaof(iteracao)
            return vazaof

    @classmethod
    def _get_energiab(
        cls, iteracao: int, uow: AbstractUnitOfWork
    ) -> Optional[Energiab]:
        with uow:
            energiab = uow.files.get_energiab(iteracao)
            return energiab

    @classmethod
    def _get_enavazb(
        cls, iteracao: int, uow: AbstractUnitOfWork
    ) -> Optional[Enavazb]:
        with uow:
            enavazb = uow.files.get_enavazb(iteracao)
            return enavazb

    @classmethod
    def _get_vazaob(
        cls, iteracao: int, uow: AbstractUnitOfWork
    ) -> Optional[Vazaob]:
        with uow:
            vazaob = uow.files.get_vazaob(iteracao)
            return vazaob

    @classmethod
    def _get_energias(cls, uow: AbstractUnitOfWork) -> Optional[Energias]:
        with uow:
            energias = uow.files.get_energias()
            return energias

    @classmethod
    def _get_enavazs(cls, uow: AbstractUnitOfWork) -> Optional[Energias]:
        with uow:
            enavazs = uow.files.get_enavazs()
            return enavazs

    @classmethod
    def _get_vazaos(cls, uow: AbstractUnitOfWork) -> Optional[Vazaos]:
        with uow:
            vazaos = uow.files.get_vazaos()
            return vazaos

    @classmethod
    def _get_cortes(cls, uow: AbstractUnitOfWork) -> Optional[Nwlistcfrel]:
        with uow:
            cortes = uow.files.get_nwlistcf_cortes()
            return cortes

    @classmethod
    def _get_estados(cls, uow: AbstractUnitOfWork) -> Optional[Estados]:
        with uow:
            estados = uow.files.get_nwlistcf_estados()
            return estados

    @classmethod
    def _validate_data(cls, data, type: Type[T], msg: str = "dados") -> T:
        if not isinstance(data, type):
            msg = f"Erro de validação: {msg}"
            cls._log(msg, ERROR)
            raise AssertionError(msg, ERROR)
        return data

    @classmethod
    def dger(cls, uow: AbstractUnitOfWork) -> Dger:
        dger = cls.DECK_DATA_CACHING.get("dger")
        if dger is None:
            dger = cls._validate_data(
                cls._get_dger(uow),
                Dger,
                "processamento do dger.dat",
            )
            cls.DECK_DATA_CACHING["dger"] = dger
        return dger

    @classmethod
    def pmo(cls, uow: AbstractUnitOfWork) -> Pmo:
        pmo = cls.DECK_DATA_CACHING.get("pmo")
        if pmo is None:
            pmo = cls._validate_data(
                cls._get_pmo(uow),
                Pmo,
                "processamento do pmo.dat",
            )
            cls.DECK_DATA_CACHING["pmo"] = pmo
        return pmo

    @classmethod
    def curva(cls, uow: AbstractUnitOfWork) -> Curva:
        curva = cls.DECK_DATA_CACHING.get("curva")
        if curva is None:
            curva = cls._validate_data(
                cls._get_curva(uow),
                Curva,
                "processamento do curva.dat",
            )
            cls.DECK_DATA_CACHING["curva"] = curva
        return curva

    @classmethod
    def modif(cls, uow: AbstractUnitOfWork) -> Modif:
        modif = cls.DECK_DATA_CACHING.get("modif")
        if modif is None:
            modif = cls._validate_data(
                cls._get_modif(uow),
                Modif,
                "processamento do modif.dat",
            )
            cls.DECK_DATA_CACHING["modif"] = modif
        return modif

    @classmethod
    def confhd(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        confhd = cls.DECK_DATA_CACHING.get("confhd")
        if confhd is None:
            confhd = cls._validate_data(
                cls._get_confhd(uow).usinas,
                pd.DataFrame,
                "processamento do confhd.dat",
            )
            cls.DECK_DATA_CACHING["confhd"] = confhd
        return confhd.copy()

    @classmethod
    def clast(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        clast = cls.DECK_DATA_CACHING.get("clast")
        if clast is None:
            clast = cls._validate_data(
                cls._get_clast(uow).usinas,
                pd.DataFrame,
                "processamento do clast.dat",
            )
            cls.DECK_DATA_CACHING["clast"] = clast
        return clast.copy()

    @classmethod
    def term(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        term = cls.DECK_DATA_CACHING.get("term")
        if term is None:
            term = cls._validate_data(
                cls._get_term(uow).usinas,
                pd.DataFrame,
                "processamento do term.dat",
            )
            cls.DECK_DATA_CACHING["term"] = term
        return term.copy()

    @classmethod
    def manutt(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        manutt = cls.DECK_DATA_CACHING.get("manutt")
        if manutt is None:
            df_manutt = cls._get_manutt(uow).manutencoes
            if df_manutt is None:
                df_manutt = pd.DataFrame(
                    columns=[
                        "codigo_empresa",
                        "nome_empresa",
                        "codigo_usina",
                        "nome_usina",
                        "codigo_unidade",
                        "data_inicio",
                        "duracao",
                        "potencia",
                    ]
                )
            manutt = cls._validate_data(
                df_manutt,
                pd.DataFrame,
                "processamento do manutt.dat",
            )
            cls.DECK_DATA_CACHING["manutt"] = manutt
        return manutt.copy()

    @classmethod
    def expt(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        expt = cls.DECK_DATA_CACHING.get("expt")
        if expt is None:
            expt = cls._validate_data(
                cls._get_expt(uow).expansoes,
                pd.DataFrame,
                "processamento do expt.dat",
            )
            cls.DECK_DATA_CACHING["expt"] = expt
        return expt.copy()

    @classmethod
    def hidr(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        hidr = cls.DECK_DATA_CACHING.get("hidr")
        if hidr is None:
            hidr = cls._validate_data(
                cls._get_hidr(uow).cadastro,
                pd.DataFrame,
                "processamento do hidr.dat",
            )
            cls.DECK_DATA_CACHING["hidr"] = hidr
        return hidr.copy()

    @classmethod
    def newavetim(cls, uow: AbstractUnitOfWork) -> Newavetim:
        newavetim = cls.DECK_DATA_CACHING.get("newavetim")
        if newavetim is None:
            newavetim = cls._validate_data(
                cls._get_newavetim(uow),
                Newavetim,
                "processamento do newave.tim",
            )
            cls.DECK_DATA_CACHING["newavetim"] = newavetim
        return newavetim

    @classmethod
    def engnat(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        engnat = cls.DECK_DATA_CACHING.get("engnat")
        if engnat is None:
            engnat = cls._validate_data(
                cls._get_engnat(uow).series,
                pd.DataFrame,
                "processamento do engnat.dat",
            )
            cls.DECK_DATA_CACHING["engnat"] = engnat
        return engnat

    @classmethod
    def energiaf(cls, iteracao: int, uow: AbstractUnitOfWork) -> pd.DataFrame:
        arq_energiaf = cls._get_energiaf(iteracao, uow)
        if arq_energiaf is not None:
            df = arq_energiaf.series
            if df is None:
                return pd.DataFrame()
            else:
                return df.rename(
                    columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
                )
        else:
            return pd.DataFrame()

    @classmethod
    def enavazf(cls, iteracao: int, uow: AbstractUnitOfWork) -> pd.DataFrame:
        arq_enavaz = cls._get_enavazf(iteracao, uow)
        if arq_enavaz is not None:
            df = arq_enavaz.series
            if df is None:
                return pd.DataFrame()
            else:
                return df.rename(
                    columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
                )
        else:
            return pd.DataFrame()

    @classmethod
    def vazaof(cls, iteracao: int, uow: AbstractUnitOfWork) -> pd.DataFrame:
        arq_vazaof = cls._get_vazaof(iteracao, uow)
        if arq_vazaof is not None:
            df = arq_vazaof.series
            if df is None:
                return pd.DataFrame()
            else:
                return df.rename(
                    columns={"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL}
                )
        else:
            return pd.DataFrame()

    @classmethod
    def energiab(cls, iteracao: int, uow: AbstractUnitOfWork) -> pd.DataFrame:
        arq_energiab = cls._get_energiab(iteracao, uow)
        if arq_energiab is not None:
            df = arq_energiab.series
            if df is None:
                return pd.DataFrame()
            else:
                return df.rename(
                    columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
                )
        else:
            return pd.DataFrame()

    @classmethod
    def enavazb(cls, iteracao: int, uow: AbstractUnitOfWork) -> pd.DataFrame:
        arq_enavaz = cls._get_enavazb(iteracao, uow)
        if arq_enavaz is not None:
            df = arq_enavaz.series
            if df is None:
                return pd.DataFrame()
            else:
                return df.rename(
                    columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
                )
        else:
            return pd.DataFrame()

    @classmethod
    def vazaob(cls, iteracao: int, uow: AbstractUnitOfWork) -> pd.DataFrame:
        arq_vazaob = cls._get_vazaob(iteracao, uow)
        if arq_vazaob is not None:
            df = arq_vazaob.series
            if df is None:
                return pd.DataFrame()
            else:
                return df.rename(
                    columns={"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL}
                )
        else:
            return pd.DataFrame()

    @classmethod
    def energias(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        arq_energias = cls._get_energias(uow)
        if arq_energias is not None:
            df = arq_energias.series
            if df is None:
                return pd.DataFrame()
            else:
                return df.rename(
                    columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
                )
        else:
            return pd.DataFrame()

    @classmethod
    def enavazs(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        arq_enavaz = cls._get_enavazs(uow)
        if arq_enavaz is not None:
            df = arq_enavaz.series
            if df is None:
                return pd.DataFrame()
            else:
                return df.rename(
                    columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
                )
        else:
            return pd.DataFrame()

    @classmethod
    def vazaos(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        arq_vazaos = cls._get_vazaos(uow)
        if arq_vazaos is not None:
            df = arq_vazaos.series
            if df is None:
                return pd.DataFrame()
            else:
                return df.rename(
                    columns={"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL}
                )
        else:
            return pd.DataFrame()

    @classmethod
    def vazoes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        vazoes = cls.DECK_DATA_CACHING.get("vazoes")
        if vazoes is None:
            vazoes = cls._validate_data(
                cls._get_vazoes(uow).vazoes,
                pd.DataFrame,
                "processamento do vazoes.dat",
            )
            cls.DECK_DATA_CACHING["vazoes"] = vazoes
        return vazoes.copy()

    @classmethod
    def pre_study_period_starting_month(cls, uow: AbstractUnitOfWork) -> int:
        pre_study_period_starting_month = cls.DECK_DATA_CACHING.get(
            "pre_study_period_starting_month"
        )
        if pre_study_period_starting_month is None:
            dger = cls.dger(uow)
            pre_study_period_starting_month = cls._validate_data(
                dger.mes_inicio_pre_estudo,
                int,
                "mes de inicio do pre-estudo (dger.dat)",
            )
            cls.DECK_DATA_CACHING["pre_study_period_starting_month"] = (
                pre_study_period_starting_month
            )
        return pre_study_period_starting_month

    @classmethod
    def study_period_starting_month(cls, uow: AbstractUnitOfWork) -> int:
        study_period_starting_month = cls.DECK_DATA_CACHING.get(
            "study_period_starting_month"
        )
        if study_period_starting_month is None:
            dger = cls.dger(uow)
            study_period_starting_month = cls._validate_data(
                dger.mes_inicio_estudo,
                int,
                "mes de inicio do estudo (dger.dat)",
            )
            cls.DECK_DATA_CACHING["study_period_starting_month"] = (
                study_period_starting_month
            )
        return study_period_starting_month

    @classmethod
    def study_period_starting_year(cls, uow: AbstractUnitOfWork) -> int:
        study_period_starting_year = cls.DECK_DATA_CACHING.get(
            "study_period_starting_year"
        )
        if study_period_starting_year is None:
            dger = cls.dger(uow)
            study_period_starting_year = cls._validate_data(
                dger.ano_inicio_estudo,
                int,
                "ano de inicio do estudo (dger.dat)",
            )
            cls.DECK_DATA_CACHING["study_period_starting_year"] = (
                study_period_starting_year
            )
        return study_period_starting_year

    @classmethod
    def num_pre_study_period_years(cls, uow: AbstractUnitOfWork) -> int:
        num_pre_study_period_years = cls.DECK_DATA_CACHING.get(
            "num_pre_study_period_years"
        )
        if num_pre_study_period_years is None:
            dger = cls.dger(uow)
            num_pre_study_period_years = cls._validate_data(
                dger.num_anos_pre_estudo,
                int,
                "numero de anos do pre-estudo (dger.dat)",
            )
            cls.DECK_DATA_CACHING["num_pre_study_period_years"] = (
                num_pre_study_period_years
            )
        return num_pre_study_period_years

    @classmethod
    def num_study_period_years(cls, uow: AbstractUnitOfWork) -> int:
        num_study_period_years = cls.DECK_DATA_CACHING.get(
            "num_study_period_years"
        )
        if num_study_period_years is None:
            dger = cls.dger(uow)
            num_study_period_years = cls._validate_data(
                dger.num_anos_estudo,
                int,
                "numero de anos do estudo (dger.dat)",
            )
            cls.DECK_DATA_CACHING["num_study_period_years"] = (
                num_study_period_years
            )
        return num_study_period_years

    @classmethod
    def num_post_study_period_years_final_simulation(
        cls, uow: AbstractUnitOfWork
    ) -> int:
        num_post_study_period_years_final_simulation = (
            cls.DECK_DATA_CACHING.get(
                "num_post_study_period_years_final_simulation"
            )
        )
        if num_post_study_period_years_final_simulation is None:
            dger = cls.dger(uow)
            num_post_study_period_years_final_simulation = cls._validate_data(
                dger.num_anos_pos_sim_final,
                int,
                "numero de anos do pos-estudo na simulacao final (dger.dat)",
            )
            cls.DECK_DATA_CACHING[
                "num_post_study_period_years_final_simulation"
            ] = num_post_study_period_years_final_simulation
        return num_post_study_period_years_final_simulation

    @classmethod
    def num_synthetic_scenarios_final_simulation(
        cls, uow: AbstractUnitOfWork
    ) -> int:
        num_synthetic_scenarios_final_simulation = cls.DECK_DATA_CACHING.get(
            "num_synthetic_scenarios_final_simulation"
        )
        if num_synthetic_scenarios_final_simulation is None:
            dger = cls.dger(uow)
            num_synthetic_scenarios_final_simulation = cls._validate_data(
                dger.num_series_sinteticas,
                int,
                "numero de series sinteticas na simulacao final (dger.dat)",
            )
            cls.DECK_DATA_CACHING[
                "num_synthetic_scenarios_final_simulation"
            ] = num_synthetic_scenarios_final_simulation
        return num_synthetic_scenarios_final_simulation

    @classmethod
    def num_history_years(cls, uow: AbstractUnitOfWork) -> int:
        num_history_years = cls.DECK_DATA_CACHING.get("num_history_years")
        if num_history_years is None:
            shist = cls._get_shist(uow)
            span = cls._validate_data(
                shist.varredura,
                int,
                "tipo de varredura (sfhist.dat)",
            )
            if span == 1:
                history_input_starting_year = cls._validate_data(
                    cls.dger(uow).ano_inicial_historico,
                    int,
                    "ano de inicio do historico (dger.dat)",
                )
                num_history_input_years = cls.vazoes(uow).shape[0] // 12
                history_input_end_year = (
                    history_input_starting_year + num_history_input_years - 1
                )
                study_starting_month = cls.study_period_starting_month(uow)
                last_year_offset = 2 if study_starting_month != 1 else 1
                study_starting_year = cls.study_period_starting_year(uow)
                final_simulation_last_year = (
                    min([
                        history_input_end_year,
                        study_starting_year,
                    ])
                    - last_year_offset
                )

                span_starting_year = cls._validate_data(
                    shist.ano_inicio_varredura,
                    int,
                    "ano de inicio da varredura (sfhist.dat)",
                )
                num_history_years = (
                    final_simulation_last_year - span_starting_year + 1
                )
            else:
                num_history_years = len(
                    cls._validate_data(
                        shist.anos_inicio_simulacoes,
                        list,
                        "anos de inicio das simulacoes (sfhist.dat)",
                    )
                )
            cls.DECK_DATA_CACHING["num_history_years"] = num_history_years
        return num_history_years

    @classmethod
    def num_thermal_maintenance_years(cls, uow: AbstractUnitOfWork) -> int:
        num_thermal_maintenance_years = cls.DECK_DATA_CACHING.get(
            "num_thermal_maintenance_years"
        )
        if num_thermal_maintenance_years is None:
            dger = cls.dger(uow)
            num_thermal_maintenance_years = cls._validate_data(
                dger.num_anos_manutencao_utes,
                int,
                "numero de anos com manutencoes de UTEs (dger.dat)",
            )
            cls.DECK_DATA_CACHING["num_thermal_maintenance_years"] = (
                num_thermal_maintenance_years
            )
        return num_thermal_maintenance_years

    @classmethod
    def thermal_maintenance_end_date(cls, uow: AbstractUnitOfWork) -> datetime:
        thermal_maintenance_end_date = cls.DECK_DATA_CACHING.get(
            "thermal_maintenance_end_date"
        )
        if thermal_maintenance_end_date is None:
            starting_year = cls.study_period_starting_year(uow)
            num_years = cls.num_thermal_maintenance_years(uow)
            thermal_maintenance_end_date = datetime(
                starting_year + num_years, 1, 1
            )
            cls.DECK_DATA_CACHING["thermal_maintenance_end_date"] = (
                thermal_maintenance_end_date
            )
        return thermal_maintenance_end_date

    @classmethod
    def final_simulation_type(cls, uow: AbstractUnitOfWork) -> int:
        final_simulation_type = cls.DECK_DATA_CACHING.get(
            "final_simulation_type"
        )
        if final_simulation_type is None:
            dger = cls.dger(uow)
            final_simulation_type = cls._validate_data(
                dger.tipo_simulacao_final,
                int,
                "tipo de simulacao final (dger.dat)",
            )
            cls.DECK_DATA_CACHING["final_simulation_type"] = (
                final_simulation_type
            )
        return final_simulation_type

    @classmethod
    def final_simulation_aggregation(cls, uow: AbstractUnitOfWork) -> int:
        final_simulation_aggregation = cls.DECK_DATA_CACHING.get(
            "final_simulation_aggregation"
        )
        if final_simulation_aggregation is None:
            dger = cls.dger(uow)
            final_simulation_aggregation = cls._validate_data(
                dger.agregacao_simulacao_final,
                int,
                "agregacao da simulacao final (dger.dat)",
            )
            cls.DECK_DATA_CACHING["final_simulation_aggregation"] = (
                final_simulation_aggregation
            )
        return final_simulation_aggregation

    @classmethod
    def num_scenarios_final_simulation(cls, uow: AbstractUnitOfWork) -> int:
        num_scenarios_final_simulation = cls.DECK_DATA_CACHING.get(
            "num_scenarios_final_simulation"
        )
        if num_scenarios_final_simulation is None:
            if cls.final_simulation_type(uow) == 2:
                num_scenarios_final_simulation = cls.num_history_years(uow)
            else:
                num_scenarios_final_simulation = (
                    cls.num_synthetic_scenarios_final_simulation(uow)
                )
            cls.DECK_DATA_CACHING["num_scenarios_final_simulation"] = (
                num_scenarios_final_simulation
            )
        return num_scenarios_final_simulation

    @classmethod
    def num_hydro_simulation_stages_policy(cls, uow: AbstractUnitOfWork) -> int:
        num_hydro_simulation_stages_policy = cls.DECK_DATA_CACHING.get(
            "num_hydro_simulation_stages_policy"
        )
        if num_hydro_simulation_stages_policy is None:
            starting_year = cls.study_period_starting_year(uow)
            starting_moonth = cls.study_period_starting_month(uow)
            eers = cls.eers(uow)
            hydro_sim_ending_month = eers["mes_fim_individualizado"].iloc[0]
            hydro_sim_ending_year = eers["ano_fim_individualizado"].iloc[0]

            if ~np.isnan(hydro_sim_ending_month) and ~np.isnan(
                hydro_sim_ending_year
            ):
                if (
                    hydro_sim_ending_month is not None
                    and hydro_sim_ending_year is not None
                ):
                    study_starting_date = datetime(
                        year=starting_year,
                        month=starting_moonth,
                        day=1,
                    )
                    hydro_sim_ending_date = datetime(
                        year=int(hydro_sim_ending_year),
                        month=int(hydro_sim_ending_month),
                        day=1,
                    )
                    tempo_individualizado = (
                        hydro_sim_ending_date - study_starting_date
                    )
                    num_hydro_simulation_stages_policy = int(
                        round(tempo_individualizado / timedelta(days=30))
                    )
                else:
                    num_hydro_simulation_stages_policy = 0
            else:
                num_hydro_simulation_stages_policy = 0
            cls.DECK_DATA_CACHING["num_hydro_simulation_stages_policy"] = (
                num_hydro_simulation_stages_policy
            )
        return num_hydro_simulation_stages_policy

    @classmethod
    def num_hydro_simulation_stages_final_simulation(
        cls, uow: AbstractUnitOfWork
    ) -> int:
        num_hydro_simulation_stages_final_simulation = (
            cls.DECK_DATA_CACHING.get(
                "num_hydro_simulation_stages_final_simulation"
            )
        )
        if num_hydro_simulation_stages_final_simulation is None:
            aggergation = cls.final_simulation_aggregation(uow)
            starting_month = cls.study_period_starting_month(uow)
            study_years = cls.num_study_period_years(uow)
            post_study_years = cls.num_post_study_period_years_final_simulation(
                uow
            )
            if aggergation == 1:
                num_hydro_simulation_stages_final_simulation = (
                    study_years + post_study_years
                ) * 12 - (starting_month - 1)
            else:
                num_hydro_simulation_stages_final_simulation = (
                    cls.num_hydro_simulation_stages_policy(uow)
                )

            cls.DECK_DATA_CACHING[
                "num_hydro_simulation_stages_final_simulation"
            ] = num_hydro_simulation_stages_final_simulation
        return num_hydro_simulation_stages_final_simulation

    @classmethod
    def models_wind_generation(cls, uow: AbstractUnitOfWork) -> int:
        models_wind_generation = cls.DECK_DATA_CACHING.get(
            "models_wind_generation"
        )
        if models_wind_generation is None:
            models_wind_generation = cls._validate_data(
                cls.dger(uow).considera_geracao_eolica != 0,
                int,
                "consideracao de incerteza eolica (dger.dat)",
            )
            cls.DECK_DATA_CACHING["models_wind_generation"] = (
                models_wind_generation
            )
        return models_wind_generation

    @classmethod
    def scenario_generation_model_type(cls, uow: AbstractUnitOfWork) -> int:
        scenario_generation_model_type = cls.DECK_DATA_CACHING.get(
            "scenario_generation_model_type"
        )
        if scenario_generation_model_type is None:
            scenario_generation_model_type = cls._validate_data(
                cls.dger(uow).consideracao_media_anual_afluencias,
                int,
                "opcao do modelo PAR(p) (dger.dat)",
            )
            cls.DECK_DATA_CACHING["scenario_generation_model_type"] = (
                scenario_generation_model_type
            )
        return scenario_generation_model_type

    @classmethod
    def scenario_generation_model_max_order(
        cls, uow: AbstractUnitOfWork
    ) -> int:
        scenario_generation_model_max_order = cls.DECK_DATA_CACHING.get(
            "scenario_generation_model_max_order"
        )
        if scenario_generation_model_max_order is None:
            scenario_generation_model_max_order = cls._validate_data(
                cls.dger(uow).ordem_maxima_parp,
                int,
                "ordem maxima do modelo PAR(p) (dger.dat)",
            )
            cls.DECK_DATA_CACHING["scenario_generation_model_max_order"] = (
                scenario_generation_model_max_order
            )
        return scenario_generation_model_max_order

    @classmethod
    def num_stages_with_past_tendency_period(
        cls, uow: AbstractUnitOfWork
    ) -> int:
        scenario_model = cls.scenario_generation_model_type(uow)
        maximum_model_order = cls.scenario_generation_model_max_order(uow)
        past_stages = 12 if scenario_model != 0 else maximum_model_order
        return past_stages

    @classmethod
    def starting_date_with_past_tendency_period(
        cls, uow: AbstractUnitOfWork
    ) -> datetime:
        starting_year = cls.study_period_starting_year(uow)
        past_stages = cls.num_stages_with_past_tendency_period(uow)
        starting_date_with_tendency = datetime(
            year=starting_year, month=1, day=1
        ) - relativedelta(months=past_stages)
        return starting_date_with_tendency

    @classmethod
    def num_forward_series(cls, uow: AbstractUnitOfWork) -> int:
        num_forward_series = cls.DECK_DATA_CACHING.get("num_forward_series")
        if num_forward_series is None:
            dger = cls.dger(uow)
            num_forward_series = cls._validate_data(
                dger.num_forwards,
                int,
                "número de séries forward (dger.dat)",
            )
            cls.DECK_DATA_CACHING["num_forward_series"] = num_forward_series
        return num_forward_series

    @classmethod
    def ending_date_with_post_study_period(
        cls, uow: AbstractUnitOfWork
    ) -> datetime:
        starting_year = cls.study_period_starting_year(uow)
        study_years = cls.num_study_period_years(uow)
        post_study_years_in_simulation = (
            cls.num_post_study_period_years_final_simulation(uow)
        )
        ending_date_with_post_study_years = datetime(
            year=starting_year
            + study_years
            + post_study_years_in_simulation
            - 1,
            month=12,
            day=1,
        )
        return ending_date_with_post_study_years

    @classmethod
    def internal_stages_starting_dates_policy(
        cls, uow: AbstractUnitOfWork
    ) -> List[datetime]:
        internal_stages_starting_dates_policy = cls.DECK_DATA_CACHING.get(
            "internal_stages_starting_dates_policy"
        )
        if internal_stages_starting_dates_policy is None:
            internal_stages_starting_dates_policy = pd.date_range(
                datetime(cls.study_period_starting_year(uow), 1, 1),
                datetime(
                    cls.study_period_starting_year(uow)
                    + cls.num_study_period_years(uow)
                    - 1,
                    12,
                    1,
                ),
                freq="MS",
            ).tolist()
            cls.DECK_DATA_CACHING["internal_stages_starting_dates_policy"] = (
                internal_stages_starting_dates_policy
            )
        return internal_stages_starting_dates_policy

    @classmethod
    def internal_stages_starting_dates_policy_with_past_tendency(
        cls, uow: AbstractUnitOfWork
    ) -> List[datetime]:
        internal_stages_starting_dates_policy_with_past_tendency = (
            cls.DECK_DATA_CACHING.get(
                "internal_stages_starting_dates_policy_with_past_tendency"
            )
        )
        if internal_stages_starting_dates_policy_with_past_tendency is None:
            internal_stages_starting_dates_policy_with_past_tendency = (
                pd.date_range(
                    datetime(cls.study_period_starting_year(uow) - 1, 1, 1),
                    datetime(
                        cls.study_period_starting_year(uow)
                        + cls.num_study_period_years(uow)
                        - 1,
                        12,
                        1,
                    ),
                    freq="MS",
                ).tolist()
            )
            cls.DECK_DATA_CACHING[
                "internal_stages_starting_dates_policy_with_past_tendency"
            ] = internal_stages_starting_dates_policy_with_past_tendency
        return internal_stages_starting_dates_policy_with_past_tendency

    @classmethod
    def stages_starting_dates_final_simulation(
        cls, uow: AbstractUnitOfWork
    ) -> List[datetime]:
        stages_starting_dates_final_simulation = cls.DECK_DATA_CACHING.get(
            "stages_starting_dates_final_simulation"
        )
        if stages_starting_dates_final_simulation is None:
            stages_starting_dates_final_simulation = pd.date_range(
                datetime(
                    cls.study_period_starting_year(uow),
                    cls.study_period_starting_month(uow),
                    1,
                ),
                datetime(
                    cls.study_period_starting_year(uow)
                    + cls.num_study_period_years(uow)
                    + cls.num_post_study_period_years_final_simulation(uow)
                    - 1,
                    12,
                    1,
                ),
                freq="MS",
            ).tolist()
            cls.DECK_DATA_CACHING["stages_starting_dates_final_simulation"] = (
                stages_starting_dates_final_simulation
            )
        return stages_starting_dates_final_simulation

    @classmethod
    def internal_stages_starting_dates_final_simulation(
        cls, uow: AbstractUnitOfWork
    ) -> List[datetime]:
        internal_stages_starting_dates_final_simulation = (
            cls.DECK_DATA_CACHING.get(
                "internal_stages_starting_dates_final_simulation"
            )
        )
        if internal_stages_starting_dates_final_simulation is None:
            internal_stages_starting_dates_final_simulation = pd.date_range(
                datetime(cls.study_period_starting_year(uow), 1, 1),
                datetime(
                    cls.study_period_starting_year(uow)
                    + cls.num_study_period_years(uow)
                    + cls.num_post_study_period_years_final_simulation(uow)
                    - 1,
                    12,
                    1,
                ),
                freq="MS",
            ).tolist()
            cls.DECK_DATA_CACHING[
                "internal_stages_starting_dates_final_simulation"
            ] = internal_stages_starting_dates_final_simulation
        return internal_stages_starting_dates_final_simulation

    @classmethod
    def internal_stages_ending_dates_final_simulation(
        cls, uow: AbstractUnitOfWork
    ) -> List[datetime]:
        internal_stages_ending_dates_final_simulation = (
            cls.DECK_DATA_CACHING.get(
                "internal_stages_ending_dates_final_simulation"
            )
        )
        if internal_stages_ending_dates_final_simulation is None:
            internal_stages_ending_dates_final_simulation = [
                d + relativedelta(months=1)
                for d in cls.internal_stages_starting_dates_final_simulation(
                    uow
                )
            ]
            cls.DECK_DATA_CACHING[
                "internal_stages_ending_dates_final_simulation"
            ] = internal_stages_ending_dates_final_simulation
        return internal_stages_ending_dates_final_simulation

    @classmethod
    def hydro_simulation_stages_ending_date_final_simulation(
        cls, uow: AbstractUnitOfWork
    ) -> datetime:
        hydro_simulation_stages_ending_date_final_simulation = (
            cls.DECK_DATA_CACHING.get(
                "hydro_simulation_stages_ending_date_final_simulation"
            )
        )
        if hydro_simulation_stages_ending_date_final_simulation is None:
            eers = cls.eers(uow)
            starting_year = cls.study_period_starting_year(uow)
            aggregation = cls.final_simulation_aggregation(uow)
            num_study_years = cls.num_study_period_years(uow)
            post_study_years = cls.num_post_study_period_years_final_simulation(
                uow
            )
            if aggregation == 1:
                fim = datetime(
                    year=starting_year + num_study_years + post_study_years,
                    month=1,
                    day=1,
                )
            elif eers["ano_fim_individualizado"].isna().sum() > 0:
                fim = datetime(
                    year=starting_year + num_study_years + post_study_years,
                    month=1,
                    day=1,
                )
            else:
                fim = datetime(
                    year=int(eers["ano_fim_individualizado"].iloc[0]),
                    month=int(eers["mes_fim_individualizado"].iloc[0]),
                    day=1,
                )
            hydro_simulation_stages_ending_date_final_simulation = fim
            cls.DECK_DATA_CACHING[
                "hydro_simulation_stages_ending_date_final_simulation"
            ] = hydro_simulation_stages_ending_date_final_simulation
        return hydro_simulation_stages_ending_date_final_simulation

    @classmethod
    def _configurations_pmo(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        pmo = cls.pmo(uow)
        configurations = pmo.configuracoes_qualquer_modificacao
        if isinstance(configurations, pd.DataFrame):
            configurations = configurations.rename(
                columns={"data": START_DATE_COL}
            )
        return configurations

    @classmethod
    def _configurations_dger(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        dates = cls.stages_starting_dates_final_simulation(uow)
        configurations = list(range(1, len(dates) + 1))
        return pd.DataFrame(
            data={START_DATE_COL: dates, VALUE_COL: configurations}
        )

    @classmethod
    def configurations(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        configurations = cls.DECK_DATA_CACHING.get("configurations")
        if configurations is None:
            configurations = cls._configurations_pmo(uow)
            if configurations is None:
                configurations = cls._configurations_dger(uow)

            cls.DECK_DATA_CACHING["configurations"] = configurations
        return configurations.copy()

    @classmethod
    def eer_stored_energy_lower_bounds(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtem os limites inferiores de armazenamento de energia para
        cada REE, convertendo os valores de percentual para MWmes,
        para o período de estudo.
        """

        def _add_missing_eer_bounds(df: pd.DataFrame) -> pd.DataFrame:
            df = df.loc[
                df[START_DATE_COL]
                >= Deck.stages_starting_dates_final_simulation(uow)[0]
            ]
            eers_minimum_storage = df[EER_CODE_COL].unique().tolist()
            eer_codes = Deck.eer_code_order(uow)
            missing_eers = list(set(eer_codes).difference(eers_minimum_storage))
            lower_bound_dfs = [df]
            for c in missing_eers:
                eer_df = df.loc[
                    df[EER_CODE_COL] == eers_minimum_storage[0]
                ].copy()
                eer_df[EER_CODE_COL] = c
                eer_df[VALUE_COL] = 0.0
                lower_bound_dfs.append(eer_df)
            lower_bound_df = pd.concat(lower_bound_dfs, ignore_index=True)
            lower_bound_df = lower_bound_df.sort_values([
                EER_CODE_COL,
                START_DATE_COL,
            ])
            return lower_bound_df

        def _cast_perc_to_absolute(df: pd.DataFrame) -> pd.DataFrame:
            upper_bound_df = cls.stored_energy_upper_bounds(uow)
            df = df.sort_values([EER_CODE_COL, START_DATE_COL]).reset_index(
                drop=True
            )
            upper_bound_df = upper_bound_df.sort_values([
                EER_CODE_COL,
                START_DATE_COL,
            ]).reset_index(drop=True)
            df[VALUE_COL] = df[VALUE_COL] * upper_bound_df[VALUE_COL] / 100.0
            return df

        def _add_entity_data(df: pd.DataFrame) -> pd.DataFrame:
            entity_map = cls.eer_submarket_map(uow)
            return df.join(entity_map, on=EER_CODE_COL)

        eer_stored_energy_lower_bounds = cls.DECK_DATA_CACHING.get(
            "eer_stored_energy_lower_bounds"
        )
        if eer_stored_energy_lower_bounds is None:
            minimum_perc_storage_df = cls._validate_data(
                cls.curva(uow).curva_seguranca,
                pd.DataFrame,
                "curva de seguranca (curva.dat)",
            )
            minimum_perc_storage_df = minimum_perc_storage_df.rename(
                columns={"data": START_DATE_COL}
            )
            lower_bound_df = _add_missing_eer_bounds(minimum_perc_storage_df)
            lower_bound_df = _cast_perc_to_absolute(lower_bound_df)
            eer_stored_energy_lower_bounds = _add_entity_data(lower_bound_df)
            eer_stored_energy_lower_bounds = cls._consider_post_study_years(
                eer_stored_energy_lower_bounds, uow
            )
            cls.DECK_DATA_CACHING["eer_stored_energy_lower_bounds"] = (
                eer_stored_energy_lower_bounds
            )
        return eer_stored_energy_lower_bounds.copy()

    @classmethod
    def _stored_energy_upper_bounds_inputs(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame | None:
        def _join_drop_data(
            df: pd.DataFrame, stage_date: datetime
        ) -> pd.DataFrame:
            drops_df = cls.hydro_drops_in_stages(uow)
            drops_df = drops_df.loc[
                drops_df[START_DATE_COL] == stage_date
            ].set_index(HYDRO_CODE_COL)
            return df.drop(columns=["usina"]).join(drops_df, how="inner")

        def _join_hydros_data(df: pd.DataFrame) -> pd.DataFrame:
            hydros = cls.hydros(uow)
            return df.join(hydros[[FOLLOWING_HYDRO_COL]], how="inner")

        def _join_bounds_data(df: pd.DataFrame) -> pd.DataFrame:
            bounds_df = cls.hydro_volume_bounds_with_changes(uow)
            return df.join(
                bounds_df[[LOWER_BOUND_COL, UPPER_BOUND_COL]], how="inner"
            )

        def _volume_to_energy(df: pd.DataFrame) -> pd.DataFrame:
            df.loc[df[VOLUME_REGULATION_COL] != "M", ABSOLUTE_VALUE_COL] = 0.0
            df[ABSOLUTE_VALUE_COL] *= df[PRODUCTIVITY_TMP_COL]
            return df

        def _cast_to_eers_and_fill_missing(
            df: pd.DataFrame, configurations_df: pd.DataFrame
        ) -> pd.DataFrame:
            df = (
                df[
                    [
                        START_DATE_COL,
                        CONFIG_COL,
                        EER_CODE_COL,
                        EER_NAME_COL,
                        SUBMARKET_CODE_COL,
                        SUBMARKET_NAME_COL,
                        ABSOLUTE_VALUE_COL,
                    ]
                ]
                .groupby([
                    START_DATE_COL,
                    CONFIG_COL,
                    EER_CODE_COL,
                    EER_NAME_COL,
                    SUBMARKET_CODE_COL,
                    SUBMARKET_NAME_COL,
                ])
                .sum()
            ).reset_index()
            eer_codes = cls.eer_code_order(uow)
            eers = cls.eer_submarket_map(uow)
            missing_eers = [
                eer for eer in eer_codes if eer not in df[EER_CODE_COL].tolist()
            ]
            missing_dfs: list[pd.DataFrame] = []
            dates = df[START_DATE_COL].unique()
            for eer in missing_eers:
                missing_df = pd.DataFrame({
                    START_DATE_COL: dates,
                    CONFIG_COL: configurations_df.loc[
                        configuration_df[START_DATE_COL].isin(dates),
                        VALUE_COL,
                    ].to_numpy(),
                    EER_CODE_COL: [eer] * len(dates),
                    EER_NAME_COL: [eers.at[eer, EER_NAME_COL]] * len(dates),
                    SUBMARKET_CODE_COL: [eers.at[eer, SUBMARKET_CODE_COL]]
                    * len(dates),
                    SUBMARKET_NAME_COL: [eers.at[eer, SUBMARKET_NAME_COL]]
                    * len(dates),
                    ABSOLUTE_VALUE_COL: [0.0] * len(dates),
                })
                missing_dfs.append(missing_df)
            df = pd.concat([df] + missing_dfs, ignore_index=True)
            df = df.sort_values([EER_CODE_COL, START_DATE_COL, CONFIG_COL])
            return df

        ABSOLUTE_VALUE_COL = "valor_hm3"

        df = cls.initial_stored_volume(uow).set_index(HYDRO_CODE_COL)
        dfs: list[pd.DataFrame] = []
        configuration_df = cls.configurations(uow)
        dates = cls.stages_starting_dates_final_simulation(uow)
        for _, line in configuration_df.iterrows():
            configuration_date = line[START_DATE_COL]
            if configuration_date not in dates:
                continue
            # Calcula prodts no máximo
            stage_df = df.copy()
            stage_df = _join_drop_data(stage_df, configuration_date)
            stage_df = _join_bounds_data(stage_df)
            stage_df = _join_hydros_data(stage_df)
            stage_df[ABSOLUTE_VALUE_COL] = (
                stage_df[UPPER_BOUND_COL] - stage_df[LOWER_BOUND_COL]
            )
            stage_df = cls._evaluate_productivity(
                stage_df, volume_col=ABSOLUTE_VALUE_COL
            )
            stage_df = cls._accumulate_productivity(stage_df)
            stage_df[CONFIG_COL] = line[VALUE_COL]
            dfs.append(stage_df)

        df = pd.concat(dfs, ignore_index=True)
        df = _volume_to_energy(df)
        df = _cast_to_eers_and_fill_missing(df, configuration_df)

        df = df.rename(columns={ABSOLUTE_VALUE_COL: VALUE_COL})

        df = df[
            [
                START_DATE_COL,
                CONFIG_COL,
                EER_CODE_COL,
                EER_NAME_COL,
                SUBMARKET_CODE_COL,
                SUBMARKET_NAME_COL,
                VALUE_COL,
            ]
        ].reset_index(drop=True)
        return df

    @classmethod
    def _stored_energy_upper_bounds_pmo(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtem os limites superiores de armazenamento de energia para
        cada REE em MWmes, para o período de estudo.
        """

        def _filter_study_period(df: pd.DataFrame) -> pd.DataFrame:
            dates = cls.stages_starting_dates_final_simulation(uow)
            df = df.loc[df[START_DATE_COL].between(dates[0], dates[-1])]
            return df

        def _add_entity_data(df: pd.DataFrame) -> pd.DataFrame:
            eers = cls.eer_code_order(uow)
            num_configs = df.shape[0]
            df = pd.concat([df] * len(eers), ignore_index=True)
            df[EER_CODE_COL] = np.repeat(eers, num_configs)
            entity_map = cls.eer_submarket_map(uow)
            return df.join(entity_map, on=EER_CODE_COL)

        def _add_values(
            df: pd.DataFrame, maximum_storage_df: pd.DataFrame
        ) -> pd.DataFrame:
            df[VALUE_COL] = df.apply(
                lambda line: maximum_storage_df.loc[
                    (maximum_storage_df[EER_NAME_COL] == line[EER_NAME_COL])
                    & (maximum_storage_df[CONFIG_COL] == line[CONFIG_COL]),
                    VALUE_COL,
                ].iloc[0],
                axis=1,
            )
            return df

        maximum_storage_df = cls.pmo(uow).energia_armazenada_maxima

        if maximum_storage_df is None:
            return None

        maximum_storage_df = maximum_storage_df.rename(
            columns={
                "nome_ree": EER_NAME_COL,
                "data": START_DATE_COL,
                "valor_MWmes": VALUE_COL,
            }
        )
        configs_df = cls.configurations(uow)
        configs_df = configs_df.rename(
            columns={
                VALUE_COL: CONFIG_COL,
            }
        )
        configs_df = _filter_study_period(configs_df)
        configs_df = _add_entity_data(configs_df)
        configs_df = _add_values(configs_df, maximum_storage_df)
        stored_energy_upper_bounds = configs_df.sort_values([
            EER_CODE_COL,
            START_DATE_COL,
        ])

        return stored_energy_upper_bounds.reset_index(drop=True)

    @classmethod
    def stored_energy_upper_bounds(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        stored_energy_upper_bounds = cls.DECK_DATA_CACHING.get(
            "stored_energy_upper_bounds"
        )
        if stored_energy_upper_bounds is None:
            bounds_df = cls._stored_energy_upper_bounds_pmo(uow)
            if bounds_df is None:
                bounds_df = cls._stored_energy_upper_bounds_inputs(uow)
            stored_energy_upper_bounds = bounds_df
            cls.DECK_DATA_CACHING["stored_energy_upper_bounds"] = (
                stored_energy_upper_bounds
            )
        return stored_energy_upper_bounds.copy()

    @classmethod
    def convergence(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        convergence = cls.DECK_DATA_CACHING.get("convergence")
        if convergence is None:
            pmo = cls.pmo(uow)
            convergence = cls._validate_data(
                pmo.convergencia,
                pd.DataFrame,
                "processo iterativo de convergencia (pmo.dat)",
            )

            cls.DECK_DATA_CACHING["convergence"] = convergence
        return convergence.copy()

    @classmethod
    def _apply_thermal_bounds_maintenance_and_changes(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        pass

        def _apply_thermal_single_change(
            df: pd.DataFrame,
            thermal_code: int,
            start_date: datetime,
            end_date: datetime,
            col: str,
            value: float,
        ) -> None:
            df_filter = (
                (df[THERMAL_CODE_COL] == thermal_code)
                & (df[START_DATE_COL] >= start_date)
                & (df[START_DATE_COL] <= end_date)
            )
            df.loc[df_filter, col] = value

        def _apply_thermal_changes(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            expt = cls.expt(uow)
            stage_dates = cls.stages_starting_dates_final_simulation(uow)
            final_date = stage_dates[-1]
            expt["data_fim"] = expt["data_fim"].fillna(final_date)
            thermal_change_type_col_map: dict[str, str] = {
                "POTEF": "potencia_instalada",
                "FCMAX": "fator_capacidade_maximo",
                "TEIFT": "teif",
                "GTMIN": LOWER_BOUND_COL,
                "IPTER": "indisponibilidade_programada",
            }
            for _, line in expt.iterrows():
                _apply_thermal_single_change(
                    df,
                    line["codigo_usina"],
                    line["data_inicio"],
                    line["data_fim"],
                    thermal_change_type_col_map[line["tipo"]],
                    line["modificacao"],
                )
            return df

        def _apply_maintenance(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            manutt = cls.manutt(uow)
            thermal_codes = manutt[THERMAL_CODE_COL].unique()
            maintenance_end_date = cls.thermal_maintenance_end_date(uow)
            for code in thermal_codes:
                thermal_df = df.loc[
                    (df[THERMAL_CODE_COL] == code)
                    & (df[START_DATE_COL] < maintenance_end_date),
                    [START_DATE_COL, "potencia_instalada"],
                ].copy()
                last_month: pd.Timestamp = thermal_df[START_DATE_COL].max()
                last_day = last_month.daysinmonth
                thermal_df.loc[-1, START_DATE_COL] = last_month.replace(
                    day=last_day
                )
                thermal_df = (
                    thermal_df.set_index(START_DATE_COL).resample("D").ffill()
                )
                thermal_df["potencia_instalada"] = thermal_df[
                    "potencia_instalada"
                ].ffill()
                thermal_maintenance_df: pd.DataFrame = manutt.loc[
                    manutt[THERMAL_CODE_COL] == code
                ]
                for _, line in thermal_maintenance_df.iterrows():
                    start_date = line["data_inicio"]
                    num_days = line["duracao"]
                    value = line["potencia"]
                    end_date = start_date + timedelta(days=num_days - 1)
                    thermal_df.loc[
                        start_date:end_date, "potencia_instalada"
                    ] -= value
                thermal_df = thermal_df.resample("MS").mean().reset_index()
                df.loc[
                    (df[THERMAL_CODE_COL] == code)
                    & (df[START_DATE_COL].isin(thermal_df[START_DATE_COL])),
                    "potencia_instalada",
                ] = thermal_df["potencia_instalada"].to_numpy()

            return df

        maintenance_end_date = cls.thermal_maintenance_end_date(uow)
        # Por padrão desconsidera IP no início do estudo
        df.loc[
            df[START_DATE_COL] < maintenance_end_date,
            "indisponibilidade_programada",
        ] = 0.0
        df = _apply_thermal_changes(df, uow)
        df = _apply_maintenance(df, uow)
        return df

    @classmethod
    def _thermal_generation_bounds_term_manutt_expt(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        - codigo_usina (`int`)
        - nome_usina (`str`)
        - data_inicio (`datetime`)
        - limite_inferior (`float`)
        - limite_superior (`float`)
        """

        def _expand_to_stages(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            num_thermals = df.shape[0]
            dates = np.array(cls.stages_starting_dates_final_simulation(uow))
            num_stages = len(dates)
            df = pd.concat([df] * num_stages, ignore_index=True)
            df[START_DATE_COL] = np.repeat(dates, num_thermals)
            return df.sort_values([
                THERMAL_CODE_COL,
                START_DATE_COL,
            ]).reset_index(drop=True)

        def _add_term_lower_bounds(
            df: pd.DataFrame, term: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            stage_dates = cls.stages_starting_dates_final_simulation(uow)
            initial_date = stage_dates[0]
            initial_month = initial_date.month
            # Aparentemente o bloco de GTMIN do term.dat é rolante. Ou seja,
            # o "primeiro ano de estudo" é sempre disposto, mesmo se o caso
            # começar em outubro. Neste caso, valem os valores até setembro
            # do outro ano.
            # Segundo o manual, este "ano" era pra ser todos os anos de
            # manutenção.
            term = term.loc[term["mes"] >= initial_month].copy()
            last_term_month = term["mes"].max()
            last_term_block = term.loc[term["mes"] == last_term_month]
            num_repeats = len(stage_dates) - (12 - initial_month) - 1
            term_repeats: list[pd.DataFrame] = []
            for n in range(1, num_repeats):
                last_term_block_month = last_term_block.copy()
                last_term_block_month["mes"] += n
                term_repeats.append(last_term_block_month)
            term = pd.concat([term, *term_repeats], ignore_index=True)
            term = term.sort_values([THERMAL_CODE_COL, "mes"])
            df[LOWER_BOUND_COL] = term["geracao_minima"].to_numpy()
            return df

        def _enforce_null_lower_bounds_on_changes(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            expt = cls.expt(uow)
            # encontra as UTEs com modificacao de GTMIN
            thermals_to_nullify = expt.loc[
                expt["tipo"] == "GTMIN", "codigo_usina"
            ].unique()
            # zera o GTMIN de todas as UTEs que tiveram modificacao
            # após o primeiro ano (ou num anos manut térmicas)
            maintenance_end_date = cls.thermal_maintenance_end_date(uow)
            for code in thermals_to_nullify:
                df.loc[
                    (df[THERMAL_CODE_COL] == code)
                    & (df[START_DATE_COL] >= maintenance_end_date),
                    LOWER_BOUND_COL,
                ] = 0.0
            return df

        def _enforce_null_upper_bounds_on_changes(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            expt = cls.expt(uow)
            # encontra as UTEs com modificacao de POTEF
            thermals_to_nullify = expt.loc[
                expt["tipo"] == "POTEF", "codigo_usina"
            ].unique()
            # zera o POTEF de todas as UTEs que tiveram modificacao
            # após o primeiro ano (ou num anos manut térmicas)
            maintenance_end_date = cls.thermal_maintenance_end_date(uow)
            for code in thermals_to_nullify:
                df.loc[
                    (df[THERMAL_CODE_COL] == code)
                    & (df[START_DATE_COL] >= maintenance_end_date),
                    "potencia_instalada",
                ] = 0.0
            return df

        def _eval_upper_bounds(df: pd.DataFrame) -> pd.DataFrame:
            df[UPPER_BOUND_COL] = (
                df["potencia_instalada"]
                * (df["fator_capacidade_maximo"] / 100.0)
                * (100.0 - df["indisponibilidade_programada"])
                / 100.0
                * (100.0 - df["teif"])
                / 100.0
            )
            return df

        term = cls.term(uow)
        bounds_df = (
            term.drop_duplicates(subset=["codigo_usina", "nome_usina"])
            .copy()
            .sort_values(THERMAL_CODE_COL)
        )
        bounds_df = _expand_to_stages(bounds_df, uow)
        bounds_df = _add_term_lower_bounds(bounds_df, term, uow)
        bounds_df = _enforce_null_lower_bounds_on_changes(bounds_df, uow)
        bounds_df = _enforce_null_upper_bounds_on_changes(bounds_df, uow)
        bounds_df = cls._apply_thermal_bounds_maintenance_and_changes(
            bounds_df, uow
        )
        bounds_df = _eval_upper_bounds(bounds_df)
        bounds_df = bounds_df[
            [
                THERMAL_CODE_COL,
                "nome_usina",
                START_DATE_COL,
                LOWER_BOUND_COL,
                UPPER_BOUND_COL,
            ]
        ].copy()
        return bounds_df

    @classmethod
    def _thermal_generation_bounds_pmo(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame | None:
        """
        - codigo_usina (`int`)
        - nome_usina (`str`)
        - data_inicio (`datetime`)
        - limite_inferior (`float`)
        - limite_superior (`float`)
        """
        pmo = cls.pmo(uow)
        bounds_df = pmo.geracao_minima_usinas_termicas
        if bounds_df is None:
            return None
        if isinstance(bounds_df, pd.DataFrame):
            bounds_df = bounds_df.rename(
                columns={
                    "data": START_DATE_COL,
                    "valor_MWmed": LOWER_BOUND_COL,
                }
            )
            upper_bounds = pmo.geracao_maxima_usinas_termicas
            if isinstance(upper_bounds, pd.DataFrame):
                bounds_df[UPPER_BOUND_COL] = upper_bounds[
                    "valor_MWmed"
                ].to_numpy()
        start_date = cls.stages_starting_dates_final_simulation(uow)[0]
        bounds_df = bounds_df.loc[
            bounds_df[START_DATE_COL] >= start_date
        ].reset_index(drop=True)
        bounds_df = cls._consider_post_study_years(bounds_df, uow)
        return bounds_df

    @classmethod
    def thermal_generation_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        def _add_submarket_data(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            map = cls.thermal_submarket_map(uow)
            df = df.rename(
                columns={
                    "nome_usina": THERMAL_NAME_COL,
                    "codigo_usina": THERMAL_CODE_COL,
                }
            )
            df = df.join(
                map[[SUBMARKET_CODE_COL, SUBMARKET_NAME_COL]],
                on=THERMAL_CODE_COL,
            )
            return df

        thermal_generation_bounds = cls.DECK_DATA_CACHING.get(
            "thermal_generation_bounds"
        )
        if thermal_generation_bounds is None:
            bounds_df = cls._thermal_generation_bounds_pmo(uow)
            if bounds_df is None:
                bounds_df = cls._thermal_generation_bounds_term_manutt_expt(uow)
            bounds_df = _add_submarket_data(bounds_df, uow)

            thermal_generation_bounds = bounds_df
            cls.DECK_DATA_CACHING["thermal_generation_bounds"] = (
                thermal_generation_bounds
            )
        return thermal_generation_bounds.copy()

    @classmethod
    def exchange_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        def _drops_exchange_direction_flag(
            bounds_df: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            Inverte as colunas submercado_de e submercado_para a
            partir do valor da coluna sentido, aplicada apenas para
            o DataFrame de limites de intercâmbio do sistema.dat.
            """
            filtro = bounds_df["sentido"] == 1
            (
                bounds_df.loc[filtro, EXCHANGE_SOURCE_CODE_COL],
                bounds_df.loc[filtro, EXCHANGE_TARGET_CODE_COL],
            ) = (
                bounds_df.loc[filtro, EXCHANGE_TARGET_CODE_COL],
                bounds_df.loc[filtro, EXCHANGE_SOURCE_CODE_COL],
            )
            return bounds_df.drop(columns=["sentido"])

        def _cast_exchange_bounds_to_MWmes(
            exchange_block_bounds_df: pd.DataFrame,
            exchange_average_bounds_df: pd.DataFrame,
            block_length_df: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            Obtem limites de intercâmbio em MWmes a partir de limites
            em MWmed e P.U. e das durações de cada patamar. Estes limites
            são compatíveis com o visto no nwlistop.
            """
            exchange_block_bounds_df[VALUE_COL] = (
                exchange_block_bounds_df.apply(
                    lambda linha: exchange_average_bounds_df.loc[
                        (
                            exchange_average_bounds_df[EXCHANGE_SOURCE_CODE_COL]
                            == linha[EXCHANGE_SOURCE_CODE_COL]
                        )
                        & (
                            exchange_average_bounds_df[EXCHANGE_TARGET_CODE_COL]
                            == linha[EXCHANGE_TARGET_CODE_COL]
                        )
                        & (
                            exchange_average_bounds_df[START_DATE_COL]
                            == linha[START_DATE_COL]
                        ),
                        VALUE_COL,
                    ].iloc[0]
                    * linha[VALUE_COL],
                    axis=1,
                )
            )
            block_length_df = block_length_df.sort_values([
                START_DATE_COL,
                BLOCK_COL,
            ])
            n_pares_limites = exchange_block_bounds_df.drop_duplicates([
                EXCHANGE_SOURCE_CODE_COL,
                EXCHANGE_TARGET_CODE_COL,
            ]).shape[0]
            exchange_block_bounds_df[VALUE_COL] *= np.tile(
                block_length_df[VALUE_COL].to_numpy(), n_pares_limites
            )

            return exchange_block_bounds_df

        exchange_bounds = cls.DECK_DATA_CACHING.get("exchange_bounds")
        if exchange_bounds is None:
            exchange_average_bounds_df = cls._validate_data(
                cls._get_sistema(uow).limites_intercambio,
                pd.DataFrame,
                "limites de intercambio (sistema.dat)",
            )
            exchange_average_bounds_df = exchange_average_bounds_df.rename(
                columns={
                    "submercado_de": EXCHANGE_SOURCE_CODE_COL,
                    "submercado_para": EXCHANGE_TARGET_CODE_COL,
                    "data": START_DATE_COL,
                }
            )
            exchange_average_bounds_df = _drops_exchange_direction_flag(
                exchange_average_bounds_df
            )
            exchange_block_bounds_df = cls.exchange_block_limits(uow)
            exchange_average_bounds_df = cls._consider_post_study_years(
                exchange_average_bounds_df, uow
            )
            block_length_df = cls.block_lengths(uow)
            exchange_bounds = _cast_exchange_bounds_to_MWmes(
                exchange_block_bounds_df,
                exchange_average_bounds_df,
                block_length_df,
            )
            exchange_bounds = exchange_bounds.reset_index(drop=True)
            cls.DECK_DATA_CACHING["exchange_bounds"] = exchange_bounds
        return exchange_bounds.copy()

    @classmethod
    def costs(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        costs = cls.DECK_DATA_CACHING.get("costs")
        if costs is None:
            pmo = cls.pmo(uow)
            costs = cls._validate_data(
                pmo.custo_operacao_series_simuladas,
                pd.DataFrame,
                "custo de operacao das series simuladas (pmo.dat)",
            )

            cls.DECK_DATA_CACHING["costs"] = costs
        return costs.copy()

    @classmethod
    def num_iterations(cls, uow: AbstractUnitOfWork) -> int:
        num_iterations = cls.DECK_DATA_CACHING.get("num_iterations")
        if num_iterations is None:
            df = cls.convergence(uow)
            num_iterations = cls._validate_data(
                int(df["iteracao"].max()),
                int,
                "numero de iteracoes na convergencia (pmo.dat)",
            )

            cls.DECK_DATA_CACHING["num_iterations"] = num_iterations
        return num_iterations

    @classmethod
    def runtimes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        runtimes = cls.DECK_DATA_CACHING.get("runtimes")
        if runtimes is None:
            arq = cls.newavetim(uow)
            runtimes = cls._validate_data(
                arq.tempos_etapas,
                pd.DataFrame,
                "tempos por etapa da execucao (newave.tim)",
            )

            cls.DECK_DATA_CACHING["runtimes"] = runtimes
        return runtimes

    @classmethod
    def submarkets(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        submarkets = cls.DECK_DATA_CACHING.get("submarkets")
        if submarkets is None:
            submarkets = cls._validate_data(
                cls._get_sistema(uow).custo_deficit,
                pd.DataFrame,
                "submercados e custos de deficit (sistema.dat)",
            )
            submarkets = submarkets.rename(
                columns={
                    "codigo_submercado": SUBMARKET_CODE_COL,
                    "nome_submercado": SUBMARKET_NAME_COL,
                }
            )
            submarkets = submarkets.drop_duplicates(
                subset=[SUBMARKET_CODE_COL]
            ).reset_index(drop=True)
            submarkets = submarkets.astype({SUBMARKET_NAME_COL: STRING_DF_TYPE})
            submarkets = submarkets.set_index(SUBMARKET_CODE_COL)
            cls.DECK_DATA_CACHING["submarkets"] = submarkets
        return submarkets.copy()

    @classmethod
    def eers(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        eers = cls.DECK_DATA_CACHING.get("eers")
        if eers is None:
            eers = cls._validate_data(
                cls._get_ree(uow).rees,
                pd.DataFrame,
                "REEs e periodo individualizado (ree.dat)",
            )
            eers = eers.rename(
                columns={
                    "codigo": EER_CODE_COL,
                    "nome": EER_NAME_COL,
                    "submercado": SUBMARKET_CODE_COL,
                }
            )
            eers = eers.astype({EER_NAME_COL: STRING_DF_TYPE})
            eers = eers.set_index(EER_CODE_COL)
            cls.DECK_DATA_CACHING["eers"] = eers
        return eers.copy()

    @classmethod
    def hybrid_policy(cls, uow: AbstractUnitOfWork) -> bool:
        hybrid_policy = cls.DECK_DATA_CACHING.get("hybrid_policy")
        if hybrid_policy is None:
            eers = cls.eers(uow)
            val = bool(eers["ano_fim_individualizado"].isna().sum() == 0)
            hybrid_policy = cls._validate_data(
                val,
                bool,
                "fim do horizonte individualizado (ree.dat)",
            )
            cls.DECK_DATA_CACHING["hybrid_policy"] = hybrid_policy
        return hybrid_policy

    @classmethod
    def hydros(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        hydros = cls.DECK_DATA_CACHING.get("hydros")
        if hydros is None:
            hydros = cls._validate_data(
                cls._get_confhd(uow).usinas,
                pd.DataFrame,
                "cadastro de usinas hidreletricas (confhd.dat)",
            )
            hydros = hydros.rename(
                columns={
                    "codigo_usina": HYDRO_CODE_COL,
                    "nome_usina": HYDRO_NAME_COL,
                    "ree": EER_CODE_COL,
                }
            )
            hydros = hydros.astype({HYDRO_NAME_COL: STRING_DF_TYPE})
            hydros = hydros.set_index(HYDRO_CODE_COL)
            cls.DECK_DATA_CACHING["hydros"] = hydros
        return hydros.copy()

    @classmethod
    def flow_diversion(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        def _filter_stages(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            return df.loc[
                df[START_DATE_COL].isin(
                    cls.stages_starting_dates_final_simulation(uow)
                )
            ].copy()

        def _add_missing_hydros(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            hydros = cls.hydros(uow)
            hydro_codes = hydros.index.tolist()
            hydro_codes_in_df = df[HYDRO_CODE_COL].unique().tolist()
            missing_hydros = [
                c for c in hydro_codes if c not in hydro_codes_in_df
            ]
            dfs_missing_hydros: list[pd.DataFrame] = []
            for c in missing_hydros:
                df_hydro = df.loc[
                    df[HYDRO_CODE_COL] == hydro_codes_in_df[0]
                ].copy()
                df_hydro[HYDRO_CODE_COL] = c
                df_hydro[VALUE_COL] = 0.0
                dfs_missing_hydros.append(df_hydro)
            df = pd.concat([df] + dfs_missing_hydros, ignore_index=True)
            df = df.sort_values([HYDRO_CODE_COL, START_DATE_COL])
            return df

        def _make_bound_columns(df: pd.DataFrame) -> pd.DataFrame:
            df[VALUE_COL] *= -1
            df[LOWER_BOUND_COL] = df[VALUE_COL]
            df[UPPER_BOUND_COL] = df[VALUE_COL]
            df[LOWER_BOUND_UNIT_COL] = Unit.m3s.value
            df[UPPER_BOUND_UNIT_COL] = Unit.m3s.value
            return df

        def _repeat_by_block(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df[BLOCK_COL] = 0
            return df

        flow_diversion = cls.DECK_DATA_CACHING.get("flow_diversion")
        if flow_diversion is None:
            flow_diversion = cls._validate_data(
                cls._get_dsvagua(uow).desvios,
                pd.DataFrame,
                "retiradas de agua das usinas hidreletricas (dsvagua.dat)",
            )
            flow_diversion = flow_diversion.rename(
                columns={
                    "codigo_usina": HYDRO_CODE_COL,
                    "data": START_DATE_COL,
                }
            )
            flow_diversion = (
                flow_diversion.groupby([HYDRO_CODE_COL, START_DATE_COL])[
                    VALUE_COL
                ]
                .sum()
                .reset_index()
            )
            flow_diversion = _filter_stages(flow_diversion, uow)
            flow_diversion = _add_missing_hydros(flow_diversion, uow)
            flow_diversion = _make_bound_columns(flow_diversion)
            flow_diversion = _repeat_by_block(flow_diversion, uow)
            flow_diversion = flow_diversion.reset_index(drop=True)
            flow_diversion = cls._consider_post_study_years(flow_diversion, uow)

            cls.DECK_DATA_CACHING["flow_diversion"] = flow_diversion
        return flow_diversion.copy()

    @classmethod
    def _get_value_and_unit_from_modif_entry(
        cls, r: Register
    ) -> Tuple[Optional[float], Optional[str]]:
        """
        Extrai um dado de um registro do modif.dat com a sua unidade.
        """
        if isinstance(r, VOLMIN | VMINT | VOLMAX | VMAXT):
            return r.volume, r.unidade
        elif isinstance(r, VAZMIN | VAZMINT | VAZMAXT):
            return r.vazao, Unit.m3s.value
        elif isinstance(r, TURBMINT | TURBMAXT):
            return r.turbinamento, Unit.m3s.value
        elif isinstance(r, CMONT | CFUGA):
            return r.nivel, ""
        return None, None

    @classmethod
    def _get_hydro_data_changes_from_modif(
        cls,
        df: pd.DataFrame,
        hydro_data_col: str,
        hydro_data_unit_col: str,
        register_type: Type[Union[VAZMIN, VOLMIN, VOLMAX]],
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        """
        Realiza a extração de modificações cadastrais de volumes de usinas
        hidrelétricas a partir do arquivo modif.dat, atualizando os cadastros
        conforme as declarações de modificações são encontradas.
        """
        modif = cls.modif(uow)
        for idx in df.index:
            hydro_changes = modif.modificacoes_usina(idx)
            if hydro_changes is not None:
                regs_usina = [
                    r for r in hydro_changes if isinstance(r, register_type)
                ]
                if len(regs_usina) > 0:
                    r = regs_usina[-1]
                    value, unit = cls._get_value_and_unit_from_modif_entry(r)
                    if value is not None:
                        df.at[idx, hydro_data_col] = value
                    if unit is not None:
                        df.at[idx, hydro_data_unit_col] = unit.lower()
        return df

    @classmethod
    def _get_hydro_data_changes_from_modif_to_stages(
        cls,
        df: pd.DataFrame,
        hydro_data_col: str,
        hydro_data_unit_col: str,
        register_type: Type[
            Union[
                VMINT, VMAXT, VAZMINT, VAZMAXT, TURBMINT, TURBMAXT, CFUGA, CMONT
            ]
        ],
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        """
        Realiza a extração de modificações cadastrais de volumes de usinas
        hidrelétricas a partir do arquivo modif.dat, considerando também
        modificações cadastrais com relação temporal. Os cadastros são
        expandidos para um valor por usina e estágio e são atualizados
        conforme as declarações de modificações são encontradas.
        """
        modif = cls.modif(uow)
        num_stages = cls.num_hydro_simulation_stages_final_simulation(uow)
        dates = cls.stages_starting_dates_final_simulation(uow)[:num_stages]
        hydro_codes = df[HYDRO_CODE_COL].unique().tolist()
        for i, u in enumerate(hydro_codes):
            hydro_changes = modif.modificacoes_usina(u)
            i_i = i * num_stages
            i_f = i_i + num_stages - 1
            if hydro_changes is not None:
                hydro_registers = [
                    r for r in hydro_changes if isinstance(r, register_type)
                ]
                for reg in hydro_registers:
                    if reg.data_inicio not in dates:  # type: ignore
                        continue
                    idx_data = dates.index(reg.data_inicio)  # type: ignore
                    value, unit = cls._get_value_and_unit_from_modif_entry(reg)
                    df.loc[i_i + idx_data : i_f, hydro_data_col] = value
                    df.loc[i_i + idx_data : i_f, hydro_data_unit_col] = unit
        return df

    @classmethod
    def hydro_volume_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        """
        Obtém um DataFrame com os limites cadastrais de volume armazenado
        de cada usina hidrelétrica.
        """

        def _get_hydro_data(uow: AbstractUnitOfWork) -> pd.DataFrame:
            df = cls.hidr(uow).reset_index()
            hydro_codes = cls.hydro_code_order(uow)
            df = df.loc[
                df[HYDRO_CODE_COL].isin(hydro_codes),
                [HYDRO_CODE_COL, "volume_minimo", "volume_maximo"],
            ].set_index(HYDRO_CODE_COL)
            df = df.rename(
                columns={
                    "volume_minimo": LOWER_BOUND_COL,
                    "volume_maximo": UPPER_BOUND_COL,
                }
            )
            df[LOWER_BOUND_UNIT_COL] = Unit.hm3_modif.value
            df[UPPER_BOUND_UNIT_COL] = Unit.hm3_modif.value
            return df

        hydro_volume_bounds = cls.DECK_DATA_CACHING.get("hydro_volume_bounds")
        if hydro_volume_bounds is None:
            hydro_volume_bounds = _get_hydro_data(uow)
            entities = cls.hydro_eer_submarket_map(uow)
            hydro_volume_bounds = hydro_volume_bounds.join(entities)
            cls.DECK_DATA_CACHING["hydro_volume_bounds"] = hydro_volume_bounds
        return hydro_volume_bounds.copy()

    @classmethod
    def hydro_volume_bounds_with_changes(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtém um DataFrame com os limites de volume armazenado
        de cada usina hidrelétrica, sem considerar modificações que variam
        no tempo.
        """

        def _add_hydro_bounds_changes(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = df.copy()
            df = cls._get_hydro_data_changes_from_modif(
                df, LOWER_BOUND_COL, LOWER_BOUND_UNIT_COL, VOLMIN, uow
            )
            df = cls._get_hydro_data_changes_from_modif(
                df, UPPER_BOUND_COL, UPPER_BOUND_UNIT_COL, VOLMAX, uow
            )
            return df

        def _cast_bounds_to_hm3(
            df: pd.DataFrame, hm3_df: pd.DataFrame
        ) -> pd.DataFrame:
            def _min_volume(hydro_code: int) -> float:
                return hm3_df.at[hydro_code, LOWER_BOUND_COL]

            def _net_volume(hydro_code: int) -> float:
                return (
                    hm3_df.at[hydro_code, UPPER_BOUND_COL]
                    - hm3_df.at[hydro_code, LOWER_BOUND_COL]
                )

            bound_columns = [LOWER_BOUND_COL, UPPER_BOUND_COL]
            unit_columns = [LOWER_BOUND_UNIT_COL, UPPER_BOUND_UNIT_COL]
            for col, unit_col in zip(bound_columns, unit_columns):
                bound_df = df.loc[df[unit_col] == Unit.perc_modif.value].copy()

                if not bound_df.empty:
                    bound_df[col] = bound_df.apply(
                        lambda line: line[col] * _net_volume(line.name) / 100.0
                        + _min_volume(line.name),
                        axis=1,
                    )
                    df.loc[bound_df.index, col] = bound_df[col]
                    df.loc[bound_df.index, unit_col] = Unit.hm3_modif.value

            return df

        hydro_volume_bounds_with_changes = cls.DECK_DATA_CACHING.get(
            "hydro_volume_bounds_with_changes"
        )
        if hydro_volume_bounds_with_changes is None:
            hm3_df = cls.hydro_volume_bounds(uow)
            df = _add_hydro_bounds_changes(hm3_df, uow)
            casted_df = _cast_bounds_to_hm3(df, hm3_df)
            hydro_volume_bounds_with_changes = casted_df
            cls.DECK_DATA_CACHING["hydro_volume_bounds_with_changes"] = (
                hydro_volume_bounds_with_changes
            )
        return hydro_volume_bounds_with_changes.copy()

    @classmethod
    def hydro_volume_bounds_in_stages(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtém um DataFrame com os limites de volume armazenado de cada usina
        hidrelétrica para cada estágio do problema, considerando possíveis
        modificações e convertendo para hm3.
        """

        def _expand_to_stages(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = df.reset_index()
            num_hydros = df.shape[0]
            dates = np.array(cls.stages_starting_dates_final_simulation(uow))
            num_stages = len(dates)
            df = pd.concat([df] * num_stages, ignore_index=True)
            df[START_DATE_COL] = np.repeat(dates, num_hydros)
            return df.sort_values([HYDRO_CODE_COL, START_DATE_COL]).reset_index(
                drop=True
            )

        def _add_hydro_bounds_changes_to_stages(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = cls._get_hydro_data_changes_from_modif_to_stages(
                df, LOWER_BOUND_COL, LOWER_BOUND_UNIT_COL, VMINT, uow
            )
            df = cls._get_hydro_data_changes_from_modif_to_stages(
                df, UPPER_BOUND_COL, UPPER_BOUND_UNIT_COL, VMAXT, uow
            )
            return df

        def _cast_bounds_to_hm3(
            df: pd.DataFrame, hm3_df: pd.DataFrame
        ) -> pd.DataFrame:
            def _min_volume(line: pd.Series) -> float:
                hydro_code = line[HYDRO_CODE_COL]
                date = line[START_DATE_COL]
                filter = (hm3_df[HYDRO_CODE_COL] == hydro_code) & (
                    hm3_df[START_DATE_COL] == date
                )
                return hm3_df.loc[filter, LOWER_BOUND_COL].iloc[0]

            def _net_volume(line: pd.Series) -> float:
                hydro_code = line[HYDRO_CODE_COL]
                date = line[START_DATE_COL]
                filter = (hm3_df[HYDRO_CODE_COL] == hydro_code) & (
                    hm3_df[START_DATE_COL] == date
                )
                return (
                    hm3_df.loc[filter, UPPER_BOUND_COL].iloc[0]
                    - hm3_df.loc[filter, LOWER_BOUND_COL].iloc[0]
                )

            bound_columns = [LOWER_BOUND_COL, UPPER_BOUND_COL]
            unit_columns = [LOWER_BOUND_UNIT_COL, UPPER_BOUND_UNIT_COL]
            for col, unit_col in zip(bound_columns, unit_columns):
                bound_df = df.loc[df[unit_col] == Unit.perc_modif.value].copy()

                if not bound_df.empty:
                    bound_df[col] = bound_df.apply(
                        lambda line: line[col] * _net_volume(line) / 100.0
                        + _min_volume(line),
                        axis=1,
                    )
                    df.loc[bound_df.index, col] = bound_df[col]
                    df.loc[bound_df.index, unit_col] = Unit.hm3_modif.value

            return df

        hydro_volume_bounds_in_stages = cls.DECK_DATA_CACHING.get(
            "hydro_volume_bounds_in_stages"
        )
        if hydro_volume_bounds_in_stages is None:
            hm3_df = cls.hydro_volume_bounds_with_changes(uow)
            hm3_df = _expand_to_stages(hm3_df, uow)
            df = _add_hydro_bounds_changes_to_stages(hm3_df.copy(), uow)
            casted_df = _cast_bounds_to_hm3(df, hm3_df)

            hydro_volume_bounds_in_stages = casted_df
            cls.DECK_DATA_CACHING["hydro_volume_bounds_in_stages"] = (
                hydro_volume_bounds_in_stages
            )
        return hydro_volume_bounds_in_stages.copy()

    @classmethod
    def hydro_turbined_flow_bounds(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtém um DataFrame com os limites cadastrais de vazão turbinada
        de cada usina hidrelétrica.
        """

        def _calc_turbined_flow_bounds(line: pd.Series) -> float:
            num_groups = line["numero_conjuntos_maquinas"]
            num_units_per_group = [
                f"maquinas_conjunto_{i}" for i in range(1, num_groups + 1)
            ]
            max_flow_per_unit = [
                f"vazao_nominal_conjunto_{i}" for i in range(1, num_groups + 1)
            ]
            num_units = line[num_units_per_group].to_numpy()
            flow_units = line[max_flow_per_unit].to_numpy()
            return np.sum(num_units * flow_units, dtype=np.float64)

        def _get_hydro_data(uow: AbstractUnitOfWork) -> pd.DataFrame:
            df = cls.hidr(uow).reset_index()
            hydro_codes = cls.hydro_code_order(uow)
            df[UPPER_BOUND_COL] = df.apply(
                lambda line: _calc_turbined_flow_bounds(line), axis=1
            )
            df[LOWER_BOUND_COL] = 0.0
            df = df.loc[
                df[HYDRO_CODE_COL].isin(hydro_codes),
                [HYDRO_CODE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL],
            ].set_index(HYDRO_CODE_COL)
            df[LOWER_BOUND_UNIT_COL] = Unit.m3s.value
            df[UPPER_BOUND_UNIT_COL] = Unit.m3s.value
            return df

        hydro_turbined_flow_bounds = cls.DECK_DATA_CACHING.get(
            "hydro_turbined_flow_bounds"
        )
        if hydro_turbined_flow_bounds is None:
            hydro_turbined_flow_bounds = _get_hydro_data(uow)
            entities = cls.hydro_eer_submarket_map(uow)
            hydro_turbined_flow_bounds = hydro_turbined_flow_bounds.join(
                entities
            )
            cls.DECK_DATA_CACHING["hydro_turbined_flow_bounds"] = (
                hydro_turbined_flow_bounds
            )
        return hydro_turbined_flow_bounds.copy()

    @classmethod
    def hydro_turbined_flow_bounds_with_changes(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtém um DataFrame com os limites de vazão turbinada
        de cada usina hidrelétrica, sem considerar modificações que variam
        no tempo.
        """

        def _apply_changes_to_hydro_data(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ):
            modif = cls.modif(uow)
            for idx in df.index:
                hydro_changes = modif.modificacoes_usina(idx)
                if hydro_changes is not None:
                    num_groups_registers = [
                        r for r in hydro_changes if isinstance(r, NUMCNJ)
                    ]
                    if len(num_groups_registers) > 0:
                        num_groups_register = num_groups_registers[-1]
                        df.at[idx, "numero_conjuntos_maquinas"] = (
                            num_groups_register.numero
                        )
                    num_units_registers = [
                        r for r in hydro_changes if isinstance(r, NUMMAQ)
                    ]
                    for num_units_register in num_units_registers:
                        df.at[
                            idx,
                            f"maquinas_conjunto_{num_units_register.conjunto}",
                        ] = num_units_register.numero_maquinas
            return df

        def _calc_turbined_flow_bounds(line: pd.Series) -> float:
            num_groups = line["numero_conjuntos_maquinas"]
            num_units_per_group = [
                f"maquinas_conjunto_{i}" for i in range(1, num_groups + 1)
            ]
            max_flow_per_unit = [
                f"vazao_nominal_conjunto_{i}" for i in range(1, num_groups + 1)
            ]
            num_units = line[num_units_per_group].to_numpy()
            flow_units = line[max_flow_per_unit].to_numpy()
            return np.sum(num_units * flow_units, dtype=np.float64)

        def _get_hydro_data(uow: AbstractUnitOfWork) -> pd.DataFrame:
            df = cls.hidr(uow).reset_index()
            hydro_codes = cls.hydro_code_order(uow)
            df = _apply_changes_to_hydro_data(df, uow)
            df[UPPER_BOUND_COL] = df.apply(
                lambda line: _calc_turbined_flow_bounds(line), axis=1
            )
            df[LOWER_BOUND_COL] = 0.0
            df = df.loc[
                df[HYDRO_CODE_COL].isin(hydro_codes),
                [HYDRO_CODE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL],
            ].set_index(HYDRO_CODE_COL)
            df[LOWER_BOUND_UNIT_COL] = Unit.m3s.value
            df[UPPER_BOUND_UNIT_COL] = Unit.m3s.value
            return df

        hydro_turbined_flow_bounds_with_changes = cls.DECK_DATA_CACHING.get(
            "hydro_turbined_flow_bounds_with_changes"
        )
        if hydro_turbined_flow_bounds_with_changes is None:
            hydro_turbined_flow_bounds = _get_hydro_data(uow)
            entities = cls.hydro_eer_submarket_map(uow)
            hydro_turbined_flow_bounds = hydro_turbined_flow_bounds.join(
                entities
            )
            hydro_turbined_flow_bounds_with_changes = hydro_turbined_flow_bounds
            cls.DECK_DATA_CACHING["hydro_turbined_flow_bounds_with_changes"] = (
                hydro_turbined_flow_bounds_with_changes
            )
        return hydro_turbined_flow_bounds_with_changes.copy()

    @classmethod
    def hydro_turbined_flow_bounds_in_stages(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtém um DataFrame com os limites de vazão turbinada de cada usina
        hidrelétrica para cada estágio do problema, considerando possíveis
        modificações e convertendo para hm3.
        """

        def _expand_to_stages(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = df.reset_index()
            num_hydros = df.shape[0]
            dates = np.array(cls.stages_starting_dates_final_simulation(uow))
            num_stages = len(dates)
            df = pd.concat([df] * num_stages, ignore_index=True)
            df[START_DATE_COL] = np.repeat(dates, num_hydros)
            return df.sort_values([HYDRO_CODE_COL, START_DATE_COL]).reset_index(
                drop=True
            )

        def _expand_to_blocks(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = df.reset_index(drop=True)
            dates = np.array(cls.stages_starting_dates_final_simulation(uow))
            num_stages = len(dates)
            num_hydros = df.shape[0] // num_stages
            num_blocks = cls.num_blocks(uow) + 1
            df = pd.concat([df] * num_blocks, ignore_index=True)
            df = df.sort_values([HYDRO_CODE_COL, START_DATE_COL])
            df[BLOCK_COL] = np.tile(
                np.arange(num_blocks), num_hydros * num_stages
            )
            return df.sort_values([
                HYDRO_CODE_COL,
                START_DATE_COL,
                BLOCK_COL,
            ]).reset_index(drop=True)

        def _add_hydro_bounds_changes_to_stages(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = cls._get_hydro_data_changes_from_modif_to_stages(
                df, LOWER_BOUND_COL, LOWER_BOUND_UNIT_COL, TURBMINT, uow
            )
            df = cls._get_hydro_data_changes_from_modif_to_stages(
                df, UPPER_BOUND_COL, UPPER_BOUND_UNIT_COL, TURBMAXT, uow
            )
            return df

        # TODO - analisar exph.dat

        hydro_turbined_flow_bounds_in_stages = cls.DECK_DATA_CACHING.get(
            "hydro_turbined_flow_bounds_in_stages"
        )
        if hydro_turbined_flow_bounds_in_stages is None:
            m3s_df = cls.hydro_turbined_flow_bounds_with_changes(uow)
            m3s_df = _expand_to_stages(m3s_df, uow)
            m3s_df = _add_hydro_bounds_changes_to_stages(m3s_df, uow)
            m3s_df = _expand_to_blocks(m3s_df, uow)

            hydro_turbined_flow_bounds_in_stages = m3s_df
            cls.DECK_DATA_CACHING["hydro_turbined_flow_bounds_in_stages"] = (
                hydro_turbined_flow_bounds_in_stages
            )
        return hydro_turbined_flow_bounds_in_stages.copy()

    @classmethod
    def hydro_outflow_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        """
        Obtém um DataFrame com os limites cadastrais de vazão turbinada
        de cada usina hidrelétrica.
        """

        def _get_hydro_data(uow: AbstractUnitOfWork) -> pd.DataFrame:
            df = cls.hidr(uow).reset_index()
            hydro_codes = cls.hydro_code_order(uow)
            df = df.loc[
                df[HYDRO_CODE_COL].isin(hydro_codes),
                [HYDRO_CODE_COL, "vazao_minima_historica"],
            ].set_index(HYDRO_CODE_COL)
            df = df.rename(
                columns={
                    "vazao_minima_historica": LOWER_BOUND_COL,
                }
            )
            df = df.astype({LOWER_BOUND_COL: float})
            df[UPPER_BOUND_COL] = float("inf")
            df[LOWER_BOUND_UNIT_COL] = Unit.m3s.value
            df[UPPER_BOUND_UNIT_COL] = Unit.m3s.value
            return df

        hydro_outflow_bounds = cls.DECK_DATA_CACHING.get("hydro_outflow_bounds")
        if hydro_outflow_bounds is None:
            hydro_outflow_bounds = _get_hydro_data(uow)
            entities = cls.hydro_eer_submarket_map(uow)
            hydro_outflow_bounds = hydro_outflow_bounds.join(entities)
            cls.DECK_DATA_CACHING["hydro_outflow_bounds"] = hydro_outflow_bounds
        return hydro_outflow_bounds.copy()

    @classmethod
    def hydro_outflow_bounds_with_changes(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtém um DataFrame com os limites de vazão turbinada
        de cada usina hidrelétrica, sem considerar modificações que variam
        no tempo.
        """

        def _add_hydro_bounds_changes(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = df.copy()
            df = cls._get_hydro_data_changes_from_modif(
                df, LOWER_BOUND_COL, LOWER_BOUND_UNIT_COL, VAZMIN, uow
            )
            return df

        hydro_outflow_bounds_with_changes = cls.DECK_DATA_CACHING.get(
            "hydro_outflow_bounds_with_changes"
        )
        if hydro_outflow_bounds_with_changes is None:
            # TODO - analisar modif.dat
            df = cls.hydro_outflow_bounds(uow)
            df = _add_hydro_bounds_changes(df, uow)
            hydro_outflow_bounds_with_changes = df
            cls.DECK_DATA_CACHING["hydro_outflow_bounds_with_changes"] = (
                hydro_outflow_bounds_with_changes
            )
        return hydro_outflow_bounds_with_changes.copy()

    @classmethod
    def hydro_outflow_bounds_in_stages(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Obtém um DataFrame com os limites de vazão turbinada de cada usina
        hidrelétrica para cada estágio do problema, considerando possíveis
        modificações e convertendo para hm3.
        """

        def _expand_to_stages(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = df.reset_index()
            num_hydros = df.shape[0]
            dates = np.array(cls.stages_starting_dates_final_simulation(uow))
            num_stages = len(dates)
            df = pd.concat([df] * num_stages, ignore_index=True)
            df[START_DATE_COL] = np.repeat(dates, num_hydros)
            return df.sort_values([HYDRO_CODE_COL, START_DATE_COL]).reset_index(
                drop=True
            )

        def _expand_to_blocks(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = df.reset_index()
            dates = np.array(cls.stages_starting_dates_final_simulation(uow))
            num_stages = len(dates)
            num_hydros = df.shape[0] // num_stages
            num_blocks = cls.num_blocks(uow) + 1
            df = pd.concat([df] * num_blocks, ignore_index=True)
            df = df.sort_values([HYDRO_CODE_COL, START_DATE_COL])
            df[BLOCK_COL] = np.tile(
                np.arange(num_blocks), num_hydros * num_stages
            )
            return df.sort_values([
                HYDRO_CODE_COL,
                START_DATE_COL,
                BLOCK_COL,
            ]).reset_index(drop=True)

        def _add_hydro_bounds_changes_to_stages(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = cls._get_hydro_data_changes_from_modif_to_stages(
                df, LOWER_BOUND_COL, LOWER_BOUND_UNIT_COL, VAZMINT, uow
            )
            return df

        hydro_outflow_bounds_in_stages = cls.DECK_DATA_CACHING.get(
            "hydro_outflow_bounds_in_stages"
        )
        if hydro_outflow_bounds_in_stages is None:
            m3s_df = cls.hydro_outflow_bounds_with_changes(uow)
            m3s_df = _expand_to_stages(m3s_df, uow)
            m3s_df = _add_hydro_bounds_changes_to_stages(m3s_df, uow)
            m3s_df = _expand_to_blocks(m3s_df, uow)

            hydro_outflow_bounds_in_stages = m3s_df
            cls.DECK_DATA_CACHING["hydro_outflow_bounds_in_stages"] = (
                hydro_outflow_bounds_in_stages
            )
        return hydro_outflow_bounds_in_stages.copy()

    @classmethod
    def hydro_drops(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        """
        Obtém um DataFrame com os dados cadastrais de níveis de canal
        de montante e jusante, perdas e regularização
        de cada usina hidrelétrica.
        """

        def _get_hydro_data(uow: AbstractUnitOfWork) -> pd.DataFrame:
            df = cls.hidr(uow).reset_index()
            hydro_codes = cls.hydro_code_order(uow)
            df = df.loc[
                df[HYDRO_CODE_COL].isin(hydro_codes),
                HEIGHT_POLY_COLS
                + [
                    HYDRO_CODE_COL,
                    LOWER_DROP_COL,
                    LOSS_KIND_COL,
                    LOSS_COL,
                    VOLUME_REGULATION_COL,
                    RUN_OF_RIVER_REFERENCE_VOLUME_COL,
                    SPEC_PRODUCTIVITY_COL,
                ],
            ].set_index(HYDRO_CODE_COL)
            return df

        hydro_drops = cls.DECK_DATA_CACHING.get("hydro_drops")
        if hydro_drops is None:
            hydro_drops = _get_hydro_data(uow)
            entities = cls.hydro_eer_submarket_map(uow)
            hydro_drops = hydro_drops.join(entities)
            cls.DECK_DATA_CACHING["hydro_drops"] = hydro_drops
        return hydro_drops.copy()

    @classmethod
    def hydro_drops_in_stages(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        """
        Obtém um DataFrame com os dados cadastrais de níveis de canal
        de montante e jusante, perdas e regularização
        de cada usina hidrelétrica, considerando possíveis
        modificações.
        """

        def _expand_to_stages(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = df.reset_index()
            num_hydros = df.shape[0]
            dates = np.array(cls.stages_starting_dates_final_simulation(uow))
            num_stages = len(dates)
            df = pd.concat([df] * num_stages, ignore_index=True)
            df[START_DATE_COL] = np.repeat(dates, num_hydros)
            return df.sort_values([HYDRO_CODE_COL, START_DATE_COL]).reset_index(
                drop=True
            )

        def _add_hydro_drops_changes_to_stages(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = cls._get_hydro_data_changes_from_modif_to_stages(
                df, HEIGHT_POLY_COLS[0], LOWER_BOUND_UNIT_COL, CMONT, uow
            )
            df = cls._get_hydro_data_changes_from_modif_to_stages(
                df, LOWER_DROP_COL, UPPER_BOUND_UNIT_COL, CFUGA, uow
            )
            return df

        hydro_drops_in_stages = cls.DECK_DATA_CACHING.get(
            "hydro_drops_in_stages"
        )
        if hydro_drops_in_stages is None:
            df = cls.hydro_drops(uow)
            df = _expand_to_stages(df, uow)
            df = _add_hydro_drops_changes_to_stages(df.copy(), uow)

            hydro_drops_in_stages = df
            cls.DECK_DATA_CACHING["hydro_drops_in_stages"] = (
                hydro_drops_in_stages
            )
        return hydro_drops_in_stages.copy()

    @classmethod
    def thermals(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        thermals = cls.DECK_DATA_CACHING.get("thermals")
        if thermals is None:
            thermals = cls._validate_data(
                cls._get_conft(uow).usinas,
                pd.DataFrame,
                "cadastro de usinas termeletricas (conft.dat)",
            )
            thermals = thermals.drop(columns="codigo_usina")
            thermals = thermals.rename(
                columns={
                    "classe": THERMAL_CODE_COL,
                    "nome_usina": THERMAL_NAME_COL,
                    "submercado": SUBMARKET_CODE_COL,
                }
            )
            thermals = thermals.astype({THERMAL_NAME_COL: STRING_DF_TYPE})
            thermals = thermals.set_index(THERMAL_CODE_COL)
            cls.DECK_DATA_CACHING["thermals"] = thermals
        return thermals.copy()

    @classmethod
    def num_blocks(cls, uow: AbstractUnitOfWork) -> int:
        num_blocks = cls.DECK_DATA_CACHING.get("num_blocks")
        if num_blocks is None:
            num_blocks = cls._validate_data(
                cls._get_patamar(uow).numero_patamares,
                int,
                "numero de patamares (patamar.dat)",
            )
            cls.DECK_DATA_CACHING["num_blocks"] = num_blocks
        return num_blocks

    @classmethod
    def _consider_post_study_years(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        num_years_to_add = cls.num_post_study_period_years_final_simulation(uow)
        if num_years_to_add == 0:
            return df
        else:
            years = list(set(df[START_DATE_COL].dt.year.to_list()))
            last_year = max(years)
            df_last_year = df.loc[df[START_DATE_COL].dt.year == last_year]
            dfs_post_study_years = []
            for post_year in range(1, num_years_to_add + 1):
                df_post_year = df_last_year.copy()
                df_post_year[START_DATE_COL] += pd.DateOffset(years=post_year)
                dfs_post_study_years.append(df_post_year)
            return pd.concat([df] + dfs_post_study_years, ignore_index=True)

    @classmethod
    def block_lengths(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        def __eval_pat0(df_pat: pd.DataFrame) -> pd.DataFrame:
            df_pat_0 = df_pat.groupby(START_DATE_COL, as_index=False).sum(
                numeric_only=True
            )
            df_pat_0[BLOCK_COL] = 0
            df_pat = pd.concat([df_pat, df_pat_0], ignore_index=True)
            df_pat.sort_values([START_DATE_COL, BLOCK_COL], inplace=True)
            return df_pat

        block_lengths = cls.DECK_DATA_CACHING.get("block_lengths")
        if block_lengths is None:
            block_lengths = cls._validate_data(
                cls._get_patamar(uow).duracao_mensal_patamares,
                pd.DataFrame,
                "duracao mensal em P.U. dos patamares (patamar.dat)",
            )
            block_lengths = block_lengths.rename(
                columns={"data": START_DATE_COL, "patamar": BLOCK_COL}
            )
            block_lengths = cls._consider_post_study_years(block_lengths, uow)
            block_lengths = __eval_pat0(block_lengths)
            cls.DECK_DATA_CACHING["block_lengths"] = block_lengths
        return block_lengths.copy()

    @classmethod
    def exchange_block_limits(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        def __eval_pat0(df_pat: pd.DataFrame) -> pd.DataFrame:
            df_pat_0 = df_pat.loc[df_pat[BLOCK_COL] == 1].copy()
            df_pat_0[BLOCK_COL] = 0
            df_pat_0[VALUE_COL] = 1.0
            df_pat = pd.concat([df_pat, df_pat_0], ignore_index=True)
            df_pat.sort_values(
                [
                    EXCHANGE_SOURCE_CODE_COL,
                    EXCHANGE_TARGET_CODE_COL,
                    START_DATE_COL,
                    BLOCK_COL,
                ],
                inplace=True,
            )
            return df_pat

        exchange_block_limits = cls.DECK_DATA_CACHING.get(
            "exchange_block_limits"
        )
        if exchange_block_limits is None:
            exchange_block_limits = cls._validate_data(
                cls._get_patamar(uow).intercambio_patamares,
                pd.DataFrame,
                "limites de intercambio em P.U. por patamar (patamar.dat)",
            )
            exchange_block_limits = exchange_block_limits.rename(
                columns={
                    "submercado_de": EXCHANGE_SOURCE_CODE_COL,
                    "submercado_para": EXCHANGE_TARGET_CODE_COL,
                    "data": START_DATE_COL,
                }
            )
            exchange_block_limits = cls._consider_post_study_years(
                exchange_block_limits, uow
            )
            exchange_block_limits = __eval_pat0(exchange_block_limits)
            cls.DECK_DATA_CACHING["exchange_block_limits"] = (
                exchange_block_limits
            )
        return exchange_block_limits.copy()

    @classmethod
    def _initial_stored_energy_from_pmo(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame | None:
        df_pmo = cls.pmo(uow).energia_armazenada_inicial
        if isinstance(df_pmo, pd.DataFrame):
            eers = cls.eers(uow).reset_index()
            df_pmo[EER_CODE_COL] = df_pmo["nome_ree"].apply(
                lambda x: eers.loc[eers[EER_NAME_COL] == x, EER_CODE_COL].iloc[
                    0
                ]
            )
            df_pmo = df_pmo.rename(columns={"nome_ree": EER_NAME_COL})
            df_pmo = df_pmo.set_index([EER_CODE_COL])
            df_pmo = df_pmo.sort_index()
        return df_pmo

    @classmethod
    def _evaluate_productivity(
        cls, df: pd.DataFrame, volume_col: str = VOLUME_FOR_PRODUCTIVITY_TMP_COL
    ) -> pd.DataFrame:
        def _evaluate_upper_drop_at_volume(line: pd.Series) -> float:
            coefs = [line[c] for c in HEIGHT_POLY_COLS]
            if line[VOLUME_REGULATION_COL] == "M":
                coefs_integral = [0] + [
                    c / (i + 1) for i, c in enumerate(coefs)
                ]
                min_volume = line[LOWER_BOUND_COL]
                max_volume = line[UPPER_BOUND_COL]
                net_volume = max_volume - min_volume
                percent_volume = (
                    line[volume_col] / net_volume if net_volume > 0 else 0
                )
                reversed_coefs_integral = list(reversed(coefs_integral))
                min_integral = np.polyval(
                    reversed_coefs_integral,
                    min_volume,
                )
                max_integral = np.polyval(
                    reversed_coefs_integral,
                    percent_volume * net_volume + min_volume,
                )
                hmon = (max_integral - min_integral) / (
                    percent_volume * net_volume
                )
            else:
                reversed_coefs = list(reversed(coefs))
                hmon = np.polyval(
                    reversed_coefs,
                    line[RUN_OF_RIVER_REFERENCE_VOLUME_COL],
                )
            return hmon

        def _fill_volume_run_of_river(line: pd.Series):
            if pd.isna(line[volume_col]):
                return 0.0
            else:
                return line[volume_col]

        def _apply_losses(line: pd.Series, col: str):
            if line[LOSS_KIND_COL] == 1:
                return line[col] * (1 - line[LOSS_COL])
            elif line[LOSS_KIND_COL] == 2:
                return line[col] - line[LOSS_COL]

        def _eval_productivity(df: pd.DataFrame):
            df[UPPER_DROP_COL] = df.apply(
                _evaluate_upper_drop_at_volume, axis=1
            )
            df[NET_DROP_COL] = df[UPPER_DROP_COL] - df[LOWER_DROP_COL]
            df[volume_col] = df.apply(_fill_volume_run_of_river, axis=1)
            df[PRODUCTIVITY_TMP_COL] = df[SPEC_PRODUCTIVITY_COL] * df.apply(
                partial(_apply_losses, col=NET_DROP_COL), axis=1
            )
            df[PRODUCTIVITY_TMP_COL] *= HM3_M3S_MONTHLY_FACTOR
            return df

        return _eval_productivity(df)

    @classmethod
    def _accumulate_productivity(cls, df: pd.DataFrame) -> pd.DataFrame:
        np_edges = list(
            df.reset_index()[[FOLLOWING_HYDRO_COL, HYDRO_CODE_COL]].to_numpy()
        )
        edges = [tuple(e) for e in np_edges]
        bfs = Graph(edges, directed=True).bfs(0)[1:]
        for hydro_code in bfs:
            downstream_hydro_code = df.at[
                hydro_code,
                FOLLOWING_HYDRO_COL,
            ]
            if downstream_hydro_code == 0:
                continue
            downstream_productivity = df.at[
                downstream_hydro_code,
                PRODUCTIVITY_TMP_COL,
            ]
            df.at[hydro_code, PRODUCTIVITY_TMP_COL] += downstream_productivity
        return df

    @classmethod
    def _hydro_accumulated_productivity_at_volume(
        cls,
        uow: AbstractUnitOfWork,
        df: pd.DataFrame,
        volume_col: str = VOLUME_FOR_PRODUCTIVITY_TMP_COL,
    ) -> pd.DataFrame:
        """
        Calcula a produtividade acumulada das usinas hidrelétricas
        fornecidas em `df`, em um volume dado na coluna `volume_col`.
        O `df` fornecido deve ter como `index` o código das usinas.
        """

        def _join_hidr_data(df: pd.DataFrame) -> pd.DataFrame:
            hidr = cls.hidr(uow)
            hidr_cols = [
                RUN_OF_RIVER_REFERENCE_VOLUME_COL,
                LOSS_COL,
                LOSS_KIND_COL,
                LOWER_DROP_COL,
                SPEC_PRODUCTIVITY_COL,
                VOLUME_REGULATION_COL,
            ]
            return df.join(
                hidr[hidr_cols + HEIGHT_POLY_COLS],
                how="inner",
            )

        def _join_bounds_data(df: pd.DataFrame) -> pd.DataFrame:
            bounds_df = cls.hydro_volume_bounds_with_changes(uow)
            return df.join(bounds_df, how="inner")

        def _join_hydros_data(df: pd.DataFrame) -> pd.DataFrame:
            hydros = cls.hydros(uow)
            return df.join(hydros[[FOLLOWING_HYDRO_COL]], how="inner")

        df = df.copy()
        df_cols = df.columns.tolist()
        df = _join_hidr_data(df)
        df = _join_bounds_data(df)
        df = _join_hydros_data(df)
        df = cls._evaluate_productivity(df, volume_col=volume_col)
        df = cls._accumulate_productivity(df)
        return df[df_cols + [PRODUCTIVITY_TMP_COL]]

    @classmethod
    def _initial_stored_energy_from_confhd_hidr(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame | None:
        """
        Realiza o cálculo da energia armazenada inicial a partir
        de entrada de dados do CONFHD e HIDR.

        Como premissa, para o cálculo dessa energia, considera-se:

        - Usina fio d'água contribuem para o acúmulo da produtividade,
        mas não com energia armazenada.
        - As modificações de CFUGA e CMONT são consideradas para cálculo
        da produtividade nas usinas a fio d'água.
        - As modificações de VOLMIN, VOLMAX, VMINT e VMAXT não são
        consideradas.
        """

        def _join_drop_data(df: pd.DataFrame) -> pd.DataFrame:
            drops_df = cls.hydro_drops_in_stages(uow)
            stage_date = cls.stages_starting_dates_final_simulation(uow)[0]
            drops_df = drops_df.loc[
                drops_df[START_DATE_COL] == stage_date
            ].set_index(HYDRO_CODE_COL)
            return df.drop(columns=["usina"]).join(drops_df, how="inner")

        def _join_bounds_data(df: pd.DataFrame) -> pd.DataFrame:
            bounds_df = cls.hydro_volume_bounds_with_changes(uow)
            return df.join(
                bounds_df[[LOWER_BOUND_COL, UPPER_BOUND_COL]], how="inner"
            )

        def _volume_to_energy(df: pd.DataFrame) -> pd.DataFrame:
            df.loc[df[VOLUME_REGULATION_COL] != "M", ABSOLUTE_VALUE_COL] = 0.0
            df[ABSOLUTE_VALUE_COL] *= df[PRODUCTIVITY_TMP_COL]
            df.loc[df[VOLUME_REGULATION_COL] != "M", MAX_STORED_VOLUME_COL] = (
                0.0
            )
            df[MAXIMUM_STORED_ENERGY_COL] = (
                df[MAX_STORED_VOLUME_COL] * df[MAX_PRODUCTIVITY_COL]
            )
            return df

        def _cast_to_eers_and_fill_missing(df: pd.DataFrame) -> pd.DataFrame:
            df = df.join(
                cls.hydro_eer_submarket_map(uow).drop(columns=["usina"]),
                how="inner",
            )
            df = (
                df[
                    [
                        EER_CODE_COL,
                        EER_NAME_COL,
                        ABSOLUTE_VALUE_COL,
                        MAXIMUM_STORED_ENERGY_COL,
                    ]
                ]
                .groupby([EER_CODE_COL, EER_NAME_COL])
                .sum()
            ).reset_index()
            eer_codes = cls.eer_code_order(uow)
            eers = cls.eers(uow)
            missing_eers = [
                eer for eer in eer_codes if eer not in df[EER_CODE_COL].tolist()
            ]
            missing_df = pd.DataFrame({
                EER_CODE_COL: missing_eers,
                EER_NAME_COL: eers.loc[missing_eers, EER_NAME_COL].tolist(),
                ABSOLUTE_VALUE_COL: [np.nan] * len(missing_eers),
                PERCENT_VALUE_COL: [100.0] * len(missing_eers),
            })
            if not missing_df.empty:
                df = pd.concat([df, missing_df], ignore_index=True)
            df[EER_CODE_COL] = df[EER_CODE_COL].astype(int)
            return df.set_index(EER_CODE_COL)

        def _eval_percent_value(df: pd.DataFrame) -> pd.DataFrame:
            df[PERCENT_VALUE_COL] = (
                df[ABSOLUTE_VALUE_COL] / df[MAXIMUM_STORED_ENERGY_COL] * 100.0
            )
            return df

        def _join_hydros_data(df: pd.DataFrame) -> pd.DataFrame:
            hydros = cls.hydros(uow)
            return df.join(hydros[[FOLLOWING_HYDRO_COL]], how="inner")

        MAX_PRODUCTIVITY_COL = "prod_max"
        MAX_STORED_VOLUME_COL = "varmax"
        MAXIMUM_STORED_ENERGY_COL = "earmax"
        ABSOLUTE_VALUE_COL = "valor_hm3"
        ABSOLUTE_VALUE_FINAL_COL = "valor_MWmes"
        PERCENT_VALUE_COL = "valor_percentual"

        df = cls.initial_stored_volume(uow).set_index(HYDRO_CODE_COL)

        # Calcula prodts no ponto inicial
        absolute_df = df.copy()
        absolute_df = _join_drop_data(absolute_df)
        absolute_df = _join_bounds_data(absolute_df)
        absolute_df = _join_hydros_data(absolute_df)
        absolute_df = cls._evaluate_productivity(
            absolute_df, volume_col=ABSOLUTE_VALUE_COL
        )
        absolute_df = cls._accumulate_productivity(absolute_df)

        # Calcula prodts no máximo
        df_cols = df.columns
        percent_df = _join_bounds_data(df.copy())
        percent_df[ABSOLUTE_VALUE_COL] = (
            percent_df[UPPER_BOUND_COL] - percent_df[LOWER_BOUND_COL]
        )
        percent_df = percent_df[df_cols]
        percent_df = _join_drop_data(percent_df)
        percent_df = _join_bounds_data(percent_df)
        percent_df = _join_hydros_data(percent_df)
        percent_df[ABSOLUTE_VALUE_COL] = (
            percent_df[UPPER_BOUND_COL] - percent_df[LOWER_BOUND_COL]
        )
        percent_df = cls._evaluate_productivity(
            percent_df, volume_col=ABSOLUTE_VALUE_COL
        )
        percent_df = cls._accumulate_productivity(percent_df)
        percent_df = percent_df.rename(
            columns={
                ABSOLUTE_VALUE_COL: MAX_STORED_VOLUME_COL,
                PRODUCTIVITY_TMP_COL: MAX_PRODUCTIVITY_COL,
            }
        )

        # Combina os resultados para calcular EARMi em % da EARMax
        df = absolute_df.join(
            percent_df[[MAX_STORED_VOLUME_COL, MAX_PRODUCTIVITY_COL]],
            how="inner",
        )
        df = _volume_to_energy(df)
        df = _cast_to_eers_and_fill_missing(
            df[
                [
                    ABSOLUTE_VALUE_COL,
                    MAX_STORED_VOLUME_COL,
                    MAXIMUM_STORED_ENERGY_COL,
                ]
            ]
        )
        df = _eval_percent_value(df)
        df = df.rename(columns={ABSOLUTE_VALUE_COL: ABSOLUTE_VALUE_FINAL_COL})

        return df[[EER_NAME_COL, ABSOLUTE_VALUE_FINAL_COL, PERCENT_VALUE_COL]]

    @classmethod
    def initial_stored_energy(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        initial_stored_energy = cls.DECK_DATA_CACHING.get(
            "initial_stored_energy"
        )
        if initial_stored_energy is None:
            initial_stored_energy = cls._initial_stored_energy_from_pmo(uow)
            if initial_stored_energy is None:
                initial_stored_energy = (
                    cls._initial_stored_energy_from_confhd_hidr(uow)
                )
            initial_stored_energy = cls._validate_data(
                initial_stored_energy,
                pd.DataFrame,
                "energia armazenada inicial por REE (pmo.dat ou calculada)",
            )
            cls.DECK_DATA_CACHING["initial_stored_energy"] = (
                initial_stored_energy
            )
        return initial_stored_energy.copy()

    @classmethod
    def _initial_stored_volume_from_pmo(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame | None:
        df = cls.pmo(uow).volume_armazenado_inicial
        if df is None:
            return df
        return df.rename(
            columns={
                "codigo_usina": HYDRO_CODE_COL,
                "nome_usina": HYDRO_NAME_COL,
            }
        )

    @classmethod
    def _initial_stored_volume_from_confhd_hidr(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame | None:
        df = cls.confhd(uow)[
            [HYDRO_CODE_COL, "nome_usina", "volume_inicial_percentual"]
        ].set_index(HYDRO_CODE_COL)
        df = df.rename(
            columns={
                "nome_usina": HYDRO_NAME_COL,
                "volume_inicial_percentual": "valor_percentual",
            }
        )
        hidr = cls.hidr(uow)
        volume_bounds = cls.hydro_volume_bounds_with_changes(uow)
        volume_bounds = volume_bounds[[LOWER_BOUND_COL, UPPER_BOUND_COL]]
        df = df.join(hidr, how="inner")
        df = df.join(volume_bounds, how="inner")
        df["valor_hm3"] = df.apply(
            lambda line: line["valor_percentual"]
            / 100.0
            * (line[UPPER_BOUND_COL] - line[LOWER_BOUND_COL]),
            axis=1,
        )
        df.loc[df["tipo_regulacao"] != "M", "valor_hm3"] = np.nan
        df.loc[df["tipo_regulacao"] != "M", "valor_percentual"] = 0.0

        return df[
            [HYDRO_NAME_COL, "valor_hm3", "valor_percentual"]
        ].reset_index()

    @classmethod
    def _initial_stored_volume_pre_study_condition(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        if cls.num_pre_study_period_years(uow) > 0:
            df.loc[~df["valor_hm3"].isna(), "valor_percentual"] = 100.0
        return df

    @classmethod
    def initial_stored_volume(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        initial_stored_volume = cls.DECK_DATA_CACHING.get(
            "initial_stored_volume"
        )
        if initial_stored_volume is None:
            initial_stored_volume = cls._initial_stored_volume_from_pmo(uow)
            if initial_stored_volume is None:
                initial_stored_volume = (
                    cls._initial_stored_volume_from_confhd_hidr(uow)
                )
            initial_stored_volume = cls._validate_data(
                initial_stored_volume,
                pd.DataFrame,
                "volume armazenado inicial por UHE (pmo.dat ou calculado)",
            )
            initial_stored_volume = (
                cls._initial_stored_volume_pre_study_condition(
                    initial_stored_volume, uow
                )
            )
            cls.DECK_DATA_CACHING["initial_stored_volume"] = (
                initial_stored_volume
            )
        return initial_stored_volume.copy()

    @classmethod
    def eer_code_order(cls, uow: AbstractUnitOfWork) -> List[int]:
        eer_code_order = cls.DECK_DATA_CACHING.get("eer_code_order")
        if eer_code_order is None:
            eer_code_order = cls.eers(uow).index.tolist()
            cls.DECK_DATA_CACHING["eer_code_order"] = eer_code_order
        return eer_code_order

    @classmethod
    def hydro_code_order(cls, uow: AbstractUnitOfWork) -> List[int]:
        hydro_code_order = cls.DECK_DATA_CACHING.get("hydro_code_order")
        if hydro_code_order is None:
            hydro_code_order = cls.hydros(uow).index.tolist()
            cls.DECK_DATA_CACHING["hydro_code_order"] = hydro_code_order
        return hydro_code_order

    @classmethod
    def hydro_eer_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        aux_df = cls.DECK_DATA_CACHING.get("hydro_eer_submarket_map")
        if aux_df is None:
            hydros = cls.hydros(uow)
            eers = cls.eers(uow)
            submarkets = cls.submarkets(uow)
            aux_df = hydros[[HYDRO_NAME_COL, EER_CODE_COL]].copy()
            aux_df = aux_df.join(
                eers[[EER_NAME_COL, SUBMARKET_CODE_COL]], on=EER_CODE_COL
            )
            aux_df = aux_df.join(
                submarkets[[SUBMARKET_NAME_COL]], on=SUBMARKET_CODE_COL
            )
            cls.DECK_DATA_CACHING["hydro_eer_submarket_map"] = aux_df
        return aux_df.copy()

    @classmethod
    def eer_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        aux_df = cls.DECK_DATA_CACHING.get("eer_submarket_map")
        if aux_df is None:
            aux_df = cls.hydro_eer_submarket_map(uow)
            aux_df = aux_df.drop_duplicates(subset=[EER_CODE_COL]).reset_index(
                drop=True
            )[
                [
                    EER_CODE_COL,
                    EER_NAME_COL,
                    SUBMARKET_CODE_COL,
                    SUBMARKET_NAME_COL,
                ]
            ]
            aux_df = aux_df.set_index(EER_CODE_COL)
            cls.DECK_DATA_CACHING["eer_submarket_map"] = aux_df
        return aux_df.copy()

    @classmethod
    def thermal_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        aux_df = cls.DECK_DATA_CACHING.get("thermal_submarket_map")
        if aux_df is None:
            thermals = Deck.thermals(uow).reset_index()
            thermals = thermals.set_index(THERMAL_NAME_COL)
            submarkets = cls.submarkets(uow)

            aux_df = pd.DataFrame(
                data={
                    THERMAL_CODE_COL: thermals[THERMAL_CODE_COL].tolist(),
                    THERMAL_NAME_COL: thermals.index.tolist(),
                    SUBMARKET_CODE_COL: thermals[SUBMARKET_CODE_COL].tolist(),
                }
            )
            aux_df[SUBMARKET_NAME_COL] = aux_df[SUBMARKET_CODE_COL].apply(
                lambda c: submarkets.at[c, SUBMARKET_NAME_COL]
            )
            aux_df = aux_df.set_index(THERMAL_CODE_COL)
            cls.DECK_DATA_CACHING["thermal_submarket_map"] = aux_df
        return aux_df.copy()

    @classmethod
    def _policy_df_building_block(
        cls, cut_df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("_policy_df_building_block")
        if df is None:
            stages = cut_df[STAGE_COL].unique().tolist()
            num_stages = len(stages)
            cut_indexes = cut_df[CUT_INDEX_COL].unique().tolist()
            num_series = cls.num_forward_series(uow)
            num_iterations = len(cut_indexes) // (num_series * num_stages)
            df = pd.DataFrame(
                data={
                    STAGE_COL: np.repeat(
                        np.array(stages), num_iterations * num_series
                    ),
                    CUT_INDEX_COL: cut_indexes,
                    ITERATION_COL: np.tile(
                        np.repeat(np.arange(1, num_iterations + 1), num_series)[
                            ::-1
                        ],
                        num_stages,
                    ),
                    SCENARIO_COL: np.tile(
                        np.tile(np.arange(num_series, 0, -1), num_iterations),
                        num_stages,
                    ),
                }
            )
            df[COEF_TYPE_COL] = ""
            df[ENTITY_INDEX_COL] = 0
            df[LAG_COL] = 0
            df[BLOCK_COL] = 0
            df[COEF_VALUE_COL] = np.nan
            df[STATE_VALUE_COL] = np.nan
            cls.DECK_DATA_CACHING["_policy_df_building_block"] = df
        return df.copy()

    @classmethod
    def _rhs_entities(
        cls,
        cut_df: pd.DataFrame,
        state_df: pd.DataFrame | None,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        cut_cols = cut_df.columns.tolist()
        rhs_col = "RHS"
        obj_func_col = "FUNC.OBJ."
        entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]
        num_entities = cut_df[entity_col].unique().shape[0]
        base_df = cls._policy_df_building_block(cut_df, uow)

        rhs_df = cut_df.iloc[::num_entities]
        base_df[COEF_VALUE_COL] = rhs_df[rhs_col].to_numpy()

        if state_df is not None:
            obj_df = state_df.iloc[::num_entities]
            base_df[STATE_VALUE_COL] = obj_df[obj_func_col].to_numpy()

        base_df[COEF_TYPE_COL] = RHS_COEF_CODE

        return base_df

    @classmethod
    def _eer_hydro_cut_entities(
        cls,
        entity_col: str,
        cut_value_col: str,
        state_value_col: str | None,
        coef_type_value: int,
        lag: int,
        cut_df: pd.DataFrame,
        state_df: pd.DataFrame | None,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        entity_indices = np.array(cut_df[entity_col].unique().tolist())

        num_entities = len(entity_indices)
        df = pd.concat(
            [
                cls._policy_df_building_block(cut_df, uow)
                for _ in range(num_entities)
            ],
            ignore_index=True,
        )
        df[COEF_TYPE_COL] = coef_type_value
        df[LAG_COL] = lag

        df = df.sort_values(
            [STAGE_COL, CUT_INDEX_COL, SCENARIO_COL],
            ascending=[True, False, False],
        ).reset_index(drop=True)
        num_repeats = cut_df.shape[0] // num_entities
        df[ENTITY_INDEX_COL] = np.tile(entity_indices, num_repeats)

        df[COEF_VALUE_COL] = cut_df[cut_value_col].to_numpy()
        if state_df is not None:
            df[STATE_VALUE_COL] = state_df[state_value_col].to_numpy()
        return df

    @classmethod
    def _storage_cut_entities(
        cls,
        cut_df: pd.DataFrame,
        state_df: pd.DataFrame | None,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        cut_cols = cut_df.columns.tolist()
        entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]
        cut_value_col = [c for c in cut_cols if c in ["PIEARM", "PIVARM"]][0]

        state_cols = state_df.columns.tolist() if state_df is not None else []
        state_value_col = (
            [c for c in state_cols if c in ["EARM", "VARM"]][0]
            if state_df is not None
            else None
        )

        coef_type_map = {
            "REE": EARM_COEF_CODE,
            "UHE": VARM_COEF_CODE,
        }
        coef_type_value = coef_type_map[entity_col]

        df = cls._eer_hydro_cut_entities(
            entity_col,
            cut_value_col,
            state_value_col,
            coef_type_value,
            0,
            cut_df,
            state_df,
            uow,
        )
        return df

    @classmethod
    def _inflow_cut_entities(
        cls,
        cut_df: pd.DataFrame,
        state_df: pd.DataFrame | None,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        cut_cols = cut_df.columns.tolist()
        entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]

        max_ar_lag = cls.num_stages_with_past_tendency_period(uow)

        dfs: list[pd.DataFrame] = []
        for lag in range(1, max_ar_lag + 1):
            cut_value_col = [
                c for c in cut_cols if c in [f"PIH({lag})", f"PIAFL({lag})"]
            ][0]

            state_cols = (
                state_df.columns.tolist() if state_df is not None else []
            )
            state_value_col = (
                [c for c in state_cols if c in [f"EAF({lag})", f"VAF({lag})"]][
                    0
                ]
                if state_df is not None
                else None
            )

            coef_type_map = {
                "REE": ENA_COEF_CODE,
                "UHE": QINC_COEF_CODE,
            }
            coef_type_value = coef_type_map[entity_col]
            df = cls._eer_hydro_cut_entities(
                entity_col,
                cut_value_col,
                state_value_col,
                coef_type_value,
                lag,
                cut_df,
                state_df,
                uow,
            )
            dfs.append(df)
        return pd.concat(dfs, ignore_index=True)

    @classmethod
    def _eer_in_hydro_cut_entities(
        cls,
        entity_col: str,
        cut_value_col: str,
        state_value_col: str | None,
        coef_type_value: int,
        cut_df: pd.DataFrame,
        state_df: pd.DataFrame | None,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        hydro_df = cls.hydro_eer_submarket_map(uow)
        hydro_df = hydro_df.reset_index().drop_duplicates(subset=[EER_CODE_COL])
        entity_indices = hydro_df[HYDRO_CODE_COL].tolist()
        eer_indices = hydro_df[EER_CODE_COL].tolist()

        num_entities = len(entity_indices)
        df = pd.concat(
            [
                cls._policy_df_building_block(cut_df, uow)
                for _ in range(num_entities)
            ],
            ignore_index=True,
        )
        df[COEF_TYPE_COL] = coef_type_value
        df[LAG_COL] = 0
        df[BLOCK_COL] = 0

        df = df.sort_values(
            [STAGE_COL, CUT_INDEX_COL, SCENARIO_COL],
            ascending=[True, False, False],
        ).reset_index(drop=True)
        filtered_cut_df = cut_df.loc[cut_df[entity_col].isin(entity_indices)]
        num_repeats = filtered_cut_df.shape[0] // num_entities
        df[ENTITY_INDEX_COL] = np.tile(eer_indices, num_repeats)

        df[COEF_VALUE_COL] = filtered_cut_df[cut_value_col].to_numpy()
        if state_df is not None:
            df[STATE_VALUE_COL] = state_df.loc[
                state_df[entity_col].isin(entity_indices), state_value_col
            ].to_numpy()
        return df

    @classmethod
    def _maxviol_cut_entities(
        cls,
        cut_df: pd.DataFrame,
        state_df: pd.DataFrame | None,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        cut_cols = cut_df.columns.tolist()
        entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]
        cut_value_col = "PIMX_VMN"
        state_value_col = "MX_CURVA"

        coef_type_value = MAXVIOL_COEF_CODE

        if entity_col == "REE":
            df = cls._eer_hydro_cut_entities(
                entity_col,
                cut_value_col,
                state_value_col,
                coef_type_value,
                0,
                cut_df,
                state_df,
                uow,
            )
        else:
            df = cls._eer_in_hydro_cut_entities(
                entity_col,
                cut_value_col,
                state_value_col,
                coef_type_value,
                cut_df,
                state_df,
                uow,
            )
        return df

    @classmethod
    def _submarket_cut_entities(
        cls,
        entity_col: str,
        cut_value_col: str,
        state_value_col: str | None,
        coef_type_value: int,
        lag: int,
        block: int,
        cut_df: pd.DataFrame,
        state_df: pd.DataFrame | None,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        if entity_col == "REE":
            eer_df = cls.eers(uow)
            eer_df = eer_df.reset_index().drop_duplicates(
                subset=[SUBMARKET_CODE_COL]
            )
            entity_indices = eer_df[EER_CODE_COL].tolist()
            sbm_indices = eer_df[SUBMARKET_CODE_COL].tolist()
        else:
            hydro_df = cls.hydro_eer_submarket_map(uow)
            hydro_df = hydro_df.reset_index().drop_duplicates(
                subset=[SUBMARKET_CODE_COL]
            )
            entity_indices = hydro_df[HYDRO_CODE_COL].tolist()
            sbm_indices = hydro_df[SUBMARKET_CODE_COL].tolist()

        num_entities = len(entity_indices)
        df = pd.concat(
            [
                cls._policy_df_building_block(cut_df, uow)
                for _ in range(num_entities)
            ],
            ignore_index=True,
        )
        df[COEF_TYPE_COL] = coef_type_value
        df[LAG_COL] = lag
        df[BLOCK_COL] = block

        df = df.sort_values(
            [STAGE_COL, CUT_INDEX_COL, SCENARIO_COL],
            ascending=[True, False, False],
        ).reset_index(drop=True)
        filtered_cut_df = cut_df.loc[cut_df[entity_col].isin(entity_indices)]
        num_repeats = filtered_cut_df.shape[0] // num_entities
        df[ENTITY_INDEX_COL] = np.tile(sbm_indices, num_repeats)

        df[COEF_VALUE_COL] = filtered_cut_df[cut_value_col].to_numpy()
        if state_df is not None:
            df[STATE_VALUE_COL] = state_df.loc[
                state_df[entity_col].isin(entity_indices), state_value_col
            ].to_numpy()
        return df

    @classmethod
    def _thermal_generation_cut_entities(
        cls,
        cut_df: pd.DataFrame,
        state_df: pd.DataFrame | None,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        cut_cols = cut_df.columns.tolist()
        entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]

        num_blocks = cls.num_blocks(uow)
        max_thermal_lag = MAX_THERMAL_DISPATCH_LAG

        dfs: list[pd.DataFrame] = []
        for block in range(1, num_blocks + 1):
            for lag in range(1, max_thermal_lag + 1):
                cut_value_col = [
                    c for c in cut_cols if c == f"PIGTAD(P{block}L{lag})"
                ][0]

                state_cols = (
                    state_df.columns.tolist() if state_df is not None else []
                )
                state_value_col = (
                    [c for c in state_cols if c == f"SGT(P{block}E{lag})"][0]
                    if state_df is not None
                    else None
                )

                coef_type_value = GTER_COEF_CODE
                df = cls._submarket_cut_entities(
                    entity_col,
                    cut_value_col,
                    state_value_col,
                    coef_type_value,
                    lag,
                    block,
                    cut_df,
                    state_df,
                    uow,
                )
                dfs.append(df)
        return pd.concat(dfs, ignore_index=True)

    @classmethod
    def common_policy_df(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        aux_df = cls.DECK_DATA_CACHING.get("common_policy_df")

        if aux_df is None:
            nwlistcfrel = cls._validate_data(
                cls._get_cortes(uow),
                Nwlistcfrel,
                "Relatório de cortes do NWLISTCF",
            )
            cut_df = cls._validate_data(
                nwlistcfrel.cortes,
                pd.DataFrame,
                "Relatório de cortes do NWLISTCF",
            )
            cut_df = cut_df.rename(
                columns={
                    "PERIODO": STAGE_COL,
                    "IREG": CUT_INDEX_COL,
                }
            )
            cut_df[STAGE_COL] -= cls.study_period_starting_month(uow) - 1
            estadosrel = cls._validate_data(
                cls._get_estados(uow),
                Estados,
                "Relatório de estados do NWLISTCF",
            )
            state_df = estadosrel.estados
            if state_df is not None:
                state_df = state_df.rename(
                    columns={
                        "PERIODO": STAGE_COL,
                        "IREG": CUT_INDEX_COL,
                        "ITEc": ITERATION_COL,
                        "SIMc": SCENARIO_COL,
                    }
                ).drop(columns=["ITEf"])
                state_df[STAGE_COL] -= cls.study_period_starting_month(uow) - 1

            # Builds policy df
            aux_df = pd.concat(
                [
                    cls._rhs_entities(cut_df, state_df, uow),
                    cls._storage_cut_entities(cut_df, state_df, uow),
                    cls._inflow_cut_entities(cut_df, state_df, uow),
                    cls._thermal_generation_cut_entities(cut_df, state_df, uow),
                    cls._maxviol_cut_entities(cut_df, state_df, uow),
                ],
                ignore_index=True,
            )

            cls.DECK_DATA_CACHING["common_policy_df"] = aux_df
        return aux_df.copy()

    @classmethod
    def policy_variable_units(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "policy_variable_units"
        df = cls.DECK_DATA_CACHING.get(name)
        MAP_COEF_TYPE_SHORT_NAME = {
            RHS_COEF_CODE: "RHS",
            EARM_COEF_CODE: "EARM",
            VARM_COEF_CODE: "VARM",
            ENA_COEF_CODE: "ENA",
            QINC_COEF_CODE: "QINC",
            GTER_COEF_CODE: "GTER",
            MAXVIOL_COEF_CODE: "MAXVIOL",
        }
        MAP_COEF_TYPE_LONG_NAME = {
            RHS_COEF_CODE: "Right hand side",
            EARM_COEF_CODE: "Energia armazenada",
            VARM_COEF_CODE: "Volume armazenado",
            ENA_COEF_CODE: "Energia natural afluente",
            QINC_COEF_CODE: "Vazão incremental",
            GTER_COEF_CODE: "Geração térmica antecipada",
            MAXVIOL_COEF_CODE: "Máxima violação de volume mínimo operativo",
        }
        MAP_COEF_TYPE_UNIT = {
            RHS_COEF_CODE: PolicyUnit.RS_mes_h.value,
            EARM_COEF_CODE: PolicyUnit.RS_MWh.value,
            VARM_COEF_CODE: PolicyUnit.RS_mes_hm3_MWh.value,
            ENA_COEF_CODE: PolicyUnit.RS_MWh.value,
            QINC_COEF_CODE: PolicyUnit.RS_mes_hm3_MWh.value,
            GTER_COEF_CODE: PolicyUnit.RS_MWh.value,
            MAXVIOL_COEF_CODE: PolicyUnit.RS_MWh.value,
        }
        MAP_COEF_TYPE_STATE_UNIT = {
            RHS_COEF_CODE: PolicyUnit.RS.value,
            EARM_COEF_CODE: PolicyUnit.MWmes.value,
            VARM_COEF_CODE: PolicyUnit.hm3.value,
            ENA_COEF_CODE: PolicyUnit.MWmes.value,
            QINC_COEF_CODE: PolicyUnit.hm3.value,
            GTER_COEF_CODE: PolicyUnit.MWmes.value,
            MAXVIOL_COEF_CODE: PolicyUnit.MWmes.value,
        }
        if df is None:
            cuts_df = cls.common_policy_df(uow)
            df = cuts_df[
                [
                    COEF_TYPE_COL,
                ]
            ].drop_duplicates()
            df["nome_curto_coeficiente"] = df[COEF_TYPE_COL].replace(
                MAP_COEF_TYPE_SHORT_NAME
            )
            df["nome_longo_coeficiente"] = df[COEF_TYPE_COL].replace(
                MAP_COEF_TYPE_LONG_NAME
            )
            df["unidade_coeficiente"] = df[COEF_TYPE_COL].replace(
                MAP_COEF_TYPE_UNIT
            )
            df["unidade_estado"] = df[COEF_TYPE_COL].replace(
                MAP_COEF_TYPE_STATE_UNIT
            )
            df = df.reset_index(drop=True)
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()
