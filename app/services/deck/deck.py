from inewave.newave import (
    Dger,
    Ree,
    Confhd,
    Modif,
    Conft,
    Sistema,
    Curva,
    Clast,
    Term,
    Manutt,
    Expt,
    Hidr,
    Patamar,
    Shist,
    Pmo,
    Newavetim,
    Vazoes,
    Engnat,
    Energiaf,
    Enavazf,
    Vazaof,
    Energiab,
    Enavazb,
    Vazaob,
    Energias,
    # Enavazs,
    Vazaos,
)
from inewave.newave.modelos.modif import (
    VOLMIN,
    VOLMAX,
    VMINT,
    VMAXT,
    VAZMIN,
    VAZMINT,
    VAZMAXT,
    TURBMINT,
    TURBMAXT,
    NUMCNJ,
    NUMMAQ,
)
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd  # type: ignore
import numpy as np  # type: ignore
from functools import partial
from typing import Any, Optional, TypeVar, Type, List, Tuple, Union, Dict
from cfinterface.components.register import Register
from app.services.unitofwork import AbstractUnitOfWork
from app.model.operation.unit import Unit
from app.utils.graph import Graph
from app.internal.constants import (
    STRING_DF_TYPE,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    THERMAL_CODE_COL,
    THERMAL_NAME_COL,
    EER_CODE_COL,
    EER_NAME_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    VALUE_COL,
    CONFIG_COL,
    START_DATE_COL,
    LOWER_BOUND_COL,
    UPPER_BOUND_COL,
    LOWER_BOUND_UNIT_COL,
    UPPER_BOUND_UNIT_COL,
    BLOCK_COL,
    SCENARIO_COL,
    PRODUCTIVITY_TMP_COL,
    VOLUME_FOR_PRODUCTIVITY_TMP_COL,
    HM3_M3S_MONTHLY_FACTOR,
    FOLLOWING_HYDRO_COL,
    HEIGHT_POLY_COLS,
    LOSS_KIND_COL,
    LOSS_COL,
    LOWER_DROP_COL,
    SPEC_PRODUCTIVITY_COL,
    VOLUME_REGULATION_COL,
    RUN_OF_RIVER_REFERENCE_VOLUME_COL,
    UPPER_DROP_COL,
    NET_DROP_COL,
)


class Deck:
    """
    Armazena as informações dos principais arquivos que
    são utilizados para o processo de síntese.
    """

    T = TypeVar("T")
    logger: Optional[logging.Logger] = None

    DECK_DATA_CACHING: Dict[str, Any] = {}

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
    def _get_shist(cls, uow: AbstractUnitOfWork) -> Shist:
        with uow:
            shist = uow.files.get_shist()
            if shist is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do shist.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return shist

    @classmethod
    def _get_curva(cls, uow: AbstractUnitOfWork) -> Curva:
        with uow:
            curva = uow.files.get_curva()
            if curva is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do curva.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return curva

    @classmethod
    def _get_ree(cls, uow: AbstractUnitOfWork) -> Ree:
        with uow:
            ree = uow.files.get_ree()
            if ree is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do ree.dat para" + " síntese da operação"
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
    def _get_modif(cls, uow: AbstractUnitOfWork) -> Modif:
        with uow:
            modif = uow.files.get_modif()
            if modif is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do modif.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return modif

    @classmethod
    def _get_hidr(cls, uow: AbstractUnitOfWork) -> Hidr:
        with uow:
            hidr = uow.files.get_hidr()
            if hidr is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do hidr.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return hidr

    @classmethod
    def _get_vazoes(cls, uow: AbstractUnitOfWork) -> Vazoes:
        with uow:
            vazoes = uow.files.get_vazoes()
            if vazoes is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do vazoes.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return vazoes

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
    def _get_term(cls, uow: AbstractUnitOfWork) -> Term:
        with uow:
            sist = uow.files.get_term()
            if sist is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do term.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return sist

    @classmethod
    def _get_manutt(cls, uow: AbstractUnitOfWork) -> Manutt:
        with uow:
            sist = uow.files.get_manutt()
            if sist is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do manutt.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return sist

    @classmethod
    def _get_expt(cls, uow: AbstractUnitOfWork) -> Expt:
        with uow:
            sist = uow.files.get_expt()
            if sist is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do expt.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return sist

    @classmethod
    def _get_patamar(cls, uow: AbstractUnitOfWork) -> Patamar:
        with uow:
            pat = uow.files.get_patamar()
            if pat is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do patamar.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return pat

    @classmethod
    def _get_pmo(cls, uow: AbstractUnitOfWork) -> Pmo:
        with uow:
            pmo = uow.files.get_pmo()
            if pmo is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do pmo.dat para" + " síntese da operação"
                    )
                raise RuntimeError()
            return pmo

    @classmethod
    def _get_newavetim(cls, uow: AbstractUnitOfWork) -> Newavetim:
        with uow:
            newavetim = uow.files.get_newavetim()
            if newavetim is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do newave.tim para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return newavetim

    @classmethod
    def _get_engnat(cls, uow: AbstractUnitOfWork) -> Engnat:
        with uow:
            engnat = uow.files.get_engnat()
            if engnat is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do engnat.dat para"
                        + " síntese dos cenários"
                    )
                raise RuntimeError()
            return engnat

    @classmethod
    def _get_energiaf(
        cls, iteracao: int, uow: AbstractUnitOfWork
    ) -> Optional[Energiaf]:
        with uow:
            energiaf = uow.files.get_energiaf(iteracao)
            return energiaf

    @classmethod
    def _get_enavazf(cls, iteracao: int, uow: AbstractUnitOfWork) -> Optional[Enavazf]:
        with uow:
            enavazf = uow.files.get_enavazf(iteracao)
            return enavazf

    @classmethod
    def _get_vazaof(cls, iteracao: int, uow: AbstractUnitOfWork) -> Optional[Vazaof]:
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
    def _get_enavazb(cls, iteracao: int, uow: AbstractUnitOfWork) -> Optional[Enavazb]:
        with uow:
            enavazb = uow.files.get_enavazb(iteracao)
            return enavazb

    @classmethod
    def _get_vazaob(cls, iteracao: int, uow: AbstractUnitOfWork) -> Optional[Vazaob]:
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
    def _validate_data(cls, data, type: Type[T], msg: str = "dados") -> T:
        if not isinstance(data, type):
            if cls.logger is not None:
                cls.logger.error(f"Erro na leitura de {msg}")
            raise RuntimeError()
        return data

    @classmethod
    def dger(cls, uow: AbstractUnitOfWork) -> Dger:
        dger = cls.DECK_DATA_CACHING.get("dger")
        if dger is None:
            dger = cls._validate_data(
                cls._get_dger(uow),
                Dger,
                "dger",
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
                "pmo",
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
                "curva",
            )
            cls.DECK_DATA_CACHING["curva"] = curva
        return curva

    @classmethod
    def modif(cls, uow: AbstractUnitOfWork) -> Modif:
        modif = cls.DECK_DATA_CACHING.get("modif")
        if modif is None:
            modif = cls._validate_data(cls._get_modif(uow), Modif, "modif")
            cls.DECK_DATA_CACHING["modif"] = modif
        return modif

    @classmethod
    def confhd(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        confhd = cls.DECK_DATA_CACHING.get("confhd")
        if confhd is None:
            confhd = cls._validate_data(
                cls._get_confhd(uow).usinas, pd.DataFrame, "confhd"
            )
            cls.DECK_DATA_CACHING["confhd"] = confhd
        return confhd.copy()

    @classmethod
    def clast(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        clast = cls.DECK_DATA_CACHING.get("clast")
        if clast is None:
            clast = cls._validate_data(
                cls._get_clast(uow).usinas, pd.DataFrame, "clast"
            )
            cls.DECK_DATA_CACHING["clast"] = clast
        return clast.copy()

    @classmethod
    def term(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        term = cls.DECK_DATA_CACHING.get("term")
        if term is None:
            term = cls._validate_data(cls._get_term(uow).usinas, pd.DataFrame, "term")
            cls.DECK_DATA_CACHING["term"] = term
        return term.copy()

    @classmethod
    def manutt(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        manutt = cls.DECK_DATA_CACHING.get("manutt")
        if manutt is None:
            manutt = cls._validate_data(
                cls._get_manutt(uow).manutencoes, pd.DataFrame, "manutt"
            )
            cls.DECK_DATA_CACHING["manutt"] = manutt
        return manutt.copy()

    @classmethod
    def expt(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        expt = cls.DECK_DATA_CACHING.get("expt")
        if expt is None:
            expt = cls._validate_data(
                cls._get_expt(uow).expansoes, pd.DataFrame, "expt"
            )
            cls.DECK_DATA_CACHING["expt"] = expt
        return expt.copy()

    @classmethod
    def hidr(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        hidr = cls.DECK_DATA_CACHING.get("hidr")
        if hidr is None:
            hidr = cls._validate_data(cls._get_hidr(uow).cadastro, pd.DataFrame, "hidr")
            cls.DECK_DATA_CACHING["hidr"] = hidr
        return hidr.copy()

    @classmethod
    def newavetim(cls, uow: AbstractUnitOfWork) -> Newavetim:
        newavetim = cls.DECK_DATA_CACHING.get("newavetim")
        if newavetim is None:
            newavetim = cls._validate_data(
                cls._get_newavetim(uow),
                Newavetim,
                "newavetim",
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
                "engnat",
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
                return df.rename(columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL})
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
                return df.rename(columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL})
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
                return df.rename(columns={"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL})
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
                return df.rename(columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL})
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
                return df.rename(columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL})
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
                return df.rename(columns={"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL})
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
                return df.rename(columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL})
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
                return df.rename(columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL})
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
                return df.rename(columns={"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL})
        else:
            return pd.DataFrame()

    @classmethod
    def vazoes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        vazoes = cls.DECK_DATA_CACHING.get("vazoes")
        if vazoes is None:
            vazoes = cls._validate_data(
                cls._get_vazoes(uow).vazoes, pd.DataFrame, "vazoes"
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
                "mês de início do pré-estudo",
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
                "mês de início do estudo",
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
                "ano de início do estudo",
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
                "número de anos do pré-estudo",
            )
            cls.DECK_DATA_CACHING["num_pre_study_period_years"] = (
                num_pre_study_period_years
            )
        return num_pre_study_period_years

    @classmethod
    def num_study_period_years(cls, uow: AbstractUnitOfWork) -> int:
        num_study_period_years = cls.DECK_DATA_CACHING.get("num_study_period_years")
        if num_study_period_years is None:
            dger = cls.dger(uow)
            num_study_period_years = cls._validate_data(
                dger.num_anos_estudo,
                int,
                "número de anos do estudo",
            )
            cls.DECK_DATA_CACHING["num_study_period_years"] = num_study_period_years
        return num_study_period_years

    @classmethod
    def num_post_study_period_years_final_simulation(
        cls, uow: AbstractUnitOfWork
    ) -> int:
        num_post_study_period_years_final_simulation = cls.DECK_DATA_CACHING.get(
            "num_post_study_period_years_final_simulation"
        )
        if num_post_study_period_years_final_simulation is None:
            dger = cls.dger(uow)
            num_post_study_period_years_final_simulation = cls._validate_data(
                dger.num_anos_pos_sim_final,
                int,
                "número de anos do período pós-estudo na simulação",
            )
            cls.DECK_DATA_CACHING["num_post_study_period_years_final_simulation"] = (
                num_post_study_period_years_final_simulation
            )
        return num_post_study_period_years_final_simulation

    @classmethod
    def num_synthetic_scenarios_final_simulation(cls, uow: AbstractUnitOfWork) -> int:
        num_synthetic_scenarios_final_simulation = cls.DECK_DATA_CACHING.get(
            "num_synthetic_scenarios_final_simulation"
        )
        if num_synthetic_scenarios_final_simulation is None:
            dger = cls.dger(uow)
            num_synthetic_scenarios_final_simulation = cls._validate_data(
                dger.num_series_sinteticas,
                int,
                "número de séries sintéticas na simulação",
            )
            cls.DECK_DATA_CACHING["num_synthetic_scenarios_final_simulation"] = (
                num_synthetic_scenarios_final_simulation
            )
        return num_synthetic_scenarios_final_simulation

    @classmethod
    def num_history_years(cls, uow: AbstractUnitOfWork) -> int:
        num_history_years = cls.DECK_DATA_CACHING.get("num_history_years")
        if num_history_years is None:
            shist = cls._get_shist(uow)
            study_starting_year = cls.study_period_starting_year(uow)
            history_starting_year = cls._validate_data(
                shist.ano_inicio_varredura,
                int,
                "número de séries históricas na simulação",
            )
            num_history_years = study_starting_year - history_starting_year - 2
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
                "número de anos de manutenção de UTEs",
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
            thermal_maintenance_end_date = datetime(starting_year + num_years, 1, 1)
            cls.DECK_DATA_CACHING["thermal_maintenance_end_date"] = (
                thermal_maintenance_end_date
            )
        return thermal_maintenance_end_date

    @classmethod
    def final_simulation_type(cls, uow: AbstractUnitOfWork) -> int:
        final_simulation_type = cls.DECK_DATA_CACHING.get("final_simulation_type")
        if final_simulation_type is None:
            dger = cls.dger(uow)
            final_simulation_type = cls._validate_data(
                dger.tipo_simulacao_final,
                int,
                "tipo da simulação final",
            )
            cls.DECK_DATA_CACHING["final_simulation_type"] = final_simulation_type
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
                "tipo da simulação final",
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
            ano_inicio = cls.study_period_starting_year(uow)
            mes_inicio = cls.study_period_starting_month(uow)
            eers = cls.eers(uow)
            mes_fim_hib = eers["mes_fim_individualizado"].iloc[0]
            ano_fim_hib = eers["ano_fim_individualizado"].iloc[0]

            if mes_fim_hib is not None and ano_fim_hib is not None:
                data_inicio_estudo = datetime(
                    year=ano_inicio,
                    month=mes_inicio,
                    day=1,
                )
                data_fim_individualizado = datetime(
                    year=int(ano_fim_hib),
                    month=int(mes_fim_hib),
                    day=1,
                )
                tempo_individualizado = data_fim_individualizado - data_inicio_estudo
                num_hydro_simulation_stages_policy = int(
                    round(tempo_individualizado / timedelta(days=30))
                )
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
        num_hydro_simulation_stages_final_simulation = cls.DECK_DATA_CACHING.get(
            "num_hydro_simulation_stages_final_simulation"
        )
        if num_hydro_simulation_stages_final_simulation is None:
            aggergation = cls.final_simulation_aggregation(uow)
            starting_month = cls.study_period_starting_month(uow)
            study_years = cls.num_study_period_years(uow)
            post_study_years = cls.num_post_study_period_years_final_simulation(uow)
            if aggergation == 1:
                num_hydro_simulation_stages_final_simulation = (
                    study_years + post_study_years
                ) * 12 - (starting_month - 1)
            else:
                num_hydro_simulation_stages_final_simulation = (
                    cls.num_hydro_simulation_stages_policy(uow)
                )

            cls.DECK_DATA_CACHING["num_hydro_simulation_stages_final_simulation"] = (
                num_hydro_simulation_stages_final_simulation
            )
        return num_hydro_simulation_stages_final_simulation

    @classmethod
    def models_wind_generation(cls, uow: AbstractUnitOfWork) -> int:
        models_wind_generation = cls.DECK_DATA_CACHING.get("models_wind_generation")
        if models_wind_generation is None:
            models_wind_generation = cls._validate_data(
                cls.dger(uow).considera_geracao_eolica != 0,
                int,
                "consideração da geração eólica",
            )
            cls.DECK_DATA_CACHING["models_wind_generation"] = models_wind_generation
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
                "opção do modelo PAR(p)",
            )
            cls.DECK_DATA_CACHING["scenario_generation_model_type"] = (
                scenario_generation_model_type
            )
        return scenario_generation_model_type

    @classmethod
    def scenario_generation_model_max_order(cls, uow: AbstractUnitOfWork) -> int:
        scenario_generation_model_max_order = cls.DECK_DATA_CACHING.get(
            "scenario_generation_model_max_order"
        )
        if scenario_generation_model_max_order is None:
            scenario_generation_model_max_order = cls._validate_data(
                cls.dger(uow).ordem_maxima_parp,
                int,
                "ordem máxima do modelo PAR(p)",
            )
            cls.DECK_DATA_CACHING["scenario_generation_model_max_order"] = (
                scenario_generation_model_max_order
            )
        return scenario_generation_model_max_order

    @classmethod
    def num_stages_with_past_tendency_period(cls, uow: AbstractUnitOfWork) -> int:
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
    def ending_date_with_post_study_period(cls, uow: AbstractUnitOfWork) -> datetime:
        starting_year = cls.study_period_starting_year(uow)
        study_years = cls.num_study_period_years(uow)
        post_study_years_in_simulation = (
            cls.num_post_study_period_years_final_simulation(uow)
        )
        ending_date_with_post_study_years = datetime(
            year=starting_year + study_years + post_study_years_in_simulation - 1,
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
            internal_stages_starting_dates_policy_with_past_tendency = pd.date_range(
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
        internal_stages_starting_dates_final_simulation = cls.DECK_DATA_CACHING.get(
            "internal_stages_starting_dates_final_simulation"
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
            cls.DECK_DATA_CACHING["internal_stages_starting_dates_final_simulation"] = (
                internal_stages_starting_dates_final_simulation
            )
        return internal_stages_starting_dates_final_simulation

    @classmethod
    def internal_stages_ending_dates_final_simulation(
        cls, uow: AbstractUnitOfWork
    ) -> List[datetime]:
        internal_stages_ending_dates_final_simulation = cls.DECK_DATA_CACHING.get(
            "internal_stages_ending_dates_final_simulation"
        )
        if internal_stages_ending_dates_final_simulation is None:
            internal_stages_ending_dates_final_simulation = [
                d + relativedelta(months=1)
                for d in cls.internal_stages_starting_dates_final_simulation(uow)
            ]
            cls.DECK_DATA_CACHING["internal_stages_ending_dates_final_simulation"] = (
                internal_stages_ending_dates_final_simulation
            )
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
            post_study_years = cls.num_post_study_period_years_final_simulation(uow)
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
            configurations = configurations.rename(columns={"data": START_DATE_COL})
        return configurations

    @classmethod
    def _configurations_dger(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        dates = cls.stages_starting_dates_final_simulation(uow)
        conigurations = list(range(1, len(dates) + 1))
        return pd.DataFrame(data={VALUE_COL: conigurations, START_DATE_COL: dates})

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
    def eer_stored_energy_lower_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
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
                eer_df = df.loc[df[EER_CODE_COL] == eers_minimum_storage[0]].copy()
                eer_df[EER_CODE_COL] = c
                eer_df[VALUE_COL] = 0.0
                lower_bound_dfs.append(eer_df)
            lower_bound_df = pd.concat(lower_bound_dfs, ignore_index=True)
            lower_bound_df = lower_bound_df.sort_values([EER_CODE_COL, START_DATE_COL])
            return lower_bound_df

        def _cast_perc_to_absolute(df: pd.DataFrame) -> pd.DataFrame:
            upper_bound_df = cls.stored_energy_upper_bounds(uow)
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
                "curva de segurança",
            )
            minimum_perc_storage_df = minimum_perc_storage_df.rename(
                columns={"data": START_DATE_COL}
            )
            lower_bound_df = _add_missing_eer_bounds(minimum_perc_storage_df)
            lower_bound_df = _cast_perc_to_absolute(lower_bound_df)
            eer_stored_energy_lower_bounds = _add_entity_data(lower_bound_df)
            cls.DECK_DATA_CACHING["eer_stored_energy_lower_bounds"] = (
                eer_stored_energy_lower_bounds
            )
        return eer_stored_energy_lower_bounds.copy()

    @classmethod
    def _stored_energy_upper_bounds_inputs(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame | None:
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

        def _join_hydros_data(df: pd.DataFrame) -> pd.DataFrame:
            hydros = cls.hydros(uow)
            return df.join(hydros[[FOLLOWING_HYDRO_COL]], how="inner")

        def _join_bounds_data(df: pd.DataFrame, stage_date: datetime) -> pd.DataFrame:
            bounds_df = cls.hydro_volume_bounds_in_stages(uow)
            bounds_df = bounds_df.loc[
                bounds_df[START_DATE_COL] == stage_date
            ].set_index(HYDRO_CODE_COL)
            return df.join(bounds_df, how="inner")

        def _volume_to_energy(df: pd.DataFrame) -> pd.DataFrame:
            df[ABSOLUTE_VALUE_COL] *= df[PRODUCTIVITY_TMP_COL]
            return df

        def _cast_to_eers_and_fill_missing(df: pd.DataFrame) -> pd.DataFrame:
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
                .groupby(
                    [
                        START_DATE_COL,
                        CONFIG_COL,
                        EER_CODE_COL,
                        EER_NAME_COL,
                        SUBMARKET_CODE_COL,
                        SUBMARKET_NAME_COL,
                    ]
                )
                .sum()
            ).reset_index()
            eer_codes = cls.eer_code_order(uow)
            eers = cls.eer_submarket_map(uow)
            missing_eers = [
                eer for eer in eer_codes if eer not in df[EER_CODE_COL].tolist()
            ]
            missing_dfs: list[pd.DataFrame] = []
            dates = df[START_DATE_COL].unique()
            configurations = df[CONFIG_COL].unique()
            for eer in missing_eers:
                missing_df = pd.DataFrame(
                    {
                        START_DATE_COL: dates,
                        CONFIG_COL: configurations,
                        EER_CODE_COL: [eer] * len(dates),
                        EER_NAME_COL: [eers.at[eer, EER_NAME_COL]] * len(dates),
                        SUBMARKET_CODE_COL: [eers.at[eer, SUBMARKET_CODE_COL]]
                        * len(dates),
                        SUBMARKET_NAME_COL: [eers.at[eer, SUBMARKET_NAME_COL]]
                        * len(dates),
                        ABSOLUTE_VALUE_COL: [0.0] * len(dates),
                    }
                )
                missing_dfs.append(missing_df)
            df = pd.concat([df] + missing_dfs, ignore_index=True)
            df[EER_CODE_COL] = pd.Categorical(df[EER_CODE_COL], categories=eer_codes)
            df = df.sort_values([START_DATE_COL, CONFIG_COL, EER_CODE_COL])
            df[EER_CODE_COL] = df[EER_CODE_COL].astype(int)
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
            stage_df = _join_hidr_data(stage_df)
            stage_df = _join_bounds_data(stage_df, configuration_date)
            stage_df = _join_hydros_data(stage_df)
            stage_df[ABSOLUTE_VALUE_COL] = stage_df[UPPER_BOUND_COL]
            stage_df = cls._evaluate_productivity(
                stage_df, volume_col=ABSOLUTE_VALUE_COL
            )
            stage_df = cls._accumulate_productivity(stage_df)
            stage_df[CONFIG_COL] = line[VALUE_COL]
            dfs.append(stage_df)

        df = pd.concat(dfs, ignore_index=True)
        df = _volume_to_energy(df)
        df = _cast_to_eers_and_fill_missing(df)

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
        ]

        # print(df)
        # df.to_csv("teste.csv", index=False)

        return df

    @classmethod
    def _stored_energy_upper_bounds_pmo(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
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

        stored_energy_upper_bounds = cls.DECK_DATA_CACHING.get(
            "stored_energy_upper_bounds"
        )
        if stored_energy_upper_bounds is None:
            maximum_storage_df = cls._validate_data(
                cls.pmo(uow).energia_armazenada_maxima,
                pd.DataFrame,
                "energia armazenada máxima",
            )
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
            stored_energy_upper_bounds = configs_df.sort_values(
                [EER_CODE_COL, START_DATE_COL]
            )
            cls.DECK_DATA_CACHING["stored_energy_upper_bounds"] = (
                stored_energy_upper_bounds
            )
        return stored_energy_upper_bounds.copy()

    @classmethod
    def stored_energy_upper_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        stored_energy_upper_bounds = cls.DECK_DATA_CACHING.get(
            "stored_energy_upper_bounds"
        )
        if stored_energy_upper_bounds is None:
            bounds_df = None
            # bounds_df = cls._stored_energy_upper_bounds_pmo(uow)
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
                "convergência",
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
                thermal_df.loc[-1, START_DATE_COL] = last_month.replace(day=last_day)
                thermal_df = thermal_df.set_index(START_DATE_COL).resample("D").ffill()
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
                    thermal_df.loc[start_date:end_date, "potencia_instalada"] -= value
                thermal_df = thermal_df.resample("MS").mean().reset_index()
                df.loc[
                    (df[THERMAL_CODE_COL] == code)
                    & (df[START_DATE_COL].isin(thermal_df[START_DATE_COL])),
                    "potencia_instalada",
                ] = thermal_df["potencia_instalada"].to_numpy()

            return df

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
            return df.sort_values([THERMAL_CODE_COL, START_DATE_COL]).reset_index(
                drop=True
            )

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
            # TODO - encontra as UTEs com modificacao de GTMIN
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

        def _eval_upper_bounds(df: pd.DataFrame) -> pd.DataFrame:
            maintenance_end_date = cls.thermal_maintenance_end_date(uow)
            df.loc[
                df[START_DATE_COL] < maintenance_end_date,
                "indisponibilidade_programada",
            ] = 0.0
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
        bounds_df = cls._apply_thermal_bounds_maintenance_and_changes(bounds_df, uow)
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
    def _thermal_generation_bounds_pmo(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        """
        - codigo_usina (`int`)
        - nome_usina (`str`)
        - data_inicio (`datetime`)
        - limite_inferior (`float`)
        - limite_superior (`float`)
        """
        pmo = cls.pmo(uow)
        bounds_df = pmo.geracao_minima_usinas_termicas
        if isinstance(bounds_df, pd.DataFrame):
            bounds_df = bounds_df.rename(
                columns={
                    "data": START_DATE_COL,
                    "valor_MWmed": LOWER_BOUND_COL,
                }
            )
            upper_bounds = pmo.geracao_maxima_usinas_termicas
            if isinstance(bounds_df, pd.DataFrame):
                bounds_df[UPPER_BOUND_COL] = upper_bounds["valor_MWmed"].to_numpy()
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
            exchange_block_bounds_df[VALUE_COL] = exchange_block_bounds_df.apply(
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
            block_length_df = block_length_df.sort_values([START_DATE_COL, BLOCK_COL])
            n_pares_limites = exchange_block_bounds_df.drop_duplicates(
                [EXCHANGE_SOURCE_CODE_COL, EXCHANGE_TARGET_CODE_COL]
            ).shape[0]
            exchange_block_bounds_df[VALUE_COL] *= np.tile(
                block_length_df[VALUE_COL].to_numpy(), n_pares_limites
            )

            return exchange_block_bounds_df

        exchange_bounds = cls.DECK_DATA_CACHING.get("exchange_bounds")
        if exchange_bounds is None:
            exchange_average_bounds_df = cls._validate_data(
                cls._get_sistema(uow).limites_intercambio,
                pd.DataFrame,
                "limites de intercâmbio",
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
            block_length_df = cls.block_lengths(uow)
            exchange_bounds = _cast_exchange_bounds_to_MWmes(
                exchange_block_bounds_df,
                exchange_average_bounds_df,
                block_length_df,
            )
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
                "custos de operação",
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
                "número de iterações",
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
                "tempos de execução",
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
                "submercados",
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
            cls.DECK_DATA_CACHING["submarkets"] = submarkets
        return submarkets.copy()

    @classmethod
    def eers(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        eers = cls.DECK_DATA_CACHING.get("eers")
        if eers is None:
            eers = cls._validate_data(cls._get_ree(uow).rees, pd.DataFrame, "REEs")
            eers = eers.rename(
                columns={
                    "codigo": EER_CODE_COL,
                    "nome": EER_NAME_COL,
                    "submercado": SUBMARKET_CODE_COL,
                }
            )
            eers = eers.astype({EER_NAME_COL: STRING_DF_TYPE})
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
                "política híbrida",
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
                "configuração de hidrelétricas",
            )
            hydros = hydros.rename(
                columns={
                    "codigo_usina": HYDRO_CODE_COL,
                    "nome_usina": HYDRO_NAME_COL,
                    "ree": EER_CODE_COL,
                }
            )
            hydros = hydros.astype({HYDRO_NAME_COL: STRING_DF_TYPE})
            cls.DECK_DATA_CACHING["hydros"] = hydros
        return hydros.copy()

    @classmethod
    def _get_value_and_unit_from_modif_entry(
        cls, r: Register
    ) -> Tuple[Optional[float], Optional[str]]:
        """
        Extrai um dado de um registro do modif.dat com a sua unidade.
        """
        if isinstance(r, VOLMIN):
            return r.volume, r.unidade
        elif isinstance(r, VMINT):
            return r.volume, r.unidade
        elif isinstance(r, VOLMAX):
            return r.volume, r.unidade
        elif isinstance(r, VMAXT):
            return r.volume, r.unidade
        elif isinstance(r, VAZMIN):
            return r.vazao, Unit.m3s.value
        elif isinstance(r, VAZMINT):
            return r.vazao, Unit.m3s.value
        elif isinstance(r, VAZMAXT):
            return r.vazao, Unit.m3s.value
        elif isinstance(r, TURBMINT):
            return r.turbinamento, Unit.m3s.value
        elif isinstance(r, TURBMAXT):
            return r.turbinamento, Unit.m3s.value
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
                regs_usina = [r for r in hydro_changes if isinstance(r, register_type)]
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
        register_type: Type[Union[VMINT, VMAXT, VAZMINT, VAZMAXT, TURBMINT, TURBMAXT]],
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
    def hydro_volume_bounds_with_changes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
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

        def _cast_bounds_to_hm3(df: pd.DataFrame, hm3_df: pd.DataFrame) -> pd.DataFrame:
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
    def hydro_volume_bounds_in_stages(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
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

        def _cast_bounds_to_hm3(df: pd.DataFrame, hm3_df: pd.DataFrame) -> pd.DataFrame:
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
    def hydro_turbined_flow_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
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
            hydro_turbined_flow_bounds = hydro_turbined_flow_bounds.join(entities)
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

        def _apply_changes_to_hydro_data(df: pd.DataFrame, uow: AbstractUnitOfWork):
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
            hydro_turbined_flow_bounds = hydro_turbined_flow_bounds.join(entities)
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
            df[BLOCK_COL] = np.tile(np.arange(num_blocks), num_hydros * num_stages)
            return df.sort_values(
                [HYDRO_CODE_COL, START_DATE_COL, BLOCK_COL]
            ).reset_index(drop=True)

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
    def hydro_outflow_bounds_with_changes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
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
    def hydro_outflow_bounds_in_stages(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
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
            df[BLOCK_COL] = np.tile(np.arange(num_blocks), num_hydros * num_stages)
            return df.sort_values(
                [HYDRO_CODE_COL, START_DATE_COL, BLOCK_COL]
            ).reset_index(drop=True)

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
    def thermals(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        thermals = cls.DECK_DATA_CACHING.get("thermals")
        if thermals is None:
            thermals = cls._validate_data(
                cls._get_conft(uow).usinas, pd.DataFrame, "UTEs"
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
            cls.DECK_DATA_CACHING["thermals"] = thermals
        return thermals.copy()

    @classmethod
    def num_blocks(cls, uow: AbstractUnitOfWork) -> int:
        num_blocks = cls.DECK_DATA_CACHING.get("num_blocks")
        if num_blocks is None:
            num_blocks = cls._validate_data(
                cls._get_patamar(uow).numero_patamares,
                int,
                "número de patamares",
            )
            cls.DECK_DATA_CACHING["num_blocks"] = num_blocks
        return num_blocks

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
                "duração dos patamares",
            )
            block_lengths = block_lengths.rename(
                columns={"data": START_DATE_COL, "patamar": BLOCK_COL}
            )
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

        exchange_block_limits = cls.DECK_DATA_CACHING.get("exchange_block_limits")
        if exchange_block_limits is None:
            exchange_block_limits = cls._validate_data(
                cls._get_patamar(uow).intercambio_patamares,
                pd.DataFrame,
                "limites de intercâmbio dos patamares",
            )
            exchange_block_limits = exchange_block_limits.rename(
                columns={
                    "submercado_de": EXCHANGE_SOURCE_CODE_COL,
                    "submercado_para": EXCHANGE_TARGET_CODE_COL,
                    "data": START_DATE_COL,
                }
            )
            exchange_block_limits = __eval_pat0(exchange_block_limits)
            cls.DECK_DATA_CACHING["exchange_block_limits"] = exchange_block_limits
        return exchange_block_limits.copy()

    @classmethod
    def _initial_stored_energy_from_pmo(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame | None:
        return cls.pmo(uow).energia_armazenada_inicial

    @classmethod
    def _evaluate_productivity(
        cls, df: pd.DataFrame, volume_col: str = VOLUME_FOR_PRODUCTIVITY_TMP_COL
    ) -> pd.DataFrame:
        def _evaluate_upper_drop_at_volume(line: pd.Series) -> float:
            coefs = [line[c] for c in HEIGHT_POLY_COLS]
            if line[VOLUME_REGULATION_COL] == "M":
                coefs_integral = [0] + [c / (i + 1) for i, c in enumerate(coefs)]
                min_volume = line[LOWER_BOUND_COL]
                max_volume = line[UPPER_BOUND_COL]
                net_volume = max_volume - min_volume
                percent_volume = line[volume_col] / net_volume if net_volume > 0 else 0
                reversed_coefs_integral = list(reversed(coefs_integral))
                min_integral = np.polyval(
                    reversed_coefs_integral,
                    min_volume,
                )
                max_integral = np.polyval(
                    reversed_coefs_integral,
                    percent_volume * net_volume + min_volume,
                )
                hmon = (max_integral - min_integral) / (percent_volume * net_volume)
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
            df[UPPER_DROP_COL] = df.apply(_evaluate_upper_drop_at_volume, axis=1)
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
        np_edges = list(df.reset_index()[[FOLLOWING_HYDRO_COL, "index"]].to_numpy())
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
        def _join_bounds_data(df: pd.DataFrame) -> pd.DataFrame:
            bounds_df = cls.hydro_volume_bounds_in_stages(uow)
            starting_date = cls.stages_starting_dates_final_simulation(uow)[0]
            bounds_df = bounds_df.loc[
                bounds_df[START_DATE_COL] == starting_date
            ].set_index(HYDRO_CODE_COL)
            return df.join(bounds_df, how="inner")

        def _volume_to_energy(df: pd.DataFrame) -> pd.DataFrame:
            df[ABSOLUTE_VALUE_COL] *= df[PRODUCTIVITY_TMP_COL]
            df[MAXIMUM_STORED_ENERGY_COL] = (
                df[MAX_STORED_VOLUME_COL] * df[MAX_PRODUCTIVITY_COL]
            )
            return df

        def _cast_to_eers_and_fill_missing(df: pd.DataFrame) -> pd.DataFrame:
            df = df.join(cls.hydro_eer_submarket_map(uow), how="inner")
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
            missing_df = pd.DataFrame(
                {
                    EER_CODE_COL: missing_eers,
                    EER_NAME_COL: eers.loc[missing_eers, EER_NAME_COL].tolist(),
                    ABSOLUTE_VALUE_COL: [np.nan] * len(missing_eers),
                    PERCENT_VALUE_COL: [100.0] * len(missing_eers),
                }
            )
            df = pd.concat([df, missing_df], ignore_index=True)
            return df.set_index(EER_CODE_COL)

        def _eval_percent_value(df: pd.DataFrame) -> pd.DataFrame:
            df[PERCENT_VALUE_COL] = (
                df[ABSOLUTE_VALUE_COL] / df[MAXIMUM_STORED_ENERGY_COL] * 100.0
            )
            return df

        MAX_PRODUCTIVITY_COL = "prod_max"
        MAX_STORED_VOLUME_COL = "varmax"
        MAXIMUM_STORED_ENERGY_COL = "earmax"
        ABSOLUTE_VALUE_COL = "valor_hm3"
        ABSOLUTE_VALUE_FINAL_COL = "valor_MWmes"
        PERCENT_VALUE_COL = "valor_percentual"

        df = cls.initial_stored_volume(uow).set_index(HYDRO_CODE_COL)

        # Calcula prodts no ponto inicial
        df_absolute = cls._hydro_accumulated_productivity_at_volume(
            uow, df.copy(), volume_col=ABSOLUTE_VALUE_COL
        )

        # Calcula prodts no máximo
        df_percent = _join_bounds_data(df.copy())
        df_percent = df_percent[[UPPER_BOUND_COL]].rename(
            columns={
                UPPER_BOUND_COL: MAX_STORED_VOLUME_COL,
            }
        )
        df_percent = cls._hydro_accumulated_productivity_at_volume(
            uow, df_percent, volume_col=MAX_STORED_VOLUME_COL
        )
        df_percent = df_percent.rename(
            columns={PRODUCTIVITY_TMP_COL: MAX_PRODUCTIVITY_COL}
        )

        df = df_absolute.join(df_percent, how="inner")

        df = _volume_to_energy(df)
        df = _cast_to_eers_and_fill_missing(df)
        df = _eval_percent_value(df)

        df = df.rename(columns={ABSOLUTE_VALUE_COL: ABSOLUTE_VALUE_FINAL_COL})

        return df[[EER_NAME_COL, ABSOLUTE_VALUE_FINAL_COL, PERCENT_VALUE_COL]]

    @classmethod
    def initial_stored_energy(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        initial_stored_energy = cls.DECK_DATA_CACHING.get("initial_stored_energy")
        if initial_stored_energy is None:
            initial_stored_energy = cls._initial_stored_energy_from_pmo(uow)
            if initial_stored_energy is None:
                initial_stored_energy = cls._initial_stored_energy_from_confhd_hidr(uow)
            initial_stored_energy = cls._validate_data(
                initial_stored_energy, pd.DataFrame, "EARM inicial"
            )
            cls.DECK_DATA_CACHING["initial_stored_energy"] = initial_stored_energy
        return initial_stored_energy.copy()

    @classmethod
    def _initial_stored_volume_from_pmo(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame | None:
        return cls.pmo(uow).volume_armazenado_inicial

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
        volume_bounds = cls.hydro_volume_bounds_in_stages(uow)
        starting_date = cls.stages_starting_dates_final_simulation(uow)[0]
        volume_bounds = volume_bounds.loc[
            volume_bounds[START_DATE_COL] == starting_date,
            [LOWER_BOUND_COL, UPPER_BOUND_COL, HYDRO_CODE_COL],
        ].set_index(HYDRO_CODE_COL, drop=True)
        df = df.join(hidr, how="inner")
        df = df.join(volume_bounds, how="inner")
        df["valor_hm3"] = df.apply(
            lambda line: line["valor_percentual"]
            / 100.0
            * (line[UPPER_BOUND_COL] - line[LOWER_BOUND_COL])
            + line[LOWER_BOUND_COL],
            axis=1,
        )
        df.loc[df["tipo_regulacao"] != "M", "valor_hm3"] = np.nan
        df.loc[df["tipo_regulacao"] != "M", "valor_percentual"] = 0.0

        return df[[HYDRO_NAME_COL, "valor_hm3", "valor_percentual"]].reset_index()

    @classmethod
    def _initial_stored_volume_pre_study_condition(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        if cls.num_pre_study_period_years(uow) > 0:
            df.loc[~df["valor_hm3"].isna(), "valor_percentual"] = 100.0
        return df

    @classmethod
    def initial_stored_volume(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        initial_stored_volume = cls.DECK_DATA_CACHING.get("initial_stored_volume")
        if initial_stored_volume is None:
            initial_stored_volume = cls._initial_stored_volume_from_pmo(uow)
            if initial_stored_volume is None:
                initial_stored_volume = cls._initial_stored_volume_from_confhd_hidr(uow)
            initial_stored_volume = cls._validate_data(
                initial_stored_volume, pd.DataFrame, "VARM inicial"
            )
            initial_stored_volume = cls._initial_stored_volume_pre_study_condition(
                initial_stored_volume, uow
            )
            cls.DECK_DATA_CACHING["initial_stored_volume"] = initial_stored_volume
        return initial_stored_volume.copy()

    @classmethod
    def eer_code_order(cls, uow: AbstractUnitOfWork) -> List[int]:
        eer_code_order = cls.DECK_DATA_CACHING.get("eer_code_order")
        if eer_code_order is None:
            eer_code_order = cls.eers(uow)[EER_CODE_COL].tolist()
            cls.DECK_DATA_CACHING["eer_code_order"] = eer_code_order
        return eer_code_order

    @classmethod
    def hydro_code_order(cls, uow: AbstractUnitOfWork) -> List[int]:
        hydro_code_order = cls.DECK_DATA_CACHING.get("hydro_code_order")
        if hydro_code_order is None:
            hydro_code_order = cls.hydros(uow)[HYDRO_CODE_COL].tolist()
            cls.DECK_DATA_CACHING["hydro_code_order"] = hydro_code_order
        return hydro_code_order

    @classmethod
    def hydro_eer_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        aux_df = cls.DECK_DATA_CACHING.get("hydro_eer_submarket_map")
        if aux_df is None:
            hydros = cls.hydros(uow).set_index(HYDRO_CODE_COL)
            eers = cls.eers(uow).set_index(EER_CODE_COL)
            submarkets = cls.submarkets(uow).set_index(SUBMARKET_CODE_COL)
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
            thermals = Deck.thermals(uow)
            thermals = thermals.set_index(THERMAL_NAME_COL)
            submarkets = cls.submarkets(uow).set_index(SUBMARKET_CODE_COL)

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
