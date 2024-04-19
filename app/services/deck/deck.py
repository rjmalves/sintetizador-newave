from inewave.newave import (
    Dger,
    Ree,
    Confhd,
    Modif,
    Conft,
    Sistema,
    Curva,
    Clast,
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
from typing import Any, Optional, TypeVar, Type, List, Tuple, Union
from cfinterface.components.register import Register

from app.services.unitofwork import AbstractUnitOfWork
from app.model.operation.unit import Unit
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
)


class Deck:
    """
    Armazena as informações dos principais arquivos que
    são utilizados para o processo de síntese.
    """

    T = TypeVar("T")
    logger: Optional[logging.Logger] = None

    DECK_DATA_CACHING: dict[str, Any] = {}

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
                        "Erro no processamento do pmo.dat para"
                        + " síntese da operação"
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
    def hidr(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        hidr = cls.DECK_DATA_CACHING.get("hidr")
        if hidr is None:
            hidr = cls._validate_data(
                cls._get_hidr(uow).cadastro, pd.DataFrame, "hidr"
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
    def num_study_period_years(cls, uow: AbstractUnitOfWork) -> int:
        num_study_period_years = cls.DECK_DATA_CACHING.get(
            "num_study_period_years"
        )
        if num_study_period_years is None:
            dger = cls.dger(uow)
            num_study_period_years = cls._validate_data(
                dger.num_anos_estudo,
                int,
                "número de anos do estudo",
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
                "número de anos do período pós-estudo na simulação",
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
                "número de séries sintéticas na simulação",
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
            study_starting_year = cls.study_period_starting_year(uow)
            history_starting_year = cls._validate_data(
                shist.ano_inicio_varredura,
                int,
                "número de séries históricas na simulação",
            )
            num_history_years = study_starting_year - history_starting_year
            cls.DECK_DATA_CACHING["num_history_years"] = num_history_years
        return num_history_years

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
                "tipo da simulação final",
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
            if cls.final_simulation_type(uow) == 0:
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
    def num_hydro_simulation_stages_policy(
        cls, uow: AbstractUnitOfWork
    ) -> int:
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
                tempo_individualizado = (
                    data_fim_individualizado - data_inicio_estudo
                )
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
        num_hydro_simulation_stages_final_simulation = (
            cls.DECK_DATA_CACHING.get(
                "num_hydro_simulation_stages_final_simulation"
            )
        )
        if num_hydro_simulation_stages_final_simulation is None:
            aggergation = cls.final_simulation_aggregation(uow)
            starting_month = cls.study_period_starting_month(uow)
            study_years = cls.num_study_period_years(uow)
            post_study_years = (
                cls.num_post_study_period_years_final_simulation(uow)
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
                "consideração da geração eólica",
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
                "opção do modelo PAR(p)",
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
                "ordem máxima do modelo PAR(p)",
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
            post_study_years = (
                cls.num_post_study_period_years_final_simulation(uow)
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
    def configurations(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        configurations = cls.DECK_DATA_CACHING.get("configurations")
        if configurations is None:
            pmo = cls.pmo(uow)
            configurations = cls._validate_data(
                pmo.configuracoes_qualquer_modificacao,
                pd.DataFrame,
                "configurations",
            )
            configurations = configurations.rename(
                columns={"data": START_DATE_COL}
            )

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
            missing_eers = list(
                set(eer_codes).difference(eers_minimum_storage)
            )
            lower_bound_dfs = [df]
            for c in missing_eers:
                eer_df = df.loc[
                    df[EER_CODE_COL] == eers_minimum_storage[0]
                ].copy()
                eer_df[EER_CODE_COL] = c
                eer_df[VALUE_COL] = 0.0
                lower_bound_dfs.append(eer_df)
            lower_bound_df = pd.concat(lower_bound_dfs, ignore_index=True)
            lower_bound_df = lower_bound_df.sort_values(
                [EER_CODE_COL, START_DATE_COL]
            )
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
    def stored_energy_upper_bounds(
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

        stored_energy_upper_bounds = cls.DECK_DATA_CACHING.get(
            "stored_energy_upper_bounds"
        )
        if stored_energy_upper_bounds is None:
            maximum_storage_df = cls._validate_data(
                cls.pmo(uow).energia_armazenada_maxima,
                pd.DataFrame,
                "stored_energy_upper_bounds",
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
    def convergence(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        convergence = cls.DECK_DATA_CACHING.get("convergence")
        if convergence is None:
            pmo = cls.pmo(uow)
            convergence = cls._validate_data(
                pmo.convergencia,
                pd.DataFrame,
                "convergence",
            )

            cls.DECK_DATA_CACHING["convergence"] = convergence
        return convergence.copy()

    @classmethod
    def thermal_generation_bounds(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:

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
            pmo = cls.pmo(uow)
            bounds_df = cls._validate_data(
                pmo.geracao_minima_usinas_termicas,
                pd.DataFrame,
                "geracao_minima_usinas_termicas",
            )
            bounds_df = bounds_df.rename(
                columns={
                    "data": START_DATE_COL,
                    "valor_MWmed": LOWER_BOUND_COL,
                }
            )
            bounds_df[UPPER_BOUND_COL] = cls._validate_data(
                pmo.geracao_maxima_usinas_termicas,
                pd.DataFrame,
                "geracao_maxima_usinas_termicas",
            )["valor_MWmed"].to_numpy()
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
                            exchange_average_bounds_df[
                                EXCHANGE_SOURCE_CODE_COL
                            ]
                            == linha[EXCHANGE_SOURCE_CODE_COL]
                        )
                        & (
                            exchange_average_bounds_df[
                                EXCHANGE_TARGET_CODE_COL
                            ]
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
            block_length_df = block_length_df.sort_values(
                [START_DATE_COL, BLOCK_COL]
            )
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
                "costs",
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
                "num_iterations",
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
                "runtimes",
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
                "submarkets",
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
            submarkets = submarkets.astype(
                {SUBMARKET_NAME_COL: STRING_DF_TYPE}
            )
            cls.DECK_DATA_CACHING["submarkets"] = submarkets
        return submarkets.copy()

    @classmethod
    def eers(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        eers = cls.DECK_DATA_CACHING.get("eers")
        if eers is None:
            eers = cls._validate_data(
                cls._get_ree(uow).rees, pd.DataFrame, "eers"
            )
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
                "REEs",
            )
            cls.DECK_DATA_CACHING["hybrid_policy"] = hybrid_policy
        return hybrid_policy

    @classmethod
    def hydros(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        hydros = cls.DECK_DATA_CACHING.get("hydros")
        if hydros is None:
            hydros = cls._validate_data(
                cls._get_confhd(uow).usinas, pd.DataFrame, "hydros"
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
            Union[VMINT, VMAXT, VAZMINT, VAZMAXT, TURBMINT, TURBMAXT]
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
            return df.sort_values(
                [HYDRO_CODE_COL, START_DATE_COL]
            ).reset_index(drop=True)

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
            hydro_turbined_flow_bounds_with_changes = (
                hydro_turbined_flow_bounds
            )
            cls.DECK_DATA_CACHING[
                "hydro_turbined_flow_bounds_with_changes"
            ] = hydro_turbined_flow_bounds_with_changes
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

        def _expand_to_stages_and_blocks(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = df.reset_index()
            num_hydros = df.shape[0]
            dates = np.array(cls.stages_starting_dates_final_simulation(uow))
            num_stages = len(dates)
            num_blocks = cls.num_blocks(uow) + 1
            df = pd.concat([df] * num_stages * num_blocks, ignore_index=True)
            df[START_DATE_COL] = np.repeat(dates, num_hydros * num_blocks)
            df[BLOCK_COL] = np.tile(
                np.arange(num_blocks), num_hydros * num_stages
            )
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
            m3s_df = _expand_to_stages_and_blocks(m3s_df, uow)
            df = _add_hydro_bounds_changes_to_stages(m3s_df.copy(), uow)
            hydro_turbined_flow_bounds_in_stages = df
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

        hydro_outflow_bounds = cls.DECK_DATA_CACHING.get(
            "hydro_outflow_bounds"
        )
        if hydro_outflow_bounds is None:
            hydro_outflow_bounds = _get_hydro_data(uow)
            entities = cls.hydro_eer_submarket_map(uow)
            hydro_outflow_bounds = hydro_outflow_bounds.join(entities)
            cls.DECK_DATA_CACHING["hydro_outflow_bounds"] = (
                hydro_outflow_bounds
            )
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

        def _expand_to_stages_and_blocks(
            df: pd.DataFrame, uow: AbstractUnitOfWork
        ) -> pd.DataFrame:
            df = df.reset_index()
            num_hydros = df.shape[0]
            dates = np.array(cls.stages_starting_dates_final_simulation(uow))
            num_stages = len(dates)
            num_blocks = cls.num_blocks(uow) + 1
            df = pd.concat([df] * num_stages * num_blocks, ignore_index=True)
            df[START_DATE_COL] = np.repeat(dates, num_hydros * num_blocks)
            df[BLOCK_COL] = np.tile(
                np.arange(num_blocks), num_hydros * num_stages
            )
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
            m3s_df = _expand_to_stages_and_blocks(m3s_df, uow)
            df = _add_hydro_bounds_changes_to_stages(m3s_df.copy(), uow)
            hydro_outflow_bounds_in_stages = df
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

        exchange_block_limits = cls.DECK_DATA_CACHING.get(
            "exchange_block_limits"
        )
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
            cls.DECK_DATA_CACHING["exchange_block_limits"] = (
                exchange_block_limits
            )
        return exchange_block_limits.copy()

    @classmethod
    def initial_stored_energy(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        initial_stored_energy = cls.DECK_DATA_CACHING.get(
            "initial_stored_energy"
        )
        if initial_stored_energy is None:
            initial_stored_energy = cls._validate_data(
                cls.pmo(uow).energia_armazenada_inicial,
                pd.DataFrame,
                "EARM inicial",
            )
            cls.DECK_DATA_CACHING["initial_stored_energy"] = (
                initial_stored_energy
            )
        return initial_stored_energy.copy()

    @classmethod
    def initial_stored_volume(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        initial_stored_volume = cls.DECK_DATA_CACHING.get(
            "initial_stored_volume"
        )
        if initial_stored_volume is None:
            initial_stored_volume = cls._validate_data(
                cls.pmo(uow).volume_armazenado_inicial,
                pd.DataFrame,
                "VARM inicial",
            )
            cls.DECK_DATA_CACHING["initial_stored_volume"] = (
                initial_stored_volume
            )
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
