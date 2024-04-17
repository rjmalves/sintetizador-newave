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
    VALUE_COL,
    CONFIG_COL,
    START_DATE_COL,
    LOWER_BOUND_COL,
    UPPER_BOUND_COL,
    LOWER_BOUND_UNIT_COL,
    UPPER_BOUND_UNIT_COL,
    BLOCK_COL,
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
    def _get_curva(cls, uow: AbstractUnitOfWork) -> Ree:
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
    def newavetim(cls, uow: AbstractUnitOfWork) -> Newavetim:
        newavetim = cls.DECK_DATA_CACHING.get("newavetim")
        if newavetim is None:
            newavetim = cls._validate_data(
                cls._get_newavetim(uow),
                Pmo,
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
                return df
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
                return df
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
                return df
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
                return df
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
                return df
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
                return df
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
                return df
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
                return df
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
                return df
        else:
            return pd.DataFrame()

    @classmethod
    def mes_inicio_pre_estudo(cls, uow: AbstractUnitOfWork) -> int:
        mes_inicio_pre_estudo = cls.DECK_DATA_CACHING.get(
            "mes_inicio_pre_estudo"
        )
        if mes_inicio_pre_estudo is None:
            dger = cls.dger(uow)
            mes_inicio_pre_estudo = cls._validate_data(
                dger.mes_inicio_pre_estudo,
                int,
                "mês de início do estudo",
            )
            cls.DECK_DATA_CACHING["mes_inicio_pre_estudo"] = (
                mes_inicio_pre_estudo
            )
        return mes_inicio_pre_estudo

    @classmethod
    def mes_inicio_estudo(cls, uow: AbstractUnitOfWork) -> int:
        mes_inicio_estudo = cls.DECK_DATA_CACHING.get("mes_inicio_estudo")
        if mes_inicio_estudo is None:
            dger = cls.dger(uow)
            mes_inicio_estudo = cls._validate_data(
                dger.mes_inicio_estudo,
                int,
                "mês de início do estudo",
            )
            cls.DECK_DATA_CACHING["mes_inicio_estudo"] = mes_inicio_estudo
        return mes_inicio_estudo

    @classmethod
    def ano_inicio_estudo(cls, uow: AbstractUnitOfWork) -> int:
        ano_inicio_estudo = cls.DECK_DATA_CACHING.get("ano_inicio_estudo")
        if ano_inicio_estudo is None:
            dger = cls.dger(uow)
            ano_inicio_estudo = cls._validate_data(
                dger.ano_inicio_estudo,
                int,
                "ano de início do estudo",
            )
            cls.DECK_DATA_CACHING["ano_inicio_estudo"] = ano_inicio_estudo
        return ano_inicio_estudo

    @classmethod
    def num_anos_estudo(cls, uow: AbstractUnitOfWork) -> int:
        num_anos_estudo = cls.DECK_DATA_CACHING.get("num_anos_estudo")
        if num_anos_estudo is None:
            dger = cls.dger(uow)
            num_anos_estudo = cls._validate_data(
                dger.num_anos_estudo,
                int,
                "número de anos do estudo",
            )
            cls.DECK_DATA_CACHING["num_anos_estudo"] = num_anos_estudo
        return num_anos_estudo

    @classmethod
    def num_anos_pos_sim_final(cls, uow: AbstractUnitOfWork) -> int:
        num_anos_pos_sim_final = cls.DECK_DATA_CACHING.get(
            "num_anos_pos_sim_final"
        )
        if num_anos_pos_sim_final is None:
            dger = cls.dger(uow)
            num_anos_pos_sim_final = cls._validate_data(
                dger.num_anos_pos_sim_final,
                int,
                "número de anos do período pós-estudo na simulação",
            )
            cls.DECK_DATA_CACHING["num_anos_pos_sim_final"] = (
                num_anos_pos_sim_final
            )
        return num_anos_pos_sim_final

    @classmethod
    def num_series_sinteticas(cls, uow: AbstractUnitOfWork) -> int:
        num_series_sinteticas = cls.DECK_DATA_CACHING.get(
            "num_series_sinteticas"
        )
        if num_series_sinteticas is None:
            dger = cls.dger(uow)
            num_series_sinteticas = cls._validate_data(
                dger.num_series_sinteticas,
                int,
                "número de séries sintéticas na simulação",
            )
            cls.DECK_DATA_CACHING["num_series_sinteticas"] = (
                num_series_sinteticas
            )
        return num_series_sinteticas

    @classmethod
    def num_anos_historico(cls, uow: AbstractUnitOfWork) -> int:
        num_anos_historico = cls.DECK_DATA_CACHING.get("num_anos_historico")
        if num_anos_historico is None:
            shist = cls._get_shist(uow)
            ano_inicio_estudo = cls.ano_inicio_estudo(uow)
            ano_inicio_varredura = cls._validate_data(
                shist.ano_inicio_varredura,
                int,
                "número de séries históricas na simulação",
            )
            # TODO - conferir o número de anos passados desconsiderados
            num_anos_historico = ano_inicio_estudo - ano_inicio_varredura
            cls.DECK_DATA_CACHING["num_anos_historico"] = num_anos_historico
        return num_anos_historico

    @classmethod
    def tipo_simulacao_final(cls, uow: AbstractUnitOfWork) -> int:
        tipo_simulacao_final = cls.DECK_DATA_CACHING.get(
            "tipo_simulacao_final"
        )
        if tipo_simulacao_final is None:
            dger = cls.dger(uow)
            tipo_simulacao_final = cls._validate_data(
                dger.tipo_simulacao_final,
                int,
                "tipo da simulação final",
            )
            cls.DECK_DATA_CACHING["tipo_simulacao_final"] = (
                tipo_simulacao_final
            )
        return tipo_simulacao_final

    @classmethod
    def agregacao_simulacao_final(cls, uow: AbstractUnitOfWork) -> int:
        agregacao_simulacao_final = cls.DECK_DATA_CACHING.get(
            "agregacao_simulacao_final"
        )
        if agregacao_simulacao_final is None:
            dger = cls.dger(uow)
            agregacao_simulacao_final = cls._validate_data(
                dger.agregacao_simulacao_final,
                int,
                "tipo da simulação final",
            )
            cls.DECK_DATA_CACHING["agregacao_simulacao_final"] = (
                agregacao_simulacao_final
            )
        return agregacao_simulacao_final

    @classmethod
    def numero_cenarios_simulacao_final(cls, uow: AbstractUnitOfWork) -> int:
        numero_cenarios_simulacao_final = cls.DECK_DATA_CACHING.get(
            "numero_cenarios_simulacao_final"
        )
        if numero_cenarios_simulacao_final is None:
            if cls.tipo_simulacao_final(uow) == 0:
                numero_cenarios_simulacao_final = cls.num_anos_historico(uow)
            else:
                numero_cenarios_simulacao_final = cls.num_series_sinteticas(
                    uow
                )
            cls.DECK_DATA_CACHING["numero_cenarios_simulacao_final"] = (
                numero_cenarios_simulacao_final
            )
        return numero_cenarios_simulacao_final

    @classmethod
    def num_estagios_individualizados_politica(
        cls, uow: AbstractUnitOfWork
    ) -> int:
        num_estagios_individualizados_politica = cls.DECK_DATA_CACHING.get(
            "num_estagios_individualizados_politica"
        )
        if num_estagios_individualizados_politica is None:
            ano_inicio = cls.ano_inicio_estudo(uow)
            mes_inicio = cls.mes_inicio_estudo(uow)
            rees = cls.rees(uow)
            mes_fim_hib = rees["mes_fim_individualizado"].iloc[0]
            ano_fim_hib = rees["ano_fim_individualizado"].iloc[0]

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
                num_estagios_individualizados_politica = int(
                    round(tempo_individualizado / timedelta(days=30))
                )
            else:
                num_estagios_individualizados_politica = 0
            cls.DECK_DATA_CACHING["num_estagios_individualizados_politica"] = (
                num_estagios_individualizados_politica
            )
        return num_estagios_individualizados_politica

    @classmethod
    def num_estagios_individualizados_sf(cls, uow: AbstractUnitOfWork) -> int:
        num_estagios_individualizados_sf = cls.DECK_DATA_CACHING.get(
            "num_estagios_individualizados_sf"
        )
        if num_estagios_individualizados_sf is None:
            agregacao = cls.agregacao_simulacao_final(uow)
            mes_inicio = cls.mes_inicio_estudo(uow)
            anos_estudo = cls.num_anos_estudo(uow)
            anos_pos_sf = cls.num_anos_pos_sim_final(uow)
            if agregacao == 1:
                num_estagios_individualizados_sf = (
                    anos_estudo + anos_pos_sf
                ) * 12 - (mes_inicio - 1)
            else:
                num_estagios_individualizados_sf = (
                    cls.num_estagios_individualizados_politica(uow)
                )

            cls.DECK_DATA_CACHING["num_estagios_individualizados_sf"] = (
                num_estagios_individualizados_sf
            )
        return num_estagios_individualizados_sf

    @classmethod
    def considera_geracao_eolica(cls, uow: AbstractUnitOfWork) -> int:
        considera_geracao_eolica = cls.DECK_DATA_CACHING.get(
            "considera_geracao_eolica"
        )
        if considera_geracao_eolica is None:
            considera_geracao_eolica = cls._validate_data(
                cls.dger(uow).considera_geracao_eolica != 0,
                int,
                "consideração da geração eólica",
            )
            cls.DECK_DATA_CACHING["considera_geracao_eolica"] = (
                considera_geracao_eolica
            )
        return considera_geracao_eolica

    @classmethod
    def consideracao_media_anual_afluencias(
        cls, uow: AbstractUnitOfWork
    ) -> int:
        consideracao_media_anual_afluencias = cls.DECK_DATA_CACHING.get(
            "consideracao_media_anual_afluencias"
        )
        if consideracao_media_anual_afluencias is None:
            consideracao_media_anual_afluencias = cls._validate_data(
                cls.dger(uow).consideracao_media_anual_afluencias,
                int,
                "opção do modelo PAR(p)",
            )
            cls.DECK_DATA_CACHING["consideracao_media_anual_afluencias"] = (
                consideracao_media_anual_afluencias
            )
        return consideracao_media_anual_afluencias

    @classmethod
    def ordem_maxima_parp(cls, uow: AbstractUnitOfWork) -> int:
        ordem_maxima_parp = cls.DECK_DATA_CACHING.get("ordem_maxima_parp")
        if ordem_maxima_parp is None:
            ordem_maxima_parp = cls._validate_data(
                cls.dger(uow).ordem_maxima_parp,
                int,
                "ordem máxima do modelo PAR(p)",
            )
            cls.DECK_DATA_CACHING["ordem_maxima_parp"] = ordem_maxima_parp
        return ordem_maxima_parp

    @classmethod
    def num_estagios_tendencia_hidrologica(
        cls, uow: AbstractUnitOfWork
    ) -> int:
        scenario_model = cls.consideracao_media_anual_afluencias(uow)
        maximum_model_order = cls.ordem_maxima_parp(uow)
        past_stages = 12 if scenario_model != 0 else maximum_model_order
        return past_stages

    @classmethod
    def data_inicio_com_tendencia_hidrologica(
        cls, uow: AbstractUnitOfWork
    ) -> datetime:
        starting_year = cls.ano_inicio_estudo(uow)
        past_stages = cls.num_estagios_tendencia_hidrologica(uow)
        starting_date_with_tendency = datetime(
            year=starting_year, month=1, day=1
        ) - relativedelta(months=past_stages)
        return starting_date_with_tendency

    @classmethod
    def data_fim_com_pos_estudo(cls, uow: AbstractUnitOfWork) -> datetime:
        starting_year = cls.ano_inicio_estudo(uow)
        study_years = cls.num_anos_estudo(uow)
        post_study_years_in_simulation = cls.num_anos_pos_sim_final(uow)
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
    def datas_inicio_estagios_internos_politica(
        cls, uow: AbstractUnitOfWork
    ) -> List[datetime]:
        datas_inicio_estagios_internos_politica = cls.DECK_DATA_CACHING.get(
            "datas_inicio_estagios_internos_politica"
        )
        if datas_inicio_estagios_internos_politica is None:
            datas_inicio_estagios_internos_politica = pd.date_range(
                datetime(cls.ano_inicio_estudo(uow), 1, 1),
                datetime(
                    cls.ano_inicio_estudo(uow) + cls.num_anos_estudo(uow) - 1,
                    12,
                    1,
                ),
                freq="MS",
            ).tolist()
            cls.DECK_DATA_CACHING[
                "datas_inicio_estagios_internos_politica"
            ] = datas_inicio_estagios_internos_politica
        return datas_inicio_estagios_internos_politica

    @classmethod
    def datas_inicio_estagios_internos_politica_com_tendencia(
        cls, uow: AbstractUnitOfWork
    ) -> List[datetime]:
        datas_inicio_estagios_internos_politica_com_tendencia = (
            cls.DECK_DATA_CACHING.get(
                "datas_inicio_estagios_internos_politica_com_tendencia"
            )
        )
        if datas_inicio_estagios_internos_politica_com_tendencia is None:
            datas_inicio_estagios_internos_politica_com_tendencia = (
                pd.date_range(
                    datetime(cls.ano_inicio_estudo(uow) - 1, 1, 1),
                    datetime(
                        cls.ano_inicio_estudo(uow)
                        + cls.num_anos_estudo(uow)
                        - 1,
                        12,
                        1,
                    ),
                    freq="MS",
                ).tolist()
            )
            cls.DECK_DATA_CACHING[
                "datas_inicio_estagios_internos_politica_com_tendencia"
            ] = datas_inicio_estagios_internos_politica_com_tendencia
        return datas_inicio_estagios_internos_politica_com_tendencia

    @classmethod
    def datas_inicio_estagios_sim_final(
        cls, uow: AbstractUnitOfWork
    ) -> List[datetime]:
        datas_inicio_estagios_sim_final = cls.DECK_DATA_CACHING.get(
            "datas_inicio_estagios_sim_final"
        )
        if datas_inicio_estagios_sim_final is None:
            datas_inicio_estagios_sim_final = pd.date_range(
                datetime(
                    cls.ano_inicio_estudo(uow), cls.mes_inicio_estudo(uow), 1
                ),
                datetime(
                    cls.ano_inicio_estudo(uow)
                    + cls.num_anos_estudo(uow)
                    + cls.num_anos_pos_sim_final(uow)
                    - 1,
                    12,
                    1,
                ),
                freq="MS",
            ).tolist()
            cls.DECK_DATA_CACHING["datas_inicio_estagios_sim_final"] = (
                datas_inicio_estagios_sim_final
            )
        return datas_inicio_estagios_sim_final

    @classmethod
    def datas_inicio_estagios_internos_sim_final(
        cls, uow: AbstractUnitOfWork
    ) -> List[datetime]:
        datas_inicio_estagios_internos_sim_final = cls.DECK_DATA_CACHING.get(
            "datas_inicio_estagios_internos_sim_final"
        )
        if datas_inicio_estagios_internos_sim_final is None:
            datas_inicio_estagios_internos_sim_final = pd.date_range(
                datetime(cls.ano_inicio_estudo(uow), 1, 1),
                datetime(
                    cls.ano_inicio_estudo(uow)
                    + cls.num_anos_estudo(uow)
                    + cls.num_anos_pos_sim_final(uow)
                    - 1,
                    12,
                    1,
                ),
                freq="MS",
            ).tolist()
            cls.DECK_DATA_CACHING[
                "datas_inicio_estagios_internos_sim_final"
            ] = datas_inicio_estagios_internos_sim_final
        return datas_inicio_estagios_internos_sim_final

    @classmethod
    def datas_fim_estagios_internos_sim_final(
        cls, uow: AbstractUnitOfWork
    ) -> List[datetime]:
        datas_fim_estagios_internos_sim_final = cls.DECK_DATA_CACHING.get(
            "datas_fim_estagios_internos_sim_final"
        )
        if datas_fim_estagios_internos_sim_final is None:
            datas_fim_estagios_internos_sim_final = [
                d + relativedelta(months=1)
                for d in cls.datas_inicio_estagios_internos_sim_final(uow)
            ]
            cls.DECK_DATA_CACHING["datas_fim_estagios_internos_sim_final"] = (
                datas_fim_estagios_internos_sim_final
            )
        return datas_fim_estagios_internos_sim_final

    @classmethod
    def data_fim_estagios_individualizados_sim_final(
        cls, uow: AbstractUnitOfWork
    ) -> datetime:
        data_fim_estagios_individualizados_sim_final = (
            cls.DECK_DATA_CACHING.get(
                "data_fim_estagios_individualizados_sim_final"
            )
        )
        if data_fim_estagios_individualizados_sim_final is None:
            rees = cls.rees(uow)
            ano_inicio = cls.ano_inicio_estudo(uow)
            agregacao_sim_final = cls.agregacao_simulacao_final(uow)
            anos_estudo = cls.num_anos_estudo(uow)
            anos_pos_sim_final = cls.num_anos_pos_sim_final(uow)
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
            data_fim_estagios_individualizados_sim_final = fim
            cls.DECK_DATA_CACHING[
                "data_fim_estagios_individualizados_sim_final"
            ] = data_fim_estagios_individualizados_sim_final
        return data_fim_estagios_individualizados_sim_final

    @classmethod
    def configuracoes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        configuracoes = cls.DECK_DATA_CACHING.get("configuracoes")
        if configuracoes is None:
            pmo = cls.pmo(uow)
            configuracoes = cls._validate_data(
                pmo.configuracoes_qualquer_modificacao,
                pd.DataFrame,
                "configuracoes",
            )
            configuracoes = configuracoes.rename(
                columns={"data": START_DATE_COL}
            )

            cls.DECK_DATA_CACHING["configuracoes"] = configuracoes
        return configuracoes.copy()

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
                >= Deck.datas_inicio_estagios_sim_final(uow)[0]
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
            entity_map = cls.hydro_eer_submarket_map(uow)
            entity_map = (
                entity_map.reset_index()
                .drop_duplicates(subset=[EER_CODE_COL])
                .set_index(EER_CODE_COL)
            )
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
            dates = cls.datas_inicio_estagios_sim_final(uow)
            df = df.loc[df[START_DATE_COL].between(dates[0], dates[-1])]
            return df

        def _add_entity_data(df: pd.DataFrame) -> pd.DataFrame:
            eers = cls.eer_code_order(uow)
            num_configs = df.shape[0]
            df = pd.concat([df] * len(eers), ignore_index=True)
            df[EER_CODE_COL] = np.repeat(eers, num_configs)
            entity_map = cls.hydro_eer_submarket_map(uow)
            entity_map = (
                entity_map.reset_index()
                .drop_duplicates(subset=[EER_CODE_COL])
                .set_index(EER_CODE_COL)
            )
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
            configs_df = cls.configuracoes(uow)
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
    def convergencia(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        convergencia = cls.DECK_DATA_CACHING.get("convergencia")
        if convergencia is None:
            pmo = cls.pmo(uow)
            convergencia = cls._validate_data(
                pmo.convergencia,
                pd.DataFrame,
                "convergencia",
            )

            cls.DECK_DATA_CACHING["convergencia"] = convergencia
        return convergencia.copy()

    @classmethod
    def custos(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        custos = cls.DECK_DATA_CACHING.get("custos")
        if custos is None:
            pmo = cls.pmo(uow)
            custos = cls._validate_data(
                pmo.custo_operacao_series_simuladas,
                pd.DataFrame,
                "custos",
            )

            cls.DECK_DATA_CACHING["custos"] = custos
        return custos.copy()

    @classmethod
    def num_iteracoes(cls, uow: AbstractUnitOfWork) -> int:
        num_iteracoes = cls.DECK_DATA_CACHING.get("num_iteracoes")
        if num_iteracoes is None:
            df = cls.convergencia(uow)
            num_iteracoes = cls._validate_data(
                int(df["iteracao"].max()),
                int,
                "num_iteracoes",
            )

            cls.DECK_DATA_CACHING["num_iteracoes"] = num_iteracoes
        return num_iteracoes

    @classmethod
    def tempos_etapas(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        tempos_etapas = cls.DECK_DATA_CACHING.get("tempos_etapas")
        if tempos_etapas is None:
            arq = cls.newavetim(uow)
            tempos_etapas = cls._validate_data(
                arq.tempos_etapas,
                pd.DataFrame,
                "tempos_etapas",
            )

            cls.DECK_DATA_CACHING["tempos_etapas"] = tempos_etapas
        return tempos_etapas

    @classmethod
    def submercados(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        submercados = cls.DECK_DATA_CACHING.get("submercados")
        if submercados is None:
            submercados = cls._validate_data(
                cls._get_sistema(uow).custo_deficit,
                pd.DataFrame,
                "submercados",
            )
            submercados = submercados.drop_duplicates(
                subset=["codigo_submercado"]
            ).reset_index(drop=True)
            cls.DECK_DATA_CACHING["submercados"] = submercados
        return submercados.copy()

    @classmethod
    def rees(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        rees = cls.DECK_DATA_CACHING.get("rees")
        if rees is None:
            rees = cls._validate_data(
                cls._get_ree(uow).rees, pd.DataFrame, "REEs"
            )
            cls.DECK_DATA_CACHING["rees"] = rees
        return rees.copy()

    @classmethod
    def politica_hibrida(cls, uow: AbstractUnitOfWork) -> bool:
        politica_hibrida = cls.DECK_DATA_CACHING.get("politica_hibrida")
        if politica_hibrida is None:
            rees = cls.rees(uow)
            val = bool(rees["ano_fim_individualizado"].isna().sum() == 0)
            politica_hibrida = cls._validate_data(
                val,
                bool,
                "REEs",
            )
            cls.DECK_DATA_CACHING["politica_hibrida"] = politica_hibrida
        return politica_hibrida

    @classmethod
    def uhes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        uhes = cls.DECK_DATA_CACHING.get("uhes")
        if uhes is None:
            uhes = cls._validate_data(
                cls._get_confhd(uow).usinas, pd.DataFrame, "UHEs"
            )
            cls.DECK_DATA_CACHING["uhes"] = uhes
        return uhes.copy()

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
        num_stages = cls.num_estagios_individualizados_sf(uow)
        dates = cls.datas_inicio_estagios_sim_final(uow)[:num_stages]
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
            entities = (
                cls.hydro_eer_submarket_map(uow)
                .reset_index()
                .set_index(HYDRO_CODE_COL)
            )
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
            dates = cls.datas_inicio_estagios_sim_final(uow)
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
            entities = (
                cls.hydro_eer_submarket_map(uow)
                .reset_index()
                .set_index(HYDRO_CODE_COL)
            )
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
            entities = (
                cls.hydro_eer_submarket_map(uow)
                .reset_index()
                .set_index(HYDRO_CODE_COL)
            )
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
            dates = cls.datas_inicio_estagios_sim_final(uow)
            num_stages = len(dates)
            num_blocks = cls.numero_patamares(uow) + 1
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
            entities = (
                cls.hydro_eer_submarket_map(uow)
                .reset_index()
                .set_index(HYDRO_CODE_COL)
            )
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
            dates = cls.datas_inicio_estagios_sim_final(uow)
            num_stages = len(dates)
            num_blocks = cls.numero_patamares(uow) + 1
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
    def vazoes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        vazoes = cls.DECK_DATA_CACHING.get("vazoes")
        if vazoes is None:
            vazoes = cls._validate_data(
                cls._get_vazoes(uow).vazoes, pd.DataFrame, "vazoes"
            )
            cls.DECK_DATA_CACHING["vazoes"] = vazoes
        return vazoes.copy()

    @classmethod
    def utes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        utes = cls.DECK_DATA_CACHING.get("utes")
        if utes is None:
            utes = cls._validate_data(
                cls._get_conft(uow).usinas, pd.DataFrame, "UTEs"
            )
            cls.DECK_DATA_CACHING["utes"] = utes
        return utes.copy()

    @classmethod
    def numero_patamares(cls, uow: AbstractUnitOfWork) -> int:
        numero_patamares = cls.DECK_DATA_CACHING.get("numero_patamares")
        if numero_patamares is None:
            numero_patamares = cls._validate_data(
                cls._get_patamar(uow).numero_patamares,
                int,
                "número de patamares",
            )
            cls.DECK_DATA_CACHING["numero_patamares"] = numero_patamares
        return numero_patamares

    @classmethod
    def duracao_mensal_patamares(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:

        def __eval_pat0(df_pat: pd.DataFrame) -> pd.DataFrame:
            df_pat_0 = df_pat.groupby("data", as_index=False).sum(
                numeric_only=True
            )
            df_pat_0["patamar"] = 0
            df_pat = pd.concat([df_pat, df_pat_0], ignore_index=True)
            df_pat.sort_values(["data", "patamar"], inplace=True)
            return df_pat

        duracao_mensal_patamares = cls.DECK_DATA_CACHING.get(
            "duracao_mensal_patamares"
        )
        if duracao_mensal_patamares is None:
            duracao_mensal_patamares = cls._validate_data(
                cls._get_patamar(uow).duracao_mensal_patamares,
                pd.DataFrame,
                "duração dos patamares",
            )
            duracao_mensal_patamares = __eval_pat0(duracao_mensal_patamares)
            cls.DECK_DATA_CACHING["duracao_mensal_patamares"] = (
                duracao_mensal_patamares
            )
        return duracao_mensal_patamares.copy()

    @classmethod
    def energia_armazenada_inicial(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        energia_armazenada_inicial = cls.DECK_DATA_CACHING.get(
            "energia_armazenada_inicial"
        )
        if energia_armazenada_inicial is None:
            energia_armazenada_inicial = cls._validate_data(
                cls.pmo(uow).energia_armazenada_inicial,
                pd.DataFrame,
                "EARM inicial",
            )
            cls.DECK_DATA_CACHING["energia_armazenada_inicial"] = (
                energia_armazenada_inicial
            )
        return energia_armazenada_inicial.copy()

    @classmethod
    def volume_armazenado_inicial(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        volume_armazenado_inicial = cls.DECK_DATA_CACHING.get(
            "volume_armazenado_inicial"
        )
        if volume_armazenado_inicial is None:
            volume_armazenado_inicial = cls._validate_data(
                cls.pmo(uow).volume_armazenado_inicial,
                pd.DataFrame,
                "VARM inicial",
            )
            cls.DECK_DATA_CACHING["volume_armazenado_inicial"] = (
                volume_armazenado_inicial
            )
        return volume_armazenado_inicial.copy()

    @classmethod
    def eer_code_order(cls, uow: AbstractUnitOfWork) -> List[int]:
        eer_code_order = cls.DECK_DATA_CACHING.get("eer_code_order")
        if eer_code_order is None:
            eer_code_order = cls.rees(uow)["codigo"].tolist()
            cls.DECK_DATA_CACHING["eer_code_order"] = eer_code_order
        return eer_code_order

    @classmethod
    def hydro_code_order(cls, uow: AbstractUnitOfWork) -> List[int]:
        hydro_code_order = cls.DECK_DATA_CACHING.get("hydro_code_order")
        if hydro_code_order is None:
            hydro_code_order = cls.uhes(uow)["codigo_usina"].tolist()
            cls.DECK_DATA_CACHING["hydro_code_order"] = hydro_code_order
        return hydro_code_order

    @classmethod
    def hydro_eer_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df_aux = cls.DECK_DATA_CACHING.get("hydro_eer_submarket_map")
        if df_aux is None:
            confhd = cls.uhes(uow).astype({"nome_usina": STRING_DF_TYPE})
            confhd = confhd.set_index("nome_usina")
            rees = cls.rees(uow).astype({"nome": STRING_DF_TYPE})
            rees = rees.set_index("codigo")
            sistema = cls.submercados(uow).astype(
                {"nome_submercado": STRING_DF_TYPE}
            )
            sistema = sistema.drop_duplicates(
                ["codigo_submercado", "nome_submercado"]
            ).set_index("codigo_submercado")
            df_aux = pd.DataFrame(data={HYDRO_NAME_COL: confhd.index.tolist()})
            df_aux[HYDRO_CODE_COL] = df_aux[HYDRO_NAME_COL].apply(
                lambda u: confhd.at[u, "codigo_usina"]
            )
            df_aux[EER_CODE_COL] = df_aux[HYDRO_NAME_COL].apply(
                lambda u: confhd.at[u, "ree"]
            )
            df_aux[EER_NAME_COL] = df_aux[HYDRO_NAME_COL].apply(
                lambda u: rees.at[confhd.at[u, "ree"], "nome"]
            )
            df_aux[SUBMARKET_CODE_COL] = df_aux[HYDRO_NAME_COL].apply(
                lambda u: rees.at[confhd.at[u, "ree"], "submercado"]
            )
            df_aux[SUBMARKET_NAME_COL] = df_aux[SUBMARKET_CODE_COL].apply(
                lambda c: sistema.at[c, "nome_submercado"]
            )
            df_aux = df_aux.set_index(HYDRO_NAME_COL)
            cls.DECK_DATA_CACHING["hydro_eer_submarket_map"] = df_aux
        return df_aux.copy()

    @classmethod
    def thermal_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df_aux = cls.DECK_DATA_CACHING.get("thermal_submarket_map")
        if df_aux is None:
            utes = Deck.utes(uow).astype({"nome_usina": STRING_DF_TYPE})
            utes = utes.set_index("nome_usina")
            sistema = cls.submercados(uow).astype(
                {"nome_submercado": STRING_DF_TYPE}
            )
            sistema = sistema.drop_duplicates(
                ["codigo_submercado", "nome_submercado"]
            ).set_index("codigo_submercado")
            df_aux = pd.DataFrame(
                data={
                    THERMAL_CODE_COL: utes["classe"].tolist(),
                    THERMAL_NAME_COL: utes.index.tolist(),
                    SUBMARKET_CODE_COL: utes["submercado"].tolist(),
                }
            )
            df_aux[SUBMARKET_NAME_COL] = df_aux["codigo_submercado"].apply(
                lambda c: sistema.at[c, "nome_submercado"]
            )
            df_aux = df_aux.set_index(THERMAL_CODE_COL)
            cls.DECK_DATA_CACHING["thermal_submarket_map"] = df_aux
        return df_aux.copy()
