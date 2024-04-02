from inewave.newave import (
    Dger,
    Ree,
    Confhd,
    Conft,
    Sistema,
    Clast,
    Hidr,
    Patamar,
    Shist,
    Pmo,
)
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd  # type: ignore
from typing import Any, Optional, TypeVar, Type, List

from app.services.unitofwork import AbstractUnitOfWork


class Deck:

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
    def considera_geracao_eolica(cls, uow: AbstractUnitOfWork) -> int:
        considera_geracao_eolica = cls.DECK_DATA_CACHING.get(
            "considera_geracao_eolica"
        )
        if considera_geracao_eolica is None:
            considera_geracao_eolica = cls._validate_data(
                cls.dger(uow).considera_geracao_eolica,
                int,
                "consideração da geração eólica",
            )
            cls.DECK_DATA_CACHING["considera_geracao_eolica"] = (
                considera_geracao_eolica
            )
        return considera_geracao_eolica

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
    def uhes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        uhes = cls.DECK_DATA_CACHING.get("uhes")
        if uhes is None:
            uhes = cls._validate_data(
                cls._get_confhd(uow).usinas, pd.DataFrame, "UHEs"
            )
            cls.DECK_DATA_CACHING["uhes"] = uhes
        return uhes.copy()

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
        return energia_armazenada_inicial

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
        return volume_armazenado_inicial

    @classmethod
    def uhes_rees_submercados_map(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        df_aux = cls.DECK_DATA_CACHING.get("uhes_rees_submercados_map")
        if df_aux is None:
            confhd = cls.uhes(uow).set_index("nome_usina")
            rees = cls.rees(uow).set_index("codigo")
            sistema = cls.submercados(uow)
            sistema = sistema.drop_duplicates(
                ["codigo_submercado", "nome_submercado"]
            ).set_index("codigo_submercado")
            df_aux = pd.DataFrame(data={"uhes": confhd.index.tolist()})
            df_aux["ree"] = df_aux["uhes"].apply(
                lambda u: rees.at[confhd.at[u, "ree"], "nome"]
            )
            df_aux["codigo_submercado"] = df_aux["uhes"].apply(
                lambda u: rees.at[confhd.at[u, "ree"], "submercado"]
            )
            df_aux["submercado"] = df_aux["codigo_submercado"].apply(
                lambda c: sistema.at[c, "nome_submercado"]
            )
            df_aux = df_aux.set_index("uhes")
            cls.DECK_DATA_CACHING["uhes_rees_submercados_map"] = df_aux
        return df_aux.copy()
