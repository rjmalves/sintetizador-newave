import pandas as pd  # type: ignore
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Callable, Type, TypeVar, Optional, Union
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.operationsynthesis import OperationSynthesis
from sintetizador.services.unitofwork import AbstractUnitOfWork
from time import time

from inewave.newave import Hidr, Modif
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
)
from cfinterface.components.register import Register


class OperationVariableBounds:
    """
    Entidade responsável por calcular os limites das variáveis de operação
    existentes nos arquivos de saída do NEWAVE, que são processadas no
    processo de síntese da operação.
    """

    STAGE_DURATION_HOURS = 730.0
    HM3_M3S_FACTOR = 1 / 2.36

    T = TypeVar("T")

    MAPPINGS: Dict[OperationSynthesis, Callable] = {
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow: OperationVariableBounds._earm_ree_bounds(df, uow),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow: OperationVariableBounds._earm_ree_bounds(df, uow),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._varm_varp_uhe_bounds(
            df, uow, unidade_sintese="'h'"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._varm_varp_uhe_bounds(
            df, uow, unidade_sintese="'h'"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._varm_varp_uhe_bounds(
            df, uow, unidade_sintese="'%'"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._varm_varp_uhe_bounds(
            df, uow, unidade_sintese="'%'"
        ),
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qdef_vdef_uhe_bounds(
            df, uow, unidade_sintese="hm3"
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qdef_vdef_uhe_bounds(
            df, uow, unidade_sintese="m3/s"
        ),
    }

    @classmethod
    def _get_hidr(cls, uow: AbstractUnitOfWork) -> Hidr:
        with uow:
            hidr = uow.files.get_hidr()
            if hidr is None:
                raise RuntimeError("Erro na leitura do arquivo hidr.dat")
            return hidr

    @classmethod
    def _get_modif(cls, uow: AbstractUnitOfWork) -> Modif:
        with uow:
            modif = uow.files.get_modif()
            if modif is None:
                raise RuntimeError("Erro na leitura do arquivo modif.dat")
            return modif

    @classmethod
    def _validate_data(cls, data, type: Type[T]) -> T:
        if not isinstance(data, type):
            raise RuntimeError("Erro na validação dos dados.")
        return data

    @classmethod
    def _earm_ree_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Energia Armazenada Absoluta (EARM) para cada REE.
        """
        # Obtem rees do DF na ordem em que aparecem
        rees_df = df["ree"].unique().tolist()
        codigos_rees: List[int] = []
        limites_inferiores = np.zeros_like(rees_df, dtype=np.float64)
        limites_superiores = np.zeros_like(rees_df, dtype=np.float64)

    @classmethod
    def _codigos_usinas(
        cls,
        df: pd.DataFrame,
        df_hidr: pd.DataFrame,
        n_estagios: int,
        n_patamares: int,
    ) -> np.ndarray:
        """
        Retorna os códigos únicos das usinas, na ordem em que aparecem
        no DataFrame da síntese em processamento.
        """
        usinas_df = df["usina"].unique().tolist()
        codigos_usinas = []
        for u in usinas_df:
            codigos_usinas.append(
                df_hidr.loc[df_hidr["nome_usina"] == u].index[0]
            )
        return np.repeat(codigos_usinas, n_estagios * n_patamares)

    @classmethod
    def _duracoes_patamares_estagios(cls, df: pd.DataFrame) -> np.ndarray:
        """
        Retorna as durações dos patamares em cada estágio, na ordem em que aparecem
        no DataFrame da síntese em processamento.
        """
        ex = df["serie"].unique().tolist()[0]
        return df.loc[df["serie"] == ex, "duracaoPatamar"].to_numpy()

    @classmethod
    def _dado_cadastral_hidr_uhes(
        cls,
        df_hidr: pd.DataFrame,
        codigos_usinas: np.ndarray,
        coluna: str,
    ) -> np.ndarray:
        """
        Constroi um array com um dado cadastral `coluna` das usinas
        hidrelétricas extraído do `df_hidr`, na mesma ordem em que foram
        fornecidos os códigos `codigos_usinas`.
        """
        dados = np.zeros_like(codigos_usinas, dtype=np.float64)
        for i, u in enumerate(codigos_usinas):
            dados[i] = df_hidr.at[u, coluna]
        return dados

    @classmethod
    def _converte_volume_percentual_hm3(
        cls,
        volume_minimo_atual: float,
        volume_maximo_atual: float,
        volume_modif: float,
    ) -> float:
        """
        Realiza a conversão de um volume fornecido em percentual para
        um volume em hm3, considerando os limites inferior e superior.
        """
        return volume_minimo_atual + volume_modif / 100.0 * (
            volume_maximo_atual - volume_minimo_atual
        )

    @classmethod
    def _converte_volume_hm3_percentual(
        cls,
        volume_minimo_atual: float,
        volume_maximo_atual: float,
        volume_modif: float,
    ) -> float:
        """
        Realiza a conversão de um volume fornecido em hm3 para
        um volume em percentual, considerando os limites inferior e superior.
        """
        return (
            (volume_modif - volume_minimo_atual)
            * 100
            / (volume_maximo_atual - volume_minimo_atual)
        )

    @classmethod
    def _converte_volume_vazao(
        cls,
        duracao_patamar_horas: float,
        volume_modif: float,
    ) -> float:
        """
        Realiza a conversão de um volume fornecido em hm3 para
        uma vazão em m3/s, considerando um período de horas para cálculo.
        """
        return (
            volume_modif
            * (cls.STAGE_DURATION_HOURS * cls.HM3_M3S_FACTOR)
            / duracao_patamar_horas
        )

    @classmethod
    def _converte_vazao_volume(
        cls,
        duracao_patamar_horas: float,
        vazao_modif: float,
    ) -> float:
        """
        Realiza a conversão de uma vazão em m3/s para um volume fornecido
        em hm3, considerando um período de horas para cálculo.
        """
        return (
            vazao_modif
            * duracao_patamar_horas
            / (cls.STAGE_DURATION_HOURS * cls.HM3_M3S_FACTOR)
        )

    @classmethod
    def _converte_unidade_cadastro_unidade_sintese(
        cls,
        valor_cadastro: Optional[float],
        unidade_cadastro: str,
        unidade_sintese: str,
        *args,
        **kwargs,
    ) -> Optional[float]:
        """
        Converte a unidade de um valor de alteração cadastral fornecido
        em um cadastro para a unidade de síntese da variável.

        Dependendo da conversão que é feita, são esperados argumentos adicionais:

        - Para conversão de volume percentual para hm3, espera-se os limites
            inferior e superior do volume cadastral (`limite_inferior_cadastral`
             e `limite_superior_cadastral`).
        - Para conversão de volume em hm3 para percentual, espera-se os limites
            inferior e superior do volume cadastral (`limite_inferior_cadastral`
             e `limite_superior_cadastral`).
        - Para conversão de vazão em m3/s para hm3, espera-se a duração do
            patamar em horas (`duracao_patamar_horas`).
        - Para conversão de volume em hm3 para vazão em m3/s, espera-se a duração
            do patamar em horas (`duracao_patamar_horas`).

        """
        if valor_cadastro is None:
            return valor_cadastro
        limite_inferior = kwargs.get("limite_inferior_cadastral", 0)
        limite_superior = kwargs.get("limite_superior_cadastral", float("inf"))
        duracao_patamar_horas = kwargs.get(
            "duracao_patamar_horas", cls.STAGE_DURATION_HOURS
        )
        if unidade_cadastro == "'%'" and unidade_sintese == "hm3":
            return cls._converte_volume_percentual_hm3(
                limite_inferior,
                limite_superior,
                valor_cadastro,
            )
        elif unidade_cadastro == "'h'" and unidade_sintese == "%":
            return cls._converte_volume_hm3_percentual(
                limite_inferior,
                limite_superior,
                valor_cadastro,
            )
        elif unidade_cadastro == "hm3" and unidade_sintese == "m3/s":
            return cls._converte_volume_vazao(
                duracao_patamar_horas, valor_cadastro
            )
        elif unidade_cadastro == "m3/s" and unidade_sintese == "hm3":
            return cls._converte_vazao_volume(
                duracao_patamar_horas, valor_cadastro
            )
        else:
            return valor_cadastro

    @classmethod
    def _converte_unidades_cadastro_unidades_sintese(
        cls,
        df: pd.DataFrame,
        dados_cadastrais: np.ndarray,
        unidades_cadastrais: np.ndarray,
        unidade_sintese: str,
        *args,
        **kwargs,
    ):
        duracoes_patamares_horas = cls._duracoes_patamares_estagios(df)
        limites_inferiores = kwargs.get("limites_inferiores", None)
        limites_superiores = kwargs.get("limites_superiores", None)
        for i in range(len(dados_cadastrais)):
            dados_cadastrais[i] = (
                cls._converte_unidade_cadastro_unidade_sintese(
                    dados_cadastrais[i],
                    unidades_cadastrais[i],
                    unidade_sintese,
                    duracao_patamar_horas=duracoes_patamares_horas[i],
                    limite_inferior_cadastral=(
                        limites_inferiores[i]
                        if limites_inferiores is not None
                        else 0.0
                    ),
                    limite_superior_cadastral=(
                        limites_superiores[i]
                        if limites_superiores is not None
                        else float("inf")
                    ),
                )
            )
        return dados_cadastrais

    @classmethod
    def _extrai_dados_modif_uhe(
        cls, registro: Register
    ) -> Tuple[Optional[float], Optional[str]]:
        """
        Extrai um dado de um registro do modif.dat com a sua unidade.
        """
        if isinstance(registro, VOLMIN):
            return registro.volume, registro.unidade
        elif isinstance(registro, VMINT):
            return registro.volume, registro.unidade
        elif isinstance(registro, VOLMAX):
            return registro.volume, registro.unidade
        elif isinstance(registro, VMAXT):
            return registro.volume, registro.unidade
        elif isinstance(registro, VAZMIN):
            return registro.vazao, "m3/s"
        elif isinstance(registro, VAZMINT):
            return registro.vazao, "m3/s"
        elif isinstance(registro, VAZMAXT):
            return registro.vazao, "m3/s"
        elif isinstance(registro, TURBMINT):
            return registro.turbinamento, "m3/s"
        elif isinstance(registro, TURBMAXT):
            return registro.turbinamento, "m3/s"
        return None, None

    @classmethod
    def _modificacoes_cadastro_uhes(
        cls,
        dados_cadastrais: np.ndarray,
        unidades: np.ndarray,
        arq_modif: Modif,
        tipo_registro_modif: Type[Union[VAZMIN, VOLMIN, VOLMAX]],
        codigos_usinas: np.ndarray,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Realiza a extração de modificações cadastrais de volumes de usinas
        hidrelétricas a partir do arquivo modif.dat, atualizando os cadastros
        conforme as declarações de modificações são encontradas.
        """
        for i, u in enumerate(codigos_usinas):
            modificacoes_usina = arq_modif.modificacoes_usina(u)
            if modificacoes_usina is not None:
                regs_usina = [
                    r
                    for r in modificacoes_usina
                    if isinstance(r, tipo_registro_modif)
                ]
                if len(regs_usina) > 0:
                    registro = regs_usina[-1]
                    valor, unidade = cls._extrai_dados_modif_uhe(registro)
                    if valor is not None:
                        dados_cadastrais[i] = valor
                    if unidade is not None:
                        unidades[i] = unidade.lower()
        return dados_cadastrais, unidades

    @classmethod
    def _modificacoes_cadastro_temporais_uhes(
        cls,
        dados_cadastrais: np.ndarray,
        unidades: np.ndarray,
        datas: List[datetime],
        n_estagios: int,
        n_patamares: int,
        arq_modif: Modif,
        tipo_registro_modif: Type[
            Union[VMINT, VMAXT, VAZMINT, TURBMINT, TURBMAXT]
        ],
        codigos_usinas: np.ndarray,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Realiza a extração de modificações cadastrais de volumes de usinas
        hidrelétricas a partir do arquivo modif.dat, considerando também
        modificações cadastrais com relação temporal. Os cadastros são
        expandidos para um valor por usina e estágio e são atualizados
        conforme as declarações de modificações são encontradas.
        """
        for i, u in enumerate(codigos_usinas):
            modificacoes_usina = arq_modif.modificacoes_usina(u)
            i_i = i * n_estagios * n_patamares
            i_f = i_i + n_estagios * n_patamares
            if modificacoes_usina is not None:
                registros_usina = [
                    r
                    for r in modificacoes_usina
                    if isinstance(r, tipo_registro_modif)
                ]
                for reg in registros_usina:
                    idx_data = datas.index(reg.data_inicio)
                    valor, unidade = cls._extrai_dados_modif_uhe(reg)
                    dados_cadastrais[i_i + idx_data : i_f] = valor
                    unidades[i_i + idx_data : i_f] = unidade
        return dados_cadastrais, unidades

    @classmethod
    def _expande_dados_para_cenarios(
        cls,
        dados_cadastrais: np.ndarray,
        n_entidades: int,
        n_estagios: int,
        n_cenarios: int,
        n_patamares: int,
    ):
        """
        Expande os dados cadastrais para cada cenário, mantendo a ordem dos
        patamares internamente.
        """
        dados_cadastrais_cenarios = np.zeros(
            (len(dados_cadastrais) * n_cenarios,), dtype=np.float64
        )
        for i in range(n_entidades):
            for j in range(n_estagios):
                i_i = i * n_estagios * n_patamares + j * n_patamares
                i_f = i_i + n_patamares
                dados_cadastrais_cenarios[
                    i_i * n_cenarios : i_f * n_cenarios
                ] = np.tile(dados_cadastrais[i_i:i_f], n_cenarios)
        return dados_cadastrais_cenarios

    @classmethod
    def _varm_varp_uhe_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, unidade_sintese: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Armazenado Absoluto (VARM) e Volume
        Armazenado Percentual (VARP) para cada UHE.
        """
        datas_inicio = df["dataInicio"].unique().tolist()
        n_usinas = len(df["usina"].unique())
        n_estagios = len(datas_inicio)
        n_cenarios = len(df["serie"].unique())
        n_patamares = len(df["patamar"].unique())
        # Lê hidr.dat
        arq_hidr = cls._get_hidr(uow)
        df_hidr = cls._validate_data(arq_hidr.cadastro, pd.DataFrame)
        # Obtem usinas do df na ordem em que aparecem e durações dos patamares
        codigos_usinas = cls._codigos_usinas(
            df, df_hidr, n_estagios, n_patamares
        )
        # Inicializa limites com valores do hidr.dat
        limites_inferiores = cls._dado_cadastral_hidr_uhes(
            df_hidr, codigos_usinas, "volume_minimo"
        )
        unidades_limites_inferiores = np.array(
            ["'h'"] * len(limites_inferiores)
        )
        limites_superiores = cls._dado_cadastral_hidr_uhes(
            df_hidr, codigos_usinas, "volume_maximo"
        )
        unidades_limites_superiores = np.array(
            ["'h'"] * len(limites_inferiores)
        )
        # Lê modif.dat
        arq_modif = cls._get_modif(uow)
        # Atualiza limites com valores dos VOLMIN e VOLMAX do modif.dat
        limites_inferiores, unidades_limites_inferiores = (
            cls._modificacoes_cadastro_uhes(
                limites_inferiores,
                unidades_limites_inferiores,
                arq_modif,
                VOLMIN,
                codigos_usinas,
            )
        )
        limites_superiores, unidades_limites_superiores = (
            cls._modificacoes_cadastro_uhes(
                limites_superiores,
                unidades_limites_superiores,
                arq_modif,
                VOLMAX,
                codigos_usinas,
            )
        )
        # Atualiza limites com valores de VMINT e VMAXT do modif.dat
        limites_inferiores, unidades_limites_inferiores = (
            cls._modificacoes_cadastro_temporais_uhes(
                limites_inferiores,
                unidades_limites_inferiores,
                datas_inicio,
                n_estagios,
                n_patamares,
                arq_modif,
                VMINT,
                codigos_usinas,
            )
        )
        limites_superiores, unidades_limites_superiores = (
            cls._modificacoes_cadastro_temporais_uhes(
                limites_superiores,
                unidades_limites_superiores,
                datas_inicio,
                n_estagios,
                n_patamares,
                arq_modif,
                VMAXT,
                codigos_usinas,
            )
        )
        # Converte limites para a unidade de síntese
        limites_inferiores = cls._converte_unidades_cadastro_unidades_sintese(
            df,
            limites_inferiores,
            unidades_limites_inferiores,
            unidade_sintese,
            limites_inferiores=limites_inferiores,
            limites_superiores=limites_superiores,
        )
        limites_superiores = cls._converte_unidades_cadastro_unidades_sintese(
            df,
            limites_superiores,
            unidades_limites_superiores,
            unidade_sintese,
            limites_inferiores=limites_inferiores,
            limites_superiores=limites_superiores,
        )
        # Constroi limites para cada estágio e cenario
        limites_inferiores_cenarios = cls._expande_dados_para_cenarios(
            limites_inferiores, n_usinas, n_estagios, n_cenarios, n_patamares
        )
        limites_superiores_cenarios = cls._expande_dados_para_cenarios(
            limites_superiores, n_usinas, n_estagios, n_cenarios, n_patamares
        )
        # Adiciona ao df e retorna
        df["limiteInferior"] = np.round(limites_inferiores_cenarios, 2)
        df["limiteSuperior"] = np.round(limites_superiores_cenarios, 2)
        # Específico do VARM: soma o limite inferior cadastral, pois o representado
        # nos arquivos de saída é em volume útil (hm3).
        if unidade_sintese == "'h'":
            df["valor"] += limites_inferiores_cenarios
        return df

    @classmethod
    def _qdef_vdef_uhe_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, unidade_sintese: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Defluente (VDEF) e Vazão Defluente (QDEF)
        para cada UHE.
        """
        datas_inicio = df["dataInicio"].unique().tolist()
        n_usinas = len(df["usina"].unique())
        n_estagios = len(datas_inicio)
        n_cenarios = len(df["serie"].unique())
        n_patamares = len(df["patamar"].unique())
        # Lê hidr.dat
        arq_hidr = cls._get_hidr(uow)
        df_hidr = cls._validate_data(arq_hidr.cadastro, pd.DataFrame)
        # Obtem usinas do df na ordem em que aparecem e durações dos patamares
        codigos_usinas = cls._codigos_usinas(
            df, df_hidr, n_estagios, n_patamares
        )
        # Inicializa limites com valores do hidr.dat
        limites_inferiores = cls._dado_cadastral_hidr_uhes(
            df_hidr, codigos_usinas, "vazao_minima_historica"
        )
        unidades = np.array(["m3/s"] * len(limites_inferiores))
        # Lê modif.dat
        arq_modif = cls._get_modif(uow)
        # Atualiza limites com valores dos VAZMIN do modif.dat
        limites_inferiores, unidades = cls._modificacoes_cadastro_uhes(
            limites_inferiores, unidades, arq_modif, VAZMIN, codigos_usinas
        )
        # Atualiza limites com valores de VAZMINT do modif.dat
        limites_inferiores, unidades = (
            cls._modificacoes_cadastro_temporais_uhes(
                limites_inferiores,
                unidades,
                datas_inicio,
                n_estagios,
                n_patamares,
                arq_modif,
                VAZMINT,
                codigos_usinas,
            )
        )
        # Converte limites para a unidade de síntese
        limites_inferiores = cls._converte_unidades_cadastro_unidades_sintese(
            df, limites_inferiores, unidades, unidade_sintese
        )
        # Constroi limites para cada estágio e cenario
        limites_inferiores_cenarios = cls._expande_dados_para_cenarios(
            limites_inferiores, n_usinas, n_estagios, n_cenarios, n_patamares
        )
        # Adiciona ao df e retorna
        df["valor"] = np.round(df["valor"], 2)
        df["limiteInferior"] = np.round(limites_inferiores_cenarios, 2)
        df["limiteSuperior"] = float("inf")
        return df

    @classmethod
    def resolve_bounds(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        if s in cls.MAPPINGS:
            return cls.MAPPINGS[s](df, uow)
        return df
