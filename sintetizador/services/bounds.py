import pandas as pd
import numpy as np
from typing import Dict, Callable, Type, TypeVar
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.operationsynthesis import OperationSynthesis
from sintetizador.services.unitofwork import AbstractUnitOfWork

from inewave.newave import Hidr, Modif
from inewave.newave.modelos.modif import VOLMIN, VOLMAX, VMINT, VMAXT


class OperationVariableBounds:

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
        ): lambda df, uow: OperationVariableBounds._varm_uhe_bounds(df, uow),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._varm_uhe_bounds(df, uow),
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
        # Obtem rees do DF na ordem em que aparecem
        rees_df = df["ree"].unique().tolist()
        codigos_rees = []
        limites_inferiores = np.zeros_like(rees_df, dtype=np.float64)
        limites_superiores = np.zeros_like(rees_df, dtype=np.float64)

    @classmethod
    def _varm_uhe_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:

        def _obtem_volume_hm3(
            volume_minimo_atual: float,
            volume_maximo_atual: float,
            volume_modif: float,
            unidade_volume: str,
        ) -> float:
            if unidade_volume == "'h'":
                return volume_modif
            else:
                return volume_minimo_atual + volume_modif / 100.0 * (
                    volume_maximo_atual - volume_minimo_atual
                )

        # Obtem usinas do DF na ordem em que aparecem
        usinas_df = df["usina"].unique().tolist()
        codigos_usinas = []
        limites_inferiores = np.zeros_like(usinas_df, dtype=np.float64)
        limites_superiores = np.zeros_like(usinas_df, dtype=np.float64)
        # Lê hidr.dat
        arq_hidr = cls._get_hidr(uow)
        df_hidr = cls._validate_data(arq_hidr.cadastro, pd.DataFrame)
        # Inicializa limites com valores do hidr.dat
        for i, u in enumerate(usinas_df):
            codigos_usinas.append(
                df_hidr.loc[df_hidr["nome_usina"] == u].index[0]
            )
            limites_inferiores[i] = df_hidr.loc[
                df_hidr["nome_usina"] == u, "volume_minimo"
            ].iloc[0]
            limites_superiores[i] = df_hidr.loc[
                df_hidr["nome_usina"] == u, "volume_maximo"
            ].iloc[0]
        # Lê modif.dat
        arq_modif = cls._get_modif(uow)
        # Atualiza limites com valores dos VOLMIN e VOLMAX do modif.dat
        for i, u in enumerate(codigos_usinas):
            modificacoes_usina = arq_modif.modificacoes_usina(u)
            if modificacoes_usina is not None:
                volmin_usina = [
                    r for r in modificacoes_usina if isinstance(r, VOLMIN)
                ]
                if len(volmin_usina) > 0:
                    limites_inferiores[i] = volmin_usina[-1].volume
                volmax_usina = [
                    r for r in modificacoes_usina if isinstance(r, VOLMAX)
                ]
                if len(volmax_usina) > 0:
                    limites_superiores[i] = volmax_usina[-1].volume
        # Atualiza limites com valores de VMINT e VMAXT do modif.dat
        datas_inicio = df["dataInicio"].unique().tolist()
        n_estagios = len(datas_inicio)
        limites_inferiores_estagios = np.repeat(limites_inferiores, n_estagios)
        limites_superiores_estagios = np.repeat(limites_superiores, n_estagios)
        for i, u in enumerate(codigos_usinas):
            modificacoes_usina = arq_modif.modificacoes_usina(u)
            i_i = i * n_estagios
            i_f = i_i + n_estagios
            if modificacoes_usina is not None:
                vmint_usina = [
                    r for r in modificacoes_usina if isinstance(r, VMINT)
                ]
                for r_vmin in vmint_usina:
                    idx_data = datas_inicio.index(r_vmin.data_inicio)
                    limites_inferiores_estagios[i_i + idx_data : i_f] = (
                        _obtem_volume_hm3(
                            limites_inferiores[i],
                            limites_superiores[i],
                            r_vmin.volume,
                            r_vmin.unidade,
                        )
                    )
                vmaxt_usina = [
                    r for r in modificacoes_usina if isinstance(r, VMAXT)
                ]
                for r_vmax in vmaxt_usina:
                    idx_data = datas_inicio.index(r_vmax.data_inicio)
                    limites_superiores_estagios[i_i + idx_data : i_f] = (
                        _obtem_volume_hm3(
                            limites_inferiores[i],
                            limites_superiores[i],
                            r_vmax.volume,
                            r_vmax.unidade,
                        )
                    )
        # Adiciona ao df e retorna
        n_cenarios = len(df["serie"].unique())
        limite_inferior_cadastral = np.round(
            np.repeat(limites_inferiores, n_cenarios * n_estagios), 2
        )
        df["limiteInferior"] = np.round(
            np.repeat(limites_inferiores_estagios, n_cenarios), 2
        )
        df["limiteSuperior"] = np.round(
            np.repeat(limites_superiores_estagios, n_cenarios), 2
        )
        df["valor"] += limite_inferior_cadastral
        return df

    @classmethod
    def resolve_bounds(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        if s in cls.MAPPINGS:
            return cls.MAPPINGS[s](df, uow)
        return df
