import asyncio
import pathlib
import platform
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from os.path import join
from typing import Any, Callable, Dict, Optional, Tuple, Type, TypeVar

import numpy as np
import pandas as pd
from cfinterface.files.blockfile import BlockFile
from inewave.libs.eolica import Eolica
from inewave.newave.arquivos import Arquivos
from inewave.newave.caso import Caso
from inewave.newave.clast import Clast
from inewave.newave.confhd import Confhd
from inewave.newave.conft import Conft
from inewave.newave.curva import Curva
from inewave.newave.dger import Dger
from inewave.newave.dsvagua import Dsvagua
from inewave.newave.enavazb import Enavazb
from inewave.newave.enavazf import Enavazf
from inewave.newave.energiab import Energiab
from inewave.newave.energiaf import Energiaf
from inewave.newave.energias import Energias
from inewave.newave.engnat import Engnat
from inewave.newave.expt import Expt
from inewave.newave.hidr import Hidr
from inewave.newave.manutt import Manutt
from inewave.newave.modif import Modif
from inewave.newave.newavetim import Newavetim
from inewave.newave.patamar import Patamar
from inewave.newave.pmo import Pmo
from inewave.newave.ree import Ree
from inewave.newave.shist import Shist
from inewave.newave.sistema import Sistema
from inewave.newave.term import Term
from inewave.newave.vazaob import Vazaob
from inewave.newave.vazaof import Vazaof
from inewave.newave.vazaos import Vazaos
from inewave.newave.vazoes import Vazoes
from inewave.nwlistcf import Estados, Nwlistcfrel  # type: ignore[attr-defined]
from inewave.nwlistop.cmarg import Cmarg
from inewave.nwlistop.cmargmed import Cmargmed

from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.model.settings import Settings
from app.utils.encoding import converte_codificacao

if platform.system() == "Windows":
    Dger.ENCODING = "iso-8859-1"


class AbstractFilesRepository(ABC):
    T = TypeVar("T")

    def _validate_data(self, data: Any, type: Type[T]) -> T:
        if not isinstance(data, type):
            raise RuntimeError()
        return data

    @property
    @abstractmethod
    def caso(self) -> Caso:
        raise NotImplementedError

    @property
    @abstractmethod
    def arquivos(self) -> Arquivos:
        raise NotImplementedError

    @property
    @abstractmethod
    def indices(self) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def get_dger(self) -> Optional[Dger]:
        raise NotImplementedError

    @abstractmethod
    def get_shist(self) -> Optional[Shist]:
        raise NotImplementedError

    @abstractmethod
    def get_confhd(self) -> Optional[Confhd]:
        raise NotImplementedError

    @abstractmethod
    def get_dsvagua(self) -> Optional[Dsvagua]:
        raise NotImplementedError

    @abstractmethod
    def get_modif(self) -> Optional[Modif]:
        raise NotImplementedError

    @abstractmethod
    def get_conft(self) -> Optional[Conft]:
        raise NotImplementedError

    @abstractmethod
    def get_clast(self) -> Optional[Clast]:
        raise

    @abstractmethod
    def get_term(self) -> Optional[Term]:
        raise NotImplementedError

    @abstractmethod
    def get_manutt(self) -> Optional[Manutt]:
        raise NotImplementedError

    @abstractmethod
    def get_expt(self) -> Optional[Expt]:
        raise NotImplementedError

    @abstractmethod
    def get_ree(self) -> Optional[Ree]:
        raise NotImplementedError

    @abstractmethod
    def get_curva(self) -> Optional[Curva]:
        raise NotImplementedError

    @abstractmethod
    def get_sistema(self) -> Optional[Sistema]:
        raise NotImplementedError

    @abstractmethod
    def get_patamar(self) -> Optional[Patamar]:
        raise NotImplementedError

    @abstractmethod
    def get_pmo(self) -> Optional[Pmo]:
        raise NotImplementedError

    @abstractmethod
    def get_newavetim(self) -> Optional[Newavetim]:
        raise NotImplementedError

    @abstractmethod
    def get_eolica(self) -> Optional[Eolica]:
        raise NotImplementedError

    @abstractmethod
    def get_nwlistop(
        self,
        variable: Variable,
        spatial_resolution: SpatialResolution,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[pd.DataFrame]:
        pass

    @abstractmethod
    def get_nwlistcf_cortes(self) -> Optional[Nwlistcfrel]:
        raise NotImplementedError

    @abstractmethod
    def get_nwlistcf_estados(self) -> Optional[Estados]:
        raise NotImplementedError

    @abstractmethod
    def get_energiaf(self, iteracao: int) -> Optional[Energiaf]:
        pass

    @abstractmethod
    def get_energiab(self, iteracao: int) -> Optional[Energiab]:
        pass

    @abstractmethod
    def get_vazaof(self, iteracao: int) -> Optional[Vazaof]:
        pass

    @abstractmethod
    def get_vazaob(self, iteracao: int) -> Optional[Vazaob]:
        pass

    @abstractmethod
    def get_enavazf(self, iteracao: int) -> Optional[Enavazf]:
        pass

    @abstractmethod
    def get_enavazb(self, iteracao: int) -> Optional[Enavazb]:
        pass

    @abstractmethod
    def get_energias(self) -> Optional[Energias]:
        pass

    @abstractmethod
    def get_enavazs(self) -> Optional[Energias]:
        pass

    @abstractmethod
    def get_vazaos(self) -> Optional[Vazaos]:
        pass

    @abstractmethod
    def get_vazoes(self) -> Optional[Vazoes]:
        pass

    @abstractmethod
    def get_engnat(self) -> Optional[Engnat]:
        pass

    @abstractmethod
    def get_hidr(self) -> Optional[Hidr]:
        pass

    @abstractmethod
    def _numero_estagios_individualizados_politica(self) -> int:
        pass

    @abstractmethod
    def _numero_estagios_individualizados_sf(self) -> int:
        pass


class RawFilesRepository(AbstractFilesRepository):
    def __init__(self, tmppath: str, version: str = "latest"):
        self.__tmppath = tmppath
        self.__version = version
        self.__caso = Caso.read(join(str(self.__tmppath), "caso.dat"))
        self.__arquivos: Optional[Arquivos] = None
        self.__indices: Optional[pd.DataFrame] = None
        self.__dger: Optional[Dger] = None
        self.__shist: Optional[Shist] = None
        self.__patamar: Optional[Patamar] = None
        self.__sistema: Optional[Sistema] = None
        self.__pmo: Optional[Pmo] = None
        self.__newavetim: Optional[Newavetim] = None
        self.__curva: Optional[Curva] = None
        self.__ree: Optional[Ree] = None
        self.__confhd: Optional[Confhd] = None
        self.__dsvagua: Optional[Dsvagua] = None
        self.__modif: Optional[Modif] = None
        self.__conft: Optional[Conft] = None
        self.__clast: Optional[Clast] = None
        self.__term: Optional[Term] = None
        self.__manutt: Optional[Manutt] = None
        self.__expt: Optional[Expt] = None
        self.__eolica: Optional[Eolica] = None
        self.__nwlistcf: Optional[Nwlistcfrel] = None
        self.__estados: Optional[Estados] = None
        self.__energiaf: Dict[int, Energiaf] = {}
        self.__energiab: Dict[int, Energiab] = {}
        self.__vazaof: Dict[int, Vazaof] = {}
        self.__vazaob: Dict[int, Vazaob] = {}
        self.__enavazf: Dict[int, Enavazf] = {}
        self.__enavazb: Dict[int, Enavazb] = {}
        self.__energias: Optional[Energias] = None
        self.__enavazs: Optional[Energias] = None
        self.__vazaos: Optional[Vazaos] = None
        self.__vazoes: Optional[Vazoes] = None
        self.__engnat: Optional[Engnat] = None
        self.__hidr: Optional[Hidr] = None
        from app.adapters.repository.mappings import build_regras

        self._regras: Dict[
            Tuple[Variable, SpatialResolution], Callable[..., Any]
        ] = build_regras(self)

    def _read_nwlistop_setting_version(
        self, reader: Type[BlockFile], path: str
    ) -> Optional[pd.DataFrame]:
        df: Optional[pd.DataFrame] = reader.read(  # type: ignore[union-attr]
            path, version=self.__version
        ).valores
        if df is not None and "valor" in df.columns:
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        return df

    def _fix_indices_cenarios(self, df: pd.DataFrame) -> pd.DataFrame:
        anos = df["data"].dt.year.unique().tolist()
        num_patamares = (
            1 if "patamar" not in df.columns else len(df["patamar"].unique())
        )
        num_series = df.loc[df["data"].dt.year == anos[0]].shape[0] // (
            12 * num_patamares
        )
        df["serie"] = np.tile(
            np.repeat(np.arange(1, num_series + 1), 12 * num_patamares),
            len(anos),
        )
        return df

    def _agg_cmo_dfs(self, dir: str, submercado: int) -> pd.DataFrame:
        df_med = Cmargmed.read(  # type: ignore[union-attr]
            join(dir, f"cmarg{str(submercado).zfill(3)}-med.out"),
            version=self.__version,
        ).valores
        df_med["patamar"] = 0
        df_med = self._fix_indices_cenarios(df_med)
        df_pats = Cmarg.read(  # type: ignore[union-attr]
            join(dir, f"cmarg{str(submercado).zfill(3)}.out"),
            version=self.__version,
        ).valores
        df_pats = self._fix_indices_cenarios(df_pats)
        df = pd.concat(
            [df_med, df_pats],
            ignore_index=True,
        )
        df = df.sort_values(["data", "serie", "patamar"]).reset_index(drop=True)
        return df

    def _add_block_column(self, df: pd.DataFrame) -> pd.DataFrame:
        df["patamar"] = 0
        df = self._fix_indices_cenarios(df)
        return df

    def _replace_block_column(
        self, df: pd.DataFrame, col: str = "TOTAL"
    ) -> pd.DataFrame:
        df.loc[df["patamar"] == col, "patamar"] = "0"
        df = df.astype({"patamar": int})
        df = self._fix_indices_cenarios(df)
        return df

    def _eval_block_0_sum(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.astype({"patamar": int})
        df = self._fix_indices_cenarios(df)
        df_pat0 = df.copy()
        df_pat0["patamar"] = 0
        df_pat0 = df_pat0.groupby(["data", "serie"], as_index=False).sum(
            numeric_only=True
        )
        df_pat0 = pd.concat([df, df_pat0], ignore_index=True)
        return df_pat0.sort_values(["data", "serie", "patamar"])

    def _eval_block_0_sum_gter_ute(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.astype({"patamar": int})
        df = self._fix_indices_cenarios(df)
        df_pat0 = df.copy()
        df_pat0["patamar"] = 0
        df_pat0 = df_pat0.groupby(
            ["classe", "data", "serie"], as_index=False
        ).sum(numeric_only=True)
        df_pat0 = pd.concat([df, df_pat0], ignore_index=True)
        return df_pat0.sort_values(["classe", "data", "serie", "patamar"])

    @property
    def caso(self) -> Caso:
        return self.__caso  # type: ignore[no-any-return]

    @property
    def arquivos(self) -> Arquivos:
        if self.__arquivos is None:
            caminho_arquivos = join(self.__tmppath, self.__caso.arquivos)
            if not pathlib.Path(caminho_arquivos).exists():
                raise RuntimeError("Nomes dos arquivos não encontrados")
            self.__arquivos = Arquivos.read(caminho_arquivos)
        return self.__arquivos

    @property
    def indices(self) -> Optional[pd.DataFrame]:
        if self.__indices is None:
            caminho = pathlib.Path(self.__tmppath).joinpath("indices.csv")
            self.__indices = pd.read_csv(
                caminho, sep=";", header=None, index_col=0
            )
            self.__indices.columns = ["vazio", "arquivo"]
            self.__indices.index = [
                i.strip() for i in list(self.__indices.index)
            ]
        self.__indices["arquivo"] = self.__indices.apply(
            lambda linha: linha["arquivo"].strip(), axis=1
        )
        return self.__indices

    def get_dger(self) -> Optional[Dger]:
        if self.__dger is None:
            arq_dger = self.arquivos.dger
            if arq_dger is None:
                raise RuntimeError("Nome do dger não encontrado")
            caminho = pathlib.Path(self.__tmppath).joinpath(arq_dger)
            _installdir = Settings().installdir
            if _installdir is None:
                raise RuntimeError("APP_INSTALLDIR not set")
            script = pathlib.Path(_installdir).joinpath(
                Settings().encoding_script
            )
            asyncio.run(converte_codificacao(str(caminho), str(script)))
            self.__dger = Dger.read(join(self.__tmppath, arq_dger))
        return self.__dger

    def get_shist(self) -> Optional[Shist]:
        if self.__shist is None:
            arq_shist = self.arquivos.shist
            if arq_shist is None:
                raise RuntimeError("Nome do shist não encontrado")
            self.__shist = Shist.read(join(self.__tmppath, arq_shist))
        return self.__shist

    def get_patamar(self) -> Optional[Patamar]:
        if self.__patamar is None:
            if self.arquivos.patamar is not None:
                self.__patamar = Patamar.read(
                    join(self.__tmppath, self.arquivos.patamar)
                )
        return self.__patamar

    def get_confhd(self) -> Optional[Confhd]:
        if self.__confhd is None:
            if self.arquivos.confhd is not None:
                self.__confhd = Confhd.read(
                    join(self.__tmppath, self.arquivos.confhd)
                )
        return self.__confhd

    def get_dsvagua(self) -> Optional[Dsvagua]:
        if self.__dsvagua is None:
            if self.arquivos.dsvagua is not None:
                self.__dsvagua = Dsvagua.read(
                    join(self.__tmppath, self.arquivos.dsvagua)
                )
        return self.__dsvagua

    def get_modif(self) -> Optional[Modif]:
        if self.__modif is None:
            if self.arquivos.modif is not None:
                self.__modif = Modif.read(
                    join(self.__tmppath, self.arquivos.modif)
                )
        return self.__modif

    def get_conft(self) -> Optional[Conft]:
        if self.__conft is None:
            if self.arquivos.conft is not None:
                self.__conft = Conft.read(
                    join(self.__tmppath, self.arquivos.conft)
                )
        return self.__conft

    def get_clast(self) -> Optional[Clast]:
        if self.__clast is None:
            if self.arquivos.clast is not None:
                self.__clast = Clast.read(
                    join(self.__tmppath, self.arquivos.clast)
                )
        return self.__clast

    def get_term(self) -> Optional[Term]:
        if self.__term is None:
            if self.arquivos.term is not None:
                self.__term = Term.read(
                    join(self.__tmppath, self.arquivos.term)
                )
        return self.__term

    def get_manutt(self) -> Optional[Manutt]:
        if self.__manutt is None:
            if self.arquivos.manutt is not None:
                self.__manutt = Manutt.read(
                    join(self.__tmppath, self.arquivos.manutt)
                )
        return self.__manutt

    def get_expt(self) -> Optional[Expt]:
        if self.__expt is None:
            if self.arquivos.expt is not None:
                self.__expt = Expt.read(
                    join(self.__tmppath, self.arquivos.expt)
                )
        return self.__expt

    def get_ree(self) -> Optional[Ree]:
        if self.__ree is None:
            if self.arquivos.ree is not None:
                self.__ree = Ree.read(join(self.__tmppath, self.arquivos.ree))
        return self.__ree

    def get_curva(self) -> Optional[Curva]:
        if self.__curva is None:
            if self.arquivos.curva is not None:
                self.__curva = Curva.read(
                    join(self.__tmppath, self.arquivos.curva)
                )
        return self.__curva

    def get_sistema(self) -> Optional[Sistema]:
        if self.__sistema is None:
            if self.arquivos.sistema is not None:
                self.__sistema = Sistema.read(
                    join(self.__tmppath, self.arquivos.sistema)
                )
        return self.__sistema

    def get_pmo(self) -> Optional[Pmo]:
        if self.__pmo is None:
            if self.arquivos.pmo is not None:
                self.__pmo = Pmo.read(join(self.__tmppath, self.arquivos.pmo))
        return self.__pmo

    def get_newavetim(self) -> Optional[Newavetim]:
        if self.__newavetim is None:
            try:
                self.__newavetim = Newavetim.read(
                    join(self.__tmppath, "newave.tim")
                )
            except Exception:
                pass
        return self.__newavetim

    def get_eolica(self) -> Optional[Eolica]:
        if self.__eolica is None:
            df_indices = self.indices
            if df_indices is not None:
                arq: str = df_indices.at[
                    "PARQUE-EOLICO-EQUIVALENTE-CADASTRO", "arquivo"
                ]
                self.__eolica = Eolica.read(join(self.__tmppath, arq))
        return self.__eolica

    def get_nwlistop(
        self,
        variable: Variable,
        spatial_resolution: SpatialResolution,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[pd.DataFrame]:
        try:
            regra = self._regras.get((variable, spatial_resolution))
            if regra is None:
                return None
            df = regra(self.__tmppath, *args, **kwargs)
            return df
        except Exception:
            return None

    def get_nwlistcf_cortes(self) -> Optional[Nwlistcfrel]:
        if self.__nwlistcf is None:
            try:
                self.__nwlistcf = Nwlistcfrel.read(
                    join(self.__tmppath, "nwlistcf.rel")
                )
            except Exception:
                pass
        return self.__nwlistcf

    def get_nwlistcf_estados(self) -> Optional[Estados]:
        if self.__estados is None:
            try:
                self.__estados = Estados.read(
                    join(self.__tmppath, "estados.rel")
                )
            except Exception:
                pass
        return self.__estados

    def _numero_estagios_individualizados_politica(self) -> int:
        dger = self.get_dger()
        if dger is None:
            raise RuntimeError(
                "Erro no processamento do dger.dat para"
                + " número de estágios individualizados"
            )
        ano_inicio = self._validate_data(dger.ano_inicio_estudo, int)
        mes_inicio = self._validate_data(dger.mes_inicio_estudo, int)
        arq_ree = self.get_ree()
        if arq_ree is None:
            raise RuntimeError(
                "Erro no processamento do ree.dat para"
                + " número de estágios individualizados"
            )
        rees = self._validate_data(arq_ree.rees, pd.DataFrame)
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
            return int(round(tempo_individualizado / timedelta(days=30)))
        else:
            return 0

    def _numero_estagios_individualizados_sf(self) -> int:
        dger = self.get_dger()
        if dger is None:
            raise RuntimeError(
                "Erro no processamento do dger.dat para"
                + " número de estágios individualizados"
            )
        agregacao = (
            self._validate_data(dger.agregacao_simulacao_final, int)
            if dger.agregacao_simulacao_final is not None
            else None
        )
        mes_inicio = self._validate_data(dger.mes_inicio_estudo, int)
        anos_estudo = self._validate_data(dger.num_anos_estudo, int)
        anos_pos_sf = self._validate_data(dger.num_anos_pos_sim_final, int)
        if agregacao == 1:
            return (anos_estudo + anos_pos_sf) * 12 - (mes_inicio - 1)
        else:
            return self._numero_estagios_individualizados_politica()

    def get_energiaf(self, iteracao: int) -> Optional[Energiaf]:
        nome_arq = (
            f"energiaf{str(iteracao).zfill(3)}.dat"
            if iteracao != 1
            else "energiaf.dat"
        )
        if self.__energiaf.get(iteracao) is None:
            dger = self.get_dger()
            if dger is None:
                raise RuntimeError(
                    "dger.dat não encontrado para síntese" + " dos cenários"
                )
            anos_estudo = self._validate_data(dger.num_anos_estudo, int)
            num_forwards = self._validate_data(dger.num_forwards, int)
            parpa = self._validate_data(
                dger.consideracao_media_anual_afluencias, int
            )
            ordem_maxima = self._validate_data(dger.ordem_maxima_parp, int)
            arq_rees = self.get_ree()
            if arq_rees is None:
                raise RuntimeError(
                    "ree.dat não encontrado para síntese" + " dos cenários"
                )
            n_rees = self._validate_data(arq_rees.rees, pd.DataFrame).shape[0]

            n_estagios = anos_estudo * 12
            n_estagios_th = 12 if parpa == 3 else ordem_maxima
            caminho_arq = join(self.__tmppath, nome_arq)
            if pathlib.Path(caminho_arq).exists():
                self.__energiaf[iteracao] = Energiaf.read(
                    caminho_arq,
                    num_forwards,
                    n_rees,
                    n_estagios,
                    n_estagios_th,
                )
        return self.__energiaf.get(iteracao)

    def get_vazaof(self, iteracao: int) -> Optional[Vazaof]:
        nome_arq = (
            f"vazaof{str(iteracao).zfill(3)}.dat"
            if iteracao != 1
            else "vazaof.dat"
        )
        if self.__vazaof.get(iteracao) is None:
            dger = self.get_dger()
            if dger is None:
                raise RuntimeError(
                    "dger.dat não encontrado para síntese" + " dos cenários"
                )
            mes_inicio = self._validate_data(dger.mes_inicio_estudo, int)
            num_forwards = self._validate_data(dger.num_forwards, int)

            parpa = self._validate_data(
                dger.consideracao_media_anual_afluencias, int
            )
            ordem_maxima = self._validate_data(dger.ordem_maxima_parp, int)

            arq_uhes = self.get_confhd()
            if arq_uhes is None:
                raise RuntimeError(
                    "confhd.dat não encontrado para síntese" + " dos cenários"
                )
            n_uhes = self._validate_data(arq_uhes.usinas, pd.DataFrame).shape[0]

            n_estagios = (
                self._numero_estagios_individualizados_politica()
                + mes_inicio
                - 1
            )
            n_estagios_th = 12 if parpa == 3 else ordem_maxima
            caminho_arq = join(self.__tmppath, nome_arq)
            if pathlib.Path(caminho_arq).exists():
                self.__vazaof[iteracao] = Vazaof.read(
                    caminho_arq,
                    num_forwards,
                    n_uhes,
                    n_estagios,
                    n_estagios_th,
                )

        return self.__vazaof.get(iteracao)

    def get_energiab(self, iteracao: int) -> Optional[Energiab]:
        nome_arq = (
            f"energiab{str(iteracao).zfill(3)}.dat"
            if iteracao != 1
            else "energiab.dat"
        )
        if self.__energiab.get(iteracao) is None:
            dger = self.get_dger()
            if dger is None:
                raise RuntimeError(
                    "dger.dat não encontrado para síntese" + " dos cenários"
                )
            anos_estudo = self._validate_data(dger.num_anos_estudo, int)
            num_forwards = self._validate_data(dger.num_forwards, int)
            num_aberturas = self._validate_data(dger.num_aberturas, int)
            arq_rees = self.get_ree()
            if arq_rees is None:
                raise RuntimeError(
                    "ree.dat não encontrado para síntese" + " dos cenários"
                )
            n_rees = self._validate_data(arq_rees.rees, pd.DataFrame).shape[0]
            n_estagios = anos_estudo * 12
            caminho_arq = join(self.__tmppath, nome_arq)
            if pathlib.Path(caminho_arq).exists():
                self.__energiab[iteracao] = Energiab.read(
                    caminho_arq,
                    num_forwards,
                    num_aberturas,
                    n_rees,
                    n_estagios,
                )

        return self.__energiab.get(iteracao)

    def get_vazaob(self, iteracao: int) -> Optional[Vazaob]:
        nome_arq = (
            f"vazaob{str(iteracao).zfill(3)}.dat"
            if iteracao != 1
            else "vazaob.dat"
        )
        if self.__vazaob.get(iteracao) is None:
            dger = self.get_dger()
            if dger is None:
                raise RuntimeError(
                    "dger.dat não encontrado para síntese" + " dos cenários"
                )
            mes_inicio = self._validate_data(dger.mes_inicio_estudo, int)
            num_forwards = self._validate_data(dger.num_forwards, int)
            num_aberturas = self._validate_data(dger.num_aberturas, int)

            arq_uhes = self.get_confhd()
            if arq_uhes is None:
                raise RuntimeError(
                    "confhd.dat não encontrado para síntese" + " dos cenários"
                )
            n_uhes = self._validate_data(arq_uhes.usinas, pd.DataFrame).shape[0]

            n_estagios_hib = (
                self._numero_estagios_individualizados_politica()
                + mes_inicio
                - 1
            )
            caminho_arq = join(self.__tmppath, nome_arq)
            if pathlib.Path(caminho_arq).exists():
                self.__vazaob[iteracao] = Vazaob.read(
                    caminho_arq,
                    num_forwards,
                    num_aberturas,
                    n_uhes,
                    n_estagios_hib,
                )

        return self.__vazaob.get(iteracao)

    def get_enavazf(self, iteracao: int) -> Optional[Enavazf]:
        nome_arq = (
            f"enavazf{str(iteracao).zfill(3)}.dat"
            if iteracao != 1
            else "enavazf.dat"
        )
        if self.__enavazf.get(iteracao) is None:
            dger = self.get_dger()
            if dger is None:
                raise RuntimeError(
                    "dger.dat não encontrado para síntese" + " dos cenários"
                )
            mes_inicio = self._validate_data(dger.mes_inicio_estudo, int)
            num_forwards = self._validate_data(dger.num_forwards, int)
            parpa = self._validate_data(
                dger.consideracao_media_anual_afluencias, int
            )
            ordem_maxima = self._validate_data(dger.ordem_maxima_parp, int)

            arq_rees = self.get_ree()
            if arq_rees is None:
                raise RuntimeError(
                    "ree.dat não encontrado para síntese" + " dos cenários"
                )
            n_rees = self._validate_data(arq_rees.rees, pd.DataFrame).shape[0]
            n_estagios = (
                self._numero_estagios_individualizados_politica()
                + mes_inicio
                - 1
            )
            n_estagios_th = 12 if parpa == 3 else ordem_maxima
            caminho_arq = join(self.__tmppath, nome_arq)
            if pathlib.Path(caminho_arq).exists():
                self.__enavazf[iteracao] = Enavazf.read(
                    caminho_arq,
                    num_forwards,
                    n_rees,
                    n_estagios,
                    n_estagios_th,
                )

        return self.__enavazf.get(iteracao)

    def get_enavazb(self, iteracao: int) -> Optional[Enavazb]:
        nome_arq = (
            f"enavazb{str(iteracao).zfill(3)}.dat"
            if iteracao != 1
            else "enavazb.dat"
        )
        if self.__enavazb.get(iteracao) is None:
            dger = self.get_dger()
            if dger is None:
                raise RuntimeError(
                    "dger.dat não encontrado para síntese" + " dos cenários"
                )
            mes_inicio = self._validate_data(dger.mes_inicio_estudo, int)
            num_forwards = self._validate_data(dger.num_forwards, int)
            num_aberturas = self._validate_data(dger.num_aberturas, int)

            arq_rees = self.get_ree()
            if arq_rees is None:
                raise RuntimeError(
                    "ree.dat não encontrado para síntese" + " dos cenários"
                )
            n_rees = self._validate_data(arq_rees.rees, pd.DataFrame).shape[0]
            n_estagios = (
                self._numero_estagios_individualizados_politica()
                + mes_inicio
                - 1
            )
            caminho_arq = join(self.__tmppath, nome_arq)
            if pathlib.Path(caminho_arq).exists():
                self.__enavazb[iteracao] = Enavazb.read(
                    caminho_arq,
                    num_forwards,
                    num_aberturas,
                    n_rees,
                    n_estagios,
                )

        return self.__enavazb.get(iteracao)

    def get_energias(self) -> Optional[Energias]:
        if self.__energias is None:
            dger = self.get_dger()
            if dger is None:
                raise RuntimeError(
                    "dger.dat não encontrado para síntese" + " dos cenários"
                )
            anos_estudo = self._validate_data(dger.num_anos_estudo, int)
            ano_inicio = self._validate_data(dger.ano_inicio_estudo, int)
            ano_inicio_historico = self._validate_data(
                dger.ano_inicial_historico, int
            )
            num_series_sinteticas = self._validate_data(
                dger.num_series_sinteticas, int
            )
            tipo_simulacao_final = self._validate_data(
                dger.tipo_simulacao_final, int
            )
            parpa = self._validate_data(
                dger.consideracao_media_anual_afluencias, int
            )
            ordem_maxima = self._validate_data(dger.ordem_maxima_parp, int)

            arq_rees = self.get_ree()
            if arq_rees is None:
                raise RuntimeError(
                    "ree.dat não encontrado para síntese" + " dos cenários"
                )
            n_rees = self._validate_data(arq_rees.rees, pd.DataFrame).shape[0]
            n_estagios = anos_estudo * 12
            n_estagios_th = 12 if parpa == 3 else ordem_maxima
            if tipo_simulacao_final == 1:
                num_series = num_series_sinteticas
            else:
                num_series = ano_inicio - ano_inicio_historico - 1
            caminho_arq = join(self.__tmppath, "energias.dat")
            if pathlib.Path(caminho_arq).exists():
                self.__energias = Energias.read(
                    caminho_arq,
                    num_series,
                    n_rees,
                    n_estagios,
                    n_estagios_th,
                )

        return self.__energias

    def get_enavazs(self) -> Optional[Energias]:
        if self.__enavazs is None:
            dger = self.get_dger()
            if dger is None:
                raise RuntimeError(
                    "dger.dat não encontrado para síntese" + " dos cenários"
                )
            mes_inicio = self._validate_data(dger.mes_inicio_estudo, int)
            ano_inicio = self._validate_data(dger.ano_inicio_estudo, int)
            ano_inicio_historico = self._validate_data(
                dger.ano_inicial_historico, int
            )
            num_series_sinteticas = self._validate_data(
                dger.num_series_sinteticas, int
            )
            tipo_simulacao_final = self._validate_data(
                dger.tipo_simulacao_final, int
            )
            parpa = self._validate_data(
                dger.consideracao_media_anual_afluencias, int
            )
            ordem_maxima = self._validate_data(dger.ordem_maxima_parp, int)

            arq_rees = self.get_ree()
            if arq_rees is None:
                raise RuntimeError(
                    "ree.dat não encontrado para síntese" + " dos cenários"
                )
            n_rees = self._validate_data(arq_rees.rees, pd.DataFrame).shape[0]
            n_estagios = (
                self._numero_estagios_individualizados_sf() + mes_inicio - 1
            )
            n_estagios_th = 12 if parpa == 3 else ordem_maxima
            if tipo_simulacao_final == 1:
                num_series = num_series_sinteticas
            else:
                num_series = ano_inicio - ano_inicio_historico - 1
            caminho_arq = join(self.__tmppath, "enavazs.dat")
            if pathlib.Path(caminho_arq).exists():
                self.__enavazs = Energias.read(
                    caminho_arq,
                    num_series,
                    n_rees,
                    n_estagios,
                    n_estagios_th,
                )
        return self.__enavazs

    def get_vazaos(self) -> Optional[Vazaos]:
        if self.__vazaos is None:
            dger = self.get_dger()
            if dger is None:
                raise RuntimeError(
                    "dger.dat não encontrado para síntese" + " dos cenários"
                )
            mes_inicio = self._validate_data(dger.mes_inicio_estudo, int)
            parpa = self._validate_data(
                dger.consideracao_media_anual_afluencias, int
            )
            ordem_maxima = self._validate_data(dger.ordem_maxima_parp, int)
            num_series_sinteticas = self._validate_data(
                dger.num_series_sinteticas, int
            )
            ano_inicio = self._validate_data(dger.ano_inicio_estudo, int)
            ano_inicial_historico = self._validate_data(
                dger.ano_inicial_historico, int
            )
            arq_uhes = self.get_confhd()
            if arq_uhes is None:
                raise RuntimeError(
                    "confhd.dat não encontrado para síntese" + " dos cenários"
                )
            n_uhes = self._validate_data(arq_uhes.usinas, pd.DataFrame).shape[0]

            n_estagios = (
                self._numero_estagios_individualizados_sf() + mes_inicio - 1
            )
            n_estagios_th = 12 if parpa == 3 else ordem_maxima
            if dger.tipo_simulacao_final == 1:
                num_series = num_series_sinteticas
            else:
                num_series = ano_inicio - ano_inicial_historico - 1
            caminho_arq = join(self.__tmppath, "vazaos.dat")
            if pathlib.Path(caminho_arq).exists():
                self.__vazaos = Vazaos.read(
                    caminho_arq,
                    num_series,
                    n_uhes,
                    n_estagios,
                    n_estagios_th,
                )
        return self.__vazaos

    def get_vazoes(self) -> Optional[Vazoes]:
        if self.__vazoes is None:
            try:
                self.__vazoes = Vazoes.read(join(self.__tmppath, "vazoes.dat"))
            except Exception:
                raise RuntimeError()
        return self.__vazoes

    def get_hidr(self) -> Optional[Hidr]:
        if self.__hidr is None:
            try:
                self.__hidr = Hidr.read(
                    join(self.__tmppath, "hidr.dat"),
                )
            except Exception:
                raise RuntimeError()
        return self.__hidr

    def get_engnat(self) -> Optional[Engnat]:
        if self.__engnat is None:
            try:
                dger = self.get_dger()
                if dger is None:
                    raise RuntimeError(
                        "dger.dat não encontrado para síntese" + " dos cenários"
                    )
                ano_inicio_historico = self._validate_data(
                    dger.ano_inicial_historico, int
                )
                pmo = self.get_pmo()
                if pmo is None:
                    raise RuntimeError(
                        "pmo.dat não encontrado para síntese" + " dos cenários"
                    )
                df_configuracoes = self._validate_data(
                    pmo.configuracoes_qualquer_modificacao, pd.DataFrame
                )
                arq_rees = self.get_ree()
                if arq_rees is None:
                    raise RuntimeError(
                        "ree.dat não encontrado para síntese" + " dos cenários"
                    )
                n_rees = self._validate_data(arq_rees.rees, pd.DataFrame).shape[
                    0
                ]
                self.__engnat = Engnat.read(
                    join(self.__tmppath, "engnat.dat"),
                    ano_inicio_historico=ano_inicio_historico,
                    numero_rees=n_rees,
                    numero_configuracoes=df_configuracoes["valor"]
                    .unique()
                    .shape[0],
                )
            except Exception:
                pass
        return self.__engnat


def factory(kind: str, *args: Any, **kwargs: Any) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind, RawFilesRepository)(*args, **kwargs)
