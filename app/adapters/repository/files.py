from abc import ABC, abstractmethod
from typing import Dict, Type, Optional, Tuple, Callable, TypeVar
import pandas as pd  # type: ignore
from datetime import datetime, timedelta
import pathlib
import asyncio
from os.path import join

from inewave.newave.caso import Caso
from inewave.newave.arquivos import Arquivos
from inewave.newave.patamar import Patamar
from inewave.newave.dger import Dger
from inewave.newave.shist import Shist
from inewave.newave.confhd import Confhd
from inewave.newave.modif import Modif
from inewave.newave.conft import Conft
from inewave.newave.clast import Clast
from inewave.libs.eolica import Eolica
from inewave.newave.ree import Ree
from inewave.newave.curva import Curva
from inewave.newave.sistema import Sistema
from inewave.newave.pmo import Pmo
from inewave.newave.newavetim import Newavetim
from inewave.newave.vazoes import Vazoes
from inewave.newave.hidr import Hidr

from inewave.newave.energiaf import Energiaf
from inewave.newave.energiab import Energiab
from inewave.newave.energias import Energias
from inewave.newave.vazaof import Vazaof
from inewave.newave.vazaob import Vazaob
from inewave.newave.vazaos import Vazaos
from inewave.newave.enavazf import Enavazf
from inewave.newave.enavazb import Enavazb
from inewave.newave.engnat import Engnat

from inewave.nwlistop.cmarg import Cmarg
from inewave.nwlistop.cmargmed import Cmargmed
from inewave.nwlistop.cterm import Cterm
from inewave.nwlistop.ctermsin import Ctermsin
from inewave.nwlistop.coper import Coper
from inewave.nwlistop.eafb import Eafb
from inewave.nwlistop.eafbm import Eafbm
from inewave.nwlistop.eafbsin import Eafbsin
from inewave.nwlistop.eaf import Eaf
from inewave.nwlistop.eafm import Eafm
from inewave.nwlistop.intercambio import Intercambio
from inewave.nwlistop.deficit import Def
from inewave.nwlistop.exces import Exces
from inewave.nwlistop.excessin import Excessin
from inewave.nwlistop.cdef import Cdef
from inewave.nwlistop.cdefsin import Cdefsin
from inewave.nwlistop.mercl import Mercl
from inewave.nwlistop.merclsin import Merclsin

from inewave.nwlistop.earmfp import Earmfp
from inewave.nwlistop.earmfpm import Earmfpm
from inewave.nwlistop.earmfpsin import Earmfpsin
from inewave.nwlistop.earmf import Earmf
from inewave.nwlistop.earmfm import Earmfm
from inewave.nwlistop.earmfsin import Earmfsin
from inewave.nwlistop.ghidr import Ghidr
from inewave.nwlistop.ghidrm import Ghidrm
from inewave.nwlistop.ghidrsin import Ghidrsin
from inewave.nwlistop.ghtot import Ghtot
from inewave.nwlistop.ghtotm import Ghtotm
from inewave.nwlistop.ghtotsin import Ghtotsin
from inewave.nwlistop.gtert import Gtert
from inewave.nwlistop.gttot import Gttot
from inewave.nwlistop.gttotsin import Gttotsin
from inewave.nwlistop.evert import Evert
from inewave.nwlistop.evertm import Evertm
from inewave.nwlistop.evertsin import Evertsin
from inewave.nwlistop.edesvc import Edesvc
from inewave.nwlistop.edesvcm import Edesvcm
from inewave.nwlistop.edesvcsin import Edesvcsin
from inewave.nwlistop.evapo import Evapo
from inewave.nwlistop.evapom import Evapom
from inewave.nwlistop.evaporsin import Evaporsin
from inewave.nwlistop.mevmin import Mevmin
from inewave.nwlistop.mevminm import Mevminm
from inewave.nwlistop.mevminsin import Mevminsin
from inewave.nwlistop.vmort import Vmort
from inewave.nwlistop.vmortm import Vmortm
from inewave.nwlistop.vmortsin import Vmortsin
from inewave.nwlistop.perdf import Perdf
from inewave.nwlistop.perdfm import Perdfm
from inewave.nwlistop.perdfsin import Perdfsin
from inewave.nwlistop.verturb import Verturb
from inewave.nwlistop.verturbm import Verturbm
from inewave.nwlistop.verturbsin import Verturbsin
from inewave.nwlistop.vagua import Vagua
from inewave.nwlistop.vevmin import Vevmin
from inewave.nwlistop.vevminm import Vevminm
from inewave.nwlistop.vevminsin import Vevminsin
from inewave.nwlistop.vghminuh import Vghminuh
from inewave.nwlistop.vghmin import Vghmin
from inewave.nwlistop.vghminm import Vghminm
from inewave.nwlistop.vghminsin import Vghminsin

from inewave.nwlistop.vento import Vento
from inewave.nwlistop.geol import Geol
from inewave.nwlistop.geolm import Geolm
from inewave.nwlistop.geolsin import Geolsin
from inewave.nwlistop.corteolm import Corteolm

from inewave.nwlistop.qafluh import Qafluh
from inewave.nwlistop.qincruh import Qincruh
from inewave.nwlistop.ghiduh import Ghiduh
from inewave.nwlistop.vturuh import Vturuh
from inewave.nwlistop.vertuh import Vertuh
from inewave.nwlistop.varmuh import Varmuh
from inewave.nwlistop.varmpuh import Varmpuh
from inewave.nwlistop.dfphauh import Dfphauh
from inewave.nwlistop.pivarm import Pivarm
from inewave.nwlistop.pivarmincr import Pivarmincr
from inewave.nwlistop.desvuh import Desvuh
from inewave.nwlistop.vdesviouh import Vdesviouh
from inewave.nwlistop.hmont import Hmont
from inewave.nwlistop.hjus import Hjus
from inewave.nwlistop.hliq import Hliq
from inewave.nwlistop.vevapuh import Vevapuh
from inewave.nwlistop.dposevap import Dposevap
from inewave.nwlistop.dnegevap import Dnegevap

from inewave.nwlistcf import Nwlistcfrel
from inewave.nwlistcf import Estados

from app.model.settings import Settings
from app.utils.encoding import converte_codificacao
from app.model.operation.variable import Variable
from app.model.operation.spatialresolution import SpatialResolution

import platform

if platform.system() == "Windows":
    Dger.ENCODING = "iso-8859-1"


class AbstractFilesRepository(ABC):
    T = TypeVar("T")

    def _validate_data(self, data, type: Type[T]) -> T:
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
    def get_modif(self) -> Optional[Modif]:
        raise NotImplementedError

    @abstractmethod
    def get_conft(self) -> Optional[Conft]:
        raise NotImplementedError

    @abstractmethod
    def get_clast(self) -> Optional[Clast]:
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
        *args,
        **kwargs,
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
    def __init__(self, tmppath: str):
        self.__tmppath = tmppath
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
        self.__modif: Optional[Modif] = None
        self.__conft: Optional[Conft] = None
        self.__clast: Optional[Clast] = None
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
        self.__regras: Dict[Tuple[Variable, SpatialResolution], Callable] = {
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__agrega_dfs_cmo(
                dir, submercado
            ),
            (
                Variable.VALOR_AGUA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Vagua.read(join(dir, f"vagua{str(ree).zfill(3)}.out")).valores
            ),
            (
                Variable.VALOR_AGUA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__adiciona_coluna_patamar(
                Pivarm.read(
                    join(dir, f"pivarm{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VALOR_AGUA_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__adiciona_coluna_patamar(
                Pivarmincr.read(
                    join(dir, f"pivarmincr{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Cterm.read(
                    join(dir, f"cterm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Ctermsin.read(join(dir, "ctermsin.out")).valores
            ),
            (
                Variable.CUSTO_OPERACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Coper.read(join(dir, "coper.out")).valores
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Eafb.read(join(dir, f"eafb{str(ree).zfill(3)}.out")).valores
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Eafbm.read(
                    join(dir, f"eafbm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Eafbsin.read(join(dir, "eafbsin.out")).valores
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Eaf.read(join(dir, f"eaf{str(ree).zfill(3)}.out")).valores
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Eafm.read(
                    join(dir, f"eafm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                # TODO - substituir quando existir na inewave
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Eafbsin.read(join(dir, "eafmsin.out")).valores
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                # TODO - substituir quando existir na inewave
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Eaf.read(join(dir, f"efdf{str(ree).zfill(3)}.out")).valores
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
                SpatialResolution.SUBMERCADO,
                # TODO - substituir quando existir na inewave
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Eafm.read(
                    join(dir, f"efdfm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                # TODO - substituir quando existir na inewave
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Eafbsin.read(join(dir, "efdfsin.out")).valores
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Earmfp.read(
                    join(dir, f"earmfp{str(ree).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Earmfpm.read(
                    join(dir, f"earmfpm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Earmfpsin.read(join(dir, "earmfpsin.out")).valores
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Earmf.read(join(dir, f"earmf{str(ree).zfill(3)}.out")).valores
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Earmfm.read(
                    join(dir, f"earmfm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Earmfsin.read(join(dir, "earmfsin.out")).valores
            ),
            (
                Variable.GERACAO_HIDRAULICA_RESERVATORIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__substitui_coluna_patamar(
                Ghidr.read(join(dir, f"ghidr{str(ree).zfill(3)}.out")).valores,
            ),
            (
                Variable.GERACAO_HIDRAULICA_RESERVATORIO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__substitui_coluna_patamar(
                Ghidrm.read(
                    join(dir, f"ghidrm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.GERACAO_HIDRAULICA_RESERVATORIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__substitui_coluna_patamar(
                Ghidrsin.read(join(dir, "ghidrsin.out")).valores
            ),
            (
                Variable.GERACAO_HIDRAULICA_FIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                # TODO - Substituir quando existir na inewave
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Evert.read(join(dir, f"gfiol{str(ree).zfill(3)}.out")).valores
            ),
            (
                Variable.GERACAO_HIDRAULICA_FIO,
                SpatialResolution.SUBMERCADO,
                # TODO - Substituir quando existir na inewave
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Evertm.read(
                    join(dir, f"gfiolm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.GERACAO_HIDRAULICA_FIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                # TODO - Substituir quando existir na inewave
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Evertsin.read(join(dir, "gfiolsin.out")).valores
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__substitui_coluna_patamar(
                Ghtot.read(join(dir, f"ghtot{str(ree).zfill(3)}.out")).valores
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__substitui_coluna_patamar(
                Ghtotm.read(
                    join(dir, f"ghtotm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__substitui_coluna_patamar(
                Ghtotsin.read(join(dir, "ghtotsin.out")).valores
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
            ): lambda dir, submercado=1: self.__calcula_patamar_medio_soma_gter_ute(
                Gtert.read(
                    join(dir, f"gtert{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__substitui_coluna_patamar(
                Gttot.read(
                    join(dir, f"gttot{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__substitui_coluna_patamar(
                Gttotsin.read(join(dir, "gttotsin.out")).valores
            ),
            (
                Variable.ENERGIA_VERTIDA_RESERV,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Evert.read(join(dir, f"evert{str(ree).zfill(3)}.out")).valores
            ),
            (
                Variable.ENERGIA_VERTIDA_RESERV,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Evertm.read(
                    join(dir, f"evertm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_VERTIDA_RESERV,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Evertsin.read(join(dir, "evertsin.out")).valores
            ),
            (
                Variable.ENERGIA_VERTIDA_FIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Perdf.read(join(dir, f"perdf{str(ree).zfill(3)}.out")).valores
            ),
            (
                Variable.ENERGIA_VERTIDA_FIO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Perdfm.read(
                    join(dir, f"perdfm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_VERTIDA_FIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Perdfsin.read(join(dir, "perdfsin.out")).valores
            ),
            (
                Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Verturb.read(
                    join(dir, f"verturb{str(ree).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Verturbm.read(
                    join(dir, f"verturbm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Verturbsin.read(join(dir, "verturbsin.out")).valores
            ),
            (
                Variable.ENERGIA_DESVIO_RESERVATORIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Edesvc.read(
                    join(dir, f"edesvc{str(ree).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_DESVIO_RESERVATORIO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Edesvcm.read(
                    join(dir, f"edesvcm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_DESVIO_RESERVATORIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Edesvcsin.read(join(dir, "edesvcsin.out")).valores
            ),
            (
                # TODO - substituir quando existir na inewave
                Variable.ENERGIA_DESVIO_FIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Edesvc.read(
                    join(dir, f"edesvf{str(ree).zfill(3)}.out")
                ).valores
            ),
            (
                # TODO - substituir quando existir na inewave
                Variable.ENERGIA_DESVIO_FIO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Edesvcm.read(
                    join(dir, f"edesvfm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                # TODO - substituir quando existir na inewave
                Variable.ENERGIA_DESVIO_FIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Edesvcsin.read(join(dir, "edesvfsin.out")).valores
            ),
            (
                Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Mevmin.read(
                    join(dir, f"mevmin{str(ree).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Mevminm.read(
                    join(dir, f"mevminm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Mevminsin.read(join(dir, "mevminsin.out")).valores
            ),
            (
                Variable.ENERGIA_VOLUME_MORTO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Vmort.read(join(dir, f"vmort{str(ree).zfill(3)}.out")).valores
            ),
            (
                Variable.ENERGIA_VOLUME_MORTO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Vmortm.read(
                    join(dir, f"vmortm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_VOLUME_MORTO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Vmortsin.read(join(dir, "vmortsin.out")).valores
            ),
            (
                Variable.ENERGIA_EVAPORACAO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Evapo.read(join(dir, f"evapo{str(ree).zfill(3)}.out")).valores
            ),
            (
                Variable.ENERGIA_EVAPORACAO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Evapom.read(
                    join(dir, f"evapom{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.ENERGIA_EVAPORACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Evaporsin.read(join(dir, "evaporsin.out")).valores
            ),
            (
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__adiciona_coluna_patamar(
                Qafluh.read(
                    join(dir, f"qafluh{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__adiciona_coluna_patamar(
                Qincruh.read(
                    join(dir, f"qincruh{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VOLUME_TURBINADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__calcula_patamar_medio_soma(
                Vturuh.read(
                    join(dir, f"vturuh{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VOLUME_VERTIDO,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__calcula_patamar_medio_soma(
                Vertuh.read(
                    join(dir, f"vertuh{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__adiciona_coluna_patamar(
                Varmuh.read(
                    join(dir, f"varmuh{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__adiciona_coluna_patamar(
                Varmpuh.read(
                    join(dir, f"varmpuh{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__calcula_patamar_medio_soma(
                Ghiduh.read(
                    join(dir, f"ghiduh{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VELOCIDADE_VENTO,
                SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
            ): lambda dir, uee=1: self.__adiciona_coluna_patamar(
                Vento.read(join(dir, f"vento{str(uee).zfill(3)}.out")).valores
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
            ): lambda dir, uee=1: self.__substitui_coluna_patamar(
                Geol.read(join(dir, f"geol{str(uee).zfill(3)}.out")).valores,
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__substitui_coluna_patamar(
                Geolm.read(
                    join(dir, f"geolm{str(submercado).zfill(3)}.out")
                ).valores,
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__substitui_coluna_patamar(
                Geolsin.read(join(dir, "geolsin.out")).valores
            ),
            (
                Variable.CORTE_GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__substitui_coluna_patamar(
                Corteolm.read(
                    join(dir, f"corteolm{str(submercado).zfill(3)}.out")
                ).valores,
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__substitui_coluna_patamar(
                Def.read(
                    join(dir, f"def{str(submercado).zfill(3)}p001.out")
                ).valores,
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__substitui_coluna_patamar(
                Def.read(join(dir, "defsinp001.out")).valores
            ),
            (
                Variable.EXCESSO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__substitui_coluna_patamar(
                Exces.read(
                    join(dir, f"exces{str(submercado).zfill(3)}.out")
                ).valores,
            ),
            (
                Variable.EXCESSO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__substitui_coluna_patamar(
                Excessin.read(join(dir, "excessin.out")).valores
            ),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
            ): lambda dir, submercados=(1, 2): self.__substitui_coluna_patamar(
                Intercambio.read(
                    join(
                        dir,
                        f"int{str(submercados[0]).zfill(3)}"
                        + f"{str(submercados[1]).zfill(3)}.out",
                    )
                ).valores,
            ),
            (
                Variable.CUSTO_DEFICIT,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Cdef.read(
                    join(dir, f"cdef{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.CUSTO_DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Cdefsin.read(join(dir, "cdefsin.out")).valores
            ),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Mercl.read(
                    join(dir, f"mercl{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Merclsin.read(join(dir, "merclsin.out")).valores
            ),
            (
                Variable.VIOLACAO_FPHA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__calcula_patamar_medio_soma(
                Dfphauh.read(
                    join(dir, f"dfphauh{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__adiciona_coluna_patamar(
                Vevmin.read(
                    join(dir, f"vevmin{str(ree).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__adiciona_coluna_patamar(
                Vevminm.read(
                    join(dir, f"vevminm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__adiciona_coluna_patamar(
                Vevminsin.read(join(dir, "vevminsin.out")).valores
            ),
            (
                Variable.VOLUME_RETIRADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__adiciona_coluna_patamar(
                Desvuh.read(
                    join(dir, f"desvuh{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VOLUME_DESVIADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__calcula_patamar_medio_soma(
                Vdesviouh.read(
                    join(dir, f"vdesviouh{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__calcula_patamar_medio_soma(
                Vghminuh.read(
                    join(dir, f"vghminuh{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__substitui_coluna_patamar(
                Vghmin.read(
                    join(dir, f"vghmin{str(ree).zfill(3)}.out")
                ).valores,
            ),
            (
                Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__substitui_coluna_patamar(
                Vghminm.read(
                    join(dir, f"vghminm{str(submercado).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__substitui_coluna_patamar(
                Vghminsin.read(join(dir, "vghminsin.out")).valores
            ),
            (
                Variable.COTA_MONTANTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__adiciona_coluna_patamar(
                Hmont.read(join(dir, f"hmont{str(uhe).zfill(3)}.out")).valores
            ),
            (
                Variable.COTA_JUSANTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__substitui_coluna_patamar(
                Hjus.read(join(dir, f"hjus{str(uhe).zfill(3)}.out")).valores
            ),
            (
                Variable.QUEDA_LIQUIDA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__substitui_coluna_patamar(
                Hliq.read(join(dir, f"hliq{str(uhe).zfill(3)}.out")).valores
            ),
            (
                Variable.VOLUME_EVAPORADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__adiciona_coluna_patamar(
                Vevapuh.read(
                    join(dir, f"vevapuh{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VIOLACAO_POSITIVA_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__adiciona_coluna_patamar(
                Dposevap.read(
                    join(dir, f"dpos_evap{str(uhe).zfill(3)}.out")
                ).valores
            ),
            (
                Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__adiciona_coluna_patamar(
                Dnegevap.read(
                    join(dir, f"dneg_evap{str(uhe).zfill(3)}.out")
                ).valores.fillna(0.0)
            ),
        }

    def __agrega_dfs_cmo(self, dir: str, submercado: int) -> pd.DataFrame:
        df_med = Cmargmed.read(
            join(dir, f"cmarg{str(submercado).zfill(3)}-med.out")
        ).valores
        df_med["patamar"] = 0
        return pd.concat(
            [
                df_med,
                Cmarg.read(
                    join(dir, f"cmarg{str(submercado).zfill(3)}.out")
                ).valores,
            ],
            ignore_index=True,
        )

    def __adiciona_coluna_patamar(self, df: pd.DataFrame) -> pd.DataFrame:
        df["patamar"] = 0
        return df

    def __substitui_coluna_patamar(
        self, df: pd.DataFrame, col: str = "TOTAL"
    ) -> pd.DataFrame:
        df.loc[df["patamar"] == col, "patamar"] = "0"
        return df.astype({"patamar": int})

    def __calcula_patamar_medio_soma(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.astype({"patamar": int})
        df_pat0 = df.copy()
        df_pat0["patamar"] = 0
        df_pat0 = df_pat0.groupby(["data", "serie"], as_index=False).sum(
            numeric_only=True
        )
        df_pat0 = pd.concat([df, df_pat0], ignore_index=True)
        return df_pat0.sort_values(["data", "serie", "patamar"])

    def __calcula_patamar_medio_soma_gter_ute(
        self, df: pd.DataFrame
    ) -> pd.DataFrame:
        df = df.astype({"patamar": int})
        df_pat0 = df.copy()
        df_pat0["patamar"] = 0
        df_pat0 = df_pat0.groupby(
            ["classe", "data", "serie"], as_index=False
        ).sum(numeric_only=True)
        df_pat0 = pd.concat([df, df_pat0], ignore_index=True)
        return df_pat0.sort_values(["classe", "data", "serie", "patamar"])

    @property
    def caso(self) -> Caso:
        return self.__caso

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
            script = pathlib.Path(Settings().installdir).joinpath(
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
        *args,
        **kwargs,
    ) -> Optional[pd.DataFrame]:
        try:
            regra = self.__regras.get((variable, spatial_resolution))
            if regra is None:
                return None
            return regra(self.__tmppath, *args, **kwargs)
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
            n_uhes = self._validate_data(arq_uhes.usinas, pd.DataFrame).shape[
                0
            ]

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
            n_uhes = self._validate_data(arq_uhes.usinas, pd.DataFrame).shape[
                0
            ]

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
            n_uhes = self._validate_data(arq_uhes.usinas, pd.DataFrame).shape[
                0
            ]

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
                        "dger.dat não encontrado para síntese"
                        + " dos cenários"
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
                n_rees = self._validate_data(
                    arq_rees.rees, pd.DataFrame
                ).shape[0]
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


def factory(kind: str, *args, **kwargs) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind, RawFilesRepository)(*args, **kwargs)
