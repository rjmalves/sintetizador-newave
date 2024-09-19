import asyncio
import pathlib
import platform
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from os.path import join
from typing import Callable, Dict, Optional, Tuple, Type, TypeVar

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
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
from inewave.nwlistcf import Estados, Nwlistcfrel
from inewave.nwlistop.cdef import Cdef
from inewave.nwlistop.cdefsin import Cdefsin
from inewave.nwlistop.cmarg import Cmarg
from inewave.nwlistop.cmargmed import Cmargmed
from inewave.nwlistop.coper import Coper
from inewave.nwlistop.corteolm import Corteolm
from inewave.nwlistop.cterm import Cterm
from inewave.nwlistop.ctermsin import Ctermsin
from inewave.nwlistop.custo_futuro import CustoFuturo
from inewave.nwlistop.deficit import Def
from inewave.nwlistop.eaf import Eaf
from inewave.nwlistop.eafb import Eafb
from inewave.nwlistop.eafbm import Eafbm
from inewave.nwlistop.eafbsin import Eafbsin
from inewave.nwlistop.eafm import Eafm
from inewave.nwlistop.earmf import Earmf
from inewave.nwlistop.earmfm import Earmfm
from inewave.nwlistop.earmfp import Earmfp
from inewave.nwlistop.earmfpm import Earmfpm
from inewave.nwlistop.earmfpsin import Earmfpsin
from inewave.nwlistop.earmfsin import Earmfsin
from inewave.nwlistop.edesvc import Edesvc
from inewave.nwlistop.edesvcm import Edesvcm
from inewave.nwlistop.edesvcsin import Edesvcsin
from inewave.nwlistop.evapo import Evapo
from inewave.nwlistop.evapom import Evapom
from inewave.nwlistop.evaporsin import Evaporsin
from inewave.nwlistop.evert import Evert
from inewave.nwlistop.evertm import Evertm
from inewave.nwlistop.evertsin import Evertsin
from inewave.nwlistop.exces import Exces
from inewave.nwlistop.excessin import Excessin
from inewave.nwlistop.geol import Geol
from inewave.nwlistop.geolm import Geolm
from inewave.nwlistop.geolsin import Geolsin
from inewave.nwlistop.ghidr import Ghidr
from inewave.nwlistop.ghidrm import Ghidrm
from inewave.nwlistop.ghidrsin import Ghidrsin
from inewave.nwlistop.ghiduh import Ghiduh
from inewave.nwlistop.ghtot import Ghtot
from inewave.nwlistop.ghtotm import Ghtotm
from inewave.nwlistop.ghtotsin import Ghtotsin
from inewave.nwlistop.gtert import Gtert
from inewave.nwlistop.gttot import Gttot
from inewave.nwlistop.gttotsin import Gttotsin
from inewave.nwlistop.hjus import Hjus
from inewave.nwlistop.hliq import Hliq
from inewave.nwlistop.hmont import Hmont
from inewave.nwlistop.intercambio import Intercambio
from inewave.nwlistop.mercl import Mercl
from inewave.nwlistop.merclsin import Merclsin
from inewave.nwlistop.mevmin import Mevmin
from inewave.nwlistop.mevminm import Mevminm
from inewave.nwlistop.mevminsin import Mevminsin
from inewave.nwlistop.perdf import Perdf
from inewave.nwlistop.perdfm import Perdfm
from inewave.nwlistop.perdfsin import Perdfsin
from inewave.nwlistop.pivarm import Pivarm
from inewave.nwlistop.pivarmincr import Pivarmincr
from inewave.nwlistop.qafluh import Qafluh
from inewave.nwlistop.qdesviouh import Qdesviouh
from inewave.nwlistop.qincruh import Qincruh
from inewave.nwlistop.qturuh import Qturuh
from inewave.nwlistop.qvertuh import Qvertuh
from inewave.nwlistop.valor_agua import ValorAgua
from inewave.nwlistop.varmpuh import Varmpuh
from inewave.nwlistop.varmuh import Varmuh
from inewave.nwlistop.vento import Vento
from inewave.nwlistop.verturb import Verturb
from inewave.nwlistop.verturbm import Verturbm
from inewave.nwlistop.verturbsin import Verturbsin
from inewave.nwlistop.vevapuh import Vevapuh
from inewave.nwlistop.viol_evmin import ViolEvmin
from inewave.nwlistop.viol_evminm import ViolEvminm
from inewave.nwlistop.viol_evminsin import ViolEvminsin
from inewave.nwlistop.viol_fpha import ViolFpha
from inewave.nwlistop.viol_ghmin import ViolGhmin
from inewave.nwlistop.viol_ghminm import ViolGhminm
from inewave.nwlistop.viol_ghminsin import ViolGhminsin
from inewave.nwlistop.viol_ghminuh import ViolGhminuh
from inewave.nwlistop.viol_neg_evap import ViolNegEvap
from inewave.nwlistop.viol_pos_evap import ViolPosEvap
from inewave.nwlistop.vmort import Vmort
from inewave.nwlistop.vmortm import Vmortm
from inewave.nwlistop.vmortsin import Vmortsin
from inewave.nwlistop.vretiradauh import Vretiradauh

from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.model.settings import Settings
from app.utils.encoding import converte_codificacao

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
        self.__regras: Dict[Tuple[Variable, SpatialResolution], Callable] = {
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__agg_cmo_dfs(dir, submercado),
            (
                Variable.VALOR_AGUA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    ValorAgua, join(dir, f"valor_agua{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.VALOR_AGUA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Pivarm, join(dir, f"pivarm{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.VALOR_AGUA_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Pivarmincr, join(dir, f"pivarmincr{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Cterm, join(dir, f"cterm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Ctermsin, join(dir, "ctermsin.out")
                )
            ),
            (
                Variable.CUSTO_OPERACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Coper, join(dir, "coper.out")
                )
            ),
            (
                Variable.CUSTO_FUTURO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    CustoFuturo, join(dir, "custo_futuro.out")
                )
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Eafb, join(dir, f"eafb{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Eafbm, join(dir, f"eafbm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Eafbsin, join(dir, "eafbsin.out")
                )
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Eaf, join(dir, f"eaf{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Eafm, join(dir, f"eafm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                # TODO - substituir quando existir na inewave
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Eafbsin, join(dir, "eafmsin.out")
                )
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                # TODO - substituir quando existir na inewave
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Eaf, join(dir, f"efdf{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
                SpatialResolution.SUBMERCADO,
                # TODO - substituir quando existir na inewave
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Eafm, join(dir, f"efdfm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                # TODO - substituir quando existir na inewave
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Eafbsin, join(dir, "efdfsin.out")
                )
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Earmfp, join(dir, f"earmfp{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Earmfpm, join(dir, f"earmfpm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Earmfpsin, join(dir, "earmfpsin.out")
                )
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Earmf, join(dir, f"earmf{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Earmfm, join(dir, f"earmfm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Earmfsin, join(dir, "earmfsin.out")
                )
            ),
            (
                Variable.GERACAO_HIDRAULICA_RESERVATORIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Ghidr, join(dir, f"ghidr{str(ree).zfill(3)}.out")
                ),
            ),
            (
                Variable.GERACAO_HIDRAULICA_RESERVATORIO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Ghidrm, join(dir, f"ghidrm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.GERACAO_HIDRAULICA_RESERVATORIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Ghidrsin, join(dir, "ghidrsin.out")
                )
            ),
            (
                Variable.GERACAO_HIDRAULICA_FIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                # TODO - Substituir quando existir na inewave
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Evert, join(dir, f"gfiol{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.GERACAO_HIDRAULICA_FIO,
                SpatialResolution.SUBMERCADO,
                # TODO - Substituir quando existir na inewave
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Evertm, join(dir, f"gfiolm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.GERACAO_HIDRAULICA_FIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                # TODO - Substituir quando existir na inewave
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Evertsin, join(dir, "gfiolsin.out")
                )
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Ghtot, join(dir, f"ghtot{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Ghtotm, join(dir, f"ghtotm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Ghtotsin, join(dir, "ghtotsin.out")
                )
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
            ): lambda dir, submercado=1: self.__eval_block_0_sum_gter_ute(
                self.__read_nwlistop_setting_version(
                    Gtert, join(dir, f"gtert{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Gttot, join(dir, f"gttot{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Gttotsin, join(dir, "gttotsin.out")
                )
            ),
            (
                Variable.ENERGIA_VERTIDA_RESERV,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Evert, join(dir, f"evert{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_VERTIDA_RESERV,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Evertm, join(dir, f"evertm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_VERTIDA_RESERV,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Evertsin, join(dir, "evertsin.out")
                )
            ),
            (
                Variable.ENERGIA_VERTIDA_FIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Perdf, join(dir, f"perdf{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_VERTIDA_FIO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Perdfm, join(dir, f"perdfm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_VERTIDA_FIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Perdfsin, join(dir, "perdfsin.out")
                )
            ),
            (
                Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Verturb, join(dir, f"verturb{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Verturbm,
                    join(dir, f"verturbm{str(submercado).zfill(3)}.out"),
                )
            ),
            (
                Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Verturbsin, join(dir, "verturbsin.out")
                )
            ),
            (
                Variable.ENERGIA_DESVIO_RESERVATORIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Edesvc, join(dir, f"edesvc{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_DESVIO_RESERVATORIO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Edesvcm, join(dir, f"edesvcm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_DESVIO_RESERVATORIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Edesvcsin, join(dir, "edesvcsin.out")
                )
            ),
            (
                # TODO - substituir quando existir na inewave
                Variable.ENERGIA_DESVIO_FIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Edesvc, join(dir, f"edesvf{str(ree).zfill(3)}.out")
                )
            ),
            (
                # TODO - substituir quando existir na inewave
                Variable.ENERGIA_DESVIO_FIO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Edesvcm, join(dir, f"edesvfm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                # TODO - substituir quando existir na inewave
                Variable.ENERGIA_DESVIO_FIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Edesvcsin, join(dir, "edesvfsin.out")
                )
            ),
            (
                Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Mevmin, join(dir, f"mevmin{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Mevminm, join(dir, f"mevminm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Mevminsin, join(dir, "mevminsin.out")
                )
            ),
            (
                Variable.ENERGIA_VOLUME_MORTO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Vmort, join(dir, f"vmort{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_VOLUME_MORTO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Vmortm, join(dir, f"vmortm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_VOLUME_MORTO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Vmortsin, join(dir, "vmortsin.out")
                )
            ),
            (
                Variable.ENERGIA_EVAPORACAO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Evapo, join(dir, f"evapo{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_EVAPORACAO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Evapom, join(dir, f"evapom{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.ENERGIA_EVAPORACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Evaporsin, join(dir, "evaporsin.out")
                )
            ),
            (
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Qafluh, join(dir, f"qafluh{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Qincruh, join(dir, f"qincruh{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.VAZAO_TURBINADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Qturuh, join(dir, f"qturuh{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Qvertuh, join(dir, f"qvertuh{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Varmuh, join(dir, f"varmuh{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Varmpuh, join(dir, f"varmpuh{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__eval_block_0_sum(
                self.__read_nwlistop_setting_version(
                    Ghiduh, join(dir, f"ghiduh{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.VELOCIDADE_VENTO,
                SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
            ): lambda dir, uee=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Vento, join(dir, f"vento{str(uee).zfill(3)}.out")
                )
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
            ): lambda dir, uee=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Geol, join(dir, f"geol{str(uee).zfill(3)}.out")
                )
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Geolm, join(dir, f"geolm{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Geolsin, join(dir, "geolsin.out")
                )
            ),
            (
                Variable.CORTE_GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Corteolm,
                    join(dir, f"corteolm{str(submercado).zfill(3)}.out"),
                )
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Def, join(dir, f"def{str(submercado).zfill(3)}p001.out")
                )
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Def, join(dir, "defsinp001.out")
                )
            ),
            (
                Variable.EXCESSO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Exces, join(dir, f"exces{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.EXCESSO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Excessin, join(dir, "excessin.out")
                )
            ),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
            ): lambda dir, submercados=(1, 2): self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Intercambio,
                    join(
                        dir,
                        f"int{str(submercados[0]).zfill(3)}"
                        + f"{str(submercados[1]).zfill(3)}.out",
                    ),
                )
            ),
            (
                Variable.CUSTO_DEFICIT,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Cdef, join(dir, f"cdef{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.CUSTO_DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Cdefsin, join(dir, "cdefsin.out")
                )
            ),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Mercl, join(dir, f"mercl{str(submercado).zfill(3)}.out")
                )
            ),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Merclsin, join(dir, "merclsin.out")
                )
            ),
            (
                Variable.VIOLACAO_FPHA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__eval_block_0_sum(
                self.__read_nwlistop_setting_version(
                    ViolFpha, join(dir, f"viol_fpha{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    ViolEvmin, join(dir, f"viol_evmin{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    ViolEvminm,
                    join(dir, f"vevminm{str(submercado).zfill(3)}.out"),
                )
            ),
            (
                Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    ViolEvminsin, join(dir, "viol_evminsin.out")
                )
            ),
            (
                Variable.VOLUME_RETIRADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Vretiradauh,
                    join(dir, f"vretiradauh{str(uhe).zfill(3)}.out"),
                )
            ),
            (
                Variable.VAZAO_DESVIADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Qdesviouh, join(dir, f"qdesviouh{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__eval_block_0_sum(
                self.__read_nwlistop_setting_version(
                    ViolGhminuh,
                    join(dir, f"viol_ghminuh{str(uhe).zfill(3)}.out"),
                )
            ),
            (
                Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda dir, ree=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    ViolGhmin, join(dir, f"viol_ghmin{str(ree).zfill(3)}.out")
                )
            ),
            (
                Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
                SpatialResolution.SUBMERCADO,
            ): lambda dir, submercado=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    # TODO - atualizar o nome do arquivo quando for alterado
                    ViolGhminm,
                    join(dir, f"vghminm{str(submercado).zfill(3)}.out"),
                )
            ),
            (
                Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda dir, _: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    ViolGhminsin, join(dir, "viol_ghminsin.out")
                )
            ),
            (
                Variable.COTA_MONTANTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Hmont, join(dir, f"hmont{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.COTA_JUSANTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Hjus, join(dir, f"hjus{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.QUEDA_LIQUIDA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__replace_block_column(
                self.__read_nwlistop_setting_version(
                    Hliq, join(dir, f"hliq{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.VOLUME_EVAPORADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    Vevapuh, join(dir, f"vevapuh{str(uhe).zfill(3)}.out")
                )
            ),
            (
                Variable.VIOLACAO_POSITIVA_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    ViolPosEvap,
                    join(dir, f"viol_pos_evap{str(uhe).zfill(3)}.out"),
                )
            ),
            (
                Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda dir, uhe=1: self.__add_block_column(
                self.__read_nwlistop_setting_version(
                    ViolNegEvap,
                    join(dir, f"viol_neg_evap{str(uhe).zfill(3)}.out"),
                ).fillna(0.0)  # type: ignore
            ),
        }

    def __read_nwlistop_setting_version(
        self, reader: Type[BlockFile], path: str
    ) -> Optional[pd.DataFrame]:
        reader.set_version(self.__version)
        return reader.read(path).valores

    def __fix_indices_cenarios(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def __agg_cmo_dfs(self, dir: str, submercado: int) -> pd.DataFrame:
        Cmargmed.set_version(self.__version)
        df_med = Cmargmed.read(
            join(dir, f"cmarg{str(submercado).zfill(3)}-med.out")
        ).valores
        df_med["patamar"] = 0
        df_med = self.__fix_indices_cenarios(df_med)
        Cmarg.set_version(self.__version)
        df_pats = Cmarg.read(
            join(dir, f"cmarg{str(submercado).zfill(3)}.out")
        ).valores
        df_pats = self.__fix_indices_cenarios(df_pats)
        df = pd.concat(
            [df_med, df_pats],
            ignore_index=True,
        )
        df = df.sort_values(["data", "serie", "patamar"]).reset_index(drop=True)
        return df

    def __add_block_column(self, df: pd.DataFrame) -> pd.DataFrame:
        df["patamar"] = 0
        df = self.__fix_indices_cenarios(df)
        return df

    def __replace_block_column(
        self, df: pd.DataFrame, col: str = "TOTAL"
    ) -> pd.DataFrame:
        df.loc[df["patamar"] == col, "patamar"] = "0"
        df = df.astype({"patamar": int})
        df = self.__fix_indices_cenarios(df)
        return df

    def __eval_block_0_sum(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.astype({"patamar": int})
        df = self.__fix_indices_cenarios(df)
        df_pat0 = df.copy()
        df_pat0["patamar"] = 0
        df_pat0 = df_pat0.groupby(["data", "serie"], as_index=False).sum(
            numeric_only=True
        )
        df_pat0 = pd.concat([df, df_pat0], ignore_index=True)
        return df_pat0.sort_values(["data", "serie", "patamar"])

    def __eval_block_0_sum_gter_ute(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.astype({"patamar": int})
        df = self.__fix_indices_cenarios(df)
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
        *args,
        **kwargs,
    ) -> Optional[pd.DataFrame]:
        try:
            regra = self.__regras.get((variable, spatial_resolution))
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


def factory(kind: str, *args, **kwargs) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind, RawFilesRepository)(*args, **kwargs)
