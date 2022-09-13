from abc import ABC, abstractmethod
from typing import Dict, Type, Optional, Tuple, Callable
import pandas as pd  # type: ignore

from inewave.newave.caso import Caso
from inewave.newave.arquivos import Arquivos
from inewave.newave.dger import DGer
from inewave.newave.ree import REE
from inewave.newave.sistema import Sistema

from inewave.nwlistop.cmargmed import CmargMed
from inewave.nwlistop.cterm import Cterm
from inewave.nwlistop.ctermsin import CtermSIN
from inewave.nwlistop.coper import Coper
from inewave.nwlistop.eafb import Eafb
from inewave.nwlistop.eafbm import Eafbm
from inewave.nwlistop.eafbsin import EafbSIN

# from inewave.nwlistop.vento import Vento
from inewave.nwlistop.earmfp import Earmfp
from inewave.nwlistop.earmfpm import Earmfpm
from inewave.nwlistop.earmfpsin import EarmfpSIN
from inewave.nwlistop.earmfm import Earmfm
from inewave.nwlistop.earmfsin import EarmfSIN
from inewave.nwlistop.gttot import Gttot
from inewave.nwlistop.gttotsin import GttotSIN

# from inewave.nwlistop.geol import Geol
# from inewave.nwlistop.geolm import Geolm
# from inewave.nwlistop.geolsin import GeolSIN
# from inewave.nwlistop.verturb import Verturb
# from inewave.nwlistop.verturbm import Verturbm
# from inewave.nwlistop.verturbsin import VerturbSIN
# from inewave.nwlistop.qafluh import QaflUH
# from inewave.nwlistop.qincruh import QincrUH
# from inewave.nwlistop.vturuh import VturUH
# from inewave.nwlistop.vertuh import VertUH
# from inewave.nwlistop.varmuh import VarmUH
# from inewave.nwlistop.varmpuh import VarmpUH

from sintetizador.utils.log import Log
from sintetizador.model.variable import Variable
from sintetizador.model.spatialresolution import SpatialResolution
from sintetizador.model.temporalresolution import TemporalResolution


class AbstractFilesRepository(ABC):
    @property
    @abstractmethod
    def caso(self) -> Caso:
        raise NotImplementedError

    @property
    @abstractmethod
    def arquivos(self) -> Arquivos:
        raise NotImplementedError

    @abstractmethod
    def get_dger(self) -> DGer:
        raise NotImplementedError

    @abstractmethod
    def get_ree(self) -> REE:
        raise NotImplementedError

    @abstractmethod
    def get_sistema(self) -> Sistema:
        raise NotImplementedError

    @abstractmethod
    def get_nwlistop(
        self,
        variable: Variable,
        spatial_resolution: SpatialResolution,
        temporal_resolution: TemporalResolution,
        *args,
        **kwargs,
    ) -> Optional[pd.DataFrame]:
        pass


class RawFilesRepository(AbstractFilesRepository):

    REGRAS: Dict[
        Tuple[Variable, SpatialResolution, TemporalResolution], Callable
    ] = {
        (
            Variable.CUSTO_MARGINAL_OPERACAO,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.MES,
        ): lambda dir, submercado=1: CmargMed.le_arquivo(
            dir, f"cmarg{str(submercado).zfill(3)}-med.out"
        ).valores,
        (
            Variable.CUSTO_GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.MES,
        ): lambda dir, submercado=1: Cterm.le_arquivo(
            dir, f"cterm{str(submercado).zfill(3)}.out"
        ).valores,
        (
            Variable.CUSTO_GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.MES,
        ): lambda dir, _: CtermSIN.le_arquivo(dir, f"ctermsin.out").valores,
        (
            Variable.CUSTO_OPERACAO,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.MES,
        ): lambda dir, _: Coper.le_arquivo(dir, f"coper.out").valores,
        (
            Variable.ENERGIA_NATURAL_AFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.MES,
        ): lambda dir, ree=1: Eafb.le_arquivo(
            dir, f"eafb{str(ree).zfill(3)}.out"
        ).valores,
        (
            Variable.ENERGIA_NATURAL_AFLUENTE,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.MES,
        ): lambda dir, submercado=1: Eafbm.le_arquivo(
            dir, f"eafbm{str(submercado).zfill(3)}.out"
        ).valores,
        (
            Variable.ENERGIA_NATURAL_AFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.MES,
        ): lambda dir, _: EafbSIN.le_arquivo(dir, f"eafbsin.out").valores,
        (
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.MES,
        ): lambda dir, ree=1: Earmfp.le_arquivo(
            dir, f"earmfp{str(ree).zfill(3)}.out"
        ).valores,
        (
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.MES,
        ): lambda dir, submercado=1: Earmfpm.le_arquivo(
            dir, f"earmfpm{str(submercado).zfill(3)}.out"
        ).valores,
        (
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.MES,
        ): lambda dir, _: EarmfpSIN.le_arquivo(dir, "earmfpsin.out").valores,
        (
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.MES,
        ): lambda dir, submercado=1: Earmfm.le_arquivo(
            dir, f"earmfm{str(submercado).zfill(3)}.out"
        ).valores,
        (
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.MES,
        ): lambda dir, _: EarmfSIN.le_arquivo(dir, "earmfsin.out").valores,
        (
            Variable.GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.MES,
        ): lambda dir, submercado=1: Gttot.le_arquivo(
            dir, f"gttot{str(submercado).zfill(3)}.out"
        ).valores,
        (
            Variable.GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.MES,
        ): lambda dir, _: GttotSIN.le_arquivo(dir, "gttotsin.out").valores,
    }

    def __init__(self, tmppath: str):
        self.__tmppath = tmppath
        self.__caso = Caso.le_arquivo(str(self.__tmppath))
        self.__arquivos: Optional[Arquivos] = None

    @property
    def caso(self) -> Caso:
        return self.__caso

    @property
    def arquivos(self) -> Arquivos:
        if self.__arquivos is None:
            self.__arquivos = Arquivos.le_arquivo(
                self.__tmppath, self.__caso.arquivos
            )
        return self.__arquivos

    def get_dger(self) -> DGer:
        Log.log().info(f"Lendo arquivo {self.arquivos.dger}")
        return DGer.le_arquivo(self.__tmppath, self.arquivos.dger)

    def get_ree(self) -> REE:
        Log.log().info(f"Lendo arquivo {self.arquivos.ree}")
        return REE.le_arquivo(self.__tmppath, self.arquivos.ree)

    def get_sistema(self) -> Sistema:
        Log.log().info(f"Lendo arquivo {self.arquivos.sistema}")
        return Sistema.le_arquivo(self.__tmppath, self.arquivos.sistema)

    def get_nwlistop(
        self,
        variable: Variable,
        spatial_resolution: SpatialResolution,
        temporal_resolution: TemporalResolution,
        *args,
        **kwargs,
    ) -> Optional[pd.DataFrame]:
        if (spatial_resolution == SpatialResolution.SUBMERCADO) and (
            "submercado" not in kwargs.keys()
        ):
            return None
        if (
            spatial_resolution == SpatialResolution.RESERVATORIO_EQUIVALENTE
        ) and ("ree" not in kwargs.keys()):
            return None
        return RawFilesRepository.REGRAS[
            (variable, spatial_resolution, temporal_resolution)
        ](self.__tmppath, *args, **kwargs)


def factory(kind: str, *args, **kwargs) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind)(*args, **kwargs)
