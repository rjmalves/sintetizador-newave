from abc import ABC, abstractmethod
from typing import Dict, Type, Optional, Tuple, Callable
import pandas as pd  # type: ignore
import pathlib

from inewave.newave.caso import Caso
from inewave.newave.arquivos import Arquivos
from inewave.newave.dger import DGer
from inewave.newave.ree import REE
from inewave.newave.sistema import Sistema
from inewave.nwlistop.earmfp import Earmfp
from inewave.nwlistop.earmfpm import Earmfpm
from inewave.nwlistop.earmfpsin import EarmfpSIN

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
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
            TemporalResolution.MES,
        ): lambda _: EarmfpSIN.le_arquivo(".", "earmfpsin.out").valores,
        (
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL,
            SpatialResolution.SUBMERCADO,
            TemporalResolution.MES,
        ): lambda sbm: Earmfpm.le_arquivo(
            ".", f"earmfpm{str(sbm).zfill(3)}.out"
        ).valores,
        (
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            TemporalResolution.MES,
        ): lambda ree: Earmfp.le_arquivo(
            ".", f"earmfp{str(ree).zfill(3)}.out"
        ).valores,
    }

    def __init__(self, path: str):
        self.__path = path
        self.__caso = Caso.le_arquivo(str(self.__path))
        self.__arquivos: Optional[Arquivos] = None

    @property
    def caminho(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

    @property
    def caso(self) -> Caso:
        return self.__caso

    @property
    def arquivos(self) -> Arquivos:
        if self.__arquivos is None:
            self.__arquivos = Arquivos.le_arquivo(
                self.__path, self.__caso.arquivos
            )
        return self.__arquivos

    def get_dger(self) -> DGer:
        return DGer.le_arquivo(self.__path, self.arquivos.dger)

    def get_ree(self) -> REE:
        return REE.le_arquivo(self.__path, self.arquivos.ree)

    def get_sistema(self) -> Sistema:
        return Sistema.le_arquivo(self.__path, self.arquivos.sistema)

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
        ](*args, **kwargs)


def factory(kind: str) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind)()
