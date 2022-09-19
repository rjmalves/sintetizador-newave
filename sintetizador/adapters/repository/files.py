from abc import ABC, abstractmethod
from typing import Dict, Type, Optional, Tuple, Callable
import pandas as pd  # type: ignore

from inewave.newave.caso import Caso
from inewave.newave.arquivos import Arquivos
from inewave.newave.patamar import Patamar
from inewave.newave.dger import DGer
from inewave.newave.confhd import Confhd
from inewave.newave.conft import ConfT
from inewave.newave.eolicacadastro import EolicaCadastro
from inewave.newave.ree import REE
from inewave.newave.sistema import Sistema

from inewave.nwlistop.cmargmed import CmargMed
from inewave.nwlistop.cterm import Cterm
from inewave.nwlistop.ctermsin import CtermSIN
from inewave.nwlistop.coper import Coper
from inewave.nwlistop.eafb import Eafb
from inewave.nwlistop.eafbm import Eafbm
from inewave.nwlistop.eafbsin import EafbSIN

from inewave.nwlistop.earmfp import Earmfp
from inewave.nwlistop.earmfpm import Earmfpm
from inewave.nwlistop.earmfpsin import EarmfpSIN
from inewave.nwlistop.earmf import Earmf
from inewave.nwlistop.earmfm import Earmfm
from inewave.nwlistop.earmfsin import EarmfSIN
from inewave.nwlistop.ghtot import Ghtot
from inewave.nwlistop.ghtotm import Ghtotm
from inewave.nwlistop.ghtotsin import GhtotSIN
from inewave.nwlistop.gttot import Gttot
from inewave.nwlistop.gttotsin import GttotSIN
from inewave.nwlistop.verturb import Verturb
from inewave.nwlistop.verturbm import Verturbm
from inewave.nwlistop.verturbsin import VerturbSIN
from inewave.nwlistop.vagua import Vagua

from inewave.nwlistop.vento import Vento
from inewave.nwlistop.geol import Geol
from inewave.nwlistop.geolm import Geolm
from inewave.nwlistop.geolsin import GeolSIN

from inewave.nwlistop.qafluh import QaflUH
from inewave.nwlistop.qincruh import QincrUH
from inewave.nwlistop.ghiduh import GhidUH
from inewave.nwlistop.vturuh import VturUH
from inewave.nwlistop.vertuh import VertUH
from inewave.nwlistop.varmuh import VarmUH
from inewave.nwlistop.varmpuh import VarmpUH

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
    def get_confhd(self) -> Confhd:
        raise NotImplementedError

    @abstractmethod
    def get_conft(self) -> ConfT:
        raise NotImplementedError

    @abstractmethod
    def get_ree(self) -> REE:
        raise NotImplementedError

    @abstractmethod
    def get_sistema(self) -> Sistema:
        raise NotImplementedError

    @abstractmethod
    def get_patamar(self) -> Patamar:
        raise NotImplementedError

    @abstractmethod
    def get_eolicacadastro(self) -> EolicaCadastro:
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
    def __init__(self, tmppath: str):
        self.__tmppath = tmppath
        self.__caso = Caso.le_arquivo(str(self.__tmppath))
        self.__arquivos: Optional[Arquivos] = None
        self.__regras: Dict[
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
                Variable.VALOR_AGUA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.MES,
            ): lambda dir, ree=1: Vagua.le_arquivo(
                dir, f"vagua{str(ree).zfill(3)}.out"
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
            ): lambda dir, _: CtermSIN.le_arquivo(
                dir, f"ctermsin.out"
            ).valores,
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
            ): lambda dir, _: EarmfpSIN.le_arquivo(
                dir, "earmfpsin.out"
            ).valores,
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.MES,
            ): lambda dir, ree=1: Earmf.le_arquivo(
                dir, f"earmf{str(ree).zfill(3)}.out"
            ).valores,
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
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.MES,
            ): lambda dir, ree=1: self.__extrai_patamares_df(
                Ghtot.le_arquivo(dir, f"ghtot{str(ree).zfill(3)}.out").valores,
                ["TOTAL"],
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.MES,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Ghtotm.le_arquivo(
                    dir, f"ghtotm{str(submercado).zfill(3)}.out"
                ).valores,
                ["TOTAL"],
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.MES,
            ): lambda dir, _: self.__extrai_patamares_df(
                GhtotSIN.le_arquivo(dir, "ghtotsin.out").valores, ["TOTAL"]
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.PATAMAR,
            ): lambda dir, ree=1: self.__extrai_patamares_df(
                Ghtot.le_arquivo(dir, f"ghtot{str(ree).zfill(3)}.out").valores,
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Ghtotm.le_arquivo(
                    dir, f"ghtotm{str(submercado).zfill(3)}.out"
                ).valores,
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda dir, _: self.__extrai_patamares_df(
                GhtotSIN.le_arquivo(dir, "ghtotsin.out").valores,
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.MES,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Gttot.le_arquivo(
                    dir, f"gttot{str(submercado).zfill(3)}.out"
                ).valores,
                ["TOTAL"],
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.MES,
            ): lambda dir, _: self.__extrai_patamares_df(
                GttotSIN.le_arquivo(dir, "gttotsin.out").valores, ["TOTAL"]
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Gttot.le_arquivo(
                    dir, f"gttot{str(submercado).zfill(3)}.out"
                ).valores,
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda dir, _: self.__extrai_patamares_df(
                GttotSIN.le_arquivo(dir, "gttotsin.out").valores,
            ),
            (
                Variable.ENERGIA_VERTIDA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.MES,
            ): lambda dir, ree=1: Verturb.le_arquivo(
                dir, f"verturb{str(ree).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_VERTIDA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.MES,
            ): lambda dir, submercado=1: Verturbm.le_arquivo(
                dir, f"verturbm{str(submercado).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_VERTIDA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.MES,
            ): lambda dir, _: VerturbSIN.le_arquivo(
                dir, "verturbsin.out"
            ).valores,
            (
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.MES,
            ): lambda dir, uhe=1: QaflUH.le_arquivo(
                dir, f"qafluh{str(uhe).zfill(3)}.out"
            ).valores,
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.MES,
            ): lambda dir, uhe=1: QincrUH.le_arquivo(
                dir, f"qincruh{str(uhe).zfill(3)}.out"
            ).valores,
            (
                Variable.VOLUME_TURBINADO,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.MES,
            ): lambda dir, uhe=1: VturUH.le_arquivo(
                dir, f"vturuh{str(uhe).zfill(3)}.out"
            ).valores,
            (
                Variable.VOLUME_VERTIDO,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.MES,
            ): lambda dir, uhe=1: VertUH.le_arquivo(
                dir, f"vertuh{str(uhe).zfill(3)}.out"
            ).valores,
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.MES,
            ): lambda dir, uhe=1: VarmUH.le_arquivo(
                dir, f"varmuh{str(uhe).zfill(3)}.out"
            ).valores,
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.MES,
            ): lambda dir, uhe=1: VarmpUH.le_arquivo(
                dir, f"varmpuh{str(uhe).zfill(3)}.out"
            ).valores,
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda dir, uhe=1: self.__extrai_patamares_df(
                GhidUH.le_arquivo(
                    dir, f"ghiduh{str(uhe).zfill(3)}.out"
                ).valores
            ),
            (
                Variable.VELOCIDADE_VENTO,
                SpatialResolution.USINA_EOLICA,
                TemporalResolution.MES,
            ): lambda dir, uee=1: Vento.le_arquivo(
                dir, f"vento{str(uee).zfill(3)}.out"
            ).valores,
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.USINA_EOLICA,
                TemporalResolution.MES,
            ): lambda dir, uee=1: self.__extrai_patamares_df(
                Geol.le_arquivo(dir, f"geol{str(uee).zfill(3)}.out").valores,
                ["TOTAL"],
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.MES,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Geolm.le_arquivo(
                    dir, f"geolm{str(submercado).zfill(3)}.out"
                ).valores,
                ["TOTAL"],
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.MES,
            ): lambda dir, _: self.__extrai_patamares_df(
                GeolSIN.le_arquivo(dir, f"geolsin.out").valores, ["TOTAL"]
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.USINA_EOLICA,
                TemporalResolution.PATAMAR,
            ): lambda dir, uee=1: self.__extrai_patamares_df(
                Geol.le_arquivo(dir, f"geol{str(uee).zfill(3)}.out").valores
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Geolm.le_arquivo(
                    dir, f"geolm{str(submercado).zfill(3)}.out"
                ).valores
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda dir, _: self.__extrai_patamares_df(
                GeolSIN.le_arquivo(dir, f"geolsin.out").valores
            ),
        }

    def __extrai_patamares_df(
        self, df: pd.DataFrame, patamares: list = None
    ) -> pd.DataFrame:
        if patamares is None:
            patamares = [
                str(i)
                for i in range(1, self.get_patamar().numero_patamares + 1)
            ]
        return df.loc[df["Patamar"].isin(patamares), :]

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

    def get_patamar(self) -> Patamar:
        return Patamar.le_arquivo(self.__tmppath, self.arquivos.patamar)

    def get_confhd(self) -> Confhd:
        Log.log().info(f"Lendo arquivo {self.arquivos.confhd}")
        return Confhd.le_arquivo(self.__tmppath, self.arquivos.confhd)

    def get_conft(self) -> ConfT:
        Log.log().info(f"Lendo arquivo {self.arquivos.conft}")
        return ConfT.le_arquivo(self.__tmppath, self.arquivos.conft)

    def get_ree(self) -> REE:
        Log.log().info(f"Lendo arquivo {self.arquivos.ree}")
        return REE.le_arquivo(self.__tmppath, self.arquivos.ree)

    def get_sistema(self) -> Sistema:
        Log.log().info(f"Lendo arquivo {self.arquivos.sistema}")
        return Sistema.le_arquivo(self.__tmppath, self.arquivos.sistema)

    def get_eolicacadastro(self) -> EolicaCadastro:
        Log.log().info(f"Lendo arquivo eolica-cadastro.csv")
        return EolicaCadastro.le_arquivo(self.__tmppath, "eolica-cadastro.csv")

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
        if (spatial_resolution == SpatialResolution.USINA_HIDROELETRICA) and (
            "uhe" not in kwargs.keys()
        ):
            return None
        if (spatial_resolution == SpatialResolution.USINA_TERMELETRICA) and (
            "ute" not in kwargs.keys()
        ):
            return None
        if (spatial_resolution == SpatialResolution.USINA_EOLICA) and (
            "uee" not in kwargs.keys()
        ):
            return None
        try:
            regra = self.__regras.get(
                (variable, spatial_resolution, temporal_resolution)
            )
            if regra is None:
                Log.log().warning(
                    "Não encontrados dados de operação para "
                    + (variable, spatial_resolution, temporal_resolution)
                )
                return None
            return regra(self.__tmppath, *args, **kwargs)
        except FileNotFoundError:
            return None


def factory(kind: str, *args, **kwargs) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind)(*args, **kwargs)
