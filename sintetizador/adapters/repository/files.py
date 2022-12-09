from abc import ABC, abstractmethod
from typing import Dict, Type, Optional, Tuple, Callable
import pandas as pd  # type: ignore
import pathlib
import asyncio

from inewave.newave.caso import Caso
from inewave.newave.arquivos import Arquivos
from inewave.newave.patamar import Patamar
from inewave.newave.dger import DGer
from inewave.newave.confhd import Confhd
from inewave.newave.conft import ConfT
from inewave.newave.eolicacadastro import EolicaCadastro
from inewave.newave.ree import REE
from inewave.newave.sistema import Sistema
from inewave.newave.pmo import PMO
from inewave.newave.newavetim import NewaveTim

from inewave.nwlistop.cmarg import Cmarg
from inewave.nwlistop.cmargmed import CmargMed
from inewave.nwlistop.cterm import Cterm
from inewave.nwlistop.ctermsin import CtermSIN
from inewave.nwlistop.coper import Coper
from inewave.nwlistop.eafb import Eafb
from inewave.nwlistop.eafbm import Eafbm
from inewave.nwlistop.eafbsin import EafbSIN
from inewave.nwlistop.intercambio import Intercambio
from inewave.nwlistop.deficit import Def
from inewave.nwlistop.defsin import DefSIN

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
from inewave.nwlistop.evert import Evert
from inewave.nwlistop.evertm import Evertm
from inewave.nwlistop.evertsin import EvertSIN
from inewave.nwlistop.perdf import Perdf
from inewave.nwlistop.perdfm import Perdfm
from inewave.nwlistop.perdfsin import PerdfSIN
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
from sintetizador.model.settings import Settings
from sintetizador.utils.encoding import converte_codificacao
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.temporalresolution import TemporalResolution


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
    def get_pmo(self) -> PMO:
        raise NotImplementedError

    @abstractmethod
    def get_newavetim(self) -> NewaveTim:
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
        self.__dger: Optional[DGer] = None
        self.__patamar: Optional[Patamar] = None
        self.__sistema: Optional[Sistema] = None
        self.__pmo: Optional[PMO] = None
        self.__newavetim: Optional[NewaveTim] = None
        self.__ree: Optional[REE] = None
        self.__confhd: Optional[Confhd] = None
        self.__conft: Optional[ConfT] = None
        self.__eolicacadastro: Optional[EolicaCadastro] = None
        self.__regras: Dict[
            Tuple[Variable, SpatialResolution, TemporalResolution], Callable
        ] = {
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: CmargMed.le_arquivo(
                dir, f"cmarg{str(submercado).zfill(3)}-med.out"
            ).valores,
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda dir, submercado=1: Cmarg.le_arquivo(
                dir, f"cmarg{str(submercado).zfill(3)}.out"
            ).valores,
            (
                Variable.VALOR_AGUA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda dir, ree=1: Vagua.le_arquivo(
                dir, f"vagua{str(ree).zfill(3)}.out"
            ).valores,
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: Cterm.le_arquivo(
                dir, f"cterm{str(submercado).zfill(3)}.out"
            ).valores,
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, _: CtermSIN.le_arquivo(
                dir, f"ctermsin.out"
            ).valores,
            (
                Variable.CUSTO_OPERACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, _: Coper.le_arquivo(dir, f"coper.out").valores,
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda dir, ree=1: Eafb.le_arquivo(
                dir, f"eafb{str(ree).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: Eafbm.le_arquivo(
                dir, f"eafbm{str(submercado).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, _: EafbSIN.le_arquivo(dir, f"eafbsin.out").valores,
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda dir, ree=1: Earmfp.le_arquivo(
                dir, f"earmfp{str(ree).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: Earmfpm.le_arquivo(
                dir, f"earmfpm{str(submercado).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, _: EarmfpSIN.le_arquivo(
                dir, "earmfpsin.out"
            ).valores,
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda dir, ree=1: Earmf.le_arquivo(
                dir, f"earmf{str(ree).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: Earmfm.le_arquivo(
                dir, f"earmfm{str(submercado).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, _: EarmfSIN.le_arquivo(dir, "earmfsin.out").valores,
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda dir, ree=1: self.__extrai_patamares_df(
                Ghtot.le_arquivo(dir, f"ghtot{str(ree).zfill(3)}.out").valores,
                ["TOTAL"],
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Ghtotm.le_arquivo(
                    dir, f"ghtotm{str(submercado).zfill(3)}.out"
                ).valores,
                ["TOTAL"],
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
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
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Gttot.le_arquivo(
                    dir, f"gttot{str(submercado).zfill(3)}.out"
                ).valores,
                ["TOTAL"],
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
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
                Variable.ENERGIA_VERTIDA_RESERV,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda dir, ree=1: Evert.le_arquivo(
                dir, f"evert{str(ree).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_VERTIDA_RESERV,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: Evertm.le_arquivo(
                dir, f"evertm{str(submercado).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_VERTIDA_RESERV,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, _: EvertSIN.le_arquivo(dir, "evertsin.out").valores,
            (
                Variable.ENERGIA_VERTIDA_FIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda dir, ree=1: Perdf.le_arquivo(
                dir, f"perdf{str(ree).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_VERTIDA_FIO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: Perdfm.le_arquivo(
                dir, f"perdfm{str(submercado).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_VERTIDA_FIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, _: PerdfSIN.le_arquivo(dir, "perdfsin.out").valores,
            (
                Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda dir, ree=1: Verturb.le_arquivo(
                dir, f"verturb{str(ree).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: Verturbm.le_arquivo(
                dir, f"verturbm{str(submercado).zfill(3)}.out"
            ).valores,
            (
                Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, _: VerturbSIN.le_arquivo(
                dir, "verturbsin.out"
            ).valores,
            (
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda dir, uhe=1: QaflUH.le_arquivo(
                dir, f"qafluh{str(uhe).zfill(3)}.out"
            ).valores,
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda dir, uhe=1: QincrUH.le_arquivo(
                dir, f"qincruh{str(uhe).zfill(3)}.out"
            ).valores,
            (
                Variable.VOLUME_TURBINADO,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda dir, uhe=1: VturUH.le_arquivo(
                dir, f"vturuh{str(uhe).zfill(3)}.out"
            ).valores,
            (
                Variable.VOLUME_VERTIDO,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda dir, uhe=1: VertUH.le_arquivo(
                dir, f"vertuh{str(uhe).zfill(3)}.out"
            ).valores,
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda dir, uhe=1: VarmUH.le_arquivo(
                dir, f"varmuh{str(uhe).zfill(3)}.out"
            ).valores,
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
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
                TemporalResolution.ESTAGIO,
            ): lambda dir, uee=1: Vento.le_arquivo(
                dir, f"vento{str(uee).zfill(3)}.out"
            ).valores,
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.USINA_EOLICA,
                TemporalResolution.ESTAGIO,
            ): lambda dir, uee=1: self.__extrai_patamares_df(
                Geol.le_arquivo(dir, f"geol{str(uee).zfill(3)}.out").valores,
                ["TOTAL"],
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Geolm.le_arquivo(
                    dir, f"geolm{str(submercado).zfill(3)}.out"
                ).valores,
                ["TOTAL"],
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
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
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Def.le_arquivo(
                    dir, f"def{str(submercado).zfill(3)}p001.out"
                ).valores,
                ["TOTAL"],
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, _: self.__extrai_patamares_df(
                Def.le_arquivo(dir, f"defsinp001.out").valores, ["TOTAL"]
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Def.le_arquivo(
                    dir, f"def{str(submercado).zfill(3)}p001.out"
                ).valores
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda dir, _: self.__extrai_patamares_df(
                Def.le_arquivo(dir, f"defsinp001.out").valores
            ),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercados=(1, 2): self.__extrai_patamares_df(
                Intercambio.le_arquivo(
                    dir,
                    f"int{str(submercados[0]).zfill(3)}"
                    + f"{str(submercados[1]).zfill(3)}.out",
                ).valores,
                ["TOTAL"],
            ),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
                TemporalResolution.PATAMAR,
            ): lambda dir, submercados=(1, 2): self.__extrai_patamares_df(
                Intercambio.le_arquivo(
                    dir,
                    f"int{str(submercados[0]).zfill(3)}"
                    + f"{str(submercados[1]).zfill(3)}.out",
                ).valores
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
        if self.__dger is None:
            caminho = pathlib.Path(self.__tmppath).joinpath(self.arquivos.dger)
            script = pathlib.Path(Settings().installdir).joinpath(
                Settings().encoding_script
            )
            asyncio.run(converte_codificacao(caminho, script))
            Log.log().info(f"Lendo arquivo {self.arquivos.dger}")
            self.__dger = DGer.le_arquivo(self.__tmppath, self.arquivos.dger)
        return self.__dger

    def get_patamar(self) -> Patamar:
        if self.__patamar is None:
            Log.log().info(f"Lendo arquivo {self.arquivos.patamar}")
            self.__patamar = Patamar.le_arquivo(
                self.__tmppath, self.arquivos.patamar
            )
        return self.__patamar

    def get_confhd(self) -> Confhd:
        if self.__confhd is None:
            Log.log().info(f"Lendo arquivo {self.arquivos.confhd}")
            self.__confhd = Confhd.le_arquivo(
                self.__tmppath, self.arquivos.confhd
            )
        return self.__confhd

    def get_conft(self) -> ConfT:
        if self.__conft is None:
            Log.log().info(f"Lendo arquivo {self.arquivos.conft}")
            self.__conft = ConfT.le_arquivo(
                self.__tmppath, self.arquivos.conft
            )
        return self.__conft

    def get_ree(self) -> REE:
        if self.__ree is None:
            Log.log().info(f"Lendo arquivo {self.arquivos.ree}")
            self.__ree = REE.le_arquivo(self.__tmppath, self.arquivos.ree)
        return self.__ree

    def get_sistema(self) -> Sistema:
        if self.__sistema is None:
            Log.log().info(f"Lendo arquivo {self.arquivos.sistema}")
            self.__sistema = Sistema.le_arquivo(
                self.__tmppath, self.arquivos.sistema
            )
        return self.__sistema

    def get_pmo(self) -> PMO:
        if self.__pmo is None:
            Log.log().info(f"Lendo arquivo {self.arquivos.pmo}")
            self.__pmo = PMO.le_arquivo(self.__tmppath, self.arquivos.pmo)
        return self.__pmo

    def get_newavetim(self) -> NewaveTim:
        if self.__newavetim is None:
            Log.log().info(f"Lendo arquivo newave.tim")
            self.__newavetim = NewaveTim.le_arquivo(
                self.__tmppath, "newave.tim"
            )
        return self.__newavetim

    def get_eolicacadastro(self) -> EolicaCadastro:
        if self.__eolicacadastro is None:
            Log.log().info(f"Lendo arquivo eolica-cadastro.csv")
            self.__eolicacadastro = EolicaCadastro.le_arquivo(
                self.__tmppath, "eolica-cadastro.csv"
            )
        return self.__eolicacadastro

    def get_nwlistop(
        self,
        variable: Variable,
        spatial_resolution: SpatialResolution,
        temporal_resolution: TemporalResolution,
        *args,
        **kwargs,
    ) -> Optional[pd.DataFrame]:
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
        except FileNotFoundError as f:
            Log.log().warning(
                "Arquivo não encontrado para "
                + f"{variable.value}_{spatial_resolution.value}"
                + f"_{temporal_resolution.value}: {kwargs}"
            )
            return None


def factory(kind: str, *args, **kwargs) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind)(*args, **kwargs)
