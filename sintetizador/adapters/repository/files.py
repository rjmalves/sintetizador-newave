from abc import ABC, abstractmethod
from typing import Dict, Type, Optional, Tuple, Callable
import pandas as pd  # type: ignore
import numpy as np  # type: ignore
from datetime import datetime, timedelta
import pathlib
import asyncio

from inewave.newave.caso import Caso
from inewave.newave.arquivos import Arquivos
from inewave.newave.patamar import Patamar
from inewave.newave.dger import DGer
from inewave.newave.confhd import Confhd
from inewave.newave.conft import ConfT
from inewave.newave.clast import ClasT
from inewave.newave.eolicacadastro import EolicaCadastro
from inewave.newave.ree import REE
from inewave.newave.sistema import Sistema
from inewave.newave.pmo import PMO
from inewave.newave.newavetim import NewaveTim

from inewave.newave.energiaf import Energiaf
from inewave.newave.energiab import Energiab
from inewave.newave.energias import Energias
from inewave.newave.vazaof import Vazaof
from inewave.newave.vazaob import Vazaob
from inewave.newave.vazaos import Vazaos
from inewave.newave.enavazf import Enavazf
from inewave.newave.enavazb import Enavazb

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
from inewave.nwlistop.cdef import Cdef
from inewave.nwlistop.cdefsin import CdefSIN

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
from inewave.nwlistop.vevmin import Vevmin
from inewave.nwlistop.vevminm import Vevminm
from inewave.nwlistop.vevminsin import VevminSIN

from inewave.nwlistop.vento import Vento
from inewave.nwlistop.geol import Geol
from inewave.nwlistop.geolm import Geolm
from inewave.nwlistop.geolsin import GeolSIN
from inewave.nwlistop.corteolm import Corteolm

from inewave.nwlistop.qafluh import QaflUH
from inewave.nwlistop.qincruh import QincrUH
from inewave.nwlistop.ghiduh import GhidUH
from inewave.nwlistop.vturuh import VturUH
from inewave.nwlistop.vertuh import VertUH
from inewave.nwlistop.varmuh import VarmUH
from inewave.nwlistop.varmpuh import VarmpUH
from inewave.nwlistop.dtbmax import Dtbmax
from inewave.nwlistop.dtbmin import Dtbmin
from inewave.nwlistop.dvazmax import Dvazmax
from inewave.nwlistop.depminuh import Depminuh
from inewave.nwlistop.dfphauh import Dfphauh

from inewave.nwlistcf import Nwlistcf
from inewave.nwlistcf import Estados

from sintetizador.utils.log import Log
from sintetizador.model.settings import Settings
from sintetizador.utils.encoding import converte_codificacao
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.temporalresolution import TemporalResolution

import platform

if platform.system() == "Windows":
    DGer.ENCODING = "iso-8859-1"


class AbstractFilesRepository(ABC):
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
    def get_dger(self) -> DGer:
        raise NotImplementedError

    @abstractmethod
    def get_confhd(self) -> Confhd:
        raise NotImplementedError

    @abstractmethod
    def get_conft(self) -> ConfT:
        raise NotImplementedError

    @abstractmethod
    def get_clast(self) -> ClasT:
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

    @abstractmethod
    def get_nwlistcf_cortes(self) -> Nwlistcf:
        raise NotImplementedError

    @abstractmethod
    def get_nwlistcf_estados(self) -> Estados:
        raise NotImplementedError

    @abstractmethod
    def get_energiaf(self, iteracao: int) -> Energiaf:
        pass

    @abstractmethod
    def get_energiab(self) -> Energiab:
        pass

    @abstractmethod
    def get_vazaof(self, iteracao: int) -> Vazaof:
        pass

    @abstractmethod
    def get_vazaob(self) -> Vazaob:
        pass

    @abstractmethod
    def get_enavazf(self, iteracao: int) -> Enavazf:
        pass

    @abstractmethod
    def get_enavazb(self) -> Enavazb:
        pass

    @abstractmethod
    def get_energias(self) -> Energias:
        pass

    @abstractmethod
    def get_enavazs(self) -> Enavazf:
        pass

    @abstractmethod
    def get_vazaos(self) -> Vazaos:
        pass

    @abstractmethod
    def _numero_estagios_individualizados(self) -> int:
        pass


class RawFilesRepository(AbstractFilesRepository):
    def __init__(self, tmppath: str):
        self.__tmppath = tmppath
        self.__caso = Caso.le_arquivo(str(self.__tmppath))
        self.__arquivos: Optional[Arquivos] = None
        self.__indices: Optional[pd.DataFrame] = None
        self.__dger: Optional[DGer] = None
        self.__patamar: Optional[Patamar] = None
        self.__sistema: Optional[Sistema] = None
        self.__pmo: Optional[PMO] = None
        self.__newavetim: Optional[NewaveTim] = None
        self.__ree: Optional[REE] = None
        self.__confhd: Optional[Confhd] = None
        self.__conft: Optional[ConfT] = None
        self.__clast: Optional[ClasT] = None
        self.__eolicacadastro: Optional[EolicaCadastro] = None
        self.__nwlistcf: Optional[Nwlistcf] = None
        self.__estados: Optional[Estados] = None
        self.__energiaf: Dict[int, Energiaf] = {}
        self.__energiab: Optional[Energiab] = None
        self.__vazaof: Dict[int, Vazaof] = {}
        self.__vazaob: Optional[Vazaob] = None
        self.__enavazf: Dict[int, Enavazf] = {}
        self.__enavazb: Optional[Enavazb] = None
        self.__energias: Optional[Energias] = None
        self.__enavazs: Optional[Enavazf] = None
        self.__vazaos: Optional[Vazaos] = None
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
            ): lambda dir, _: CtermSIN.le_arquivo(dir, "ctermsin.out").valores,
            (
                Variable.CUSTO_OPERACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, _: Coper.le_arquivo(dir, "coper.out").valores,
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
            ): lambda dir, _: EafbSIN.le_arquivo(dir, "eafbsin.out").valores,
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
                SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda dir, uee=1: Vento.le_arquivo(
                dir, f"vento{str(uee).zfill(3)}.out"
            ).valores,
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
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
                GeolSIN.le_arquivo(dir, "geolsin.out").valores, ["TOTAL"]
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
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
                GeolSIN.le_arquivo(dir, "geolsin.out").valores
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
                Def.le_arquivo(dir, "defsinp001.out").valores, ["TOTAL"]
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
                Def.le_arquivo(dir, "defsinp001.out").valores
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
            (
                Variable.CUSTO_DEFICIT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: Cdef.le_arquivo(
                dir, f"cdef{str(submercado).zfill(3)}.out"
            ).valores,
            (
                Variable.CUSTO_DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, _: CdefSIN.le_arquivo(dir, "cdefsin.out").valores,
            (
                Variable.CORTE_GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Corteolm.le_arquivo(
                    dir, f"corteolm{str(submercado).zfill(3)}.out"
                ).valores,
                ["TOTAL"],
            ),
            (
                Variable.CORTE_GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda dir, submercado=1: self.__extrai_patamares_df(
                Corteolm.le_arquivo(
                    dir, f"corteolm{str(submercado).zfill(3)}.out"
                ).valores
            ),
            (
                Variable.VIOLACAO_DEFLUENCIA_MINIMA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda dir, uhe=1: self.__extrai_patamares_df(
                Depminuh.le_arquivo(
                    dir, f"depminuh{str(uhe).zfill(3)}.out"
                ).valores
            ),
            (
                Variable.VIOLACAO_DEFLUENCIA_MAXIMA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda dir, uhe=1: self.__extrai_patamares_df(
                Dvazmax.le_arquivo(
                    dir, f"dvazmax{str(uhe).zfill(3)}.out"
                ).valores
            ),
            (
                Variable.VIOLACAO_TURBINAMENTO_MINIMO,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda dir, uhe=1: self.__extrai_patamares_df(
                Dtbmin.le_arquivo(
                    dir, f"dtbmin{str(uhe).zfill(3)}.out"
                ).valores
            ),
            (
                Variable.VIOLACAO_TURBINAMENTO_MAXIMO,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda dir, uhe=1: self.__extrai_patamares_df(
                Dtbmax.le_arquivo(
                    dir, f"dtbmax{str(uhe).zfill(3)}.out"
                ).valores
            ),
            (
                Variable.VIOLACAO_FPHA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda dir, uhe=1: self.__extrai_patamares_df(
                Dfphauh.le_arquivo(
                    dir, f"dfphauh{str(uhe).zfill(3)}.out"
                ).valores
            ),
            (
                Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda dir, ree=1: Vevmin.le_arquivo(
                dir, f"vevmin{str(ree).zfill(3)}.out"
            ).valores,
            (
                Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, submercado=1: Vevminm.le_arquivo(
                dir, f"vevminm{str(submercado).zfill(3)}.out"
            ).valores,
            (
                Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda dir, _: VevminSIN.le_arquivo(
                dir, "vevminsin.out"
            ).valores,
        }

    def __extrai_patamares_df(
        self, df: pd.DataFrame, patamares: Optional[list] = None
    ) -> pd.DataFrame:
        if patamares is None:
            num_pats = self.get_patamar().numero_patamares
            if num_pats is None:
                raise RuntimeError("Numero de patamares não encontrado")
            patamares = [str(i) for i in range(1, num_pats + 1)]
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

    @property
    def indices(self) -> pd.DataFrame:
        if self.__indices is None:
            self.__indices = pd.read_csv(
                "indices.csv", sep=";", header=None, index_col=0
            )
            self.__indices.columns = ["vazio", "arquivo"]
            self.__indices.index = [
                i.strip() for i in list(self.__indices.index)
            ]
        self.__indices["arquivo"] = self.__indices.apply(
            lambda linha: linha["arquivo"].strip(), axis=1
        )
        return self.__indices

    def get_dger(self) -> DGer:
        if self.__dger is None:
            arq_dger = self.arquivos.dger
            if arq_dger is None:
                raise RuntimeError("Nome do dger não encontrado")
            caminho = pathlib.Path(self.__tmppath).joinpath(arq_dger)
            script = pathlib.Path(Settings().installdir).joinpath(
                Settings().encoding_script
            )
            asyncio.run(converte_codificacao(str(caminho), str(script)))
            Log.log().info(f"Lendo arquivo {arq_dger}")
            self.__dger = DGer.le_arquivo(self.__tmppath, arq_dger)
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

    def get_clast(self) -> ClasT:
        if self.__clast is None:
            Log.log().info(f"Lendo arquivo {self.arquivos.clast}")
            self.__clast = ClasT.le_arquivo(
                self.__tmppath, self.arquivos.clast
            )
        return self.__clast

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
            try:
                Log.log().info("Lendo arquivo newave.tim")
                self.__newavetim = NewaveTim.le_arquivo(
                    self.__tmppath, "newave.tim"
                )
            except Exception:
                Log.log().info("Arquivo newave.tim não encontrado")
        return self.__newavetim

    def get_eolicacadastro(self) -> EolicaCadastro:
        if self.__eolicacadastro is None:
            arq = self.indices.at[
                "PARQUE-EOLICO-EQUIVALENTE-CADASTRO", "arquivo"
            ]
            Log.log().info(f"Lendo arquivo {arq}")
            self.__eolicacadastro = EolicaCadastro.le_arquivo(
                self.__tmppath, arq
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
                    + f"{variable.value}_{spatial_resolution.value}"
                    + f"_{temporal_resolution.value}"
                )
                return None
            return regra(self.__tmppath, *args, **kwargs)
        except Exception:
            Log.log().warning(
                "Arquivo não encontrado para "
                + f"{variable.value}_{spatial_resolution.value}"
                + f"_{temporal_resolution.value}: {kwargs}"
            )
            return None

    def get_nwlistcf_cortes(self) -> Nwlistcf:
        if self.__nwlistcf is None:
            Log.log().info(f"Lendo arquivo nwlistcf.rel")
            try:
                self.__nwlistcf = Nwlistcf.le_arquivo(self.__tmppath)
            except Exception:
                Log.log().warning("Arquivo nwlistcf.rel não encontrado")
        return self.__nwlistcf

    def get_nwlistcf_estados(self) -> Estados:
        if self.__estados is None:
            Log.log().info(f"Lendo arquivo estados.rel")
            try:
                self.__estados = Estados.le_arquivo(self.__tmppath)
            except Exception:
                Log.log().warning("Arquivo estados.rel não encontrado")
        return self.__estados

    def _numero_estagios_individualizados(self) -> int:
        dger = self.get_dger()
        if dger.agregacao_simulacao_final == 1:
            return dger.num_anos_estudo * 12
        rees = self.get_ree().rees
        mes_fim_hib = rees["Mês Fim Individualizado"].iloc[0]
        ano_fim_hib = rees["Ano Fim Individualizado"].iloc[0]

        if not np.isnan(mes_fim_hib) and np.isnan(ano_fim_hib):
            data_inicio_estudo = datetime(
                year=dger.ano_inicio_estudo,
                month=dger.mes_inicio_estudo,
                day=1,
            )
            data_fim_individualizado = datetime(
                year=ano_fim_hib,
                month=mes_fim_hib,
                day=1,
            )
            tempo_individualizado = (
                data_fim_individualizado - data_inicio_estudo
            )
            return int(tempo_individualizado / timedelta(days=30))
        else:
            return 0

    def get_energiaf(self, iteracao: int) -> Energiaf:
        nome_arq = (
            f"energiaf{str(iteracao).zfill(3)}.dat"
            if iteracao != 1
            else "energiaf.dat"
        )
        if self.__energiaf.get(iteracao) is None:
            Log.log().info(f"Lendo arquivo {nome_arq}")
            try:
                dger = self.get_dger()
                n_rees = self.get_ree().rees.shape[0]
                n_estagios = dger.num_anos_estudo * 12
                n_estagios_th = (
                    12
                    if dger.consideracao_media_anual_afluencias == 3
                    else dger.ordem_maxima_parp
                )
                self.__energiaf[iteracao] = Energiaf.le_arquivo(
                    self.__tmppath,
                    nome_arq,
                    dger.num_forwards,
                    n_rees,
                    n_estagios,
                    n_estagios_th,
                )
            except Exception:
                Log.log().warning(f"Arquivo {nome_arq} não encontrado")
        return self.__energiaf.get(iteracao)

    def get_vazaof(self, iteracao: int) -> Vazaof:
        nome_arq = (
            f"vazaof{str(iteracao).zfill(3)}.dat"
            if iteracao != 1
            else "vazaof.dat"
        )
        if self.__vazaof.get(iteracao) is None:
            Log.log().info(f"Lendo arquivo {nome_arq}")
            try:
                dger = self.get_dger()
                n_uhes = self.get_confhd().usinas.shape[0]
                n_estagios = (
                    self._numero_estagios_individualizados()
                    + dger.mes_inicio_estudo
                    - 1
                )
                n_estagios_th = (
                    12
                    if dger.consideracao_media_anual_afluencias == 3
                    else dger.ordem_maxima_parp
                )
                self.__vazaof[iteracao] = Vazaof.le_arquivo(
                    self.__tmppath,
                    nome_arq,
                    dger.num_forwards,
                    n_uhes,
                    n_estagios,
                    n_estagios_th,
                )
            except Exception:
                Log.log().warning(f"Arquivo {nome_arq} não encontrado")
        return self.__vazaof.get(iteracao)

    def get_energiab(self) -> Energiab:
        if self.__energiab is None:
            Log.log().info("Lendo arquivo energiab.dat")
            try:
                dger = self.get_dger()
                n_rees = self.get_ree().rees.shape[0]
                n_estagios = dger.num_anos_estudo * 12
                self.__energiab = Energiab.le_arquivo(
                    self.__tmppath,
                    "energiab.dat",
                    dger.num_forwards,
                    dger.num_aberturas,
                    n_rees,
                    n_estagios,
                )
            except Exception:
                Log.log().warning("Arquivo energiab.dat não encontrado")
        return self.__energiab

    def get_vazaob(self) -> Vazaob:
        if self.__vazaob is None:
            Log.log().info("Lendo arquivo vazaob.dat")
            try:
                dger = self.get_dger()
                n_uhes = self.get_confhd().usinas.shape[0]
                n_estagios_hib = (
                    self._numero_estagios_individualizados()
                    + dger.mes_inicio_estudo
                    - 1
                )
                self.__vazaob = Vazaob.le_arquivo(
                    self.__tmppath,
                    "vazaob.dat",
                    dger.num_forwards,
                    dger.num_aberturas,
                    n_uhes,
                    n_estagios_hib,
                )
            except Exception:
                Log.log().warning("Arquivo vazaob.dat não encontrado")
        return self.__vazaob

    def get_enavazf(self, iteracao: int) -> Enavazf:
        nome_arq = (
            f"enavazf{str(iteracao).zfill(3)}.dat"
            if iteracao != 1
            else "enavazf.dat"
        )
        if self.__enavazf.get(iteracao) is None:
            Log.log().info(f"Lendo arquivo {nome_arq}")
            try:
                dger = self.get_dger()
                n_rees = self.get_ree().rees.shape[0]
                n_estagios = (
                    self._numero_estagios_individualizados()
                    + dger.mes_inicio_estudo
                    - 1
                )
                n_estagios_th = (
                    12
                    if dger.consideracao_media_anual_afluencias == 3
                    else dger.ordem_maxima_parp
                )
                self.__enavazf[iteracao] = Enavazf.le_arquivo(
                    self.__tmppath,
                    nome_arq,
                    dger.num_forwards,
                    n_rees,
                    n_estagios,
                    n_estagios_th,
                )
            except Exception:
                Log.log().warning(f"Arquivo {nome_arq} não encontrado")
        return self.__enavazf.get(iteracao)

    def get_enavazb(self) -> Enavazb:
        if self.__enavazb is None:
            Log.log().info("Lendo arquivo enavazb.dat")
            try:
                dger = self.get_dger()
                n_rees = self.get_ree().rees.shape[0]
                n_estagios = (
                    self._numero_estagios_individualizados()
                    + dger.mes_inicio_estudo
                    - 1
                )
                self.__enavazb = Enavazb.le_arquivo(
                    self.__tmppath,
                    "enavazb.dat",
                    dger.num_forwards,
                    dger.num_aberturas,
                    n_rees,
                    n_estagios,
                )
            except Exception:
                Log.log().warning("Arquivo enavazb.dat não encontrado")
        return self.__enavazb

    def get_energias(self) -> Energias:
        if self.__energias is None:
            Log.log().info(f"Lendo arquivo energias.dat")
            try:
                dger = self.get_dger()
                n_rees = self.get_ree().rees.shape[0]
                n_estagios = dger.num_anos_estudo * 12
                n_estagios_th = (
                    12
                    if dger.consideracao_media_anual_afluencias == 3
                    else dger.ordem_maxima_parp
                )
                if dger.tipo_simulacao_final == 1:
                    num_series = dger.num_series_sinteticas
                else:
                    num_series = (
                        dger.ano_inicio_estudo - dger.ano_inicial_historico - 1
                    )
                self.__energias = Energias.le_arquivo(
                    self.__tmppath,
                    "energias.dat",
                    num_series,
                    n_rees,
                    n_estagios,
                    n_estagios_th,
                )
            except Exception:
                Log.log().warning(f"Arquivo energias.dat não encontrado")
        return self.__energias

    def get_enavazs(self) -> Enavazf:
        if self.__enavazs is None:
            Log.log().info(f"Lendo arquivo enavazs.dat")
            try:
                dger = self.get_dger()
                n_rees = self.get_ree().rees.shape[0]
                n_estagios = (
                    self._numero_estagios_individualizados()
                    + dger.mes_inicio_estudo
                    - 1
                )
                n_estagios_th = (
                    12
                    if dger.consideracao_media_anual_afluencias == 3
                    else dger.ordem_maxima_parp
                )
                if dger.tipo_simulacao_final == 1:
                    num_series = dger.num_series_sinteticas
                else:
                    num_series = (
                        dger.ano_inicio_estudo - dger.ano_inicial_historico - 1
                    )
                self.__enavazs = Enavazf.le_arquivo(
                    self.__tmppath,
                    "enavazs.dat",
                    num_series,
                    n_rees,
                    n_estagios,
                    n_estagios_th,
                )
            except Exception:
                Log.log().warning("Arquivo enavazs.dat não encontrado")
        return self.__enavazs

    def get_vazaos(self) -> Vazaos:
        if self.__vazaos is None:
            Log.log().info(f"Lendo arquivo vazaos.dat")
            try:
                dger = self.get_dger()
                n_uhes = self.get_confhd().usinas.shape[0]
                n_estagios = (
                    self._numero_estagios_individualizados()
                    + dger.mes_inicio_estudo
                    - 1
                )
                n_estagios_th = (
                    12
                    if dger.consideracao_media_anual_afluencias == 3
                    else dger.ordem_maxima_parp
                )
                if dger.tipo_simulacao_final == 1:
                    num_series = dger.num_series_sinteticas
                else:
                    num_series = (
                        dger.ano_inicio_estudo - dger.ano_inicial_historico - 1
                    )
                self.__vazaos = Vazaos.le_arquivo(
                    self.__tmppath,
                    "vazaos.dat",
                    num_series,
                    n_uhes,
                    n_estagios,
                    n_estagios_th,
                )
            except Exception:
                Log.log().warning(f"Arquivo vazaos.dat não encontrado")
        return self.__vazaos


def factory(kind: str, *args, **kwargs) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind, RawFilesRepository)(*args, **kwargs)
