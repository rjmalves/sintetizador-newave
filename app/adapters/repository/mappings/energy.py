from __future__ import annotations

from os.path import join
from typing import TYPE_CHECKING, Any, Callable, Dict, Tuple

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
from inewave.nwlistop.perdf import Perdf
from inewave.nwlistop.perdfm import Perdfm
from inewave.nwlistop.perdfsin import Perdfsin
from inewave.nwlistop.verturb import Verturb
from inewave.nwlistop.verturbm import Verturbm
from inewave.nwlistop.verturbsin import Verturbsin
from inewave.nwlistop.vmort import Vmort
from inewave.nwlistop.vmortm import Vmortm
from inewave.nwlistop.vmortsin import Vmortsin

from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable

if TYPE_CHECKING:
    from app.adapters.repository.files import RawFilesRepository


def get_rules(
    repo: "RawFilesRepository",
) -> Dict[Tuple[Variable, SpatialResolution], Callable[..., Any]]:
    """Return variable-to-reader mappings for energy-related variables."""
    return {
        (
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Eafb, join(dir, f"eafb{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Eafbm, join(dir, f"eafbm{str(submercado).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Eafbsin, join(dir, "eafbsin.out")
            )
        ),
        (
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Eaf, join(dir, f"eaf{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Eafm, join(dir, f"eafm{str(submercado).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
            # TODO - substituir quando existir na inewave
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Eafbsin, join(dir, "eafmsin.out")
            )
        ),
        (
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            # TODO - substituir quando existir na inewave
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Eaf, join(dir, f"efdf{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
            SpatialResolution.SUBMERCADO,
            # TODO - substituir quando existir na inewave
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Eafm, join(dir, f"efdfm{str(submercado).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
            # TODO - substituir quando existir na inewave
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Eafbsin, join(dir, "efdfsin.out")
            )
        ),
        (
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Earmfp, join(dir, f"earmfp{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Earmfpm,
                join(dir, f"earmfpm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Earmfpsin, join(dir, "earmfpsin.out")
            )
        ),
        (
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Earmf, join(dir, f"earmf{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Earmfm,
                join(dir, f"earmfm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Earmfsin, join(dir, "earmfsin.out")
            )
        ),
        (
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Evert, join(dir, f"evert{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Evertm,
                join(dir, f"evertm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Evertsin, join(dir, "evertsin.out")
            )
        ),
        (
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Perdf, join(dir, f"perdf{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Perdfm,
                join(dir, f"perdfm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Perdfsin, join(dir, "perdfsin.out")
            )
        ),
        (
            Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Verturb, join(dir, f"verturb{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Verturbm,
                join(dir, f"verturbm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Verturbsin, join(dir, "verturbsin.out")
            )
        ),
        (
            Variable.ENERGIA_DESVIO_RESERVATORIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Edesvc, join(dir, f"edesvc{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_DESVIO_RESERVATORIO,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Edesvcm,
                join(dir, f"edesvcm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.ENERGIA_DESVIO_RESERVATORIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Edesvcsin, join(dir, "edesvcsin.out")
            )
        ),
        (
            # TODO - substituir quando existir na inewave
            Variable.ENERGIA_DESVIO_FIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Edesvc, join(dir, f"edesvf{str(ree).zfill(3)}.out")
            )
        ),
        (
            # TODO - substituir quando existir na inewave
            Variable.ENERGIA_DESVIO_FIO,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Edesvcm,
                join(dir, f"edesvfm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            # TODO - substituir quando existir na inewave
            Variable.ENERGIA_DESVIO_FIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Edesvcsin, join(dir, "edesvfsin.out")
            )
        ),
        (
            Variable.ENERGIA_EVAPORACAO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Evapo, join(dir, f"evapo{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_EVAPORACAO,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Evapom,
                join(dir, f"evapom{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.ENERGIA_EVAPORACAO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Evaporsin, join(dir, "evaporsin.out")
            )
        ),
        (
            Variable.ENERGIA_VOLUME_MORTO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Vmort, join(dir, f"vmort{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.ENERGIA_VOLUME_MORTO,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Vmortm,
                join(dir, f"vmortm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.ENERGIA_VOLUME_MORTO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Vmortsin, join(dir, "vmortsin.out")
            )
        ),
    }
