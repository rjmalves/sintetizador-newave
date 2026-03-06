from __future__ import annotations

from os.path import join
from typing import TYPE_CHECKING, Callable, Dict, Tuple

from inewave.nwlistop.corteolm import Corteolm
from inewave.nwlistop.evert import Evert
from inewave.nwlistop.evertm import Evertm
from inewave.nwlistop.evertsin import Evertsin
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

from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable

if TYPE_CHECKING:
    from app.adapters.repository.files import RawFilesRepository


def get_rules(
    repo: "RawFilesRepository",
) -> Dict[Tuple[Variable, SpatialResolution], Callable]:
    """Return variable-to-reader mappings for generation-related variables."""
    return {
        (
            Variable.GERACAO_HIDRAULICA_RESERVATORIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Ghidr, join(dir, f"ghidr{str(ree).zfill(3)}.out")
            ),
        ),
        (
            Variable.GERACAO_HIDRAULICA_RESERVATORIO,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Ghidrm,
                join(dir, f"ghidrm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.GERACAO_HIDRAULICA_RESERVATORIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Ghidrsin, join(dir, "ghidrsin.out")
            )
        ),
        (
            Variable.GERACAO_HIDRAULICA_FIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            # TODO - Substituir quando existir na inewave
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Evert, join(dir, f"gfiol{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.GERACAO_HIDRAULICA_FIO,
            SpatialResolution.SUBMERCADO,
            # TODO - Substituir quando existir na inewave
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Evertm,
                join(dir, f"gfiolm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.GERACAO_HIDRAULICA_FIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
            # TODO - Substituir quando existir na inewave
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Evertsin, join(dir, "gfiolsin.out")
            )
        ),
        (
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Ghtot, join(dir, f"ghtot{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Ghtotm,
                join(dir, f"ghtotm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Ghtotsin, join(dir, "ghtotsin.out")
            )
        ),
        (
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._eval_block_0_sum(
            repo._read_nwlistop_setting_version(
                Ghiduh, join(dir, f"ghiduh{str(uhe).zfill(3)}.out")
            )
        ),
        (
            Variable.GERACAO_TERMICA,
            SpatialResolution.USINA_TERMELETRICA,
        ): lambda dir, submercado=1: repo._eval_block_0_sum_gter_ute(
            repo._read_nwlistop_setting_version(
                Gtert, join(dir, f"gtert{str(submercado).zfill(3)}.out")
            )
        ),
        (
            Variable.GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Gttot, join(dir, f"gttot{str(submercado).zfill(3)}.out")
            )
        ),
        (
            Variable.GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Gttotsin, join(dir, "gttotsin.out")
            )
        ),
        (
            Variable.GERACAO_EOLICA,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Geolm,
                join(dir, f"geolm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.GERACAO_EOLICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Geolsin, join(dir, "geolsin.out")
            )
        ),
        (
            Variable.CORTE_GERACAO_EOLICA,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Corteolm,
                join(dir, f"corteolm{str(submercado).zfill(3)}.out"),
            )
        ),
    }
