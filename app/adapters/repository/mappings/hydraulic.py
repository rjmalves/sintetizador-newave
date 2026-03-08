from __future__ import annotations

from os.path import join
from typing import TYPE_CHECKING, Any, Callable, Dict, Tuple

from inewave.nwlistop.hjus import Hjus
from inewave.nwlistop.hliq import Hliq
from inewave.nwlistop.hmont import Hmont
from inewave.nwlistop.mercl import Mercl
from inewave.nwlistop.merclsin import Merclsin
from inewave.nwlistop.mevmin import Mevmin
from inewave.nwlistop.mevminm import Mevminm
from inewave.nwlistop.mevminsin import Mevminsin
from inewave.nwlistop.viol_evmin import ViolEvmin
from inewave.nwlistop.viol_evminm import ViolEvminm
from inewave.nwlistop.viol_evminsin import ViolEvminsin
from inewave.nwlistop.viol_ghmin import ViolGhmin
from inewave.nwlistop.viol_ghminm import ViolGhminm
from inewave.nwlistop.viol_ghminsin import ViolGhminsin
from inewave.nwlistop.viol_ghminuh import ViolGhminuh

from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable

if TYPE_CHECKING:
    from app.adapters.repository.files import RawFilesRepository


def get_rules(
    repo: "RawFilesRepository",
) -> Dict[Tuple[Variable, SpatialResolution], Callable[..., Any]]:
    """Return variable-to-reader mappings for hydraulic-related variables."""
    return {
        (
            Variable.COTA_MONTANTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Hmont, join(dir, f"hmont{str(uhe).zfill(3)}.out")
            )
        ),
        (
            Variable.COTA_JUSANTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Hjus, join(dir, f"hjus{str(uhe).zfill(3)}.out")
            )
        ),
        (
            Variable.QUEDA_LIQUIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Hliq, join(dir, f"hliq{str(uhe).zfill(3)}.out")
            )
        ),
        (
            Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Mevmin, join(dir, f"mevmin{str(ree).zfill(3)}.out")
            )
        ),
        (
            Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Mevminm,
                join(dir, f"mevminm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Mevminsin, join(dir, "mevminsin.out")
            )
        ),
        (
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                ViolEvmin,
                join(dir, f"viol_evmin{str(ree).zfill(3)}.out"),
            )
        ),
        (
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                ViolEvminm,
                join(dir, f"vevminm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                ViolEvminsin, join(dir, "viol_evminsin.out")
            )
        ),
        (
            Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._eval_block_0_sum(
            repo._read_nwlistop_setting_version(
                ViolGhminuh,
                join(dir, f"viol_ghminuh{str(uhe).zfill(3)}.out"),
            )
        ),
        (
            Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                ViolGhmin,
                join(dir, f"viol_ghmin{str(ree).zfill(3)}.out"),
            )
        ),
        (
            Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                # TODO - atualizar o nome do arquivo quando for alterado
                ViolGhminm,
                join(dir, f"vghminm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                ViolGhminsin, join(dir, "viol_ghminsin.out")
            )
        ),
        (
            Variable.MERCADO_LIQUIDO,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Mercl,
                join(dir, f"mercl{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.MERCADO_LIQUIDO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Merclsin, join(dir, "merclsin.out")
            )
        ),
    }
