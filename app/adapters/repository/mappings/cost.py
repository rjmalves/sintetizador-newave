from __future__ import annotations

from os.path import join
from typing import TYPE_CHECKING, Any, Callable, Dict, Tuple

from inewave.nwlistop.cdef import Cdef
from inewave.nwlistop.cdefsin import Cdefsin
from inewave.nwlistop.coper import Coper
from inewave.nwlistop.cterm import Cterm
from inewave.nwlistop.ctermsin import Ctermsin
from inewave.nwlistop.custo_futuro import CustoFuturo
from inewave.nwlistop.pivarm import Pivarm
from inewave.nwlistop.pivarmincr import Pivarmincr
from inewave.nwlistop.valor_agua import ValorAgua

from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable

if TYPE_CHECKING:
    from app.adapters.repository.files import RawFilesRepository


def get_rules(
    repo: "RawFilesRepository",
) -> Dict[Tuple[Variable, SpatialResolution], Callable[..., Any]]:
    """Return variable-to-reader mappings for cost-related variables."""
    return {
        (
            Variable.CUSTO_MARGINAL_OPERACAO,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._agg_cmo_dfs(dir, submercado),
        (
            Variable.CUSTO_GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Cterm,
                join(dir, f"cterm{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.CUSTO_GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Ctermsin, join(dir, "ctermsin.out")
            )
        ),
        (
            Variable.CUSTO_OPERACAO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(Coper, join(dir, "coper.out"))
        ),
        (
            Variable.CUSTO_FUTURO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                CustoFuturo, join(dir, "custo_futuro.out")
            )
        ),
        (
            Variable.CUSTO_DEFICIT,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Cdef,
                join(dir, f"cdef{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.CUSTO_DEFICIT,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Cdefsin, join(dir, "cdefsin.out")
            )
        ),
        (
            Variable.VALOR_AGUA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda dir, ree=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                ValorAgua,
                join(dir, f"valor_agua{str(ree).zfill(3)}.out"),
            )
        ),
        (
            Variable.VALOR_AGUA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Pivarm, join(dir, f"pivarm{str(uhe).zfill(3)}.out")
            )
        ),
        (
            Variable.VALOR_AGUA_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Pivarmincr,
                join(dir, f"pivarmincr{str(uhe).zfill(3)}.out"),
            )
        ),
    }
