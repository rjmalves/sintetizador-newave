from __future__ import annotations

from os.path import join
from typing import TYPE_CHECKING, Any, Callable, Dict, Tuple

from inewave.nwlistop.deficit import Def
from inewave.nwlistop.exces import Exces
from inewave.nwlistop.excessin import Excessin
from inewave.nwlistop.intercambio import Intercambio

from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable

if TYPE_CHECKING:
    from app.adapters.repository.files import RawFilesRepository


def get_rules(
    repo: "RawFilesRepository",
) -> Dict[Tuple[Variable, SpatialResolution], Callable[..., Any]]:
    """Return variable-to-reader mappings for exchange-related variables."""
    return {
        (
            Variable.INTERCAMBIO,
            SpatialResolution.PAR_SUBMERCADOS,
        ): lambda dir, submercados=(1, 2): repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Intercambio,
                join(
                    dir,
                    f"int{str(submercados[0]).zfill(3)}"
                    + f"{str(submercados[1]).zfill(3)}.out",
                ),
            )
        ),
        (
            Variable.EXCESSO,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Exces,
                join(dir, f"exces{str(submercado).zfill(3)}.out"),
            )
        ),
        (
            Variable.EXCESSO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Excessin, join(dir, "excessin.out")
            )
        ),
        (
            Variable.DEFICIT,
            SpatialResolution.SUBMERCADO,
        ): lambda dir, submercado=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Def,
                join(dir, f"def{str(submercado).zfill(3)}p001.out"),
            )
        ),
        (
            Variable.DEFICIT,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda dir, _: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Def, join(dir, "defsinp001.out")
            )
        ),
    }
