from __future__ import annotations

from os.path import join
from typing import TYPE_CHECKING, Callable, Dict, Tuple

from inewave.nwlistop.geol import Geol
from inewave.nwlistop.vento import Vento

from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable

if TYPE_CHECKING:
    from app.adapters.repository.files import RawFilesRepository


def get_rules(
    repo: "RawFilesRepository",
) -> Dict[Tuple[Variable, SpatialResolution], Callable]:
    """Return variable-to-reader mappings for wind-related variables."""
    return {
        (
            Variable.VELOCIDADE_VENTO,
            SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
        ): lambda dir, uee=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Vento, join(dir, f"vento{str(uee).zfill(3)}.out")
            )
        ),
        (
            Variable.GERACAO_EOLICA,
            SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
        ): lambda dir, uee=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Geol, join(dir, f"geol{str(uee).zfill(3)}.out")
            )
        ),
    }
