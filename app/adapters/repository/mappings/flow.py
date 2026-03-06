from __future__ import annotations

from os.path import join
from typing import TYPE_CHECKING, Callable, Dict, Tuple

from inewave.nwlistop.qafluh import Qafluh
from inewave.nwlistop.qdesviouh import Qdesviouh
from inewave.nwlistop.qincruh import Qincruh
from inewave.nwlistop.qturuh import Qturuh
from inewave.nwlistop.qvertuh import Qvertuh
from inewave.nwlistop.varmpuh import Varmpuh
from inewave.nwlistop.varmuh import Varmuh
from inewave.nwlistop.vevapuh import Vevapuh
from inewave.nwlistop.viol_fpha import ViolFpha
from inewave.nwlistop.viol_neg_evap import ViolNegEvap
from inewave.nwlistop.viol_pos_evap import ViolPosEvap
from inewave.nwlistop.vretiradauh import Vretiradauh

from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable

if TYPE_CHECKING:
    from app.adapters.repository.files import RawFilesRepository


def get_rules(
    repo: "RawFilesRepository",
) -> Dict[Tuple[Variable, SpatialResolution], Callable]:
    """Return variable-to-reader mappings for flow/volume-related variables."""
    return {
        (
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Qafluh, join(dir, f"qafluh{str(uhe).zfill(3)}.out")
            )
        ),
        (
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Qincruh, join(dir, f"qincruh{str(uhe).zfill(3)}.out")
            )
        ),
        (
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Qturuh, join(dir, f"qturuh{str(uhe).zfill(3)}.out")
            )
        ),
        (
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Qvertuh, join(dir, f"qvertuh{str(uhe).zfill(3)}.out")
            )
        ),
        (
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._replace_block_column(
            repo._read_nwlistop_setting_version(
                Qdesviouh,
                join(dir, f"qdesviouh{str(uhe).zfill(3)}.out"),
            )
        ),
        (
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Varmuh, join(dir, f"varmuh{str(uhe).zfill(3)}.out")
            )
        ),
        (
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Varmpuh, join(dir, f"varmpuh{str(uhe).zfill(3)}.out")
            )
        ),
        (
            Variable.VOLUME_RETIRADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Vretiradauh,
                join(dir, f"vretiradauh{str(uhe).zfill(3)}.out"),
            )
        ),
        (
            Variable.VOLUME_EVAPORADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                Vevapuh, join(dir, f"vevapuh{str(uhe).zfill(3)}.out")
            )
        ),
        (
            Variable.VIOLACAO_FPHA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._eval_block_0_sum(
            repo._read_nwlistop_setting_version(
                ViolFpha,
                join(dir, f"viol_fpha{str(uhe).zfill(3)}.out"),
            )
        ),
        (
            Variable.VIOLACAO_POSITIVA_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                ViolPosEvap,
                join(dir, f"viol_pos_evap{str(uhe).zfill(3)}.out"),
            )
        ),
        (
            Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda dir, uhe=1: repo._add_block_column(
            repo._read_nwlistop_setting_version(
                ViolNegEvap,
                join(dir, f"viol_neg_evap{str(uhe).zfill(3)}.out"),
            ).fillna(0.0)  # type: ignore
        ),
    }
