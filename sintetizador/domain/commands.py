from dataclasses import dataclass
from sintetizador.model.variable import Variable
from sintetizador.model.spatialresolution import SpatialResolution
from sintetizador.model.temporalresolution import TemporalResolution


@dataclass
class SynthetizeNwlistop:
    variable: Variable
    spatialresolution: SpatialResolution
    temporalresolution: TemporalResolution
