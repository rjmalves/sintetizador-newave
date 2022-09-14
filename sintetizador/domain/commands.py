import pandas as pd  # type: ignore
from typing import Optional
from dataclasses import dataclass
from sintetizador.model.variable import Variable
from sintetizador.model.spatialresolution import SpatialResolution
from sintetizador.model.temporalresolution import TemporalResolution


@dataclass
class FormatNwlistopDataframe:
    df: Optional[pd.DataFrame]
    temporalresolution: TemporalResolution


@dataclass
class FormatNwlistopSeriesDataframe:
    df: pd.DataFrame


@dataclass
class FormatNwlistopPatamarDataframe:
    df: pd.DataFrame


@dataclass
class ProcessSINData:
    variable: Variable
    spatialresolution: SpatialResolution
    temporalresolution: TemporalResolution


@dataclass
class ProcessSubmercadoData:
    variable: Variable
    spatialresolution: SpatialResolution
    temporalresolution: TemporalResolution


@dataclass
class ProcessREEData:
    variable: Variable
    spatialresolution: SpatialResolution
    temporalresolution: TemporalResolution


@dataclass
class ProcessUHEData:
    variable: Variable
    spatialresolution: SpatialResolution
    temporalresolution: TemporalResolution


@dataclass
class ProcessUTEData:
    variable: Variable
    spatialresolution: SpatialResolution
    temporalresolution: TemporalResolution


@dataclass
class ProcessUEEData:
    variable: Variable
    spatialresolution: SpatialResolution
    temporalresolution: TemporalResolution


@dataclass
class SynthetizeNwlistop:
    variable: Variable
    spatialresolution: SpatialResolution
    temporalresolution: TemporalResolution
