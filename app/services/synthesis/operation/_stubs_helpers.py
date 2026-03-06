"""
Private helpers for stubs.py.

This module contains array-manipulation helpers, frozenset variable
sets used by the stub dispatcher, and calc_accumulated_productivity —
all moved out of stubs.py to keep that module within the 500-line limit.
"""

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from app.internal.constants import (
    HYDRO_CODE_COL,
    PRODUCTIVITY_TMP_COL,
    SCENARIO_COL,
    STAGE_COL,
    VALUE_COL,
)
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.deck import Deck
from app.utils.graph import Graph

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


# ---------------------------------------------------------------------------
# Array helpers for initial storage stubs
# ---------------------------------------------------------------------------


def fill_initial_storage_df(
    df: pd.DataFrame,
    indices: np.ndarray,
    values: np.ndarray,
    entities: dict,
) -> pd.DataFrame:
    """Fills first-stage rows of a storage DataFrame with initial values."""
    scenarios = [s for s in entities[SCENARIO_COL] if str(s).isnumeric()]
    num_scenarios = len(scenarios)
    result = df.copy()
    arr = result[VALUE_COL].to_numpy().copy()
    arr[num_scenarios:] = arr[:-num_scenarios]
    arr[indices] = np.repeat(values, num_scenarios)
    result[VALUE_COL] = arr
    result[VALUE_COL] = result[VALUE_COL].fillna(0.0)
    return result


def build_initial_stage_indices(
    entities: dict,
    num_groups: int,
) -> np.ndarray:
    """Builds the row-indices for the initial stage in a storage DataFrame."""
    scenarios = [s for s in entities[SCENARIO_COL] if str(s).isnumeric()]
    num_scenarios = len(scenarios)
    num_stages = len(entities[STAGE_COL])
    offsets = [i * num_scenarios * num_stages for i in range(num_groups)]
    indices = np.tile(np.arange(num_scenarios), num_groups)
    indices += np.repeat(offsets, num_scenarios)
    return indices


# ---------------------------------------------------------------------------
# Two-cache arithmetic helper
# ---------------------------------------------------------------------------


def two_cache_op(
    cls: "type[OperationSynthetizer]",
    synthesis,
    var1: Variable,
    var2: Variable,
    op: str = "add",
) -> pd.DataFrame:
    """Return a DataFrame whose VALUE_COL is var1 op var2 from cache."""
    from app.model.operation.operationsynthesis import OperationSynthesis

    a = cls._get_from_cache(
        OperationSynthesis(var1, synthesis.spatial_resolution)
    )
    b = cls._get_from_cache(
        OperationSynthesis(var2, synthesis.spatial_resolution)
    )
    if op == "sub":
        result = a[VALUE_COL].to_numpy() - b[VALUE_COL].to_numpy()
    else:
        result = a[VALUE_COL].to_numpy() + b[VALUE_COL].to_numpy()
    return a.assign(**{VALUE_COL: result})


# ---------------------------------------------------------------------------
# Productivity accumulation
# ---------------------------------------------------------------------------


def calc_accumulated_productivity(
    cls: "type[OperationSynthetizer]",
    df: pd.DataFrame,
    entities: dict,
    uow,
) -> pd.DataFrame:
    hydro_df = Deck.hydros(uow).reset_index()
    hydro_codes = entities[HYDRO_CODE_COL]
    np_edges = list(
        hydro_df.loc[
            hydro_df[HYDRO_CODE_COL].isin(hydro_codes),
            ["codigo_usina_jusante", HYDRO_CODE_COL],
        ].to_numpy()
    )
    edges = [tuple(e) for e in np_edges]
    hydro_nodes_bfs = Graph(edges, directed=True).bfs(0)[1:]
    for hydro_code in hydro_nodes_bfs:
        hydro_name = hydro_df.loc[
            hydro_df[HYDRO_CODE_COL] == hydro_code, HYDRO_CODE_COL
        ].iloc[0]
        cls._log(f"Calculando prodt. acumulada para {hydro_name}...")
        downstream_code = hydro_df.loc[
            hydro_df[HYDRO_CODE_COL] == hydro_code, "codigo_usina_jusante"
        ].iloc[0]
        if downstream_code == 0:
            continue
        downstream_name = hydro_df.loc[
            hydro_df[HYDRO_CODE_COL] == downstream_code, HYDRO_CODE_COL
        ].iloc[0]
        hp = df.loc[df[HYDRO_CODE_COL] == hydro_name, PRODUCTIVITY_TMP_COL]
        dp = df.loc[
            df[HYDRO_CODE_COL] == downstream_name, PRODUCTIVITY_TMP_COL
        ].to_numpy()
        if not hp.empty and len(dp) > 0:
            hp += dp
    return df


# ---------------------------------------------------------------------------
# Variable-set constants used by stub_mappings()
# ---------------------------------------------------------------------------

# Variables that aggregate from UHE resolution (non-UHE spatial resolutions)
HYDRO_RESOLUTION_VARS = frozenset(
    [
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        Variable.VOLUME_AFLUENTE,
        Variable.VOLUME_INCREMENTAL,
        Variable.VOLUME_DEFLUENTE,
        Variable.VOLUME_VERTIDO,
        Variable.VOLUME_TURBINADO,
        Variable.VOLUME_RETIRADO,
        Variable.VOLUME_DESVIADO,
        Variable.VOLUME_EVAPORADO,
        Variable.VIOLACAO_EVAPORACAO,
        Variable.VIOLACAO_FPHA,
        Variable.VIOLACAO_POSITIVA_EVAPORACAO,
        Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
    ]
)

# Flow variables mapped to their volume equivalents at UHE level
FLOW_VOLUME_VARS = frozenset(
    [
        Variable.VAZAO_AFLUENTE,
        Variable.VAZAO_INCREMENTAL,
        Variable.VAZAO_DEFLUENTE,
        Variable.VAZAO_VERTIDA,
        Variable.VAZAO_TURBINADA,
        Variable.VAZAO_RETIRADA,
        Variable.VAZAO_DESVIADA,
        Variable.VAZAO_EVAPORADA,
    ]
)

# Volume variables converted from flow at UHE level
FLOW_TO_VOLUME_VARS = frozenset(
    [
        Variable.VOLUME_AFLUENTE,
        Variable.VOLUME_INCREMENTAL,
        Variable.VOLUME_TURBINADO,
        Variable.VOLUME_VERTIDO,
        Variable.VOLUME_DESVIADO,
    ]
)

# Initial stored energy variables resolved for non-UHE spatial resolutions
EARM_INITIAL_VARS = frozenset(
    [
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
    ]
)

# Stored energy variables resolved at UHE level via volume conversion
EARM_UHE_VARS = frozenset(
    [
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
    ]
)

# Initial stored volume variables resolved at UHE level
VARM_INITIAL_VARS = frozenset(
    [
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
    ]
)

# Spatial resolutions for which initial stored energy stubs apply
EARM_INITIAL_SPATIAL = frozenset(
    [
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        SpatialResolution.SUBMERCADO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ]
)

# Percentual volume variables mapped to absolute at non-UHE resolutions
PERCENT_VOLUME_VARS = frozenset(
    [
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
    ]
)
