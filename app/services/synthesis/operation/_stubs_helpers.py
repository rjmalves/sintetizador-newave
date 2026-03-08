"""
Private helpers for stubs.py.

This module contains array-manipulation helpers, frozenset variable
sets used by the stub dispatcher, and calc_accumulated_productivity —
all moved out of stubs.py to keep that module within the 500-line limit.
"""

from typing import TYPE_CHECKING

import numpy as np
import polars as pl

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


def fill_initial_storage_df(
    df: pl.DataFrame,
    indices: np.ndarray,
    values: np.ndarray,
    entities: dict,
) -> pl.DataFrame:
    scenarios = [s for s in entities[SCENARIO_COL] if str(s).isnumeric()]
    num_scenarios = len(scenarios)
    result = df.clone()
    arr = result[VALUE_COL].to_numpy().copy()
    arr[num_scenarios:] = arr[:-num_scenarios]
    arr[indices] = np.repeat(values, num_scenarios)
    result = result.with_columns(pl.Series(VALUE_COL, arr).alias(VALUE_COL))
    result = result.with_columns(pl.col(VALUE_COL).fill_null(0.0))
    return result


def build_initial_stage_indices(
    entities: dict,
    num_groups: int,
) -> np.ndarray:
    scenarios = [s for s in entities[SCENARIO_COL] if str(s).isnumeric()]
    num_scenarios = len(scenarios)
    num_stages = len(entities[STAGE_COL])
    offsets = [i * num_scenarios * num_stages for i in range(num_groups)]
    indices = np.tile(np.arange(num_scenarios), num_groups)
    indices += np.repeat(offsets, num_scenarios)
    return indices


def two_cache_op(
    cls: "type[OperationSynthetizer]",
    synthesis,
    var1: Variable,
    var2: Variable,
    op: str = "add",
) -> pl.DataFrame:
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
    return a.with_columns(pl.Series(VALUE_COL, result).alias(VALUE_COL))


def calc_accumulated_productivity(
    cls: "type[OperationSynthetizer]",
    df: pl.DataFrame,
    entities: dict,
    uow,
) -> pl.DataFrame:
    hydro_df = Deck.hydros(uow)
    hydro_codes = entities[HYDRO_CODE_COL]
    filtered = hydro_df.filter(
        pl.col(HYDRO_CODE_COL).is_in(hydro_codes)
    ).select([HYDRO_CODE_COL, "codigo_usina_jusante"])
    np_edges = list(
        filtered.select(["codigo_usina_jusante", HYDRO_CODE_COL]).to_numpy()
    )
    edges = [tuple(e) for e in np_edges]
    hydro_nodes_bfs = Graph(edges, directed=True).bfs(0)[1:]

    # Build a lookup: hydro_code -> downstream_code
    downstream_map = dict(
        zip(
            filtered[HYDRO_CODE_COL].to_list(),
            filtered["codigo_usina_jusante"].to_list(),
        )
    )

    # Extract productivity arrays into a mutable dict keyed by hydro code
    # df is sorted by HYDRO_CODE_COL (guaranteed by stub_EARM_UHE)
    hydro_codes_in_df = df[HYDRO_CODE_COL].to_list()
    prod_array = df[PRODUCTIVITY_TMP_COL].to_numpy().copy()

    # Build mapping from hydro_code to row-slice in prod_array
    # Each hydro occupies a contiguous block of rows
    unique_codes, first_indices, counts = np.unique(
        hydro_codes_in_df, return_index=True, return_counts=True
    )
    code_to_slice: dict = {}
    for code, start, count in zip(unique_codes, first_indices, counts):
        code_to_slice[int(code)] = (int(start), int(start + count))

    for hydro_code in hydro_nodes_bfs:
        hydro_code_int = int(hydro_code)
        cls._log(f"Calculando prodt. acumulada para {hydro_code_int}...")
        downstream_code = downstream_map.get(hydro_code_int)
        if downstream_code is None or downstream_code == 0:
            continue
        downstream_code_int = int(downstream_code)
        if hydro_code_int not in code_to_slice:
            continue
        if downstream_code_int not in code_to_slice:
            continue
        hp_start, hp_end = code_to_slice[hydro_code_int]
        dp_start, dp_end = code_to_slice[downstream_code_int]
        hp_arr = prod_array[hp_start:hp_end]
        dp_arr = prod_array[dp_start:dp_end]
        if hp_arr.size > 0 and dp_arr.size > 0:
            prod_array[hp_start:hp_end] = hp_arr + dp_arr

    return df.with_columns(
        pl.Series(PRODUCTIVITY_TMP_COL, prod_array).alias(PRODUCTIVITY_TMP_COL)
    )


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
