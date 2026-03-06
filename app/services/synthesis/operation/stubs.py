import functools
from typing import TYPE_CHECKING, Callable, Optional, Tuple

import numpy as np
import pandas as pd

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    EER_CODE_COL,
    GROUPING_TMP_COL,
    HM3_M3S_MONTHLY_FACTOR,
    HYDRO_CODE_COL,
    LOWER_BOUND_COL,
    PRODUCTIVITY_TMP_COL,
    STAGE_COL,
    STAGE_DURATION_HOURS,
    SUBMARKET_CODE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.deck import Deck
from app.services.synthesis.operation._stubs_helpers import (
    EARM_INITIAL_SPATIAL,
    EARM_INITIAL_VARS,
    EARM_UHE_VARS,
    FLOW_TO_VOLUME_VARS,
    FLOW_VOLUME_VARS,
    HYDRO_RESOLUTION_VARS,
    PERCENT_VOLUME_VARS,
    VARM_INITIAL_VARS,
    build_initial_stage_indices,
    calc_accumulated_productivity,
    fill_initial_storage_df,
    two_cache_op,
)
from app.services.synthesis.operation._stubs_market import (
    resolve_SBM_entity_MER_MERL,
    stub_GUNS,
    stub_MER_MERL,
)
from app.services.synthesis.operation.pipeline import (
    initial_stored_energy_df,
    post_resolve,
)
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.dataframe import pd_to_pl
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )

# Re-export resolve_SBM_entity_MER_MERL so orchestrator can reference it
# without importing from _stubs_market directly.
__all__ = [
    "calc_accumulated_productivity",
    "resolve_SBM_entity_MER_MERL",
    "resolve_stub",
    "stub_mappings",
]


# ---------------------------------------------------------------------------
# Simple stub functions (cache arithmetic)
# ---------------------------------------------------------------------------


def stub_QDEF(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Discharge flow = turbined + spilled."""
    return two_cache_op(
        cls, synthesis, Variable.VAZAO_TURBINADA, Variable.VAZAO_VERTIDA
    )


def stub_VDEF(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Discharge volume = turbined + spilled."""
    return two_cache_op(
        cls, synthesis, Variable.VOLUME_TURBINADO, Variable.VOLUME_VERTIDO
    )


def stub_VEVAP(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Evaporation violation = positive + negative."""
    return two_cache_op(
        cls,
        synthesis,
        Variable.VIOLACAO_POSITIVA_EVAPORACAO,
        Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
    )


def stub_CTO(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Total cost = operation + future cost."""
    return two_cache_op(
        cls, synthesis, Variable.CUSTO_OPERACAO, Variable.CUSTO_FUTURO
    )


def stub_EVER(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Spilled energy = reservoir + run-of-river."""
    return two_cache_op(
        cls,
        synthesis,
        Variable.ENERGIA_VERTIDA_RESERV,
        Variable.ENERGIA_VERTIDA_FIO,
    )


def stub_EVMIN(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Min outflow energy = target - violation."""
    return two_cache_op(
        cls,
        synthesis,
        Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
        Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
        op="sub",
    )


# ---------------------------------------------------------------------------
# Variable-mapping stubs
# ---------------------------------------------------------------------------


def hydro_resolution_variable_map(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Map synthesis to UHE aggregation."""
    return cls._get_from_cache(
        OperationSynthesis(
            variable=synthesis.variable,
            spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
        )
    )


def flow_volume_hydro_variable_map(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Map flow variable to volume equivalent at UHE level."""
    variable_map = {
        Variable.VAZAO_AFLUENTE: Variable.VOLUME_AFLUENTE,
        Variable.VAZAO_INCREMENTAL: Variable.VOLUME_INCREMENTAL,
        Variable.VAZAO_DEFLUENTE: Variable.VOLUME_DEFLUENTE,
        Variable.VAZAO_VERTIDA: Variable.VOLUME_VERTIDO,
        Variable.VAZAO_TURBINADA: Variable.VOLUME_TURBINADO,
        Variable.VAZAO_RETIRADA: Variable.VOLUME_RETIRADO,
        Variable.VAZAO_DESVIADA: Variable.VOLUME_DESVIADO,
    }
    return cls._get_from_cache(
        OperationSynthesis(
            variable=variable_map[synthesis.variable],
            spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
        )
    )


def absolute_percent_volume_variable_map(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Map percent volume to absolute volume at UHE level."""
    variable_map = {
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL: Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL: Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
    }
    return cls._get_from_cache(
        OperationSynthesis(
            variable=variable_map[synthesis.variable],
            spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
        )
    )


def convert_volume_to_flow(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Convert synthesis from volume to flow."""
    df = cls._get_from_cache(
        OperationSynthesis(
            Variable.VOLUME_RETIRADO, synthesis.spatial_resolution
        )
    )
    return df.assign(
        **{
            VALUE_COL: df[VALUE_COL]
            * HM3_M3S_MONTHLY_FACTOR
            * STAGE_DURATION_HOURS
            / df[BLOCK_DURATION_COL]
        }
    )


def convert_flow_to_volume(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Convert synthesis from flow to volume."""
    variable_map = {
        Variable.VOLUME_AFLUENTE: Variable.VAZAO_AFLUENTE,
        Variable.VOLUME_INCREMENTAL: Variable.VAZAO_INCREMENTAL,
        Variable.VOLUME_TURBINADO: Variable.VAZAO_TURBINADA,
        Variable.VOLUME_VERTIDO: Variable.VAZAO_VERTIDA,
        Variable.VOLUME_DESVIADO: Variable.VAZAO_DESVIADA,
    }
    df = cls._get_from_cache(
        OperationSynthesis(
            variable_map[synthesis.variable], synthesis.spatial_resolution
        )
    )
    return df.assign(
        **{
            VALUE_COL: df[VALUE_COL]
            * df[BLOCK_DURATION_COL]
            / (HM3_M3S_MONTHLY_FACTOR * STAGE_DURATION_HOURS)
        }
    )


# ---------------------------------------------------------------------------
# Initial stored energy / volume stubs
# ---------------------------------------------------------------------------


def stub_resolve_initial_stored_energy(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Resolve initial stored energy synthesis for REE/SBM/SIN."""
    earmi = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL
    earmf = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL
    earpi = Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL
    earpf = Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL
    variable_map = {earmi: earmf, earpi: earpf}
    grouping_col_map = {
        SpatialResolution.RESERVATORIO_EQUIVALENTE: EER_CODE_COL,
        SpatialResolution.SUBMERCADO: SUBMARKET_CODE_COL,
        SpatialResolution.SISTEMA_INTERLIGADO: None,
    }
    final_synthesis = OperationSynthesis(
        variable=variable_map[synthesis.variable],
        spatial_resolution=synthesis.spatial_resolution,
    )
    final_df = cls._get_from_cache(final_synthesis)
    entities = cls._get_ordered_entities(final_synthesis)
    init_data = initial_stored_energy_df(cls, synthesis, uow)
    value_column = (
        "valor_MWmes" if synthesis.variable == earmi else "valor_percentual"
    )
    groups = entities.get(grouping_col_map[synthesis.spatial_resolution]) or [1]
    if synthesis.spatial_resolution != SpatialResolution.SISTEMA_INTERLIGADO:
        groups = [g for g in groups if g in init_data[GROUPING_TMP_COL]]
    init_values = (
        init_data.set_index(GROUPING_TMP_COL)
        .loc[groups, value_column]
        .to_numpy()
    )
    indices = build_initial_stage_indices(entities, len(groups))
    return fill_initial_storage_df(final_df, indices, init_values, entities)


def stub_resolve_initial_stored_volumes(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Resolve initial stored volume synthesis for UHE."""
    varmi = Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL
    varmf = Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL
    varpi = Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL
    varpf = Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL
    variable_map = {varmi: varmf, varpi: varpf}
    final_synthesis = OperationSynthesis(
        variable=variable_map[synthesis.variable],
        spatial_resolution=synthesis.spatial_resolution,
    )
    final_df = cls._get_from_cache(final_synthesis)
    entities = cls._get_ordered_entities(final_synthesis)
    hydros = entities[HYDRO_CODE_COL]
    num_hydros = len(hydros)
    initial_data = Deck.initial_stored_volume(uow)
    value_column = (
        "valor_hm3" if synthesis.variable == varmi else "valor_percentual"
    )
    if synthesis.variable == varmi:
        hidr = Deck.hidr(uow)
        initial_data[value_column] += hidr.loc[
            initial_data[HYDRO_CODE_COL].to_numpy(), "volume_minimo"
        ].to_numpy()
    initial_data = initial_data.loc[initial_data[HYDRO_CODE_COL].isin(hydros)]
    init_values = (
        initial_data.set_index(HYDRO_CODE_COL)
        .loc[hydros, value_column]
        .to_numpy()
    )
    indices = build_initial_stage_indices(entities, num_hydros)
    return fill_initial_storage_df(final_df, indices, init_values, entities)


# ---------------------------------------------------------------------------
# EARM UHE stub
# ---------------------------------------------------------------------------


def stub_EARM_UHE(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Compute stored energy per UHE from volumes and drops."""
    with time_and_log(
        message_root="Tempo para conversao do VARM em EARM", logger=cls.logger
    ):
        earmi = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL
        earmf = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL
        varmi = Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL
        varmf = Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL
        energy_volume_map = {earmi: varmi, earmf: varmf}
        net_drop_synthesis = OperationSynthesis(
            Variable.QUEDA_LIQUIDA, synthesis.spatial_resolution
        )
        vol_synthesis = OperationSynthesis(
            energy_volume_map[synthesis.variable], synthesis.spatial_resolution
        )
        vol_df = cls._get_from_cache(vol_synthesis).copy()
        vol_entities = cls._get_ordered_entities(vol_synthesis)
        net_df = cls._get_from_cache(net_drop_synthesis).copy()
        net_entities = cls._get_ordered_entities(net_drop_synthesis)
        hidr = Deck.hidr(uow)
        hydro_codes = net_entities[HYDRO_CODE_COL]
        n_entries = net_df.loc[net_df[HYDRO_CODE_COL] == hydro_codes[0]].shape[
            0
        ]
        specific_prod = (
            np.repeat(
                hidr.loc[hydro_codes, "produtibilidade_especifica"].to_numpy(),
                n_entries,
            )
            * HM3_M3S_MONTHLY_FACTOR
        )
        net_df[PRODUCTIVITY_TMP_COL] = specific_prod * net_df[VALUE_COL]
        net_df = calc_accumulated_productivity(cls, net_df, net_entities, uow)
    hydro_blocks = vol_entities[BLOCK_COL]
    net_df = net_df.loc[
        net_df[HYDRO_CODE_COL].isin(vol_entities[HYDRO_CODE_COL])
        & net_df[BLOCK_COL].isin(hydro_blocks)
    ].copy()
    net_df = net_df.sort_values([HYDRO_CODE_COL, STAGE_COL, BLOCK_COL])
    vol_df = vol_df.sort_values([HYDRO_CODE_COL, STAGE_COL, BLOCK_COL])
    vol_df[VALUE_COL] = (vol_df[VALUE_COL] - vol_df[LOWER_BOUND_COL]) * net_df[
        PRODUCTIVITY_TMP_COL
    ].to_numpy()
    vol_df[LOWER_BOUND_COL] = 0.0
    vol_df[UPPER_BOUND_COL] = (
        vol_df[UPPER_BOUND_COL] - vol_df[LOWER_BOUND_COL]
    ) * net_df[PRODUCTIVITY_TMP_COL].to_numpy()
    return vol_df


# ---------------------------------------------------------------------------
# Stub dispatcher
# ---------------------------------------------------------------------------


def stub_mappings(
    cls: "type[OperationSynthetizer]", s: OperationSynthesis
) -> Optional[Callable]:
    """Get stub resolver for non-standard synthesis variables."""
    p = functools.partial
    v, sr = s.variable, s.spatial_resolution
    uhe = SpatialResolution.USINA_HIDROELETRICA

    if v == Variable.CUSTO_TOTAL:
        return p(stub_CTO, cls)
    if v == Variable.ENERGIA_VERTIDA:
        return p(stub_EVER, cls)
    if v in EARM_INITIAL_VARS and sr in EARM_INITIAL_SPATIAL:
        return p(stub_resolve_initial_stored_energy, cls)
    if v in VARM_INITIAL_VARS and sr == uhe:
        return p(stub_resolve_initial_stored_volumes, cls)
    if v in HYDRO_RESOLUTION_VARS and sr != uhe:
        return p(hydro_resolution_variable_map, cls)
    if v in FLOW_VOLUME_VARS and sr != uhe:
        return p(flow_volume_hydro_variable_map, cls)
    if v in PERCENT_VOLUME_VARS and sr != uhe:
        return p(absolute_percent_volume_variable_map, cls)
    if v == Variable.ENERGIA_DEFLUENCIA_MINIMA:
        return p(stub_EVMIN, cls)
    if v == Variable.VAZAO_RETIRADA and sr == uhe:
        return p(convert_volume_to_flow, cls)
    if v in FLOW_TO_VOLUME_VARS and sr == uhe:
        return p(convert_flow_to_volume, cls)
    if v == Variable.VAZAO_DEFLUENTE and sr == uhe:
        return p(stub_QDEF, cls)
    if v == Variable.VOLUME_DEFLUENTE and sr == uhe:
        return p(stub_VDEF, cls)
    if v == Variable.VIOLACAO_EVAPORACAO and sr == uhe:
        return p(stub_VEVAP, cls)
    if v in EARM_UHE_VARS and sr == uhe:
        return p(stub_EARM_UHE, cls)
    if v in [Variable.MERCADO, Variable.MERCADO_LIQUIDO]:
        return p(stub_MER_MERL, cls)
    if v == Variable.GERACAO_USINAS_NAO_SIMULADAS:
        return p(stub_GUNS, cls)
    return None


def resolve_stub(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> Tuple[pd.DataFrame, bool]:
    """Resolve synthesis via stub if not from NWLISTOP."""
    f = stub_mappings(cls, s)
    if f:
        df, is_stub = f(s, uow), True
    else:
        df, is_stub = pd.DataFrame(), False
    if is_stub:
        # Stub functions return pd.DataFrame (from cache). Convert at the
        # boundary so _post_resolve receives pl.DataFrame as required.
        df_pl = pd_to_pl(df) if isinstance(df, pd.DataFrame) else df
        df = post_resolve(cls, {"": df_pl}, s, uow)
        from app.services.synthesis.operation.bounds import resolve_bounds

        df = resolve_bounds(cls, s, df, uow)
    return df, is_stub
