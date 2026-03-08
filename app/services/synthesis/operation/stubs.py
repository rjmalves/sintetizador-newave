import functools
from typing import TYPE_CHECKING, Any, Callable, Optional, Tuple

import numpy as np
import polars as pl

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
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )

__all__ = [
    "calc_accumulated_productivity",
    "resolve_SBM_entity_MER_MERL",
    "resolve_stub",
    "stub_mappings",
]


def stub_QDEF(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    return two_cache_op(
        cls, synthesis, Variable.VAZAO_TURBINADA, Variable.VAZAO_VERTIDA
    )


def stub_VDEF(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    return two_cache_op(
        cls, synthesis, Variable.VOLUME_TURBINADO, Variable.VOLUME_VERTIDO
    )


def stub_VEVAP(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
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
) -> pl.DataFrame:
    return two_cache_op(
        cls, synthesis, Variable.CUSTO_OPERACAO, Variable.CUSTO_FUTURO
    )


def stub_EVER(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
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
) -> pl.DataFrame:
    return two_cache_op(
        cls,
        synthesis,
        Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
        Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
        op="sub",
    )


def hydro_resolution_variable_map(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
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
) -> pl.DataFrame:
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
) -> pl.DataFrame:
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
) -> pl.DataFrame:
    df = cls._get_from_cache(
        OperationSynthesis(
            Variable.VOLUME_RETIRADO, synthesis.spatial_resolution
        )
    )
    return df.with_columns(
        (
            pl.col(VALUE_COL)
            * HM3_M3S_MONTHLY_FACTOR
            * STAGE_DURATION_HOURS
            / pl.col(BLOCK_DURATION_COL)
        ).alias(VALUE_COL)
    )


def convert_flow_to_volume(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
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
    return df.with_columns(
        (
            pl.col(VALUE_COL)
            * pl.col(BLOCK_DURATION_COL)
            / (HM3_M3S_MONTHLY_FACTOR * STAGE_DURATION_HOURS)
        ).alias(VALUE_COL)
    )


def stub_resolve_initial_stored_energy(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
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
    grouping_col = grouping_col_map[synthesis.spatial_resolution]
    groups = (
        entities.get(grouping_col) if grouping_col is not None else None
    ) or [1]
    if synthesis.spatial_resolution != SpatialResolution.SISTEMA_INTERLIGADO:
        groups_in_data = init_data[GROUPING_TMP_COL].to_list()
        groups = [g for g in groups if g in groups_in_data]
    # Build ordered values array in the order of `groups`
    init_lookup = dict(
        zip(
            init_data[GROUPING_TMP_COL].to_list(),
            init_data[value_column].to_list(),
        )
    )
    init_values = np.array([init_lookup[g] for g in groups])
    indices = build_initial_stage_indices(entities, len(groups))
    return fill_initial_storage_df(final_df, indices, init_values, entities)


def stub_resolve_initial_stored_volumes(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
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
    # Deck.initial_stored_volume returns pd.DataFrame from the facade;
    # convert to polars immediately for uniform processing.
    initial_data = pl.from_pandas(Deck.initial_stored_volume(uow))
    value_column = (
        "valor_hm3" if synthesis.variable == varmi else "valor_percentual"
    )
    if synthesis.variable == varmi:
        hidr = Deck.hidr(uow)
        # Build lookup: hydro_code -> volume_minimo
        vol_min_map = dict(
            zip(
                hidr["codigo_usina"].to_list(),
                hidr["volume_minimo"].to_list(),
            )
        )
        vol_min_arr = np.array(
            [
                vol_min_map.get(c, 0.0)
                for c in initial_data[HYDRO_CODE_COL].to_list()
            ]
        )
        initial_data = initial_data.with_columns(
            (pl.col(value_column) + pl.Series("_vm", vol_min_arr)).alias(
                value_column
            )
        )
    initial_data = initial_data.filter(pl.col(HYDRO_CODE_COL).is_in(hydros))
    # Build ordered init_values in the order of `hydros`
    init_lookup = dict(
        zip(
            initial_data[HYDRO_CODE_COL].to_list(),
            initial_data[value_column].to_list(),
        )
    )
    init_values = np.array([init_lookup[h] for h in hydros])
    indices = build_initial_stage_indices(entities, num_hydros)
    return fill_initial_storage_df(final_df, indices, init_values, entities)


def stub_EARM_UHE(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
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
        vol_df = cls._get_from_cache(vol_synthesis)
        vol_entities = cls._get_ordered_entities(vol_synthesis)
        net_df = cls._get_from_cache(net_drop_synthesis)
        net_entities = cls._get_ordered_entities(net_drop_synthesis)
        hidr = Deck.hidr(uow)
        hydro_codes = net_entities[HYDRO_CODE_COL]
        # Count entries per hydro in net_df
        n_entries = net_df.filter(
            pl.col(HYDRO_CODE_COL) == hydro_codes[0]
        ).shape[0]
        # Build specific productivity lookup: hydro_code -> produtibilidade_especifica
        spec_prod_map = dict(
            zip(
                hidr["codigo_usina"].to_list(),
                hidr["produtibilidade_especifica"].to_list(),
            )
        )
        specific_prod = (
            np.repeat(
                np.array([spec_prod_map[c] for c in hydro_codes]),
                n_entries,
            )
            * HM3_M3S_MONTHLY_FACTOR
        )
        # Add PRODUCTIVITY_TMP_COL to net_df
        prod_values = specific_prod * net_df[VALUE_COL].to_numpy()
        net_df = net_df.with_columns(
            pl.Series(PRODUCTIVITY_TMP_COL, prod_values).alias(
                PRODUCTIVITY_TMP_COL
            )
        )
        net_df = calc_accumulated_productivity(cls, net_df, net_entities, uow)

    # Filter net_df to match vol_df hydro codes and blocks
    hydro_blocks = vol_entities[BLOCK_COL]
    vol_hydro_codes = vol_entities[HYDRO_CODE_COL]
    net_df = net_df.filter(
        pl.col(HYDRO_CODE_COL).is_in(vol_hydro_codes)
        & pl.col(BLOCK_COL).is_in(hydro_blocks)
    ).sort([HYDRO_CODE_COL, STAGE_COL, BLOCK_COL])
    vol_df = vol_df.sort([HYDRO_CODE_COL, STAGE_COL, BLOCK_COL])

    # Extract arrays for arithmetic
    net_prod_arr = net_df[PRODUCTIVITY_TMP_COL].to_numpy()
    val_arr = vol_df[VALUE_COL].to_numpy()
    lb_arr = vol_df[LOWER_BOUND_COL].to_numpy()
    ub_arr = vol_df[UPPER_BOUND_COL].to_numpy()

    new_value = (val_arr - lb_arr) * net_prod_arr
    new_upper = (ub_arr - lb_arr) * net_prod_arr
    new_lower = np.zeros_like(lb_arr)

    return vol_df.with_columns(
        [
            pl.Series(VALUE_COL, new_value).alias(VALUE_COL),
            pl.Series(LOWER_BOUND_COL, new_lower).alias(LOWER_BOUND_COL),
            pl.Series(UPPER_BOUND_COL, new_upper).alias(UPPER_BOUND_COL),
        ]
    )


def stub_mappings(
    cls: "type[OperationSynthetizer]", s: OperationSynthesis
) -> Optional[Callable[..., Any]]:
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
) -> Tuple[pl.DataFrame, bool]:
    f = stub_mappings(cls, s)
    if f:
        df, is_stub = f(s, uow), True
    else:
        df, is_stub = pl.DataFrame(), False
    if is_stub:
        resolved = post_resolve(cls, {"": df}, s, uow)
        if resolved is None:
            return pl.DataFrame(), is_stub
        from app.services.synthesis.operation.bounds import resolve_bounds

        df = resolve_bounds(cls, s, resolved, uow)
    return df, is_stub
