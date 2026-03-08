from typing import Any, Dict

import numpy as np
import polars as pl
from inewave.nwlistcf import Estados, Nwlistcfrel  # type: ignore[attr-defined]

from app.internal.constants import (
    BLOCK_COL,
    COEF_TYPE_COL,
    COEF_VALUE_COL,
    CUT_INDEX_COL,
    EARM_COEF_CODE,
    EER_CODE_COL,
    ENA_COEF_CODE,
    ENTITY_INDEX_COL,
    GTER_COEF_CODE,
    HYDRO_CODE_COL,
    ITERATION_COL,
    LAG_COL,
    MAX_THERMAL_DISPATCH_LAG,
    MAXVIOL_COEF_CODE,
    QINC_COEF_CODE,
    RHS_COEF_CODE,
    SCENARIO_COL,
    STAGE_COL,
    STATE_VALUE_COL,
    SUBMARKET_CODE_COL,
    VARM_COEF_CODE,
)
from app.model.policy.unit import Unit as PolicyUnit
from app.services.deck import entities, misc, readers, temporal
from app.services.unitofwork import AbstractUnitOfWork

_COEF_SHORT: dict[int, str] = {
    RHS_COEF_CODE: "RHS",
    EARM_COEF_CODE: "EARM",
    VARM_COEF_CODE: "VARM",
    ENA_COEF_CODE: "ENA",
    QINC_COEF_CODE: "QINC",
    GTER_COEF_CODE: "GTER",
    MAXVIOL_COEF_CODE: "MAXVIOL",
}
_COEF_LONG: dict[int, str] = {
    RHS_COEF_CODE: "Right hand side",
    EARM_COEF_CODE: "Energia armazenada",
    VARM_COEF_CODE: "Volume armazenado",
    ENA_COEF_CODE: "Energia natural afluente",
    QINC_COEF_CODE: "Vazão incremental",
    GTER_COEF_CODE: "Geração térmica antecipada",
    MAXVIOL_COEF_CODE: "Máxima violação de volume mínimo operativo",
}


def _policy_df_building_block(
    deck_cls: Any,
    cache: Dict[str, Any],
    cut_df: pl.DataFrame,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("_policy_df_building_block")
    if df is None:
        stages = cut_df[STAGE_COL].unique().sort().to_list()
        num_stages = len(stages)
        cut_indexes = (
            cut_df[CUT_INDEX_COL].unique(maintain_order=True).to_list()
        )
        num_series = misc.num_forward_series(deck_cls, cache, uow)
        num_iterations = len(cut_indexes) // (num_series * num_stages)
        df = pl.DataFrame(
            {
                STAGE_COL: np.repeat(
                    np.array(stages), num_iterations * num_series
                ),
                CUT_INDEX_COL: cut_indexes,
                ITERATION_COL: np.tile(
                    np.repeat(np.arange(1, num_iterations + 1), num_series)[
                        ::-1
                    ],
                    num_stages,
                ),
                SCENARIO_COL: np.tile(
                    np.tile(np.arange(num_series, 0, -1), num_iterations),
                    num_stages,
                ),
            }
        )
        df = df.with_columns(
            pl.lit(0, dtype=pl.Int64).alias(COEF_TYPE_COL),
            pl.lit(0, dtype=pl.Int64).alias(ENTITY_INDEX_COL),
            pl.lit(0, dtype=pl.Int64).alias(LAG_COL),
            pl.lit(0, dtype=pl.Int64).alias(BLOCK_COL),
            pl.lit(float("nan")).alias(COEF_VALUE_COL),
            pl.lit(float("nan")).alias(STATE_VALUE_COL),
        )
        cache["_policy_df_building_block"] = df
    return df


def _rhs_entities(
    deck_cls: Any,
    cache: Dict[str, Any],
    cut_df: pl.DataFrame,
    state_df: pl.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    cut_cols = cut_df.columns
    rhs_col = "RHS"
    obj_func_col = "FUNC.OBJ."
    entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]
    num_entities = cut_df[entity_col].n_unique()
    base_df = _policy_df_building_block(deck_cls, cache, cut_df, uow)
    rhs_df = cut_df[::num_entities]
    base_df = base_df.with_columns(
        pl.Series(COEF_VALUE_COL, rhs_df[rhs_col].to_numpy()),
    )
    if state_df is not None:
        obj_df = state_df[::num_entities]
        base_df = base_df.with_columns(
            pl.Series(STATE_VALUE_COL, obj_df[obj_func_col].to_numpy()),
        )
    base_df = base_df.with_columns(
        pl.lit(RHS_COEF_CODE, dtype=pl.Int64).alias(COEF_TYPE_COL)
    )
    return base_df


def _eer_hydro_cut_entities(
    deck_cls: Any,
    cache: Dict[str, Any],
    entity_col: str,
    cut_value_col: str,
    state_value_col: str | None,
    coef_type_value: int,
    lag: int,
    cut_df: pl.DataFrame,
    state_df: pl.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    entity_indices = cut_df[entity_col].unique().sort().to_numpy()
    num_entities = len(entity_indices)
    df = pl.concat(
        [
            _policy_df_building_block(deck_cls, cache, cut_df, uow)
            for _ in range(num_entities)
        ]
    )
    df = df.with_columns(
        pl.lit(coef_type_value, dtype=pl.Int64).alias(COEF_TYPE_COL),
        pl.lit(lag, dtype=pl.Int64).alias(LAG_COL),
    )
    df = df.sort(
        [STAGE_COL, CUT_INDEX_COL, SCENARIO_COL],
        descending=[False, True, True],
    )
    num_repeats = cut_df.shape[0] // num_entities
    df = df.with_columns(
        pl.Series(ENTITY_INDEX_COL, np.tile(entity_indices, num_repeats)),
        pl.Series(COEF_VALUE_COL, cut_df[cut_value_col].to_numpy()),
    )
    if state_df is not None and state_value_col is not None:
        df = df.with_columns(
            pl.Series(STATE_VALUE_COL, state_df[state_value_col].to_numpy()),
        )
    return df


def _storage_cut_entities(
    deck_cls: Any,
    cache: Dict[str, Any],
    cut_df: pl.DataFrame,
    state_df: pl.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    cut_cols = cut_df.columns
    entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]
    cut_value_col = [c for c in cut_cols if c in ["PIEARM", "PIVARM"]][0]
    state_cols = state_df.columns if state_df is not None else []
    state_value_col = (
        [c for c in state_cols if c in ["EARM", "VARM"]][0]
        if state_df is not None
        else None
    )
    coef_type_map = {"REE": EARM_COEF_CODE, "UHE": VARM_COEF_CODE}
    coef_type_value = coef_type_map[entity_col]
    return _eer_hydro_cut_entities(
        deck_cls,
        cache,
        entity_col,
        cut_value_col,
        state_value_col,
        coef_type_value,
        0,
        cut_df,
        state_df,
        uow,
    )


def _inflow_cut_entities(
    deck_cls: Any,
    cache: Dict[str, Any],
    cut_df: pl.DataFrame,
    state_df: pl.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    cut_cols = cut_df.columns
    entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]
    max_ar_lag = temporal.num_stages_with_past_tendency_period(
        deck_cls, cache, uow
    )
    dfs: list[pl.DataFrame] = []
    for lag in range(1, max_ar_lag + 1):
        cut_value_col = [
            c for c in cut_cols if c in [f"PIH({lag})", f"PIAFL({lag})"]
        ][0]
        state_cols = state_df.columns if state_df is not None else []
        state_value_col = (
            [c for c in state_cols if c in [f"EAF({lag})", f"VAF({lag})"]][0]
            if state_df is not None
            else None
        )
        coef_type_map = {"REE": ENA_COEF_CODE, "UHE": QINC_COEF_CODE}
        coef_type_value = coef_type_map[entity_col]
        dfs.append(
            _eer_hydro_cut_entities(
                deck_cls,
                cache,
                entity_col,
                cut_value_col,
                state_value_col,
                coef_type_value,
                lag,
                cut_df,
                state_df,
                uow,
            )
        )
    return pl.concat(dfs)


def _eer_in_hydro_cut_entities(
    deck_cls: Any,
    cache: Dict[str, Any],
    entity_col: str,
    cut_value_col: str,
    state_value_col: str | None,
    coef_type_value: int,
    cut_df: pl.DataFrame,
    state_df: pl.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    hydro_pl = entities.hydro_eer_submarket_map(deck_cls, cache, uow)
    hydro_pl = hydro_pl.unique(subset=[EER_CODE_COL], maintain_order=True)
    entity_indices = hydro_pl[HYDRO_CODE_COL].to_numpy()
    eer_indices = hydro_pl[EER_CODE_COL].to_numpy()
    num_entities = len(entity_indices)
    df = pl.concat(
        [
            _policy_df_building_block(deck_cls, cache, cut_df, uow)
            for _ in range(num_entities)
        ]
    )
    df = df.with_columns(
        pl.lit(coef_type_value, dtype=pl.Int64).alias(COEF_TYPE_COL),
        pl.lit(0, dtype=pl.Int64).alias(LAG_COL),
        pl.lit(0, dtype=pl.Int64).alias(BLOCK_COL),
    )
    df = df.sort(
        [STAGE_COL, CUT_INDEX_COL, SCENARIO_COL],
        descending=[False, True, True],
    )
    filtered_cut_df = cut_df.filter(
        pl.col(entity_col).is_in(entity_indices.tolist())
    )
    num_repeats = filtered_cut_df.shape[0] // num_entities
    df = df.with_columns(
        pl.Series(ENTITY_INDEX_COL, np.tile(eer_indices, num_repeats)),
        pl.Series(COEF_VALUE_COL, filtered_cut_df[cut_value_col].to_numpy()),
    )
    if state_df is not None and state_value_col is not None:
        filtered_state_df = state_df.filter(
            pl.col(entity_col).is_in(entity_indices.tolist())
        )
        df = df.with_columns(
            pl.Series(
                STATE_VALUE_COL, filtered_state_df[state_value_col].to_numpy()
            ),
        )
    return df


def _maxviol_cut_entities(
    deck_cls: Any,
    cache: Dict[str, Any],
    cut_df: pl.DataFrame,
    state_df: pl.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    cut_cols = cut_df.columns
    entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]
    cut_value_col = "PIMX_VMN"
    state_value_col = "MX_CURVA"
    coef_type_value = MAXVIOL_COEF_CODE
    if entity_col == "REE":
        return _eer_hydro_cut_entities(
            deck_cls,
            cache,
            entity_col,
            cut_value_col,
            state_value_col,
            coef_type_value,
            0,
            cut_df,
            state_df,
            uow,
        )
    else:
        return _eer_in_hydro_cut_entities(
            deck_cls,
            cache,
            entity_col,
            cut_value_col,
            state_value_col,
            coef_type_value,
            cut_df,
            state_df,
            uow,
        )


def _submarket_cut_entities(
    deck_cls: Any,
    cache: Dict[str, Any],
    entity_col: str,
    cut_value_col: str,
    state_value_col: str | None,
    coef_type_value: int,
    lag: int,
    block: int,
    cut_df: pl.DataFrame,
    state_df: pl.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    if entity_col == "REE":
        eer_pl = entities.eers(deck_cls, cache, uow)
        eer_pl = eer_pl.unique(subset=[SUBMARKET_CODE_COL], maintain_order=True)
        entity_indices = eer_pl[EER_CODE_COL].to_numpy()
        sbm_indices = eer_pl[SUBMARKET_CODE_COL].to_numpy()
    else:
        hydro_pl = entities.hydro_eer_submarket_map(deck_cls, cache, uow)
        hydro_pl = hydro_pl.unique(
            subset=[SUBMARKET_CODE_COL], maintain_order=True
        )
        entity_indices = hydro_pl[HYDRO_CODE_COL].to_numpy()
        sbm_indices = hydro_pl[SUBMARKET_CODE_COL].to_numpy()
    num_entities = len(entity_indices)
    df = pl.concat(
        [
            _policy_df_building_block(deck_cls, cache, cut_df, uow)
            for _ in range(num_entities)
        ]
    )
    df = df.with_columns(
        pl.lit(coef_type_value, dtype=pl.Int64).alias(COEF_TYPE_COL),
        pl.lit(lag, dtype=pl.Int64).alias(LAG_COL),
        pl.lit(block, dtype=pl.Int64).alias(BLOCK_COL),
    )
    df = df.sort(
        [STAGE_COL, CUT_INDEX_COL, SCENARIO_COL],
        descending=[False, True, True],
    )
    filtered_cut_df = cut_df.filter(
        pl.col(entity_col).is_in(entity_indices.tolist())
    )
    num_repeats = filtered_cut_df.shape[0] // num_entities
    df = df.with_columns(
        pl.Series(ENTITY_INDEX_COL, np.tile(sbm_indices, num_repeats)),
        pl.Series(COEF_VALUE_COL, filtered_cut_df[cut_value_col].to_numpy()),
    )
    if state_df is not None and state_value_col is not None:
        filtered_state_df = state_df.filter(
            pl.col(entity_col).is_in(entity_indices.tolist())
        )
        df = df.with_columns(
            pl.Series(
                STATE_VALUE_COL, filtered_state_df[state_value_col].to_numpy()
            ),
        )
    return df


def _thermal_generation_cut_entities(
    deck_cls: Any,
    cache: Dict[str, Any],
    cut_df: pl.DataFrame,
    state_df: pl.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    cut_cols = cut_df.columns
    entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]
    num_blocks = misc.num_blocks(deck_cls, cache, uow)
    max_thermal_lag = MAX_THERMAL_DISPATCH_LAG
    dfs: list[pl.DataFrame] = []
    for block in range(1, num_blocks + 1):
        for lag in range(1, max_thermal_lag + 1):
            cut_value_col = [
                c for c in cut_cols if c == f"PIGTAD(P{block}L{lag})"
            ][0]
            state_cols = state_df.columns if state_df is not None else []
            state_value_col = (
                [c for c in state_cols if c == f"SGT(P{block}E{lag})"][0]
                if state_df is not None
                else None
            )
            dfs.append(
                _submarket_cut_entities(
                    deck_cls,
                    cache,
                    entity_col,
                    cut_value_col,
                    state_value_col,
                    GTER_COEF_CODE,
                    lag,
                    block,
                    cut_df,
                    state_df,
                    uow,
                )
            )
    return pl.concat(dfs)


def common_policy_df(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    aux_df = cache.get("common_policy_df")
    if aux_df is None:
        nwlistcfrel = readers.validate_data(
            deck_cls,
            readers.get_cortes(deck_cls, uow),
            Nwlistcfrel,
            "Relatório de cortes do NWLISTCF",
        )
        # nwlistcfrel.cortes returns a pd.DataFrame from inewave binary files
        cut_df = pl.from_pandas(nwlistcfrel.cortes)  # type: ignore[union-attr]
        cut_df = cut_df.rename({"PERIODO": STAGE_COL, "IREG": CUT_INDEX_COL})
        cut_df = cut_df.with_columns(
            (
                pl.col(STAGE_COL)
                - (
                    temporal.study_period_starting_month(deck_cls, cache, uow)
                    - 1
                )
            ).alias(STAGE_COL)
        )
        estadosrel = readers.validate_data(
            deck_cls,
            readers.get_estados(deck_cls, uow),
            Estados,
            "Relatório de estados do NWLISTCF",
        )
        state_df_raw = estadosrel.estados  # type: ignore[union-attr]
        state_df: pl.DataFrame | None = None
        if state_df_raw is not None:
            # estadosrel.estados returns a pd.DataFrame from inewave binary files
            state_df = pl.from_pandas(state_df_raw)
            state_df = state_df.rename(
                {
                    "PERIODO": STAGE_COL,
                    "IREG": CUT_INDEX_COL,
                    "ITEc": ITERATION_COL,
                    "SIMc": SCENARIO_COL,
                }
            ).drop("ITEf")
            state_df = state_df.with_columns(
                (
                    pl.col(STAGE_COL)
                    - (
                        temporal.study_period_starting_month(
                            deck_cls, cache, uow
                        )
                        - 1
                    )
                ).alias(STAGE_COL)
            )
        aux_df = pl.concat(
            [
                _rhs_entities(deck_cls, cache, cut_df, state_df, uow),
                _storage_cut_entities(deck_cls, cache, cut_df, state_df, uow),
                _inflow_cut_entities(deck_cls, cache, cut_df, state_df, uow),
                _thermal_generation_cut_entities(
                    deck_cls, cache, cut_df, state_df, uow
                ),
                _maxviol_cut_entities(deck_cls, cache, cut_df, state_df, uow),
            ]
        )
        cache["common_policy_df"] = aux_df
    return aux_df


def policy_variable_units(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    name = "policy_variable_units"
    df = cache.get(name)
    coef_unit = {
        RHS_COEF_CODE: PolicyUnit.RS_mes_h.value,
        EARM_COEF_CODE: PolicyUnit.RS_MWh.value,
        VARM_COEF_CODE: PolicyUnit.RS_mes_hm3_MWh.value,
        ENA_COEF_CODE: PolicyUnit.RS_MWh.value,
        QINC_COEF_CODE: PolicyUnit.RS_mes_hm3_MWh.value,
        GTER_COEF_CODE: PolicyUnit.RS_MWh.value,
        MAXVIOL_COEF_CODE: PolicyUnit.RS_MWh.value,
    }
    state_unit = {
        RHS_COEF_CODE: PolicyUnit.RS.value,
        EARM_COEF_CODE: PolicyUnit.MWmes.value,
        VARM_COEF_CODE: PolicyUnit.hm3.value,
        ENA_COEF_CODE: PolicyUnit.MWmes.value,
        QINC_COEF_CODE: PolicyUnit.hm3.value,
        GTER_COEF_CODE: PolicyUnit.MWmes.value,
        MAXVIOL_COEF_CODE: PolicyUnit.MWmes.value,
    }
    if df is None:
        cuts_df = common_policy_df(deck_cls, cache, uow)
        df = cuts_df.select(COEF_TYPE_COL).unique(maintain_order=True)
        # Build lookup maps as polars DataFrames and join
        short_map = pl.DataFrame(
            {
                COEF_TYPE_COL: list(_COEF_SHORT.keys()),
                "nome_curto_coeficiente": list(_COEF_SHORT.values()),
            }
        )
        long_map = pl.DataFrame(
            {
                COEF_TYPE_COL: list(_COEF_LONG.keys()),
                "nome_longo_coeficiente": list(_COEF_LONG.values()),
            }
        )
        coef_unit_map = pl.DataFrame(
            {
                COEF_TYPE_COL: list(coef_unit.keys()),
                "unidade_coeficiente": list(coef_unit.values()),
            }
        )
        state_unit_map = pl.DataFrame(
            {
                COEF_TYPE_COL: list(state_unit.keys()),
                "unidade_estado": list(state_unit.values()),
            }
        )
        df = (
            df.join(short_map, on=COEF_TYPE_COL, how="left")
            .join(long_map, on=COEF_TYPE_COL, how="left")
            .join(coef_unit_map, on=COEF_TYPE_COL, how="left")
            .join(state_unit_map, on=COEF_TYPE_COL, how="left")
        )
        cache[name] = df
    return df
