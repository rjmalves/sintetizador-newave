from typing import Any, Dict

import numpy as np
import pandas as pd
from inewave.nwlistcf import Estados, Nwlistcfrel

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
    deck_cls,
    cache: Dict[str, Any],
    cut_df: pd.DataFrame,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("_policy_df_building_block")
    if df is None:
        stages = cut_df[STAGE_COL].unique().tolist()
        num_stages = len(stages)
        cut_indexes = cut_df[CUT_INDEX_COL].unique().tolist()
        num_series = misc.num_forward_series(deck_cls, cache, uow)
        num_iterations = len(cut_indexes) // (num_series * num_stages)
        df = pd.DataFrame(
            data={
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
        df[COEF_TYPE_COL] = ""
        df[ENTITY_INDEX_COL] = 0
        df[LAG_COL] = 0
        df[BLOCK_COL] = 0
        df[COEF_VALUE_COL] = np.nan
        df[STATE_VALUE_COL] = np.nan
        cache["_policy_df_building_block"] = df
    return df.copy()


def _rhs_entities(
    deck_cls,
    cache: Dict[str, Any],
    cut_df: pd.DataFrame,
    state_df: pd.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    cut_cols = cut_df.columns.tolist()
    rhs_col = "RHS"
    obj_func_col = "FUNC.OBJ."
    entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]
    num_entities = cut_df[entity_col].unique().shape[0]
    base_df = _policy_df_building_block(deck_cls, cache, cut_df, uow)
    rhs_df = cut_df.iloc[::num_entities]
    base_df[COEF_VALUE_COL] = rhs_df[rhs_col].to_numpy()
    if state_df is not None:
        obj_df = state_df.iloc[::num_entities]
        base_df[STATE_VALUE_COL] = obj_df[obj_func_col].to_numpy()
    base_df[COEF_TYPE_COL] = RHS_COEF_CODE
    return base_df


def _eer_hydro_cut_entities(
    deck_cls,
    cache: Dict[str, Any],
    entity_col: str,
    cut_value_col: str,
    state_value_col: str | None,
    coef_type_value: int,
    lag: int,
    cut_df: pd.DataFrame,
    state_df: pd.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    entity_indices = np.array(cut_df[entity_col].unique().tolist())
    num_entities = len(entity_indices)
    df = pd.concat(
        [
            _policy_df_building_block(deck_cls, cache, cut_df, uow)
            for _ in range(num_entities)
        ],
        ignore_index=True,
    )
    df[COEF_TYPE_COL] = coef_type_value
    df[LAG_COL] = lag
    df = df.sort_values(
        [STAGE_COL, CUT_INDEX_COL, SCENARIO_COL],
        ascending=[True, False, False],
    ).reset_index(drop=True)
    num_repeats = cut_df.shape[0] // num_entities
    df[ENTITY_INDEX_COL] = np.tile(entity_indices, num_repeats)
    df[COEF_VALUE_COL] = cut_df[cut_value_col].to_numpy()
    if state_df is not None:
        df[STATE_VALUE_COL] = state_df[state_value_col].to_numpy()
    return df


def _storage_cut_entities(
    deck_cls,
    cache: Dict[str, Any],
    cut_df: pd.DataFrame,
    state_df: pd.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    cut_cols = cut_df.columns.tolist()
    entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]
    cut_value_col = [c for c in cut_cols if c in ["PIEARM", "PIVARM"]][0]
    state_cols = state_df.columns.tolist() if state_df is not None else []
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
    deck_cls,
    cache: Dict[str, Any],
    cut_df: pd.DataFrame,
    state_df: pd.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    cut_cols = cut_df.columns.tolist()
    entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]
    max_ar_lag = temporal.num_stages_with_past_tendency_period(
        deck_cls, cache, uow
    )
    dfs: list[pd.DataFrame] = []
    for lag in range(1, max_ar_lag + 1):
        cut_value_col = [
            c for c in cut_cols if c in [f"PIH({lag})", f"PIAFL({lag})"]
        ][0]
        state_cols = state_df.columns.tolist() if state_df is not None else []
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
    return pd.concat(dfs, ignore_index=True)


def _eer_in_hydro_cut_entities(
    deck_cls,
    cache: Dict[str, Any],
    entity_col: str,
    cut_value_col: str,
    state_value_col: str | None,
    coef_type_value: int,
    cut_df: pd.DataFrame,
    state_df: pd.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    hydro_df = entities.hydro_eer_submarket_map(deck_cls, cache, uow)
    hydro_df = hydro_df.reset_index().drop_duplicates(subset=[EER_CODE_COL])
    entity_indices = hydro_df[HYDRO_CODE_COL].tolist()
    eer_indices = hydro_df[EER_CODE_COL].tolist()
    num_entities = len(entity_indices)
    df = pd.concat(
        [
            _policy_df_building_block(deck_cls, cache, cut_df, uow)
            for _ in range(num_entities)
        ],
        ignore_index=True,
    )
    df[COEF_TYPE_COL] = coef_type_value
    df[LAG_COL] = 0
    df[BLOCK_COL] = 0
    df = df.sort_values(
        [STAGE_COL, CUT_INDEX_COL, SCENARIO_COL],
        ascending=[True, False, False],
    ).reset_index(drop=True)
    filtered_cut_df = cut_df.loc[cut_df[entity_col].isin(entity_indices)]
    num_repeats = filtered_cut_df.shape[0] // num_entities
    df[ENTITY_INDEX_COL] = np.tile(eer_indices, num_repeats)
    df[COEF_VALUE_COL] = filtered_cut_df[cut_value_col].to_numpy()
    if state_df is not None:
        df[STATE_VALUE_COL] = state_df.loc[
            state_df[entity_col].isin(entity_indices), state_value_col
        ].to_numpy()
    return df


def _maxviol_cut_entities(
    deck_cls,
    cache: Dict[str, Any],
    cut_df: pd.DataFrame,
    state_df: pd.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    cut_cols = cut_df.columns.tolist()
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
    deck_cls,
    cache: Dict[str, Any],
    entity_col: str,
    cut_value_col: str,
    state_value_col: str | None,
    coef_type_value: int,
    lag: int,
    block: int,
    cut_df: pd.DataFrame,
    state_df: pd.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    if entity_col == "REE":
        eer_df = entities.eers(deck_cls, cache, uow)
        eer_df = eer_df.reset_index().drop_duplicates(
            subset=[SUBMARKET_CODE_COL]
        )
        entity_indices = eer_df[EER_CODE_COL].tolist()
        sbm_indices = eer_df[SUBMARKET_CODE_COL].tolist()
    else:
        hydro_df = entities.hydro_eer_submarket_map(deck_cls, cache, uow)
        hydro_df = hydro_df.reset_index().drop_duplicates(
            subset=[SUBMARKET_CODE_COL]
        )
        entity_indices = hydro_df[HYDRO_CODE_COL].tolist()
        sbm_indices = hydro_df[SUBMARKET_CODE_COL].tolist()
    num_entities = len(entity_indices)
    df = pd.concat(
        [
            _policy_df_building_block(deck_cls, cache, cut_df, uow)
            for _ in range(num_entities)
        ],
        ignore_index=True,
    )
    df[COEF_TYPE_COL] = coef_type_value
    df[LAG_COL] = lag
    df[BLOCK_COL] = block
    df = df.sort_values(
        [STAGE_COL, CUT_INDEX_COL, SCENARIO_COL],
        ascending=[True, False, False],
    ).reset_index(drop=True)
    filtered_cut_df = cut_df.loc[cut_df[entity_col].isin(entity_indices)]
    num_repeats = filtered_cut_df.shape[0] // num_entities
    df[ENTITY_INDEX_COL] = np.tile(sbm_indices, num_repeats)
    df[COEF_VALUE_COL] = filtered_cut_df[cut_value_col].to_numpy()
    if state_df is not None:
        df[STATE_VALUE_COL] = state_df.loc[
            state_df[entity_col].isin(entity_indices), state_value_col
        ].to_numpy()
    return df


def _thermal_generation_cut_entities(
    deck_cls,
    cache: Dict[str, Any],
    cut_df: pd.DataFrame,
    state_df: pd.DataFrame | None,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    cut_cols = cut_df.columns.tolist()
    entity_col = [c for c in cut_cols if c in ["REE", "UHE"]][0]
    num_blocks = misc.num_blocks(deck_cls, cache, uow)
    max_thermal_lag = MAX_THERMAL_DISPATCH_LAG
    dfs: list[pd.DataFrame] = []
    for block in range(1, num_blocks + 1):
        for lag in range(1, max_thermal_lag + 1):
            cut_value_col = [
                c for c in cut_cols if c == f"PIGTAD(P{block}L{lag})"
            ][0]
            state_cols = (
                state_df.columns.tolist() if state_df is not None else []
            )
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
    return pd.concat(dfs, ignore_index=True)


def common_policy_df(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    """Build common policy df from NWLISTCF cuts and states."""
    aux_df = cache.get("common_policy_df")
    if aux_df is None:
        nwlistcfrel = readers.validate_data(
            deck_cls,
            readers.get_cortes(deck_cls, uow),
            Nwlistcfrel,
            "Relatório de cortes do NWLISTCF",
        )
        cut_df = readers.validate_data(
            deck_cls,
            nwlistcfrel.cortes,
            pd.DataFrame,
            "Relatório de cortes do NWLISTCF",
        )
        cut_df = cut_df.rename(
            columns={"PERIODO": STAGE_COL, "IREG": CUT_INDEX_COL}
        )
        cut_df[STAGE_COL] -= (
            temporal.study_period_starting_month(deck_cls, cache, uow) - 1
        )
        estadosrel = readers.validate_data(
            deck_cls,
            readers.get_estados(deck_cls, uow),
            Estados,
            "Relatório de estados do NWLISTCF",
        )
        state_df = estadosrel.estados
        if state_df is not None:
            state_df = state_df.rename(
                columns={
                    "PERIODO": STAGE_COL,
                    "IREG": CUT_INDEX_COL,
                    "ITEc": ITERATION_COL,
                    "SIMc": SCENARIO_COL,
                }
            ).drop(columns=["ITEf"])
            state_df[STAGE_COL] -= (
                temporal.study_period_starting_month(deck_cls, cache, uow) - 1
            )
        aux_df = pd.concat(
            [
                _rhs_entities(deck_cls, cache, cut_df, state_df, uow),
                _storage_cut_entities(deck_cls, cache, cut_df, state_df, uow),
                _inflow_cut_entities(deck_cls, cache, cut_df, state_df, uow),
                _thermal_generation_cut_entities(
                    deck_cls, cache, cut_df, state_df, uow
                ),
                _maxviol_cut_entities(deck_cls, cache, cut_df, state_df, uow),
            ],
            ignore_index=True,
        )
        cache["common_policy_df"] = aux_df
    return aux_df.copy()


def policy_variable_units(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    """Return units for each policy coefficient type."""
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
        df = cuts_df[[COEF_TYPE_COL]].drop_duplicates()
        df["nome_curto_coeficiente"] = df[COEF_TYPE_COL].replace(_COEF_SHORT)
        df["nome_longo_coeficiente"] = df[COEF_TYPE_COL].replace(_COEF_LONG)
        df["unidade_coeficiente"] = df[COEF_TYPE_COL].replace(coef_unit)
        df["unidade_estado"] = df[COEF_TYPE_COL].replace(state_unit)
        df = df.reset_index(drop=True)
        cache[name] = df
    return df.copy()
