from typing import Any, Dict

import numpy as np
import pandas as pd
from inewave.newave.modelos.modif import (
    CFUGA,
    CMONT,
    NUMCNJ,
    NUMMAQ,
    TURBMAXT,
    TURBMINT,
    VAZMIN,
    VAZMINT,
    VMAXT,
    VMINT,
    VOLMAX,
    VOLMIN,
)

from app.internal.constants import (
    BLOCK_COL,
    HEIGHT_POLY_COLS,
    HYDRO_CODE_COL,
    LOSS_COL,
    LOSS_KIND_COL,
    LOWER_BOUND_COL,
    LOWER_BOUND_UNIT_COL,
    LOWER_DROP_COL,
    RUN_OF_RIVER_REFERENCE_VOLUME_COL,
    SPEC_PRODUCTIVITY_COL,
    START_DATE_COL,
    UPPER_BOUND_COL,
    UPPER_BOUND_UNIT_COL,
    VOLUME_REGULATION_COL,
)
from app.model.operation.unit import Unit
from app.services.deck import accessors, entities, readers, temporal
from app.services.unitofwork import AbstractUnitOfWork


def _calc_turbined_flow(line: pd.Series) -> float:
    """Compute total turbined flow from conjunto/maquina data."""
    n = line["numero_conjuntos_maquinas"]
    nums = line[[f"maquinas_conjunto_{i}" for i in range(1, n + 1)]].to_numpy()
    flows = line[
        [f"vazao_nominal_conjunto_{i}" for i in range(1, n + 1)]
    ].to_numpy()
    return float(np.sum(nums * flows))


def hydro_volume_bounds(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    """
    Obtém um DataFrame com os limites cadastrais de volume armazenado
    de cada usina hidrelétrica.
    """
    val = cache.get("hydro_volume_bounds")
    if val is None:
        df = accessors.hidr(deck_cls, cache, uow).reset_index()
        hydro_codes = entities.hydro_code_order(deck_cls, cache, uow)
        df = df.loc[
            df[HYDRO_CODE_COL].isin(hydro_codes),
            [HYDRO_CODE_COL, "volume_minimo", "volume_maximo"],
        ].set_index(HYDRO_CODE_COL)
        df = df.rename(
            columns={
                "volume_minimo": LOWER_BOUND_COL,
                "volume_maximo": UPPER_BOUND_COL,
            }
        )
        df[LOWER_BOUND_UNIT_COL] = Unit.hm3_modif.value
        df[UPPER_BOUND_UNIT_COL] = Unit.hm3_modif.value
        ents = entities.hydro_eer_submarket_map(deck_cls, cache, uow)
        val = df.join(ents)
        cache["hydro_volume_bounds"] = val
    return val.copy()


def hydro_volume_bounds_with_changes(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("hydro_volume_bounds_with_changes")
    if val is None:
        hm3_df = hydro_volume_bounds(deck_cls, cache, uow)
        df = hm3_df.copy()
        df = readers.apply_modif_changes_to_hydros(
            deck_cls,
            cache,
            df,
            LOWER_BOUND_COL,
            LOWER_BOUND_UNIT_COL,
            VOLMIN,
            uow,
        )
        df = readers.apply_modif_changes_to_hydros(
            deck_cls,
            cache,
            df,
            UPPER_BOUND_COL,
            UPPER_BOUND_UNIT_COL,
            VOLMAX,
            uow,
        )
        # cast percentage bounds to hm3
        for col, unit_col in zip(
            [LOWER_BOUND_COL, UPPER_BOUND_COL],
            [LOWER_BOUND_UNIT_COL, UPPER_BOUND_UNIT_COL],
        ):
            bound_df = df.loc[df[unit_col] == Unit.perc_modif.value].copy()
            if not bound_df.empty:
                bound_df[col] = bound_df.apply(
                    lambda line, c=col: (
                        line[c]
                        * (
                            hm3_df.at[line.name, UPPER_BOUND_COL]
                            - hm3_df.at[line.name, LOWER_BOUND_COL]
                        )
                        / 100.0
                        + hm3_df.at[line.name, LOWER_BOUND_COL]
                    ),
                    axis=1,
                )
                df.loc[bound_df.index, col] = bound_df[col]
                df.loc[bound_df.index, unit_col] = Unit.hm3_modif.value
        val = df
        cache["hydro_volume_bounds_with_changes"] = val
    return val.copy()


def _hydro_volume_bounds_in_stages(
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
    consider_lower_changes: bool = True,
) -> pd.DataFrame:
    hm3_df = hydro_volume_bounds_with_changes(deck_cls, cache, uow)
    # expand to stages
    base = hm3_df.reset_index()
    num_hydros = base.shape[0]
    dates = np.array(
        temporal.stages_starting_dates_final_simulation(deck_cls, cache, uow)
    )
    num_stages = len(dates)
    hm3_expanded = pd.concat([base] * num_stages, ignore_index=True)
    hm3_expanded[START_DATE_COL] = np.repeat(dates, num_hydros)
    hm3_expanded = hm3_expanded.sort_values(
        [HYDRO_CODE_COL, START_DATE_COL]
    ).reset_index(drop=True)

    df = hm3_expanded.copy()
    if consider_lower_changes:
        df = readers.apply_modif_changes_to_hydros_in_stages(
            deck_cls,
            cache,
            df,
            LOWER_BOUND_COL,
            LOWER_BOUND_UNIT_COL,
            VMINT,
            uow,
        )
    df = readers.apply_modif_changes_to_hydros_in_stages(
        deck_cls, cache, df, UPPER_BOUND_COL, UPPER_BOUND_UNIT_COL, VMAXT, uow
    )
    # cast percentage bounds to hm3 using expanded reference
    for col, unit_col in zip(
        [LOWER_BOUND_COL, UPPER_BOUND_COL],
        [LOWER_BOUND_UNIT_COL, UPPER_BOUND_UNIT_COL],
    ):
        bound_df = df.loc[df[unit_col] == Unit.perc_modif.value].copy()
        if not bound_df.empty:
            bound_df[col] = bound_df.apply(
                lambda line, c=col: (
                    line[c]
                    * (
                        hm3_expanded.loc[line.name, UPPER_BOUND_COL]
                        - hm3_expanded.loc[line.name, LOWER_BOUND_COL]
                    )
                    / 100.0
                    + hm3_expanded.loc[line.name, LOWER_BOUND_COL]
                ),
                axis=1,
            )
            df.loc[bound_df.index, col] = bound_df[col]
            df.loc[bound_df.index, unit_col] = Unit.hm3_modif.value
    return df.copy()


def hydro_volume_bounds_in_stages(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("hydro_volume_bounds_in_stages")
    if val is None:
        val = _hydro_volume_bounds_in_stages(deck_cls, cache, uow, True)
        cache["hydro_volume_bounds_in_stages"] = val
    return val.copy()


def hydro_volume_bounds_in_stages_for_rescaling(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("hydro_volume_bounds_in_stages_for_rescaling")
    if val is None:
        val = _hydro_volume_bounds_in_stages(deck_cls, cache, uow, False)
        cache["hydro_volume_bounds_in_stages_for_rescaling"] = val
    return val.copy()


def hydro_turbined_flow_bounds(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("hydro_turbined_flow_bounds")
    if val is None:
        df = accessors.hidr(deck_cls, cache, uow).reset_index()
        hydro_codes = entities.hydro_code_order(deck_cls, cache, uow)
        df[UPPER_BOUND_COL] = df.apply(_calc_turbined_flow, axis=1)
        df[LOWER_BOUND_COL] = 0.0
        df = df.loc[
            df[HYDRO_CODE_COL].isin(hydro_codes),
            [HYDRO_CODE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL],
        ].set_index(HYDRO_CODE_COL)
        df[LOWER_BOUND_UNIT_COL] = Unit.m3s.value
        df[UPPER_BOUND_UNIT_COL] = Unit.m3s.value
        val = df.join(entities.hydro_eer_submarket_map(deck_cls, cache, uow))
        cache["hydro_turbined_flow_bounds"] = val
    return val.copy()


def _apply_turbined_flow_changes(
    df: pd.DataFrame, deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    modif = accessors.modif(deck_cls, cache, uow)
    for idx in df.index:
        changes = modif.modificacoes_usina(idx)
        if changes is not None:
            ngrp = [r for r in changes if isinstance(r, NUMCNJ)]
            if ngrp:
                df.at[idx, "numero_conjuntos_maquinas"] = ngrp[-1].numero
            for r in [r for r in changes if isinstance(r, NUMMAQ)]:
                df.at[idx, f"maquinas_conjunto_{r.conjunto}"] = (
                    r.numero_maquinas
                )
    return df


def hydro_turbined_flow_bounds_with_changes(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("hydro_turbined_flow_bounds_with_changes")
    if val is None:
        df = accessors.hidr(deck_cls, cache, uow).reset_index()
        hydro_codes = entities.hydro_code_order(deck_cls, cache, uow)
        df = _apply_turbined_flow_changes(df, deck_cls, cache, uow)
        df[UPPER_BOUND_COL] = df.apply(_calc_turbined_flow, axis=1)
        df[LOWER_BOUND_COL] = 0.0
        df = df.loc[
            df[HYDRO_CODE_COL].isin(hydro_codes),
            [HYDRO_CODE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL],
        ].set_index(HYDRO_CODE_COL)
        df[LOWER_BOUND_UNIT_COL] = Unit.m3s.value
        df[UPPER_BOUND_UNIT_COL] = Unit.m3s.value
        val = df.join(entities.hydro_eer_submarket_map(deck_cls, cache, uow))
        cache["hydro_turbined_flow_bounds_with_changes"] = val
    return val.copy()


def hydro_turbined_flow_bounds_in_stages(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    # TODO - analisar exph.dat
    val = cache.get("hydro_turbined_flow_bounds_in_stages")
    if val is None:
        from app.services.deck import misc as misc_mod

        m3s_df = hydro_turbined_flow_bounds_with_changes(deck_cls, cache, uow)
        m3s_df = _expand_hydro_to_stages(m3s_df, deck_cls, cache, uow)
        m3s_df = readers.apply_modif_changes_to_hydros_in_stages(
            deck_cls,
            cache,
            m3s_df,
            LOWER_BOUND_COL,
            LOWER_BOUND_UNIT_COL,
            TURBMINT,
            uow,
        )
        m3s_df = readers.apply_modif_changes_to_hydros_in_stages(
            deck_cls,
            cache,
            m3s_df,
            UPPER_BOUND_COL,
            UPPER_BOUND_UNIT_COL,
            TURBMAXT,
            uow,
        )
        m3s_df = _expand_to_blocks(m3s_df, deck_cls, cache, uow, misc_mod)
        val = m3s_df
        cache["hydro_turbined_flow_bounds_in_stages"] = val
    return val.copy()


def hydro_outflow_bounds(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("hydro_outflow_bounds")
    if val is None:
        df = accessors.hidr(deck_cls, cache, uow).reset_index()
        hydro_codes = entities.hydro_code_order(deck_cls, cache, uow)
        df = df.loc[
            df[HYDRO_CODE_COL].isin(hydro_codes),
            [HYDRO_CODE_COL, "vazao_minima_historica"],
        ].set_index(HYDRO_CODE_COL)
        df = df.rename(columns={"vazao_minima_historica": LOWER_BOUND_COL})
        df = df.astype({LOWER_BOUND_COL: float})
        df[UPPER_BOUND_COL] = float("inf")
        df[LOWER_BOUND_UNIT_COL] = Unit.m3s.value
        df[UPPER_BOUND_UNIT_COL] = Unit.m3s.value
        val = df.join(entities.hydro_eer_submarket_map(deck_cls, cache, uow))
        cache["hydro_outflow_bounds"] = val
    return val.copy()


def hydro_outflow_bounds_with_changes(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("hydro_outflow_bounds_with_changes")
    if val is None:
        # TODO - analisar modif.dat
        df = hydro_outflow_bounds(deck_cls, cache, uow)
        df = readers.apply_modif_changes_to_hydros(
            deck_cls,
            cache,
            df,
            LOWER_BOUND_COL,
            LOWER_BOUND_UNIT_COL,
            VAZMIN,
            uow,
        )
        val = df
        cache["hydro_outflow_bounds_with_changes"] = val
    return val.copy()


def hydro_outflow_bounds_in_stages(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("hydro_outflow_bounds_in_stages")
    if val is None:
        from app.services.deck import misc as misc_mod

        m3s_df = hydro_outflow_bounds_with_changes(deck_cls, cache, uow)
        m3s_df = _expand_hydro_to_stages(m3s_df, deck_cls, cache, uow)
        m3s_df = readers.apply_modif_changes_to_hydros_in_stages(
            deck_cls,
            cache,
            m3s_df,
            LOWER_BOUND_COL,
            LOWER_BOUND_UNIT_COL,
            VAZMINT,
            uow,
        )
        # Note: reset_index() (not drop=True) preserves index column for
        # compatibility with test expectations (13 columns total)
        m3s_df = m3s_df.reset_index()
        dates = np.array(
            temporal.stages_starting_dates_final_simulation(
                deck_cls, cache, uow
            )
        )
        num_stages = len(dates)
        num_hydros = m3s_df.shape[0] // num_stages
        num_blocks = misc_mod.num_blocks(deck_cls, cache, uow) + 1
        m3s_df = pd.concat([m3s_df] * num_blocks, ignore_index=True)
        m3s_df = m3s_df.sort_values([HYDRO_CODE_COL, START_DATE_COL])
        m3s_df[BLOCK_COL] = np.tile(
            np.arange(num_blocks), num_hydros * num_stages
        )
        m3s_df = m3s_df.sort_values(
            [HYDRO_CODE_COL, START_DATE_COL, BLOCK_COL]
        ).reset_index(drop=True)
        val = m3s_df
        cache["hydro_outflow_bounds_in_stages"] = val
    return val.copy()


def hydro_drops(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("hydro_drops")
    if val is None:
        df = accessors.hidr(deck_cls, cache, uow).reset_index()
        hydro_codes = entities.hydro_code_order(deck_cls, cache, uow)
        cols = HEIGHT_POLY_COLS + [
            HYDRO_CODE_COL,
            LOWER_DROP_COL,
            LOSS_KIND_COL,
            LOSS_COL,
            VOLUME_REGULATION_COL,
            RUN_OF_RIVER_REFERENCE_VOLUME_COL,
            SPEC_PRODUCTIVITY_COL,
        ]
        df = df.loc[df[HYDRO_CODE_COL].isin(hydro_codes), cols].set_index(
            HYDRO_CODE_COL
        )
        val = df.join(entities.hydro_eer_submarket_map(deck_cls, cache, uow))
        cache["hydro_drops"] = val
    return val.copy()


def hydro_drops_in_stages(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("hydro_drops_in_stages")
    if val is None:
        df = hydro_drops(deck_cls, cache, uow)
        df = _expand_hydro_to_stages(df, deck_cls, cache, uow)
        df = readers.apply_modif_changes_to_hydros_in_stages(
            deck_cls,
            cache,
            df,
            HEIGHT_POLY_COLS[0],
            LOWER_BOUND_UNIT_COL,
            CMONT,
            uow,
        )
        df = readers.apply_modif_changes_to_hydros_in_stages(
            deck_cls,
            cache,
            df,
            LOWER_DROP_COL,
            UPPER_BOUND_UNIT_COL,
            CFUGA,
            uow,
        )
        val = df
        cache["hydro_drops_in_stages"] = val
    return val.copy()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _expand_hydro_to_stages(
    df: pd.DataFrame, deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    df = df.reset_index()
    num_hydros = df.shape[0]
    dates = np.array(
        temporal.stages_starting_dates_final_simulation(deck_cls, cache, uow)
    )
    num_stages = len(dates)
    df = pd.concat([df] * num_stages, ignore_index=True)
    df[START_DATE_COL] = np.repeat(dates, num_hydros)
    return df.sort_values([HYDRO_CODE_COL, START_DATE_COL]).reset_index(
        drop=True
    )


def _expand_to_blocks(
    df: pd.DataFrame,
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
    misc_mod,
) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    dates = np.array(
        temporal.stages_starting_dates_final_simulation(deck_cls, cache, uow)
    )
    num_stages = len(dates)
    num_hydros = df.shape[0] // num_stages
    num_blocks = misc_mod.num_blocks(deck_cls, cache, uow) + 1
    df = pd.concat([df] * num_blocks, ignore_index=True)
    df = df.sort_values([HYDRO_CODE_COL, START_DATE_COL])
    df[BLOCK_COL] = np.tile(np.arange(num_blocks), num_hydros * num_stages)
    return df.sort_values(
        [HYDRO_CODE_COL, START_DATE_COL, BLOCK_COL]
    ).reset_index(drop=True)
