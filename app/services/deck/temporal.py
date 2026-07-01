from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, List

import numpy as np
import polars as pl

if TYPE_CHECKING:
    import pandas as pd
from dateutil.relativedelta import relativedelta

from app.internal.constants import (
    START_DATE_COL,
    VALUE_COL,
)
from app.services.deck import accessors, readers
from app.services.unitofwork import AbstractUnitOfWork


def _dger_int(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
    key: str,
    attr: str,
    msg: str,
) -> int:
    """Read a single int field from dger, with caching."""
    val = cache.get(key)
    if val is None:
        val = readers.validate_data(
            deck_cls,
            getattr(accessors.dger(deck_cls, cache, uow), attr),
            int,
            msg,
        )
        cache[key] = val
    return int(val)


def pre_study_period_starting_month(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return _dger_int(
        deck_cls,
        cache,
        uow,
        "pre_study_period_starting_month",
        "mes_inicio_pre_estudo",
        "mes de inicio do pre-estudo (dger.dat)",
    )


def study_period_starting_month(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return _dger_int(
        deck_cls,
        cache,
        uow,
        "study_period_starting_month",
        "mes_inicio_estudo",
        "mes de inicio do estudo (dger.dat)",
    )


def study_period_starting_year(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return _dger_int(
        deck_cls,
        cache,
        uow,
        "study_period_starting_year",
        "ano_inicio_estudo",
        "ano de inicio do estudo (dger.dat)",
    )


def num_pre_study_period_years(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return _dger_int(
        deck_cls,
        cache,
        uow,
        "num_pre_study_period_years",
        "num_anos_pre_estudo",
        "numero de anos do pre-estudo (dger.dat)",
    )


def num_study_period_years(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return _dger_int(
        deck_cls,
        cache,
        uow,
        "num_study_period_years",
        "num_anos_estudo",
        "numero de anos do estudo (dger.dat)",
    )


def num_post_study_period_years_final_simulation(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return _dger_int(
        deck_cls,
        cache,
        uow,
        "num_post_study_period_years_final_simulation",
        "num_anos_pos_sim_final",
        "numero de anos do pos-estudo na simulacao final (dger.dat)",
    )


def num_synthetic_scenarios_final_simulation(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return _dger_int(
        deck_cls,
        cache,
        uow,
        "num_synthetic_scenarios_final_simulation",
        "num_series_sinteticas",
        "numero de series sinteticas na simulacao final (dger.dat)",
    )


def num_thermal_maintenance_years(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return _dger_int(
        deck_cls,
        cache,
        uow,
        "num_thermal_maintenance_years",
        "num_anos_manutencao_utes",
        "numero de anos com manutencoes de UTEs (dger.dat)",
    )


def final_simulation_type(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return _dger_int(
        deck_cls,
        cache,
        uow,
        "final_simulation_type",
        "tipo_simulacao_final",
        "tipo de simulacao final (dger.dat)",
    )


def final_simulation_aggregation(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return _dger_int(
        deck_cls,
        cache,
        uow,
        "final_simulation_aggregation",
        "agregacao_simulacao_final",
        "agregacao da simulacao final (dger.dat)",
    )


def num_history_years(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    val = cache.get("num_history_years")
    if val is None:
        shist = readers.get_shist(deck_cls, uow)
        span = readers.validate_data(
            deck_cls, shist.varredura, int, "tipo de varredura (sfhist.dat)"
        )
        if span == 1:
            dger = accessors.dger(deck_cls, cache, uow)
            _start_year_raw = dger.ano_inicial_historico
            if _start_year_raw is None:
                raise RuntimeError(
                    "ano_inicial_historico not found in dger.dat"
                )
            start_year: int = _start_year_raw
            num_input_years = (
                accessors.vazoes(deck_cls, cache, uow).shape[0] // 12
            )
            end_year = start_year + num_input_years - 1
            study_month = study_period_starting_month(deck_cls, cache, uow)
            last_year_offset = 2 if study_month != 1 else 1
            study_year = study_period_starting_year(deck_cls, cache, uow)
            last_year = min(end_year, study_year) - last_year_offset
            _span_start_raw = shist.ano_inicio_varredura
            if _span_start_raw is None:
                raise RuntimeError(
                    "ano_inicio_varredura not found in shist.dat"
                )
            span_start: int = _span_start_raw
            val = last_year - span_start + 1
        else:
            _anos_raw = shist.anos_inicio_simulacoes
            if _anos_raw is None:
                raise RuntimeError(
                    "anos_inicio_simulacoes not found in shist.dat"
                )
            val = len(_anos_raw)
        cache["num_history_years"] = val
    return val


def thermal_maintenance_end_date(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> datetime:
    val = cache.get("thermal_maintenance_end_date")
    if val is None:
        starting_year = study_period_starting_year(deck_cls, cache, uow)
        num_years = num_thermal_maintenance_years(deck_cls, cache, uow)
        val = datetime(starting_year + num_years, 1, 1)
        cache["thermal_maintenance_end_date"] = val
    return val


def num_scenarios_final_simulation(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    val = cache.get("num_scenarios_final_simulation")
    if val is None:
        if final_simulation_type(deck_cls, cache, uow) == 2:
            val = readers.get_num_scenarios_from_output(deck_cls, uow)
        else:
            val = num_synthetic_scenarios_final_simulation(deck_cls, cache, uow)
        cache["num_scenarios_final_simulation"] = val
    return val


def scenario_generation_model_type(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return _dger_int(
        deck_cls,
        cache,
        uow,
        "scenario_generation_model_type",
        "consideracao_media_anual_afluencias",
        "opcao do modelo PAR(p) (dger.dat)",
    )


def scenario_generation_model_max_order(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return _dger_int(
        deck_cls,
        cache,
        uow,
        "scenario_generation_model_max_order",
        "ordem_maxima_parp",
        "ordem maxima do modelo PAR(p) (dger.dat)",
    )


def num_stages_with_past_tendency_period(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    scenario_model = scenario_generation_model_type(deck_cls, cache, uow)
    maximum_model_order = scenario_generation_model_max_order(
        deck_cls, cache, uow
    )
    return 12 if scenario_model != 0 else maximum_model_order


def starting_date_with_past_tendency_period(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> datetime:
    starting_year = study_period_starting_year(deck_cls, cache, uow)
    past_stages = num_stages_with_past_tendency_period(deck_cls, cache, uow)
    result: datetime = datetime(
        year=starting_year, month=1, day=1
    ) - relativedelta(months=past_stages)
    return result


def ending_date_with_post_study_period(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> datetime:
    y = study_period_starting_year(deck_cls, cache, uow)
    n = num_study_period_years(deck_cls, cache, uow)
    p = _npost(deck_cls, cache, uow)
    return datetime(year=y + n + p - 1, month=12, day=1)


def num_hydro_simulation_stages_policy(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
    eers_df: "pd.DataFrame",
) -> int:
    val = cache.get("num_hydro_simulation_stages_policy")
    if val is None:
        starting_year = study_period_starting_year(deck_cls, cache, uow)
        starting_month = study_period_starting_month(deck_cls, cache, uow)
        hydro_sim_ending_month = eers_df["mes_fim_individualizado"].iloc[0]
        hydro_sim_ending_year = eers_df["ano_fim_individualizado"].iloc[0]
        if not np.isnan(hydro_sim_ending_month) and not np.isnan(
            hydro_sim_ending_year
        ):
            study_starting_date = datetime(
                year=starting_year, month=starting_month, day=1
            )
            hydro_sim_ending_date = datetime(
                year=int(hydro_sim_ending_year),
                month=int(hydro_sim_ending_month),
                day=1,
            )
            delta = hydro_sim_ending_date - study_starting_date
            val = int(round(delta / timedelta(days=30)))
        else:
            val = 0
        cache["num_hydro_simulation_stages_policy"] = val
    return val


def num_hydro_simulation_stages_final_simulation(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
    eers_df: "pd.DataFrame",
) -> int:
    val = cache.get("num_hydro_simulation_stages_final_simulation")
    if val is None:
        aggregation = final_simulation_aggregation(deck_cls, cache, uow)
        starting_month = study_period_starting_month(deck_cls, cache, uow)
        study_years = num_study_period_years(deck_cls, cache, uow)
        post_study_years = _npost(deck_cls, cache, uow)
        if aggregation == 1:
            val = (study_years + post_study_years) * 12 - (starting_month - 1)
        else:
            val = num_hydro_simulation_stages_policy(
                deck_cls, cache, uow, eers_df
            )
        cache["num_hydro_simulation_stages_final_simulation"] = val
    return val


def _month_range(start: datetime, end: datetime) -> List[datetime]:
    dates: List[datetime] = []
    current = datetime(start.year, start.month, 1)
    stop = datetime(end.year, end.month, 1)
    while current <= stop:
        dates.append(current)
        current = current + relativedelta(months=1)
    return dates


def _npost(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    """Alias for num_post_study_period_years_final_simulation."""
    return num_post_study_period_years_final_simulation(deck_cls, cache, uow)


def internal_stages_starting_dates_policy(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> List[datetime]:
    key = "internal_stages_starting_dates_policy"
    cached: List[datetime] | None = cache.get(key)
    if cached is not None:
        return cached
    y = study_period_starting_year(deck_cls, cache, uow)
    n = num_study_period_years(deck_cls, cache, uow)
    val = _month_range(datetime(y, 1, 1), datetime(y + n - 1, 12, 1))
    cache[key] = val
    return val


def internal_stages_starting_dates_policy_with_past_tendency(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> List[datetime]:
    key = "internal_stages_starting_dates_policy_with_past_tendency"
    cached: List[datetime] | None = cache.get(key)
    if cached is not None:
        return cached
    y = study_period_starting_year(deck_cls, cache, uow)
    n = num_study_period_years(deck_cls, cache, uow)
    val = _month_range(datetime(y - 1, 1, 1), datetime(y + n - 1, 12, 1))
    cache[key] = val
    return val


def stages_starting_dates_final_simulation(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> List[datetime]:
    key = "stages_starting_dates_final_simulation"
    cached: List[datetime] | None = cache.get(key)
    if cached is not None:
        return cached
    y = study_period_starting_year(deck_cls, cache, uow)
    m = study_period_starting_month(deck_cls, cache, uow)
    n = num_study_period_years(deck_cls, cache, uow)
    p = num_post_study_period_years_final_simulation(deck_cls, cache, uow)
    val = _month_range(datetime(y, m, 1), datetime(y + n + p - 1, 12, 1))
    cache[key] = val
    return val


def internal_stages_starting_dates_final_simulation(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> List[datetime]:
    key = "internal_stages_starting_dates_final_simulation"
    cached: List[datetime] | None = cache.get(key)
    if cached is not None:
        return cached
    y = study_period_starting_year(deck_cls, cache, uow)
    n = num_study_period_years(deck_cls, cache, uow)
    p = num_post_study_period_years_final_simulation(deck_cls, cache, uow)
    val = _month_range(datetime(y, 1, 1), datetime(y + n + p - 1, 12, 1))
    cache[key] = val
    return val


def internal_stages_ending_dates_final_simulation(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> List[datetime]:
    key = "internal_stages_ending_dates_final_simulation"
    cached: List[datetime] | None = cache.get(key)
    if cached is not None:
        return cached
    val = [
        d + relativedelta(months=1)
        for d in internal_stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
    ]
    cache[key] = val
    return val


def hydro_simulation_stages_ending_date_final_simulation(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
    eers_df: "pd.DataFrame",
) -> datetime:
    val = cache.get("hydro_simulation_stages_ending_date_final_simulation")
    if val is None:
        starting_year = study_period_starting_year(deck_cls, cache, uow)
        aggregation = final_simulation_aggregation(deck_cls, cache, uow)
        study_years = num_study_period_years(deck_cls, cache, uow)
        post_years = _npost(deck_cls, cache, uow)
        default_end = datetime(
            year=starting_year + study_years + post_years, month=1, day=1
        )
        if (
            aggregation == 1
            or eers_df["ano_fim_individualizado"].isna().sum() > 0
        ):
            val = default_end
        else:
            val = datetime(
                year=int(eers_df["ano_fim_individualizado"].iloc[0]),
                month=int(eers_df["mes_fim_individualizado"].iloc[0]),
                day=1,
            )
        cache["hydro_simulation_stages_ending_date_final_simulation"] = val
    return val


def configurations_pmo(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> "pl.DataFrame | None":
    pmo = accessors.pmo(deck_cls, cache, uow)
    configurations = pmo.configuracoes_qualquer_modificacao
    if configurations is not None:
        configurations = pl.from_pandas(configurations).rename(
            {"data": START_DATE_COL}
        )
        return configurations
    return None


def configurations_dger(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> "pl.DataFrame":
    dates = stages_starting_dates_final_simulation(deck_cls, cache, uow)
    configurations = list(range(1, len(dates) + 1))
    return pl.DataFrame({START_DATE_COL: dates, VALUE_COL: configurations})


def configurations(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> "pl.DataFrame":
    val = cache.get("configurations")
    if val is None:
        val = configurations_pmo(deck_cls, cache, uow)
        if val is None:
            val = configurations_dger(deck_cls, cache, uow)
        cache["configurations"] = val
    return val


def consider_post_study_years(
    deck_cls: Any,
    cache: Dict[str, Any],
    df: "pl.DataFrame",
    uow: AbstractUnitOfWork,
) -> "pl.DataFrame":
    num_years_to_add = _npost(deck_cls, cache, uow)
    if num_years_to_add == 0:
        return df
    last_year = df[START_DATE_COL].dt.year().max()
    df_last_year = df.filter(pl.col(START_DATE_COL).dt.year() == last_year)
    dfs_post = []
    for post_year in range(1, num_years_to_add + 1):
        df_post = df_last_year.with_columns(
            pl.col(START_DATE_COL)
            .dt.offset_by(f"{post_year}y")
            .alias(START_DATE_COL)
        )
        dfs_post.append(df_post)
    return pl.concat([df] + dfs_post)
