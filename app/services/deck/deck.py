from datetime import datetime
from logging import INFO, Logger
from typing import Any, Dict, List, Optional, TypeVar

import pandas as pd
import polars as pl
from inewave.newave import (
    Curva,
    Dger,
    Modif,
    Newavetim,
    Pmo,
)

from app.services.deck import accessors, entities
from app.services.deck import energy as _e
from app.services.deck import exchange as _ex
from app.services.deck import hydro as _hy
from app.services.deck import misc as _ms
from app.services.deck import policy as _pol
from app.services.deck import storage as _st
from app.services.deck import temporal as _tmp
from app.services.deck import thermal as _th
from app.services.unitofwork import AbstractUnitOfWork


# fmt: off
class Deck:
    T = TypeVar("T")
    logger: Optional[Logger] = None
    DECK_DATA_CACHING: Dict[str, Any] = {}

    @classmethod
    def _log(cls, msg: str, level: int = INFO) -> None:
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _c(cls) -> Dict[str, Any]:
        return cls.DECK_DATA_CACHING

    @classmethod
    def dger(cls, uow: AbstractUnitOfWork) -> Dger:
        return accessors.dger(cls, cls._c(), uow)
    @classmethod
    def pmo(cls, uow: AbstractUnitOfWork) -> Pmo:
        return accessors.pmo(cls, cls._c(), uow)
    @classmethod
    def curva(cls, uow: AbstractUnitOfWork) -> Curva:
        return accessors.curva(cls, cls._c(), uow)
    @classmethod
    def modif(cls, uow: AbstractUnitOfWork) -> Modif:
        return accessors.modif(cls, cls._c(), uow)
    @classmethod
    def confhd(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.confhd(cls, cls._c(), uow)
    @classmethod
    def clast(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.clast(cls, cls._c(), uow)
    @classmethod
    def term(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.term(cls, cls._c(), uow)
    @classmethod
    def manutt(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.manutt(cls, cls._c(), uow)
    @classmethod
    def expt(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.expt(cls, cls._c(), uow)
    @classmethod
    def hidr(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.hidr(cls, cls._c(), uow)
    @classmethod
    def newavetim(cls, uow: AbstractUnitOfWork) -> Newavetim:
        return accessors.newavetim(cls, cls._c(), uow)
    @classmethod
    def engnat(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.engnat(cls, cls._c(), uow)
    @classmethod
    def energiaf(cls, iteracao: int, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.energiaf(cls, uow, iteracao)
    @classmethod
    def enavazf(cls, iteracao: int, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.enavazf(cls, uow, iteracao)
    @classmethod
    def vazaof(cls, iteracao: int, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.vazaof(cls, uow, iteracao)
    @classmethod
    def energiab(cls, iteracao: int, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.energiab(cls, uow, iteracao)
    @classmethod
    def enavazb(cls, iteracao: int, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.enavazb(cls, uow, iteracao)
    @classmethod
    def vazaob(cls, iteracao: int, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.vazaob(cls, uow, iteracao)
    @classmethod
    def energias(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.energias(cls, uow)
    @classmethod
    def enavazs(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.enavazs(cls, uow)
    @classmethod
    def vazaos(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.vazaos(cls, uow)
    @classmethod
    def vazoes(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return accessors.vazoes(cls, cls._c(), uow)
    @classmethod
    def study_title(cls, uow: AbstractUnitOfWork) -> str:
        return accessors.study_title(cls, cls._c(), uow)
    @classmethod
    def version(cls, uow: AbstractUnitOfWork) -> str:
        return accessors.version(cls, cls._c(), uow)

    @classmethod
    def pre_study_period_starting_month(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.pre_study_period_starting_month(cls, cls._c(), uow)
    @classmethod
    def study_period_starting_month(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.study_period_starting_month(cls, cls._c(), uow)
    @classmethod
    def study_period_starting_year(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.study_period_starting_year(cls, cls._c(), uow)
    @classmethod
    def num_pre_study_period_years(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.num_pre_study_period_years(cls, cls._c(), uow)
    @classmethod
    def num_study_period_years(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.num_study_period_years(cls, cls._c(), uow)
    @classmethod
    def num_post_study_period_years_final_simulation(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.num_post_study_period_years_final_simulation(cls, cls._c(), uow)
    @classmethod
    def num_synthetic_scenarios_final_simulation(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.num_synthetic_scenarios_final_simulation(cls, cls._c(), uow)
    @classmethod
    def num_history_years(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.num_history_years(cls, cls._c(), uow)
    @classmethod
    def num_thermal_maintenance_years(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.num_thermal_maintenance_years(cls, cls._c(), uow)
    @classmethod
    def thermal_maintenance_end_date(cls, uow: AbstractUnitOfWork) -> datetime:
        return _tmp.thermal_maintenance_end_date(cls, cls._c(), uow)
    @classmethod
    def final_simulation_type(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.final_simulation_type(cls, cls._c(), uow)
    @classmethod
    def final_simulation_aggregation(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.final_simulation_aggregation(cls, cls._c(), uow)
    @classmethod
    def num_scenarios_final_simulation(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.num_scenarios_final_simulation(cls, cls._c(), uow)
    @classmethod
    def num_hydro_simulation_stages_policy(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.num_hydro_simulation_stages_policy(cls, cls._c(), uow, entities.eers(cls, cls._c(), uow).to_pandas())
    @classmethod
    def num_hydro_simulation_stages_final_simulation(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.num_hydro_simulation_stages_final_simulation(cls, cls._c(), uow, entities.eers(cls, cls._c(), uow).to_pandas())
    @classmethod
    def num_stages_with_past_tendency_period(cls, uow: AbstractUnitOfWork) -> int:
        return _tmp.num_stages_with_past_tendency_period(cls, cls._c(), uow)
    @classmethod
    def starting_date_with_past_tendency_period(cls, uow: AbstractUnitOfWork) -> datetime:
        return _tmp.starting_date_with_past_tendency_period(cls, cls._c(), uow)
    @classmethod
    def ending_date_with_post_study_period(cls, uow: AbstractUnitOfWork) -> datetime:
        return _tmp.ending_date_with_post_study_period(cls, cls._c(), uow)
    @classmethod
    def internal_stages_starting_dates_policy(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        return _tmp.internal_stages_starting_dates_policy(cls, cls._c(), uow)
    @classmethod
    def internal_stages_starting_dates_policy_with_past_tendency(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        return _tmp.internal_stages_starting_dates_policy_with_past_tendency(cls, cls._c(), uow)
    @classmethod
    def stages_starting_dates_final_simulation(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        return _tmp.stages_starting_dates_final_simulation(cls, cls._c(), uow)
    @classmethod
    def internal_stages_starting_dates_final_simulation(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        return _tmp.internal_stages_starting_dates_final_simulation(cls, cls._c(), uow)
    @classmethod
    def internal_stages_ending_dates_final_simulation(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        return _tmp.internal_stages_ending_dates_final_simulation(cls, cls._c(), uow)
    @classmethod
    def hydro_simulation_stages_ending_date_final_simulation(cls, uow: AbstractUnitOfWork) -> datetime:
        return _tmp.hydro_simulation_stages_ending_date_final_simulation(cls, cls._c(), uow, entities.eers(cls, cls._c(), uow).to_pandas())
    @classmethod
    def _configurations_pmo(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame | None":
        return _tmp.configurations_pmo(cls, cls._c(), uow)
    @classmethod
    def _configurations_dger(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _tmp.configurations_dger(cls, cls._c(), uow)
    @classmethod
    def configurations(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _tmp.configurations(cls, cls._c(), uow)

    @classmethod
    def submarkets(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.submarkets(cls, cls._c(), uow)
    @classmethod
    def eers(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.eers(cls, cls._c(), uow)
    @classmethod
    def hydros(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.hydros(cls, cls._c(), uow)
    @classmethod
    def thermals(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.thermals(cls, cls._c(), uow)
    @classmethod
    def eer_code_order(cls, uow: AbstractUnitOfWork) -> List[int]:
        return entities.eer_code_order(cls, cls._c(), uow)
    @classmethod
    def hydro_code_order(cls, uow: AbstractUnitOfWork) -> List[int]:
        return entities.hydro_code_order(cls, cls._c(), uow)
    @classmethod
    def hydro_eer_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.hydro_eer_submarket_map(cls, cls._c(), uow)
    @classmethod
    def eer_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.eer_submarket_map(cls, cls._c(), uow)
    @classmethod
    def thermal_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.thermal_submarket_map(cls, cls._c(), uow)
    @classmethod
    def hybrid_policy(cls, uow: AbstractUnitOfWork) -> bool:
        return entities.hybrid_policy(cls, cls._c(), uow)
    @classmethod
    def flow_diversion(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.flow_diversion(cls, cls._c(), uow)
    @classmethod
    def non_simulated_generation(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.non_simulated_generation(cls, cls._c(), uow)

    @classmethod
    def costs(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _ms.costs(cls, cls._c(), uow)
    @classmethod
    def num_iterations(cls, uow: AbstractUnitOfWork) -> int:
        return _ms.num_iterations(cls, cls._c(), uow)
    @classmethod
    def runtimes(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _ms.runtimes(cls, cls._c(), uow)
    @classmethod
    def num_blocks(cls, uow: AbstractUnitOfWork) -> int:
        return _ms.num_blocks(cls, cls._c(), uow)
    @classmethod
    def block_lengths(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _ms.block_lengths(cls, cls._c(), uow)
    @classmethod
    def models_wind_generation(cls, uow: AbstractUnitOfWork) -> int:
        return _ms.models_wind_generation(cls, cls._c(), uow)
    @classmethod
    def scenario_generation_model_type(cls, uow: AbstractUnitOfWork) -> int:
        return _ms.scenario_generation_model_type(cls, cls._c(), uow)
    @classmethod
    def scenario_generation_model_max_order(cls, uow: AbstractUnitOfWork) -> int:
        return _ms.scenario_generation_model_max_order(cls, cls._c(), uow)
    @classmethod
    def num_forward_series(cls, uow: AbstractUnitOfWork) -> int:
        return _ms.num_forward_series(cls, cls._c(), uow)

    # Energy
    @classmethod
    def convergence(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _e.convergence(cls, cls._c(), uow)
    @classmethod
    def _stored_energy_upper_bounds_inputs(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        result = _e.stored_energy_upper_bounds_inputs(cls, cls._c(), uow)
        return result.to_pandas() if result is not None else result
    @classmethod
    def _stored_energy_upper_bounds_pmo(cls, uow: AbstractUnitOfWork) -> pd.DataFrame | None:
        result = _e.stored_energy_upper_bounds_pmo(cls, cls._c(), uow)
        return result.to_pandas() if result is not None else result
    @classmethod
    def stored_energy_upper_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _e.stored_energy_upper_bounds(cls, cls._c(), uow).to_pandas()
    @classmethod
    def eer_stored_energy_lower_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _e.eer_stored_energy_lower_bounds(cls, cls._c(), uow).to_pandas()

    @classmethod
    def hydro_volume_bounds(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _hy.hydro_volume_bounds(cls, cls._c(), uow)
    @classmethod
    def hydro_volume_bounds_with_changes(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _hy.hydro_volume_bounds_with_changes(cls, cls._c(), uow)
    @classmethod
    def hydro_volume_bounds_in_stages(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _hy.hydro_volume_bounds_in_stages(cls, cls._c(), uow).to_pandas()
    @classmethod
    def hydro_volume_bounds_in_stages_for_rescaling(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _hy.hydro_volume_bounds_in_stages_for_rescaling(cls, cls._c(), uow).to_pandas()
    @classmethod
    def hydro_turbined_flow_bounds(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _hy.hydro_turbined_flow_bounds(cls, cls._c(), uow)
    @classmethod
    def hydro_turbined_flow_bounds_with_changes(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _hy.hydro_turbined_flow_bounds_with_changes(cls, cls._c(), uow)
    @classmethod
    def hydro_turbined_flow_bounds_in_stages(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        # SHIM: bounds.py still uses pandas; remove after polars migration of bounds.py
        return _hy.hydro_turbined_flow_bounds_in_stages(cls, cls._c(), uow).to_pandas()
    @classmethod
    def hydro_outflow_bounds(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _hy.hydro_outflow_bounds(cls, cls._c(), uow)
    @classmethod
    def hydro_outflow_bounds_with_changes(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _hy.hydro_outflow_bounds_with_changes(cls, cls._c(), uow)
    @classmethod
    def hydro_outflow_bounds_in_stages(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _hy.hydro_outflow_bounds_in_stages(cls, cls._c(), uow).to_pandas()
    @classmethod
    def hydro_drops(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _hy.hydro_drops(cls, cls._c(), uow)
    @classmethod
    def hydro_drops_in_stages(cls, uow: AbstractUnitOfWork) -> "pl.DataFrame":
        return _hy.hydro_drops_in_stages(cls, cls._c(), uow)

    @classmethod
    def _thermal_generation_bounds_term_manutt_expt(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _th._thermal_generation_bounds_term_manutt_expt(cls, cls._c(), uow).to_pandas()
    @classmethod
    def _thermal_generation_bounds_pmo(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        result = _th._thermal_generation_bounds_pmo(cls, cls._c(), uow)
        if result is None:
            return result  # type: ignore[return-value]
        return result.to_pandas()
    @classmethod
    def thermal_generation_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _th.thermal_generation_bounds(cls, cls._c(), uow).to_pandas()
    @classmethod
    def thermal_costs(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _th.thermal_costs(cls, cls._c(), uow).to_pandas()

    @classmethod
    def exchange_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _ex.exchange_bounds(cls, cls._c(), uow).to_pandas()
    @classmethod
    def exchange_block_limits(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _ex.exchange_block_limits(cls, cls._c(), uow)

    @classmethod
    def _initial_stored_energy_from_pmo(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        result = _st._initial_stored_energy_from_pmo(cls, cls._c(), uow)
        if result is None:
            return result  # type: ignore[return-value]
        return result.to_pandas()
    @classmethod
    def _initial_stored_energy_from_confhd_hidr(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        result = _st._initial_stored_energy_from_confhd_hidr(cls, cls._c(), uow)
        if result is None:
            return result  # type: ignore[return-value]
        return result.to_pandas()
    @classmethod
    def _initial_stored_volume_from_pmo(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        result = _st._initial_stored_volume_from_pmo(cls, cls._c(), uow)
        if result is None:
            return result  # type: ignore[return-value]
        return result.to_pandas()
    @classmethod
    def _initial_stored_volume_from_confhd_hidr(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        result = _st._initial_stored_volume_from_confhd_hidr(cls, cls._c(), uow)
        if result is None:
            return result  # type: ignore[return-value]
        return result.to_pandas()
    @classmethod
    def initial_stored_energy(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _st.initial_stored_energy(cls, cls._c(), uow).to_pandas()
    @classmethod
    def initial_stored_volume(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _st.initial_stored_volume(cls, cls._c(), uow).to_pandas()

    @classmethod
    def common_policy_df(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _pol.common_policy_df(cls, cls._c(), uow).to_pandas()
    @classmethod
    def policy_variable_units(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _pol.policy_variable_units(cls, cls._c(), uow).to_pandas()
# fmt: on
