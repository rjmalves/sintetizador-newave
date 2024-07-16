import numpy as np
import pandas as pd
from datetime import datetime
from app.services.unitofwork import factory
from app.services.deck.deck import Deck
from tests.conftest import DECK_TEST_DIR, q
from app.internal.constants import (
    START_DATE_COL,
    VALUE_COL,
    UPPER_BOUND_COL,
    LOWER_BOUND_COL,
)

uow = factory("FS", DECK_TEST_DIR, q)
deck = Deck()


def test_pre_study_period_starting_month(test_settings):
    val = deck.pre_study_period_starting_month(uow)
    assert val == 1


def test_study_period_starting_month(test_settings):
    val = deck.study_period_starting_month(uow)
    assert val == 10


def test_study_period_starting_year(test_settings):
    val = deck.study_period_starting_year(uow)
    assert val == 2023


def test_num_pre_study_period_years(test_settings):
    val = deck.num_pre_study_period_years(uow)
    assert val == 0


def test_num_study_period_years(test_settings):
    val = deck.num_study_period_years(uow)
    assert val == 5


def test_num_post_study_period_years_final_simulation(test_settings):
    val = deck.num_post_study_period_years_final_simulation(uow)
    assert val == 0


def test_num_synthetic_scenarios_final_simulation(test_settings):
    val = deck.num_synthetic_scenarios_final_simulation(uow)
    assert val == 7


def test_num_history_years(test_settings):
    val = deck.num_history_years(uow)
    assert val == 89


def test_num_thermal_maintenance_years(test_settings):
    val = deck.num_thermal_maintenance_years(uow)
    assert val == 1


def test_thermal_maintenance_end_date(test_settings):
    val = deck.thermal_maintenance_end_date(uow)
    assert val == datetime(2024, 1, 1)


def test_final_simulation_type(test_settings):
    val = deck.final_simulation_type(uow)
    assert val == 1


def test_final_simulation_aggregation(test_settings):
    val = deck.final_simulation_aggregation(uow)
    assert val == 1


def test_num_scenarios_final_simulation(test_settings):
    val = deck.num_scenarios_final_simulation(uow)
    assert val == 7


def test_num_hydro_simulation_stages_policy(test_settings):
    val = deck.num_hydro_simulation_stages_policy(uow)
    assert val == 12


def test_num_hydro_simulation_stages_final_simulation(test_settings):
    val = deck.num_hydro_simulation_stages_final_simulation(uow)
    assert val == 51


def test_models_wind_generation(test_settings):
    val = deck.models_wind_generation(uow)
    assert val == 0


def test_scenario_generation_model_type(test_settings):
    val = deck.scenario_generation_model_type(uow)
    assert val == 3


def test_starting_date_with_past_tendency_period(test_settings):
    val = deck.starting_date_with_past_tendency_period(uow)
    assert val == datetime(2022, 1, 1)


def test_ending_date_with_post_study_period(test_settings):
    val = deck.ending_date_with_post_study_period(uow)
    assert val == datetime(2027, 12, 1)


def test_internal_stages_starting_dates_policy(test_settings):
    val = deck.internal_stages_starting_dates_policy(uow)
    assert (
        val
        == pd.date_range(
            datetime(2023, 1, 1), datetime(2027, 12, 1), freq="MS"
        ).tolist()
    )


def test_internal_stages_starting_dates_policy_with_past_tendency(
    test_settings,
):
    val = deck.internal_stages_starting_dates_policy_with_past_tendency(uow)
    assert (
        val
        == pd.date_range(
            datetime(2022, 1, 1), datetime(2027, 12, 1), freq="MS"
        ).tolist()
    )


def test_stages_starting_dates_final_simulation(test_settings):
    val = deck.stages_starting_dates_final_simulation(uow)
    assert (
        val
        == pd.date_range(
            datetime(2023, 10, 1), datetime(2027, 12, 1), freq="MS"
        ).tolist()
    )


def test_internal_stages_starting_dates_final_simulation(test_settings):
    val = deck.internal_stages_starting_dates_final_simulation(uow)
    assert (
        val
        == pd.date_range(
            datetime(2023, 1, 1), datetime(2027, 12, 1), freq="MS"
        ).tolist()
    )


def test_internal_stages_ending_dates_final_simulation(test_settings):
    val = deck.internal_stages_ending_dates_final_simulation(uow)
    assert (
        val
        == pd.date_range(
            datetime(2023, 2, 1), datetime(2028, 1, 1), freq="MS"
        ).tolist()
    )


def test_hydro_simulation_stages_ending_date_final_simulation(test_settings):
    val = deck.hydro_simulation_stages_ending_date_final_simulation(uow)
    assert val == datetime(2028, 1, 1)


def test__configurations_pmo(test_settings):
    val = deck._configurations_pmo(uow)
    assert val.equals(
        pd.DataFrame(
            data={
                START_DATE_COL: pd.date_range(
                    datetime(2023, 1, 1), datetime(2032, 12, 1), freq="MS"
                ),
                VALUE_COL: [1] * 9 + list(range(1, 52)) + [51] * 60,
            }
        )
    )


def test__configurations_dger(test_settings):
    val = deck._configurations_dger(uow)
    assert val.equals(
        pd.DataFrame(
            data={
                START_DATE_COL: pd.date_range(
                    datetime(2023, 10, 1), datetime(2027, 12, 1), freq="MS"
                ),
                VALUE_COL: list(range(1, 52)),
            }
        )
    )


def test_configurations(test_settings):
    val = deck.configurations(uow)
    val_pmo = deck._configurations_pmo(uow)
    assert val.equals(val_pmo)


def test_eer_stored_energy_lower_bounds(test_settings):
    val = deck.eer_stored_energy_lower_bounds(uow)
    assert val.shape == (12 * 51, 6)


def test__stored_energy_upper_bounds_inputs(test_settings):
    val = deck._stored_energy_upper_bounds_inputs(uow)
    val_pmo = deck._stored_energy_upper_bounds_pmo(uow)
    assert np.allclose(val[VALUE_COL], val_pmo[VALUE_COL], rtol=1e-2)


def test__stored_energy_upper_bounds_pmo(test_settings):
    val = deck._stored_energy_upper_bounds_pmo(uow)
    assert val.shape == (12 * 51, 7)


def test_stored_energy_upper_bounds(test_settings):
    val = deck.stored_energy_upper_bounds(uow)
    assert val.equals(deck._stored_energy_upper_bounds_pmo(uow))


def test_convergence(test_settings):
    val = deck.convergence(uow)
    assert val.shape == (30, 8)


def test__thermal_generation_bounds_term_manutt_expt(test_settings):
    val = deck._thermal_generation_bounds_term_manutt_expt(uow)
    val_pmo = deck._thermal_generation_bounds_pmo(uow)
    assert np.allclose(
        val[LOWER_BOUND_COL], val_pmo[LOWER_BOUND_COL], rtol=1e-2
    )
    val2 = val.copy()
    val2[UPPER_BOUND_COL] -= val_pmo[UPPER_BOUND_COL]
    assert np.allclose(
        val[UPPER_BOUND_COL], val_pmo[UPPER_BOUND_COL], rtol=1e-2
    )


def test__thermal_generation_bounds_pmo(test_settings):
    val = deck._thermal_generation_bounds_pmo(uow)
    assert val.shape == (126 * 51, 5)


def test_thermal_generation_bounds(test_settings):
    val = deck.thermal_generation_bounds(uow)
    val_pmo = deck._thermal_generation_bounds_pmo(uow)
    assert val[UPPER_BOUND_COL].equals(val_pmo[UPPER_BOUND_COL])
    assert val[LOWER_BOUND_COL].equals(val_pmo[LOWER_BOUND_COL])


def test_exchange_bounds(test_settings):
    val = deck.exchange_bounds(uow)
    assert val.shape == (12 * 4 * 60, 5)


def test_costs(test_settings):
    val = deck.costs(uow)
    assert val.shape == (32, 4)


def test_num_iterations(test_settings):
    val = deck.num_iterations(uow)
    assert val == 10


def test_runtimes(test_settings):
    val = deck.runtimes(uow)
    assert val.shape == (5, 2)


def test_submarkets(test_settings):
    val = deck.submarkets(uow)
    assert val.shape == (5, 5)


def test_eers(test_settings):
    val = deck.eers(uow)
    assert val.shape == (12, 4)


def test_hybrid_policy(test_settings):
    val = deck.hybrid_policy(uow)
    assert val


def test_hydros(test_settings):
    val = deck.hydros(uow)
    assert val.shape == (165, 9)


def test_flow_diversion(test_settings):
    val = deck.flow_diversion(uow)
    assert val.shape == (165 * 51, 8)


def test_hydro_volume_bounds(test_settings):
    val = deck.hydro_volume_bounds(uow)
    assert val.shape == (165, 9)


def test_hydro_volume_bounds_with_changes(test_settings):
    val = deck.hydro_volume_bounds_with_changes(uow)
    assert val.shape == (165, 9)


def test_hydro_volume_bounds_in_stages(test_settings):
    val = deck.hydro_volume_bounds_in_stages(uow)
    assert val.shape == (165 * 51, 11)


def test_hydro_turbined_flow_bounds(test_settings):
    val = deck.hydro_turbined_flow_bounds(uow)
    assert val.shape == (165, 9)


def test_hydro_turbined_flow_bounds_with_changes(test_settings):
    val = deck.hydro_turbined_flow_bounds_with_changes(uow)
    assert val.shape == (165, 9)


def test_hydro_turbined_flow_bounds_in_stages(test_settings):
    val = deck.hydro_turbined_flow_bounds_in_stages(uow)
    assert val.shape == (165 * 51 * 4, 12)


def test_hydro_outflow_bounds(test_settings):
    val = deck.hydro_outflow_bounds(uow)
    assert val.shape == (165, 9)


def test_hydro_outflow_bounds_with_changes(test_settings):
    val = deck.hydro_outflow_bounds_with_changes(uow)
    assert val.shape == (165, 9)


def test_hydro_outflow_bounds_in_stages(test_settings):
    val = deck.hydro_outflow_bounds_in_stages(uow)
    assert val.shape == (165 * 51 * 4, 13)


def test_hydro_drops(test_settings):
    val = deck.hydro_drops(uow)
    assert val.shape == (165, 16)


def test_hydro_drops_in_stages(test_settings):
    val = deck.hydro_drops_in_stages(uow)
    assert val.shape == (165 * 51, 20)


def test_thermals(test_settings):
    val = deck.thermals(uow)
    assert val.shape == (126, 3)


def test_num_blocks(test_settings):
    val = deck.num_blocks(uow)
    assert val == 3


def test_block_lengths(test_settings):
    val = deck.block_lengths(uow)
    assert val.shape == (240, 3)


def test_exchange_block_limits(test_settings):
    val = deck.exchange_block_limits(uow)
    assert val.shape == (4 * 3 * 4 * 60, 5)


def test__initial_stored_energy_from_confhd_hidr(test_settings):
    val = deck._initial_stored_energy_from_confhd_hidr(uow)
    val_pmo = deck._initial_stored_energy_from_pmo(uow)
    assert np.allclose(val["valor_MWmes"], val_pmo["valor_MWmes"], rtol=1e-2)


def test__initial_stored_energy_from_pmo(test_settings):
    val = deck._initial_stored_energy_from_pmo(uow)
    assert val.shape == (12, 3)


def test_initial_stored_energy(test_settings):
    val = deck.initial_stored_energy(uow)
    assert val.equals(deck._initial_stored_energy_from_pmo(uow))


def test__initial_stored_volume_from_pmo(test_settings):
    val = deck._initial_stored_volume_from_pmo(uow)
    assert val.shape == (165, 4)


def test__initial_stored_volume_from_confhd_hidr(test_settings):
    val = deck._initial_stored_volume_from_confhd_hidr(uow).dropna()
    val_pmo = deck._initial_stored_volume_from_pmo(uow).dropna()
    indices_comuns = val.index.intersection(val_pmo.index)
    assert len(indices_comuns) == 73
    assert np.allclose(
        val.loc[indices_comuns, "valor_hm3"],
        val_pmo.loc[indices_comuns, "valor_hm3"],
        atol=1e-1,
    )


def test_initial_stored_volume(test_settings):
    val = deck.initial_stored_volume(uow)
    assert val.equals(deck._initial_stored_volume_from_pmo(uow))


def test_eer_code_order(test_settings):
    val = deck.eer_code_order(uow)
    assert len(val) == 12


def test_hydro_code_order(test_settings):
    val = deck.hydro_code_order(uow)
    assert len(val) == 165


def test_eer_submarket_map(test_settings):
    val = deck.eer_submarket_map(uow)
    assert val.shape == (12, 3)


def test_thermal_submarket_map(test_settings):
    val = deck.thermal_submarket_map(uow)
    assert val.shape == (126, 3)
