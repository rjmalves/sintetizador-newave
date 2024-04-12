from unittest.mock import patch, MagicMock
import pandas as pd
from typing import Optional, Tuple

from app.services.unitofwork import factory
from app.model.scenario.scenariosynthesis import ScenarioSynthesis, UNITS
from app.services.synthesis.scenario import ScenarioSynthetizer
from app.internal.constants import SCENARIO_SYNTHESIS_METADATA_OUTPUT

from tests.conftest import DECK_TEST_DIR, q

uow = factory("FS", DECK_TEST_DIR, q)


def __valida_metadata(chave: str, df_metadata: pd.DataFrame):
    s = ScenarioSynthesis.factory(chave)
    assert s is not None
    assert str(s) in df_metadata["chave"].tolist()
    assert s.variable.short_name in df_metadata["nome_curto_variavel"].tolist()
    assert s.variable.long_name in df_metadata["nome_longo_variavel"].tolist()
    assert (
        s.spatial_resolution.value
        in df_metadata["nome_curto_agregacao"].tolist()
    )
    assert (
        s.spatial_resolution.long_name
        in df_metadata["nome_longo_agregacao"].tolist()
    )
    assert s.step.value in df_metadata["nome_curto_etapa"].tolist()
    assert s.step.long_name in df_metadata["nome_longo_etapa"].tolist()
    unit_str = UNITS[s].value if s in UNITS else ""
    assert unit_str in df_metadata["unidade"].tolist()


def __sintetiza_com_mock(synthesis_str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize([synthesis_str], uow)
        ScenarioSynthetizer.clear_cache()
    m.assert_called()
    df = __obtem_dados_sintese_mock(synthesis_str, m)
    df_meta = __obtem_dados_sintese_mock(SCENARIO_SYNTHESIS_METADATA_OUTPUT, m)
    assert df is not None
    assert df_meta is not None
    return df, df_meta


def __sintetiza_com_mock_wildcard(synthesis_str) -> pd.DataFrame:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize([synthesis_str], uow)
        ScenarioSynthetizer.clear_cache()
    m.assert_called()
    df_meta = __obtem_dados_sintese_mock(SCENARIO_SYNTHESIS_METADATA_OUTPUT, m)
    assert df_meta is not None
    return df_meta


def __obtem_dados_sintese_mock(
    chave: str, mock: MagicMock
) -> Optional[pd.DataFrame]:
    for c in mock.mock_calls:
        if c.args[1] == chave:
            return c.args[0]
    return None


def test_sintese_enaa_ree_for(test_settings):
    synthesis_str = "ENAA_REE_FOR"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_enaa_ree_bkw(test_settings):
    synthesis_str = "ENAA_REE_BKW"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_enaa_ree_sf(test_settings):
    synthesis_str = "ENAA_REE_SF"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_enaa_sbm_for(test_settings):
    synthesis_str = "ENAA_SBM_FOR"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_enaa_sbm_bkw(test_settings):
    synthesis_str = "ENAA_SBM_BKW"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_enaa_sbm_sf(test_settings):
    synthesis_str = "ENAA_SBM_SF"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_enaa_sin_for(test_settings):
    synthesis_str = "ENAA_SIN_FOR"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_enaa_sin_bkw(test_settings):
    synthesis_str = "ENAA_SIN_BKW"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_enaa_sin_sf(test_settings):
    synthesis_str = "ENAA_SIN_SF"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_qinc_uhe_for(test_settings):
    synthesis_str = "QINC_UHE_FOR"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_qinc_uhe_bkw(test_settings):
    synthesis_str = "QINC_UHE_BKW"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_qinc_uhe_sf(test_settings):
    synthesis_str = "QINC_UHE_SF"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_qinc_ree_for(test_settings):
    synthesis_str = "QINC_REE_FOR"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_qinc_ree_bkw(test_settings):
    synthesis_str = "QINC_REE_BKW"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_qinc_ree_sf(test_settings):
    synthesis_str = "QINC_REE_SF"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_qinc_sbm_for(test_settings):
    synthesis_str = "QINC_SBM_FOR"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_qinc_sbm_bkw(test_settings):
    synthesis_str = "QINC_SBM_BKW"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_qinc_sbm_sf(test_settings):
    synthesis_str = "QINC_SBM_SF"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_qinc_sin_for(test_settings):
    synthesis_str = "QINC_SIN_FOR"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_qinc_sin_bkw(test_settings):
    synthesis_str = "QINC_SIN_BKW"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)


def test_sintese_qinc_sin_sf(test_settings):
    synthesis_str = "QINC_SIN_SF"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    __valida_metadata(synthesis_str, df_meta)
