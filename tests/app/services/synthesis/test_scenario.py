from unittest.mock import patch, MagicMock
import pandas as pd


from app.services.unitofwork import factory
from app.model.scenario.scenariosynthesis import ScenarioSynthesis
from app.services.synthesis.scenario import ScenarioSynthetizer

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
    unit_str = (
        ScenarioSynthetizer.UNITS[s].value
        if s in ScenarioSynthetizer.UNITS
        else ""
    )
    assert unit_str in df_metadata["unidade"].tolist()


def test_sintese_enaa_ree_for(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_REE_FOR"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAA_REE_FOR", df_meta)


def test_sintese_enaa_ree_bkw(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_REE_BKW"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAA_REE_BKW", df_meta)


def test_sintese_enaa_ree_sf(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_REE_SF"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAA_REE_SF", df_meta)


def test_sintese_enaa_sbm_for(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_SBM_FOR"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAA_SBM_FOR", df_meta)


def test_sintese_enaa_sbm_bkw(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_SBM_BKW"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAA_SBM_BKW", df_meta)


def test_sintese_enaa_sbm_sf(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_SBM_SF"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAA_SBM_SF", df_meta)


def test_sintese_enaa_sin_for(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_SIN_FOR"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAA_SIN_FOR", df_meta)


def test_sintese_enaa_sin_bkw(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_SIN_BKW"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAA_SIN_BKW", df_meta)


def test_sintese_enaa_sin_sf(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_SIN_SF"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAA_SIN_SF", df_meta)


def test_sintese_qinc_uhe_for(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_UHE_FOR"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_UHE_FOR", df_meta)


def test_sintese_qinc_uhe_bkw(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_UHE_BKW"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_UHE_BKW", df_meta)


def test_sintese_qinc_uhe_sf(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_UHE_SF"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_UHE_SF", df_meta)


def test_sintese_qinc_ree_for(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_REE_FOR"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_REE_FOR", df_meta)


def test_sintese_qinc_ree_bkw(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_REE_BKW"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_REE_BKW", df_meta)


def test_sintese_qinc_ree_sf(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_REE_SF"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_REE_SF", df_meta)


def test_sintese_qinc_sbm_for(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_SBM_FOR"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_SBM_FOR", df_meta)


def test_sintese_qinc_sbm_bkw(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_SBM_BKW"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_SBM_BKW", df_meta)


def test_sintese_qinc_sbm_sf(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_SBM_SF"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_SBM_SF", df_meta)


def test_sintese_qinc_sin_for(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_SIN_FOR"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_SIN_FOR", df_meta)


def test_sintese_qinc_sin_bkw(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_SIN_BKW"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_SIN_BKW", df_meta)


def test_sintese_qinc_sin_sf(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_SIN_SF"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_SIN_SF", df_meta)
