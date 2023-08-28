from unittest.mock import patch, MagicMock
from sintetizador.services.unitofwork import factory
from sintetizador.services.synthesis.scenario import ScenarioSynthetizer

from tests.conftest import DECK_TEST_DIR, q

uow = factory("FS", DECK_TEST_DIR, q)


def test_sintese_enaa_ree_for(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_REE_FOR"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0


def test_sintese_enaa_ree_bkw(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_REE_BKW"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0


def test_sintese_enaa_ree_sf(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_REE_SF"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0


def test_sintese_enaa_sin_sf(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["ENAA_SIN_SF"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0


def test_sintese_qinc_uhe_for(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_UHE_FOR"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0


def test_sintese_qinc_uhe_bkw(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_UHE_BKW"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0


def test_sintese_qinc_uhe_sf(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_UHE_SF"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0


def test_sintese_qinc_sin_sf(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        ScenarioSynthetizer.synthetize(["QINC_SIN_SF"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0
