from unittest.mock import patch, MagicMock
from sintetizador.services.unitofwork import factory
from sintetizador.services.synthesis.operation import OperationSynthetizer
from sintetizador.services.synthesis.operation import FATOR_HM3_M3S

from tests.conftest import DECK_TEST_DIR, q

uow = factory("FS", DECK_TEST_DIR, q)


def test_sintese_sin_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARPF_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 31.4


def test_sintese_sbm_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARPF_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 26.8


def test_sintese_ree_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARPF_REE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 19.7


def test_sintese_uhe_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QAFL_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 202.79


def test_sintese_pee_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VENTO_PEE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 4.37


def test_sintese_sbp_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["INT_SBP_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 5907.0


def test_sintese_sin_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHID_SIN_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 14125.5


def test_sintese_sbm_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GTER_SBM_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 1683.6


def test_sintese_ree_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHID_REE_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 1719.1


def test_sintese_uhe_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHID_UHE_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 6.22


def test_sintese_pee_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GEOL_PEE_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 746.6


def test_sintese_sbp_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["INT_SBP_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 1378.7


def test_sintese_ever_total(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVER_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 3845 + 165


def test_sintese_viol_uhe_agregada(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VDEFMAX_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0


def test_sintese_qtur_qver_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QTUR_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 197.1901140684411


def test_sintese_qdef_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QDEF_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 267.19011406844106


def test_sintese_earmi_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARMI_REE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 21386.9


def test_sintese_earmi_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARMI_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert round(df.at[0, "valor"], 1) == 71127.6


def test_sintese_earmi_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARMI_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 128836.7


def test_sintese_earpi_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARPI_REE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 42.0


def test_sintese_earpi_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARPI_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert round(df.at[0, "valor"], 2) == 34.81


def test_sintese_earpi_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARPI_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert round(df.at[0, "valor"], 2) == 44.38


def test_sintese_varmi_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VARMI_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 513.1


def test_sintese_varmi_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VARMI_REE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 513.1


def test_sintese_varmi_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VARMI_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 513.1


def test_sintese_varmi_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VARMI_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 513.1


def test_sintese_gter_ute_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GTER_UTE_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0


def test_sintese_gter_ute_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GTER_UTE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0
