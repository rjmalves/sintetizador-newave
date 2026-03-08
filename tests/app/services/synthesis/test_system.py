from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
from dateutil.relativedelta import relativedelta

from app.internal.constants import (
    BLOCK_COL,
    EER_CODE_COL,
    EER_NAME_COL,
    END_DATE_COL,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    STAGE_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    SYSTEM_SYNTHESIS_METADATA_OUTPUT,
    THERMAL_CODE_COL,
    THERMAL_NAME_COL,
    VALUE_COL,
)
from app.model.system.systemsynthesis import SystemSynthesis
from app.services.synthesis.system import SystemSynthetizer
from app.services.unitofwork import factory
from tests.conftest import DECK_TEST_DIR, q

uow = factory("FS", DECK_TEST_DIR, q)
synthetizer = SystemSynthetizer()


def __synthetize_with_mock(synthesis_str) -> tuple[pd.DataFrame, pd.DataFrame]:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        SystemSynthetizer.synthetize([synthesis_str], uow)

    m.assert_called()

    df = __get_synthesis_mock(synthesis_str, m)
    df_meta = __get_synthesis_mock(SYSTEM_SYNTHESIS_METADATA_OUTPUT, m)
    assert df is not None
    assert df_meta is not None
    return df, df_meta


def __get_synthesis_mock(key: str, mock: MagicMock) -> pd.DataFrame | None:
    for c in mock.mock_calls:
        if c.args[1] == key:
            return c.args[0]
    return None


def __validate_metadata(key: str, df_metadata: pd.DataFrame):
    s = SystemSynthesis.factory(key)
    assert s is not None
    assert str(s) in df_metadata["chave"].tolist()
    assert s.variable.short_name in df_metadata["nome_curto"].tolist()
    assert s.variable.long_name in df_metadata["nome_longo"].tolist()


def test_synthesis_est(test_settings):
    synthesis_str = "EST"
    df, df_meta = __synthetize_with_mock(synthesis_str)
    start_date = datetime(2023, 10, 1)
    assert df.at[0, STAGE_COL] == 1
    assert df.at[0, START_DATE_COL] == pd.Timestamp(start_date)
    assert df.at[0, END_DATE_COL] == start_date + relativedelta(months=1)
    __validate_metadata(synthesis_str, df_meta)


def test_synthesis_pat(test_settings):
    synthesis_str = "PAT"
    df, df_meta = __synthetize_with_mock(synthesis_str)
    start_date = datetime(2023, 1, 1)
    assert df.at[0, START_DATE_COL] == pd.Timestamp(start_date)
    # block_lengths sorts by [date, block]; pat0 (block=0) now comes before block 1
    assert df.at[0, BLOCK_COL] == 0
    # position 1 = block 1 of first date (2023-01-01): 0.2661 * 730 = 194.253
    assert abs(df.at[1, VALUE_COL] - 194.253) < 0.001
    __validate_metadata(synthesis_str, df_meta)


def test_synthesis_sbm(test_settings):
    synthesis_str = "SBM"
    df, df_meta = __synthetize_with_mock(synthesis_str)
    assert df.at[0, SUBMARKET_CODE_COL] == 1
    assert df.at[0, SUBMARKET_NAME_COL] == "SUDESTE"
    __validate_metadata(synthesis_str, df_meta)


def test_synthesis_ree(test_settings):
    synthesis_str = "REE"
    df, df_meta = __synthetize_with_mock(synthesis_str)
    parana_rows = df.loc[df[EER_CODE_COL] == 10]
    assert len(parana_rows) == 1
    assert parana_rows.iloc[0][EER_NAME_COL] == "PARANA"
    __validate_metadata(synthesis_str, df_meta)


def test_synthesis_cvu(test_settings):
    synthesis_str = "CVU"
    df, df_meta = __synthetize_with_mock(synthesis_str)
    assert df.at[0, THERMAL_CODE_COL] == 1
    assert df.at[0, START_DATE_COL] == datetime(2023, 10, 1)
    assert df.at[0, VALUE_COL] == 31.17
    __validate_metadata(synthesis_str, df_meta)


def test_synthesis_ute(test_settings):
    synthesis_str = "UTE"
    df, df_meta = __synthetize_with_mock(synthesis_str)
    assert df.at[0, THERMAL_CODE_COL] == 1
    assert df.at[0, THERMAL_NAME_COL] == "ANGRA 1"
    assert df.at[0, SUBMARKET_CODE_COL] == 1
    assert df.at[0, SUBMARKET_NAME_COL] == "SUDESTE"
    __validate_metadata(synthesis_str, df_meta)


def test_synthesis_uhe(test_settings):
    synthesis_str = "UHE"
    df, df_meta = __synthetize_with_mock(synthesis_str)
    assert df.at[0, HYDRO_CODE_COL] == 4
    assert df.at[0, HYDRO_NAME_COL] == "FUNIL-GRANDE"
    assert df.at[0, EER_CODE_COL] == 10
    assert df.at[0, EER_NAME_COL] == "PARANA"
    assert df.at[0, SUBMARKET_CODE_COL] == 1
    assert df.at[0, SUBMARKET_NAME_COL] == "SUDESTE"
    __validate_metadata(synthesis_str, df_meta)
