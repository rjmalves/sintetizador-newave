from sintetizador.services.unitofwork import factory
import pandas as pd
from unittest.mock import patch
from tests.conftest import DECK_TEST_DIR, q


def test_fs_uow(test_settings):
    uow = factory("FS", DECK_TEST_DIR, q)
    with uow:
        dger = uow.files.get_dger()
        assert dger is not None
        with patch("pandas.DataFrame.to_parquet"):
            uow.export.synthetize_df(pd.DataFrame(), "CMO_SBM_EST")
