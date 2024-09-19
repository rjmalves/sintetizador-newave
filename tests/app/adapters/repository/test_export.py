from unittest.mock import patch

import pandas as pd

from app.adapters.repository.export import factory
from tests.conftest import DECK_TEST_DIR


def test_export_csv(test_settings):
    repo = factory("CSV", DECK_TEST_DIR)
    with patch("pandas.DataFrame.to_csv"):
        repo.synthetize_df(pd.DataFrame(), "CMO_SBM_EST")


def test_export_parquet(test_settings):
    repo = factory("PARQUET", DECK_TEST_DIR)
    with patch("pandas.DataFrame.to_parquet"):
        repo.synthetize_df(pd.DataFrame(), "CMO_SBM_EST")
