import pytest
import pathlib
import os

DECK_TEST_DIR = "./tests/mocks/arquivos"


@pytest.fixture
def test_settings():
    BASEDIR = pathlib.Path().resolve()
    os.environ["APP_INSTALLDIR"] = str(BASEDIR)
    os.environ["APP_BASEDIR"] = str(BASEDIR)
