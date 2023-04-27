import pytest
import pathlib
import os
from multiprocessing import Manager

DECK_TEST_DIR = "./tests/mocks/arquivos"
m = Manager()
q = m.Queue(-1)


@pytest.fixture
def test_settings():
    BASEDIR = pathlib.Path().resolve()
    os.environ["APP_INSTALLDIR"] = str(BASEDIR)
    os.environ["APP_BASEDIR"] = str(BASEDIR)
