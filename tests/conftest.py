import os
import pathlib
from multiprocessing import Manager

import pytest

DECK_TEST_DIR = "./tests/mocks/arquivos"

# Set env vars at module level so they are inherited by forkserver workers
# (Python 3.14+ defaults to forkserver on Linux, which forks early before
# fixtures run, so env vars set only in fixtures are invisible to workers).
_BASEDIR = str(pathlib.Path().resolve())
os.environ.setdefault("APP_INSTALLDIR", _BASEDIR)
os.environ.setdefault("APP_BASEDIR", _BASEDIR)
os.environ.setdefault("FORMATO_SINTESE", "TEST")

m = Manager()
q = m.Queue(-1)


@pytest.fixture
def test_settings():
    os.environ["APP_INSTALLDIR"] = _BASEDIR
    os.environ["APP_BASEDIR"] = _BASEDIR
    os.environ["FORMATO_SINTESE"] = "TEST"
