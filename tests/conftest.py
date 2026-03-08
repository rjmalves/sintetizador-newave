import multiprocessing
import os
import pathlib

import pytest

# Force "spawn" start method on all Python versions to avoid fork+coverage
# deadlocks. Python 3.14+ already defaults to forkserver/spawn on Linux;
# this ensures 3.10-3.13 behave consistently.
try:
    multiprocessing.set_start_method("spawn")
except RuntimeError:
    pass  # already set

from multiprocessing import Manager  # noqa: E402

DECK_TEST_DIR = "./tests/mocks/arquivos"

# Set env vars at module level so they are inherited by spawn/forkserver
# workers (spawn starts fresh processes that don't inherit the parent's
# runtime state, so env vars must be set before Manager() is created).
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
