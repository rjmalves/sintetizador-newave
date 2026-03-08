import os
import threading
from unittest.mock import patch

import pandas as pd

from app.services.unitofwork import factory
from tests.conftest import DECK_TEST_DIR, q


def test_fs_uow(test_settings):
    uow = factory("FS", DECK_TEST_DIR, q)
    with uow:
        dger = uow.files.get_dger()
        assert dger is not None
        with patch("pyarrow.parquet.write_table"):
            uow.export.synthetize_df(pd.DataFrame(), "CMO_SBM_EST")


def test_fs_uow_does_not_change_cwd(test_settings):
    """FSUnitOfWork.__enter__ and __exit__ must not modify os.getcwd()."""
    cwd_before = os.getcwd()

    uow = factory("FS", DECK_TEST_DIR, q)
    with uow:
        cwd_inside = os.getcwd()

    cwd_after = os.getcwd()

    assert cwd_inside == cwd_before, (
        f"cwd changed inside context manager: was {cwd_before!r}, became {cwd_inside!r}"
    )
    assert cwd_after == cwd_before, (
        f"cwd changed after context manager exit: was {cwd_before!r}, became {cwd_after!r}"
    )


def test_fs_uow_concurrent_threads_stable_cwd(test_settings):
    """Multiple threads using FSUnitOfWork concurrently must not race on cwd."""
    cwd_before = os.getcwd()
    errors: list[str] = []

    def use_uow() -> None:
        uow = factory("FS", DECK_TEST_DIR, q)
        with uow:
            observed = os.getcwd()
            if observed != cwd_before:
                errors.append(
                    f"Thread {threading.current_thread().name}: "
                    f"cwd={observed!r}, expected={cwd_before!r}"
                )

    threads = [threading.Thread(target=use_uow, name=f"t{i}") for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, (
        "cwd was mutated by concurrent FSUnitOfWork usage:\n"
        + "\n".join(errors)
    )

    assert os.getcwd() == cwd_before, (
        f"cwd changed after all threads finished: {os.getcwd()!r} != {cwd_before!r}"
    )
