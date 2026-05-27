"""Helpers for ProcessPoolExecutor — multiprocessing context + worker init.

Centralizes the choice of start method (``forkserver`` on Unix, ``spawn``
on Windows), the per-worker thread-library cap, and clamping of the
requested worker count to the available CPUs.

Intentionally lightweight: only stdlib imports so it can be loaded as
part of the worker bootstrap before polars / numpy are touched. If you
add a polars-touching import here, the per-worker env-var cap may run
too late to take effect.
"""

import logging as _logging
import multiprocessing as _mp
import os as _os
import platform as _platform
from concurrent.futures import ProcessPoolExecutor
from typing import Tuple

# Windows only supports 'spawn'; Unix uses 'forkserver' to avoid
# deadlocks caused by forking a process with active threads
# (e.g. from multiprocessing.Manager).
_SAFE_MP_CONTEXT = _mp.get_context(
    "spawn" if _platform.system() == "Windows" else "forkserver"
)

_THREAD_CAP_ENV_VARS: Tuple[str, ...] = (
    "POLARS_MAX_THREADS",
    "RAYON_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
)


def _worker_init() -> None:
    """Cap thread-library pool sizes in each worker.

    With N workers each defaulting to ``num_cpus()`` polars/OpenBLAS threads,
    total thread count grows as N * num_cpus and exhausts ``RLIMIT_NPROC``
    on busy hosts. Capping to 1 here is also a small perf win: per-entity
    work in each worker is small enough that thread-pool overhead dominates
    any in-task parallelism gain.
    """
    for name in _THREAD_CAP_ENV_VARS:
        _os.environ.setdefault(name, "1")


def _resolve_max_workers(requested: int) -> int:
    """Clamp requested worker count to available logical CPUs.

    Above ``os.cpu_count()`` workers just time-slice with no throughput
    benefit. Emits a WARNING on the ``main`` logger when clamping so the
    operator sees what was actually used.
    """
    logical = _os.cpu_count() or 1
    if requested > logical:
        _logging.getLogger("main").warning(
            f"PROCESSADORES={requested} exceeds available CPUs ({logical}); "
            f"clamping to {logical}."
        )
        return logical
    return requested


def create_executor(max_workers: int) -> ProcessPoolExecutor:
    """Create a ProcessPoolExecutor with safe start method, capped threads,
    and worker count clamped to the available CPUs."""
    return ProcessPoolExecutor(
        max_workers=_resolve_max_workers(max_workers),
        mp_context=_SAFE_MP_CONTEXT,
        initializer=_worker_init,
    )
