"""Helpers for ProcessPoolExecutor — multiprocessing context + worker init.

Centralizes the choice of start method (``forkserver`` on Unix, ``spawn``
on Windows), the per-worker thread-library cap, and clamping of the
requested worker count to what the host can actually sustain.

Intentionally lightweight: only stdlib imports so it can be loaded as
part of the worker bootstrap before polars / numpy are touched. If you
add a polars-touching import here, the per-worker env-var cap may run
too late to take effect.

Worker-count clamping happens on two independent axes:

* **CPUs available to *this job*** — ``os.sched_getaffinity`` (honours a
  Slurm/cgroup cpuset or ``taskset``) rather than ``os.cpu_count()``,
  which reports the whole node and over-counts on a bound HPC job.
* **Thread budget** — ``RLIMIT_NPROC`` (``ulimit -u``) and the cgroup
  ``pids.max`` are *per-UID and node-wide*. Each worker keeps an
  irreducible floor of native threads (polars async runtime, jemalloc,
  Python) that ``POLARS_MAX_THREADS=1`` does not remove, so ``N`` workers
  cost ``N * floor`` threads against a limit shared with every other
  process the user runs. Spawning past that budget is what triggers the
  ``EAGAIN`` / ``ThreadPoolBuildError`` panic that breaks the pool.
"""

import logging as _logging
import multiprocessing as _mp
import os as _os
import platform as _platform
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from contextlib import contextmanager
from typing import Generator, Optional, Tuple

try:  # not available on Windows
    import resource as _resource
except ImportError:  # pragma: no cover - platform dependent
    _resource = None  # type: ignore[assignment]

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

# Native threads a single capped worker still holds even with
# POLARS_MAX_THREADS=1: polars' async/IO (Tokio) runtime, jemalloc
# background threads and the Python interpreter. Measured ~9 on a
# 14-core host; 16 leaves headroom for larger nodes.
_THREADS_PER_WORKER = 16

# Fraction of the per-UID thread budget we allow this job to consume,
# leaving the rest for the main process, helper processes and any other
# job the same user is running on the node.
_BUDGET_SAFETY_FRACTION = 0.8


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


def _available_cpus() -> int:
    """CPUs available to *this job*, not to the whole node.

    Prefers ``sched_getaffinity`` (respects a Slurm/cgroup cpuset or
    ``taskset``) and the Slurm allocation env vars over ``os.cpu_count()``,
    which reports every logical core on the host and therefore over-counts
    when the job is pinned to a subset.
    """
    candidates = []
    get_affinity = getattr(_os, "sched_getaffinity", None)
    if get_affinity is not None:
        try:
            candidates.append(len(get_affinity(0)))
        except OSError:
            pass
    for var in ("SLURM_CPUS_PER_TASK", "SLURM_CPUS_ON_NODE"):
        raw = _os.environ.get(var)
        if raw and raw.isdigit() and int(raw) > 0:
            candidates.append(int(raw))
    candidates.append(_os.cpu_count() or 1)
    return max(1, min(candidates))


def _read_pids_max() -> Optional[int]:
    """Read the cgroup ``pids.max`` (v2 then v1); ``None`` if absent/max."""
    for path in ("/sys/fs/cgroup/pids.max", "/sys/fs/cgroup/pids/pids.max"):
        try:
            with open(path) as fh:
                raw = fh.read().strip()
        except OSError:
            continue
        if raw == "max" or not raw.isdigit():
            return None
        return int(raw)
    return None


def raise_soft_nproc_limit() -> Optional[Tuple[int, int]]:
    """Raise the soft ``RLIMIT_NPROC`` to the hard limit for max thread
    headroom.

    ``EAGAIN`` on thread spawn (the ``ThreadPoolBuildError`` that breaks the
    pool) is driven by the per-UID *thread* ceiling, not memory. A process may
    raise its own soft limit up to the hard limit without privileges, so doing
    this at startup gives the worker pool the largest budget the host allows
    without requiring the launcher to run ``ulimit -u`` first. Children (pool
    workers, forkserver) inherit the raised soft limit.

    Returns the ``(old_soft, new_soft)`` pair when it changes, else ``None``.
    Best-effort: silently does nothing where ``resource`` is unavailable
    (Windows) or the call is refused.
    """
    if _resource is None:
        return None
    try:
        soft, hard = _resource.getrlimit(_resource.RLIMIT_NPROC)
        if soft == hard:
            return None
        _resource.setrlimit(_resource.RLIMIT_NPROC, (hard, hard))
        return (soft, hard)
    except (ValueError, OSError):
        return None


def _thread_limit() -> Optional[int]:
    """Effective per-UID thread ceiling, or ``None`` if unbounded/unknown.

    The binding limit is the smaller of the ``RLIMIT_NPROC`` soft limit and
    the cgroup ``pids.max``.
    """
    limits = []
    if _resource is not None:
        soft = _resource.getrlimit(_resource.RLIMIT_NPROC)[0]
        if soft != _resource.RLIM_INFINITY and soft > 0:
            limits.append(soft)
    pids_max = _read_pids_max()
    if pids_max is not None:
        limits.append(pids_max)
    return min(limits) if limits else None


def _max_workers_within_budget(available_cpus: int) -> Optional[int]:
    """Workers that fit the thread budget; ``None`` if the budget is unknown.

    Reserves the uncapped main process's footprint (~3x its own thread pool,
    which sizes to ``available_cpus``) plus a fixed allowance for the
    Manager, logging listener and forkserver processes.
    """
    limit = _thread_limit()
    if limit is None:
        return None
    main_reserve = 3 * available_cpus + 64
    usable = int(limit * _BUDGET_SAFETY_FRACTION) - main_reserve
    return max(1, usable // _THREADS_PER_WORKER)


def _resolve_max_workers(requested: int) -> int:
    """Clamp the requested worker count to CPUs available to the job and to
    the per-UID thread budget, whichever is smaller.

    Emits a WARNING on the ``main`` logger when clamping, naming the binding
    constraint so the operator sees what was actually used and why.
    """
    logger = _logging.getLogger("main")
    resolved = max(1, requested)

    available = _available_cpus()
    if resolved > available:
        logger.warning(
            f"PROCESSADORES={requested} exceeds CPUs available to this job "
            f"({available}); clamping to {available}."
        )
        resolved = available

    budget = _max_workers_within_budget(available)
    if budget is not None and resolved > budget:
        logger.warning(
            f"PROCESSADORES={requested} exceeds the thread budget "
            f"(RLIMIT_NPROC / cgroup pids.max allows ~{budget} workers at "
            f"{_THREADS_PER_WORKER} threads/worker); clamping to {budget} to "
            "avoid ThreadPoolBuildError/BrokenProcessPool."
        )
        resolved = budget

    return resolved


def create_executor(max_workers: int) -> ProcessPoolExecutor:
    """Create a ProcessPoolExecutor with safe start method, capped threads,
    and worker count clamped to the CPUs and thread budget of the host."""
    return ProcessPoolExecutor(
        max_workers=_resolve_max_workers(max_workers),
        mp_context=_SAFE_MP_CONTEXT,
        initializer=_worker_init,
    )


@contextmanager
def executor_scope(
    max_workers: int,
) -> Generator[ProcessPoolExecutor, None, None]:
    """Context manager around :func:`create_executor` that never blocks on a
    dead pool.

    ``ProcessPoolExecutor.__exit__`` calls ``shutdown(wait=True)``, which can
    hang when a worker died abruptly (``BrokenProcessPool``) — the exact
    "job never returns" symptom on the cluster. Here a broken pool is torn
    down with ``wait=False, cancel_futures=True`` and the error is re-raised
    for the CLI entrypoint to turn into a non-zero exit.
    """
    executor = create_executor(max_workers)
    broken = False
    try:
        yield executor
    except BrokenProcessPool:
        broken = True
        raise
    finally:
        executor.shutdown(wait=not broken, cancel_futures=broken)
