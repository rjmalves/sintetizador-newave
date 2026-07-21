"""Unit tests for app.utils.parallel — worker-count clamping (affinity +
thread budget) and the fail-fast executor scope.

Regression guards for the HPC "ThreadPoolBuildError / BrokenProcessPool"
failure: too many workers on a bound node, and a hang at exit when a worker
dies abruptly.
"""

import os
import resource
import time

import pytest

from app.utils import parallel
from app.utils.parallel import executor_scope, raise_soft_nproc_limit

# module-level helpers so they are importable by forkserver/spawn workers


def _double(x: int) -> int:
    return x * 2


def _suicide(_: int) -> None:
    # Emulate a worker dying abruptly (as polars does on ThreadPoolBuildError)
    os._exit(1)


# ---------------------------------------------------------------------------
# _available_cpus
# ---------------------------------------------------------------------------


def test_available_cpus_prefers_slurm_allocation_over_node(monkeypatch):
    """A Slurm per-task allocation must cap the count below the node size."""
    # parallel imports os as _os, so patching os patches the same module.
    monkeypatch.setattr(os, "cpu_count", lambda: 256)
    monkeypatch.setattr(os, "sched_getaffinity", lambda _pid: set(range(256)))
    monkeypatch.setenv("SLURM_CPUS_PER_TASK", "8")
    assert parallel._available_cpus() == 8


def test_available_cpus_uses_affinity_not_cpu_count(monkeypatch):
    """sched_getaffinity (cpuset) must win over the node-wide cpu_count."""
    monkeypatch.setattr(os, "cpu_count", lambda: 128)
    monkeypatch.setattr(os, "sched_getaffinity", lambda _pid: {0, 1, 2, 3})
    monkeypatch.delenv("SLURM_CPUS_PER_TASK", raising=False)
    monkeypatch.delenv("SLURM_CPUS_ON_NODE", raising=False)
    assert parallel._available_cpus() == 4


# ---------------------------------------------------------------------------
# _resolve_max_workers
# ---------------------------------------------------------------------------


def test_resolve_max_workers_clamps_to_available_cpus(monkeypatch):
    monkeypatch.setattr(parallel, "_available_cpus", lambda: 4)
    monkeypatch.setattr(parallel, "_thread_limit", lambda: None)
    assert parallel._resolve_max_workers(1000) == 4
    assert parallel._resolve_max_workers(2) == 2


def test_resolve_max_workers_clamps_to_thread_budget(monkeypatch):
    """A small per-UID thread limit must bind below the CPU count."""
    monkeypatch.setattr(parallel, "_available_cpus", lambda: 64)
    # pids.max = 200 -> (200*0.8 - (3*64+64)) // 16 -> negative -> floored to 1
    monkeypatch.setattr(parallel, "_thread_limit", lambda: 200)
    assert parallel._resolve_max_workers(64) == 1


def test_resolve_max_workers_budget_between_one_and_cpus(monkeypatch):
    monkeypatch.setattr(parallel, "_available_cpus", lambda: 64)
    # limit 2000 -> (1600 - 256)//16 = 84, but CPU count 64 binds first
    monkeypatch.setattr(parallel, "_thread_limit", lambda: 2000)
    assert parallel._resolve_max_workers(64) == 64
    # limit 800 -> (640 - 256)//16 = 24 workers, below the 64 CPUs
    monkeypatch.setattr(parallel, "_thread_limit", lambda: 800)
    assert parallel._resolve_max_workers(64) == 24


def test_resolve_max_workers_never_below_one(monkeypatch):
    monkeypatch.setattr(parallel, "_available_cpus", lambda: 1)
    monkeypatch.setattr(parallel, "_thread_limit", lambda: 1)
    assert parallel._resolve_max_workers(0) == 1


# ---------------------------------------------------------------------------
# executor_scope
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# raise_soft_nproc_limit
# ---------------------------------------------------------------------------


def test_raise_soft_nproc_limit_lifts_soft_to_hard():
    """The soft RLIMIT_NPROC must end up equal to the hard limit."""
    soft_before, hard = resource.getrlimit(resource.RLIMIT_NPROC)
    try:
        # Lower the soft limit so there is room to raise it back up.
        lowered = hard if hard == resource.RLIM_INFINITY else max(1, hard - 1)
        resource.setrlimit(resource.RLIMIT_NPROC, (lowered, hard))

        raise_soft_nproc_limit()

        new_soft, new_hard = resource.getrlimit(resource.RLIMIT_NPROC)
        assert new_soft == hard
        assert new_hard == hard
    finally:
        resource.setrlimit(resource.RLIMIT_NPROC, (soft_before, hard))


def test_raise_soft_nproc_limit_noop_when_already_maxed():
    """When soft already equals hard, it returns None and changes nothing."""
    soft_before, hard = resource.getrlimit(resource.RLIMIT_NPROC)
    try:
        resource.setrlimit(resource.RLIMIT_NPROC, (hard, hard))
        assert raise_soft_nproc_limit() is None
    finally:
        resource.setrlimit(resource.RLIMIT_NPROC, (soft_before, hard))


def test_executor_scope_normal_path_runs_tasks():
    with executor_scope(2) as ex:
        result = ex.submit(_double, 21).result(timeout=60)
    assert result == 42


def test_executor_scope_broken_pool_raises_without_hanging():
    """A worker dying abruptly must surface BrokenProcessPool promptly, not
    block forever in shutdown(wait=True)."""
    from concurrent.futures.process import BrokenProcessPool

    start = time.monotonic()
    with pytest.raises(BrokenProcessPool):
        with executor_scope(2) as ex:
            futures = [ex.submit(_suicide, i) for i in range(4)]
            for f in futures:
                f.result(timeout=60)
    assert time.monotonic() - start < 30
