import multiprocessing as _mp
import platform as _platform
from concurrent.futures import ProcessPoolExecutor  # noqa: F401

from app.services.deck.deck import Deck  # noqa: F401
from app.services.synthesis.operation.orchestrator import OperationSynthetizer

# Windows only supports 'spawn'; Unix uses 'forkserver' to avoid
# deadlocks caused by forking a process with active threads
# (e.g. from multiprocessing.Manager).
_SAFE_MP_CONTEXT = _mp.get_context(
    "spawn" if _platform.system() == "Windows" else "forkserver"
)


def create_executor(max_workers: int) -> ProcessPoolExecutor:
    """Create a ProcessPoolExecutor with a safe start method."""
    return ProcessPoolExecutor(
        max_workers=max_workers, mp_context=_SAFE_MP_CONTEXT
    )


__all__ = ["OperationSynthetizer"]
