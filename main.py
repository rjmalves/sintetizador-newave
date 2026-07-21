import multiprocessing
import os
import pathlib
import sys
from concurrent.futures.process import BrokenProcessPool

from app.app import app
from app.utils.parallel import raise_soft_nproc_limit


def main() -> None:
    os.environ["APP_INSTALLDIR"] = os.path.dirname(os.path.abspath(__file__))
    BASEDIR = pathlib.Path().resolve()
    os.environ["APP_BASEDIR"] = str(BASEDIR)
    # Take the largest thread budget the host allows before spawning any
    # worker pool; workers and the forkserver inherit the raised soft limit.
    raise_soft_nproc_limit()
    try:
        app()
    except BrokenProcessPool:
        # A worker died abruptly — typically polars' ThreadPoolBuildError from
        # thread/memory exhaustion at high --processadores. The pool is
        # unusable and a normal shutdown would hang in the concurrent.futures
        # atexit join, so the Slurm batch job would never return. Print a
        # clear reason, terminate our helper processes (Manager, logging
        # listener) so none are orphaned, then force an immediate non-zero
        # exit that cannot hang.
        print(
            "FATAL: o pool de processos foi corrompido (um worker terminou "
            "abruptamente, provavelmente por exaustao de threads/memoria). "
            "Reduza --processadores ou aumente o limite de threads do host "
            "(ulimit -u / cgroup pids.max). Abortando.",
            file=sys.stderr,
            flush=True,
        )
        for child in multiprocessing.active_children():
            child.terminate()
        sys.stderr.flush()
        sys.stdout.flush()
        os._exit(1)


if __name__ == "__main__":
    main()
