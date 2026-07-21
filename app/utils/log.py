from __future__ import annotations

import errno
import logging
import logging.handlers
import sys
import time
from multiprocessing import Process
from multiprocessing.queues import Queue as MPQueue
from typing import Optional

from app.utils.singleton import Singleton


class Log(metaclass=Singleton):
    listener: Optional[Process] = None

    @classmethod
    def logging_process(cls, q: MPQueue[logging.LogRecord]) -> None:
        cls.configure_queue_logger()
        while True:
            try:
                while not q.empty():
                    record = q.get()
                    if record is None:
                        return
                    logger = logging.getLogger(record.name)
                    logger.handle(record)
            except IOError as e:
                if e.errno == errno.EPIPE:
                    print("EPIPE")
            time.sleep(0.1)

    @classmethod
    def configure_queue_logger(cls) -> None:
        root = logging.getLogger()
        f = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        # Logger para STDOUT
        std_h = logging.StreamHandler(stream=sys.stdout)
        std_h.setFormatter(f)
        root.addHandler(std_h)
        root.setLevel(logging.DEBUG)

    @classmethod
    def configure_main_logger(
        cls, q: MPQueue[logging.LogRecord]
    ) -> logging.Logger:
        h = logging.handlers.QueueHandler(q)
        logger = logging.getLogger("main")
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
        return logger

    @classmethod
    def configure_process_logger(
        cls,
        q: MPQueue[logging.LogRecord],
        variable: str,
        member: int,
    ) -> logging.Logger:
        h = logging.handlers.QueueHandler(q)
        logger = logging.getLogger(f"worker-{variable}-{member}")
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
        return logger

    @classmethod
    def start_logging_process(cls, q: MPQueue[logging.LogRecord]) -> None:
        # daemon=True so multiprocessing's atexit handler terminates this
        # ``while True`` listener instead of join()-ing it forever. Without
        # it, an error path that skips terminate_logging_process() (e.g. a
        # BrokenProcessPool abort) leaves the process hanging at exit and the
        # Slurm batch job never returns.
        cls.listener = Process(
            target=cls.logging_process, args=(q,), daemon=True
        )
        cls.listener.start()

    @classmethod
    def terminate_logging_process(cls) -> None:
        if cls.listener is not None:
            cls.listener.terminate()
