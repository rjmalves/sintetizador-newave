import logging
import logging.handlers
from queue import Queue
from multiprocessing import Process
import errno
import time
from sintetizador.utils.singleton import Singleton


class Log(metaclass=Singleton):
    @classmethod
    def logging_process(cls, q: Queue):
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
    def configure_queue_logger(cls):
        root = logging.getLogger()
        f = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        # Logger para STDOUT
        std_h = logging.StreamHandler()
        std_h.setFormatter(f)
        root.addHandler(std_h)
        root.setLevel(logging.DEBUG)

    @classmethod
    def configure_main_logger(cls, q: Queue) -> logging.Logger:
        h = logging.handlers.QueueHandler(q)
        logger = logging.getLogger()
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
        return logger

    @classmethod
    def configure_process_logger(
        cls,
        q: Queue,
        variable: str,
        member: int,
    ) -> logging.Logger:
        h = logging.handlers.QueueHandler(q)
        logger = logging.getLogger(f"worker-{variable}-{member}")
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
        return logger

    @classmethod
    def start_logging_process(cls, q: Queue):
        cls.listener = Process(target=cls.logging_process, args=(q,))
        cls.listener.start()

    @classmethod
    def terminate_logging_process(cls):
        cls.listener.terminate()
