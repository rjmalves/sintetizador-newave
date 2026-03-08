import time
from logging import INFO, Logger
from types import TracebackType
from typing import Optional, Type


class time_and_log:
    def __init__(
        self,
        message_root: Optional[str] = None,
        logger: Optional[Logger] = None,
        level: int = INFO,
    ) -> None:
        self.message_root = message_root
        self.logger = logger
        self.level = level

    def __enter__(self) -> "time_and_log":
        self.start_time = time.perf_counter()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        end_time = time.perf_counter()
        run_time = end_time - self.start_time
        if self.logger:
            message_with_root = (
                f"{self.message_root}: {run_time:.2f} s"
                if self.message_root
                else f"Finished in {run_time:.2f} s"
            )
            self.logger.log(self.level, message_with_root)
