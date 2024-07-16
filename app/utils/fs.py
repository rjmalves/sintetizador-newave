from pathlib import Path
import os


class set_directory:
    """
    Directory changing context manager for helping specific cases
    in HPC script executions.
    """

    def __init__(self, path: str):
        self.path = Path(path)
        self.origin = Path().absolute()

    def __enter__(self):
        os.chdir(self.path)

    def __exit__(self, *args, **kwargs):
        os.chdir(self.origin)
