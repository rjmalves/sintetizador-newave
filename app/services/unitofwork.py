from abc import ABC, abstractmethod
from multiprocessing import Queue
from os import chdir, curdir
from pathlib import Path
from typing import Dict, Type

from app.adapters.repository.export import (
    AbstractExportRepository,
)
from app.adapters.repository.export import (
    factory as export_factory,
)
from app.adapters.repository.files import (
    AbstractFilesRepository,
)
from app.adapters.repository.files import (
    factory as files_factory,
)
from app.model.settings import Settings


class AbstractUnitOfWork(ABC):
    def __init__(self, q: Queue) -> None:
        self._queue = q
        self._subdir = ""
        self._version = "latest"

    def __enter__(self) -> "AbstractUnitOfWork":
        return self

    def __exit__(self, *args):
        self.rollback()

    @abstractmethod
    def rollback(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def files(self) -> AbstractFilesRepository:
        raise NotImplementedError

    @property
    @abstractmethod
    def export(self) -> AbstractExportRepository:
        raise NotImplementedError

    @property
    def version(self) -> str:
        return self._version

    @version.setter
    def version(self, s: str):
        self._version = s

    @property
    def queue(self) -> Queue:
        return self._queue

    @property
    def subdir(self) -> str:
        return self._subdir

    @subdir.setter
    def subdir(self, subdir: str):
        self._subdir = subdir


class FSUnitOfWork(AbstractUnitOfWork):
    def __init__(self, directory: str, q: Queue):
        super().__init__(q)
        self._current_path = str(Path(curdir).resolve())
        self._path = str(Path(directory).resolve())
        self._files = None
        self._exporter = None

    def __create_repository(self):
        if self._files is None:
            self._files = files_factory(
                Settings().file_repository, str(self._path), self._version
            )
        if self._exporter is None:
            synthesis_outdir = (
                Path(self._path)
                .joinpath(Settings().synthesis_dir)
                .joinpath(self._subdir)
            )
            synthesis_outdir.mkdir(parents=True, exist_ok=True)
            self._exporter = export_factory(
                Settings().synthesis_format, str(synthesis_outdir)
            )

    def __enter__(self) -> "AbstractUnitOfWork":
        chdir(self._path)
        self.__create_repository()
        return super().__enter__()

    def __exit__(self, *args):
        chdir(self._current_path)
        self._files = None
        self._exporter = None
        super().__exit__(*args)

    @property
    def files(self) -> AbstractFilesRepository:
        if self._files is None:
            raise RuntimeError()
        return self._files

    @property
    def export(self) -> AbstractExportRepository:
        if self._exporter is None:
            raise RuntimeError()
        return self._exporter

    @property
    def version(self) -> str:
        return self._version

    @version.setter
    def version(self, s: str):
        self._version = s

    def rollback(self):
        pass


def factory(kind: str, *args, **kwargs) -> AbstractUnitOfWork:
    mappings: Dict[str, Type[AbstractUnitOfWork]] = {
        "FS": FSUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)
