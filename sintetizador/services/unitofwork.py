from abc import ABC, abstractmethod
from os import chdir, curdir
from typing import Dict, Type
from pathlib import Path
from multiprocessing import Queue
from sintetizador.model.settings import Settings
from sintetizador.adapters.repository.files import (
    AbstractFilesRepository,
)
from sintetizador.adapters.repository.export import (
    AbstractExportRepository,
)
from sintetizador.adapters.repository.files import (
    factory as files_factory,
)
from sintetizador.adapters.repository.export import (
    factory as export_factory,
)


class AbstractUnitOfWork(ABC):
    def __init__(self, q: Queue) -> None:
        self._queue = q

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
    def queue(self) -> Queue:
        return self._queue


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
                Settings().file_repository, str(self._path)
            )
        if self._exporter is None:
            synthesis_outdir = Path(self._path).joinpath(
                Settings().synthesis_dir
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

    def rollback(self):
        pass


def factory(kind: str, *args, **kwargs) -> AbstractUnitOfWork:
    mappings: Dict[str, Type[AbstractUnitOfWork]] = {
        "FS": FSUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)
