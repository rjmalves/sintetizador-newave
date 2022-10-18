from abc import ABC, abstractmethod
from os import chdir, curdir
from typing import Dict
from pathlib import Path

from sintetizador.model.settings import Settings
from sintetizador.adapters.repository.files import (
    AbstractFilesRepository,
    RawFilesRepository,
)
from sintetizador.adapters.repository.export import (
    AbstractExportRepository,
)
from sintetizador.adapters.repository.export import (
    factory as export_factory,
)


class AbstractUnitOfWork(ABC):
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


class FSUnitOfWork(AbstractUnitOfWork):
    def __init__(self, path: str, directory: str):
        self._current_path = Path(curdir).resolve()
        self._synthesis_directory = directory
        self._files = None
        self._exporter = None

    def __create_repository(self):
        if self._files is None:
            self._files = RawFilesRepository(str(self._current_path))
        if self._exporter is None:
            synthesis_outdir = self._current_path.joinpath(
                self._synthesis_directory
            )
            synthesis_outdir.mkdir(parents=True, exist_ok=True)
            self._exporter = export_factory(
                Settings().synthesis_format, str(synthesis_outdir)
            )

    def __enter__(self) -> "FSUnitOfWork":
        chdir(self._current_path)
        self.__create_repository()
        return super().__enter__()

    def __exit__(self, *args):
        chdir(self._current_path)
        super().__exit__(*args)

    @property
    def files(self) -> RawFilesRepository:
        return self._files

    @property
    def export(self) -> AbstractExportRepository:
        return self._exporter

    def rollback(self):
        pass


def factory(kind: str, *args, **kwargs) -> AbstractUnitOfWork:
    mappings: Dict[str, AbstractUnitOfWork] = {
        "FS": FSUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)
