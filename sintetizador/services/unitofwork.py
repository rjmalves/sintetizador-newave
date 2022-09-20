from abc import ABC, abstractmethod
from os import chdir, curdir, listdir
import re
from typing import Optional, Dict
from zipfile import ZipFile
from pathlib import Path

from sintetizador.utils.log import Log
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

    @abstractmethod
    def extract_deck(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def extract_outputs(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def extract_nwlistop(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def extract_nwlistcf(self) -> bool:
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

    OUT_FILES_TO_EXTRACT = [
        "pmo.dat",
        "parp.dat",
        "parpeol.dat",
        "parpvaz.dat",
    ]

    def __init__(self, path: str, directory: str):
        self._current_path = Path(curdir).resolve()
        self._tmp_path = Path(path).resolve()
        self._synthesis_directory = directory
        self._files = RawFilesRepository(str(self._tmp_path))
        synthesis_outdir = self._current_path.joinpath(
            self._synthesis_directory
        )
        synthesis_outdir.mkdir(parents=True, exist_ok=True)
        self._exporter = export_factory(
            Settings().synthesis_format, str(synthesis_outdir)
        )

    def __enter__(self) -> "FSUnitOfWork":
        chdir(self._current_path)
        if len(listdir(Settings().tmpdir)) == 0:
            self.extract_deck()
            self.extract_outputs()
            self.extract_nwlistop()
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

    @staticmethod
    def __deck_zip_name() -> Optional[str]:
        deck_zip = [
            r
            for r in listdir()
            if re.match(Settings().newave_deck_pattern, r) is not None
        ]
        if len(deck_zip) == 0:
            return None
        else:
            # Lógica adicional para tratar diretório com múltiplos "deck_"
            return [d for d in deck_zip if Path(curdir).resolve().stem in d][0]

    @staticmethod
    def __out_zip_name() -> Optional[str]:
        out_zip = [
            r
            for r in listdir()
            if re.match(Settings().newave_output_pattern, r) is not None
        ]
        if len(out_zip) == 1:
            return out_zip[0]
        else:
            return None

    @staticmethod
    def __nwlistop_zip_name() -> Optional[str]:
        nwlistop_zip = [
            r
            for r in listdir()
            if re.match(Settings().nwlistop_pattern, r) is not None
        ]
        if len(nwlistop_zip) == 1:
            return nwlistop_zip[0]
        else:
            return None

    @staticmethod
    def __nwlistcf_zip_name() -> Optional[str]:
        nwlistcf_zip = [
            r
            for r in listdir()
            if re.match(Settings().nwlistcf_pattern, r) is not None
        ]
        if len(nwlistcf_zip) == 1:
            return nwlistcf_zip[0]
        else:
            return None

    def extract_deck(self) -> bool:
        zipname = FSUnitOfWork.__deck_zip_name()
        if zipname is None:
            Log.log().error(
                "Erro ao processar o deck de entrada: não encontrado."
            )
            return False
        Log.log().info(f"Extraindo deck em {zipname} para {Settings().tmpdir}")
        if zipname is not None:
            with ZipFile(zipname, "r") as obj_zip:
                obj_zip.extractall(Settings().tmpdir)
        return True

    def extract_outputs(self) -> bool:
        zipname = FSUnitOfWork.__out_zip_name()
        Log.log().info(
            f"Extraindo saídas em {zipname} para {Settings().tmpdir}"
        )
        if zipname is not None:
            with ZipFile(zipname, "r") as obj_zip:
                existing_files = [
                    f
                    for f in FSUnitOfWork.OUT_FILES_TO_EXTRACT
                    if f in obj_zip.namelist()
                ]
                obj_zip.extractall(Settings().tmpdir, existing_files)

    def extract_nwlistop(self) -> bool:
        zipname = FSUnitOfWork.__nwlistop_zip_name()
        Log.log().info(
            f"Extraindo nwlistop em {zipname} para {Settings().tmpdir}"
        )
        if zipname is not None:
            with ZipFile(zipname, "r") as obj_zip:
                obj_zip.extractall(Settings().tmpdir)

    def extract_nwlistcf(self) -> bool:
        zipname = FSUnitOfWork.__nwlistcf_zip_name()
        Log.log().info(
            f"Extracting nwlistcf em {zipname} para {Settings().tmpdir}"
        )
        if zipname is not None:
            with ZipFile(zipname, "r") as obj_zip:
                obj_zip.extractall(Settings().tmpdir)

    def rollback(self):
        pass


def factory(kind: str, *args, **kwargs) -> AbstractUnitOfWork:
    mappings: Dict[str, AbstractUnitOfWork] = {
        "FS": FSUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)
