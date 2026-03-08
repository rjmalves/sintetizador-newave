from os import getenv

from app.utils.singleton import Singleton


class Settings(metaclass=Singleton):
    def __init__(self) -> None:
        # Execution parameters
        self.installdir: str | None = getenv("APP_INSTALLDIR")
        self.basedir: str | None = getenv("APP_BASEDIR")
        self.encoding_script: str = "app/static/converte_utf8.sh"
        self.file_repository: str = getenv("REPOSITORIO_ARQUIVOS", "FS")
        self.synthesis_format: str = getenv("FORMATO_SINTESE", "PARQUET")
        self.synthesis_dir: str = getenv("DIRETORIO_SINTESE", "sintese")
        self.processors: str | int = getenv("PROCESSADORES", 1)
