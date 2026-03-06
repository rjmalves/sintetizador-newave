from os import getenv

from app.utils.singleton import Singleton


class Settings(metaclass=Singleton):
    def __init__(self):
        # Execution parameters
        self.installdir = getenv("APP_INSTALLDIR")
        self.basedir = getenv("APP_BASEDIR")
        self.encoding_script = "app/static/converte_utf8.sh"
        self.file_repository = getenv("REPOSITORIO_ARQUIVOS", "FS")
        self.synthesis_format = getenv("FORMATO_SINTESE", "PARQUET")
        self.synthesis_dir = getenv("DIRETORIO_SINTESE", "sintese")
        self.processors = getenv("PROCESSADORES", 1)
