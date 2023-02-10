from sintetizador.utils.singleton import Singleton
from os import getenv


class Settings(metaclass=Singleton):
    def __init__(self):
        # Execution parameters
        self.installdir = getenv("APP_INSTALLDIR")
        self.basedir = getenv("APP_BASEDIR")
        self.encoding_script = "sintetizador/static/converte_utf8.sh"
        self.synthesis_format = getenv("FORMATO_SINTESE", "PARQUET")
        self.synthesis_dir = getenv("DIRETORIO_SINTESE", "sintese")
