from sintetizador.utils.singleton import Singleton
from os import getenv


class Settings(metaclass=Singleton):
    def __init__(self):
        # Execution parameters
        self.basedir = getenv("APP_BASEDIR")
        self.installdir = getenv("APP_INSTALLDIR")
        self.tmpdir = getenv("TMPDIR")
        self.newave_deck_pattern = getenv("PADRAO_DECK")
        self.newave_output_pattern = getenv("PADRAO_SAIDAS")
        self.nwlistop_pattern = getenv("PADRAO_NWLISTOP")
        self.nwlistcf_pattern = getenv("PADRAO_NWLISTCF")
        self.synthesis_format = getenv("FORMATO_SINTESE")
        self.synthesis_dir = getenv("DIRETORIO_SINTESE")
