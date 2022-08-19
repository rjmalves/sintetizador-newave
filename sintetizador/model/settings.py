from sintetizador.utils.singleton import Singleton
from os import getenv


class Settings(metaclass=Singleton):
    def __init__(self):
        # Execution parameters
        self.basedir = getenv("APP_BASEDIR")
        self.installdir = getenv("APP_INSTALLDIR")
        self.clustersdir = getenv("CLUSTERSDIR")
        self.tmpdir = getenv("TMPDIR")
        self.newave_deck_zip = getenv("DECK")
        self.encoding_convert_script = "app/static/encoding.sh"
        # Input files - clustering process
        self.clusters_file = getenv("ARQUIVO_CLUSTERS")
        self.installed_capacity_file = getenv("ARQUIVO_CAPACIDADE_INSTALADA")
        self.ftm_file = getenv("ARQUIVO_FTM")
        self.average_wind_file = getenv("ARQUIVO_VENTO_MEDIO")
        # Input files - NEWAVE
        self.caso_file = getenv("ARQUIVO_CASO")
        self.parpmodel = int(getenv("OPCAO_MODELO_PARP"))
        self.orderreduction = int(getenv("OPCAO_REDUCAO_AUTOMATICA_ORDEM"))
        self.generatewind = int(getenv("CONSIDERA_GERACAO_EOLICA"))
        self.windcutpenalty = float(getenv("PENALIDADE_CORTE_GERACAO_EOLICA"))
        self.crosscorrelation = int(
            getenv("OPCAO_COMPENSACAO_CORRELACAO_CRUZADA")
        )
        self.swirlingconstraints = int(
            getenv("CONSIDERA_RESTRICOES_TURBINAMENTO")
        )
        self.defluenceconstraints = int(
            getenv("CONSIDERA_RESTRICOES_DEFLUENCIA")
        )
        self.nonsimulatedblock = int(getenv("BLOCO_NAO_SIMULADAS_EOLICA"))
        # Output files - NEWAVE
        self.static_file_path = "app/static"
        self.indice_file = "indices.csv"
        self.eolicacadastro_file = "eolica-cadastro.csv"
        self.eolicasubmercado_file = "eolica-submercado.csv"
        self.eolicaconfig_file = "eolica-config.csv"
        self.eolicafte_file = "eolica-fte.csv"
        self.histventos_file = "hist-ventos.csv"
        self.eolicageracao_file = "eolica-geracao.csv"
