import click
import os
import tempfile
from typing import List, Tuple
from sintetizador.model.settings import Settings
from sintetizador.model.operationsynthesis import OperationSynthesis
from sintetizador.model.variable import Variable
from sintetizador.model.spatialresolution import SpatialResolution
from sintetizador.model.temporalresolution import TemporalResolution
import sintetizador.domain.commands as commands
import sintetizador.services.handlers as handlers
from sintetizador.services.synthesis.operation import OperationSynthetizer
from sintetizador.services.unitofwork import factory
from sintetizador.utils.log import Log


@click.group()
def app():
    """
    Aplicação para realizar a síntese de informações em
    um modelo unificado de dados para o NEWAVE.
    """
    pass


# TODO - padronização para 'operacao'
@click.command("nwlistop")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def operacao(variaveis, formato):
    """
    Realiza a síntese dos dados da operação do NEWAVE (NWLISTOP).
    """
    os.environ["FORMATO_SINTESE"] = formato
    Log.log().info("# Realizando síntese da OPERACAO #")

    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ["TMPDIR"] = tmpdirname
        uow = factory(
            "FS",
            Settings().tmpdir,
            Settings().synthesis_dir,
        )
        command = commands.SynthetizeOperation(variaveis)
        handlers.synthetize_operation(command, uow)

    Log.log().info("# Fim da síntese #")


@click.command("limpeza")
def limpeza():
    """
    Realiza a limpeza dos dados resultantes de uma síntese.
    """
    handlers.clean()


app.add_command(operacao)
app.add_command(limpeza)
