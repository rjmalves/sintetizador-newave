import click
import os
from sintetizador.model.settings import Settings
import sintetizador.domain.commands as commands
import sintetizador.services.handlers as handlers
from sintetizador.services.unitofwork import factory
from sintetizador.utils.log import Log


@click.group()
def app():
    """
    Aplicação para realizar a síntese de informações em
    um modelo unificado de dados para o NEWAVE.
    """
    pass


@click.command("sistema")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def sistema(variaveis, formato):
    """
    Realiza a síntese dos dados do sistema do NEWAVE.
    """
    os.environ["FORMATO_SINTESE"] = formato
    Log.log().info("# Realizando síntese do SISTEMA #")

    uow = factory(
        "FS",
        Settings().synthesis_dir,
    )
    command = commands.SynthetizeSystem(variaveis)
    handlers.synthetize_system(command, uow)

    Log.log().info("# Fim da síntese #")


@click.command("execucao")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def execucao(variaveis, formato):
    """
    Realiza a síntese dos dados da execução do NEWAVE.
    """
    os.environ["FORMATO_SINTESE"] = formato
    Log.log().info("# Realizando síntese da EXECUÇÃO #")

    uow = factory(
        "FS",
        Settings().synthesis_dir,
    )
    command = commands.SynthetizeExecution(variaveis)
    handlers.synthetize_execution(command, uow)

    Log.log().info("# Fim da síntese #")


@click.command("operacao")
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

    uow = factory(
        "FS",
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


@click.command("completa")
@click.option(
    "-sistema", multiple=True, help="variável do sistema para síntese"
)
@click.option(
    "-execucao", multiple=True, help="variável da execução para síntese"
)
@click.option(
    "-operacao", multiple=True, help="variável da operação para síntese"
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def completa(sistema, execucao, operacao, formato):
    """
    Realiza a síntese completa do NEWAVE.
    """
    os.environ["FORMATO_SINTESE"] = formato
    Log.log().info("# Realizando síntese COMPLETA #")

    uow = factory(
        "FS",
        Settings().synthesis_dir,
    )
    command = commands.SynthetizeSystem(sistema)
    handlers.synthetize_system(command, uow)
    command = commands.SynthetizeExecution(execucao)
    handlers.synthetize_execution(command, uow)
    command = commands.SynthetizeOperation(operacao)
    handlers.synthetize_operation(command, uow)

    Log.log().info("# Fim da síntese #")


app.add_command(completa)
app.add_command(sistema)
app.add_command(execucao)
app.add_command(operacao)
app.add_command(limpeza)
