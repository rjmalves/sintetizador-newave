import click
import os
from sintetizador.model.settings import Settings
import sintetizador.domain.commands as commands
import sintetizador.services.handlers as handlers
from sintetizador.services.unitofwork import factory
from sintetizador.utils.log import Log
from multiprocessing import Manager


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

    m = Manager()
    q = m.Queue(-1)
    Log.start_logging_process(q)

    logger = Log.configure_main_logger(q)
    logger.info("# Realizando síntese do SISTEMA #")

    uow = factory("FS", Settings().synthesis_dir, q)
    command = commands.SynthetizeSystem(variaveis)
    handlers.synthetize_system(command, uow)

    logger.info("# Fim da síntese #")


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

    m = Manager()
    q = m.Queue(-1)
    Log.start_logging_process(q)

    logger = Log.configure_main_logger(q)

    os.environ["FORMATO_SINTESE"] = formato
    logger.info("# Realizando síntese da EXECUÇÃO #")

    uow = factory("FS", Settings().synthesis_dir, q)
    command = commands.SynthetizeExecution(variaveis)
    handlers.synthetize_execution(command, uow)

    logger.info("# Fim da síntese #")


@click.command("cenarios")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
@click.option(
    "--processadores",
    default=1,
    help="numero de processadores para paralelizar",
)
def cenarios(variaveis, formato, processadores):
    """
    Realiza a síntese dos dados de cenários do NEWAVE.
    """

    m = Manager()
    q = m.Queue(-1)
    Log.start_logging_process(q)

    logger = Log.configure_main_logger(q)

    os.environ["FORMATO_SINTESE"] = formato
    os.environ["PROCESSADORES"] = processadores
    logger.info("# Realizando síntese de CENÁRIOS #")

    uow = factory("FS", Settings().synthesis_dir, q)
    command = commands.SynthetizeScenarios(variaveis)
    handlers.synthetize_scenarios(command, uow)

    logger.info("# Fim da síntese #")


@click.command("operacao")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
@click.option(
    "--processadores",
    default=1,
    help="numero de processadores para paralelizar",
)
def operacao(variaveis, formato, processadores):
    """
    Realiza a síntese dos dados da operação do NEWAVE (NWLISTOP).
    """

    m = Manager()
    q = m.Queue(-1)
    Log.start_logging_process(q)

    logger = Log.configure_main_logger(q)

    os.environ["FORMATO_SINTESE"] = formato
    os.environ["PROCESSADORES"] = processadores
    logger.info("# Realizando síntese da OPERACAO #")

    uow = factory("FS", Settings().synthesis_dir, q)
    command = commands.SynthetizeOperation(variaveis)
    handlers.synthetize_operation(command, uow)

    logger.info("# Fim da síntese #")


@click.command("politica")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def politica(variaveis, formato):
    """
    Realiza a síntese dos dados da política do NEWAVE (NWLISTCF).
    """

    m = Manager()
    q = m.Queue(-1)
    Log.start_logging_process(q)

    logger = Log.configure_main_logger(q)
    os.environ["FORMATO_SINTESE"] = formato
    logger.info("# Realizando síntese da POLITICA #")

    uow = factory("FS", Settings().synthesis_dir, q)
    command = commands.SynthetizePolicy(variaveis)
    handlers.synthetize_policy(command, uow)

    logger.info("# Fim da síntese #")


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
    "-politica", multiple=True, help="variável da política para síntese"
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
@click.option(
    "--processadores",
    default=1,
    help="numero de processadores para paralelizar",
)
def completa(sistema, execucao, operacao, politica, formato, processadores):
    """
    Realiza a síntese completa do NEWAVE.
    """

    m = Manager()
    q = m.Queue(-1)
    Log.start_logging_process(q)

    logger = Log.configure_main_logger(q)
    os.environ["FORMATO_SINTESE"] = formato
    os.environ["PROCESSADORES"] = processadores
    logger.info("# Realizando síntese COMPLETA #")

    uow = factory("FS", Settings().synthesis_dir, q)
    command = commands.SynthetizeSystem(sistema)
    handlers.synthetize_system(command, uow)
    command = commands.SynthetizeExecution(execucao)
    handlers.synthetize_execution(command, uow)
    command = commands.SynthetizeOperation(operacao)
    handlers.synthetize_operation(command, uow)
    command = commands.SynthetizePolicy(politica)
    handlers.synthetize_policy(command, uow)

    logger.info("# Fim da síntese #")


app.add_command(completa)
app.add_command(sistema)
app.add_command(execucao)
app.add_command(cenarios)
app.add_command(operacao)
app.add_command(politica)
app.add_command(limpeza)
