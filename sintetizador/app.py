import click
import os
import tempfile
from typing import List, Tuple
from sintetizador.model.settings import Settings
from sintetizador.model.variable import Variable
from sintetizador.model.spatialresolution import SpatialResolution
from sintetizador.model.temporalresolution import TemporalResolution
import sintetizador.domain.commands as commands
import sintetizador.services.handlers as handlers
from sintetizador.services.unitofwork import factory
from sintetizador.utils.log import Log


@click.group()
def app():
    pass


DEFAULT_NWLISTOP_SYNTHESIS_ARGS: List[
    Tuple[Variable, SpatialResolution, TemporalResolution]
] = [
    (
        Variable.CUSTO_MARGINAL_OPERACAO,
        SpatialResolution.SUBMERCADO,
        TemporalResolution.MES,
    ),
    (
        Variable.VALOR_AGUA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        TemporalResolution.MES,
    ),
    (
        Variable.CUSTO_GERACAO_TERMICA,
        SpatialResolution.SUBMERCADO,
        TemporalResolution.MES,
    ),
    (
        Variable.CUSTO_GERACAO_TERMICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
        TemporalResolution.MES,
    ),
    (
        Variable.CUSTO_OPERACAO,
        SpatialResolution.SISTEMA_INTERLIGADO,
        TemporalResolution.MES,
    ),
    (
        Variable.ENERGIA_NATURAL_AFLUENTE,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        TemporalResolution.MES,
    ),
    (
        Variable.ENERGIA_NATURAL_AFLUENTE,
        SpatialResolution.SUBMERCADO,
        TemporalResolution.MES,
    ),
    (
        Variable.ENERGIA_NATURAL_AFLUENTE,
        SpatialResolution.SISTEMA_INTERLIGADO,
        TemporalResolution.MES,
    ),
    (
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        TemporalResolution.MES,
    ),
    (
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL,
        SpatialResolution.SUBMERCADO,
        TemporalResolution.MES,
    ),
    (
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
        TemporalResolution.MES,
    ),
    (
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        TemporalResolution.MES,
    ),
    (
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA,
        SpatialResolution.SUBMERCADO,
        TemporalResolution.MES,
    ),
    (
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA,
        SpatialResolution.SISTEMA_INTERLIGADO,
        TemporalResolution.MES,
    ),
    (
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        TemporalResolution.MES,
    ),
    (
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.SUBMERCADO,
        TemporalResolution.MES,
    ),
    (
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
        TemporalResolution.MES,
    ),
    (
        Variable.GERACAO_TERMICA,
        SpatialResolution.SUBMERCADO,
        TemporalResolution.MES,
    ),
    (
        Variable.GERACAO_TERMICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
        TemporalResolution.MES,
    ),
    (
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        TemporalResolution.PATAMAR,
    ),
    (
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.SUBMERCADO,
        TemporalResolution.PATAMAR,
    ),
    (
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
        TemporalResolution.PATAMAR,
    ),
    (
        Variable.GERACAO_TERMICA,
        SpatialResolution.SUBMERCADO,
        TemporalResolution.PATAMAR,
    ),
    (
        Variable.GERACAO_TERMICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
        TemporalResolution.PATAMAR,
    ),
]


@click.command("nwlistop")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def nwlistop(variaveis, formato):
    """
    Realiza a síntese do NWLISTOP.
    """
    os.environ["FORMATO_SINTESE"] = formato
    Log.log().info("# Realizando síntese do NWLISTOP #")
    if len(variaveis) == 0:
        variaveis = DEFAULT_NWLISTOP_SYNTHESIS_ARGS
    else:
        variaveis = handlers.process_nwlistop_variable_arguments(
            commands.ProcessVariableArguments(variaveis)
        )

    Log.log().info(f"Variáveis: {variaveis}")

    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ["TMPDIR"] = tmpdirname
        uow = factory(
            "FS",
            Settings().tmpdir,
            Settings().synthesis_dir,
        )
        for v in variaveis:
            command = commands.SynthetizeNwlistop(v[0], v[1], v[2])
            handlers.synthetize_nwlistop(command, uow)

    Log.log().info("# Fim da síntese #")


app.add_command(nwlistop)
