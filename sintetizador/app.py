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
    (
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        TemporalResolution.MES,
    ),
    (
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.SUBMERCADO,
        TemporalResolution.MES,
    ),
    (
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.SISTEMA_INTERLIGADO,
        TemporalResolution.MES,
    ),
    (
        Variable.VAZAO_AFLUENTE,
        SpatialResolution.USINA_HIDROELETRICA,
        TemporalResolution.MES,
    ),
    (
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.USINA_HIDROELETRICA,
        TemporalResolution.MES,
    ),
    (
        Variable.VOLUME_TURBINADO,
        SpatialResolution.USINA_HIDROELETRICA,
        TemporalResolution.MES,
    ),
    (
        Variable.VOLUME_VERTIDO,
        SpatialResolution.USINA_HIDROELETRICA,
        TemporalResolution.MES,
    ),
    (
        Variable.VOLUME_ARMAZENADO_ABSOLUTO,
        SpatialResolution.USINA_HIDROELETRICA,
        TemporalResolution.MES,
    ),
    (
        Variable.VOLUME_ARMAZENADO_PERCENTUAL,
        SpatialResolution.USINA_HIDROELETRICA,
        TemporalResolution.MES,
    ),
    (
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.USINA_HIDROELETRICA,
        TemporalResolution.PATAMAR,
    ),
    (
        Variable.VELOCIDADE_VENTO,
        SpatialResolution.USINA_EOLICA,
        TemporalResolution.MES,
    ),
    (
        Variable.GERACAO_EOLICA,
        SpatialResolution.USINA_EOLICA,
        TemporalResolution.MES,
    ),
    (
        Variable.GERACAO_EOLICA,
        SpatialResolution.SUBMERCADO,
        TemporalResolution.MES,
    ),
    (
        Variable.GERACAO_EOLICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
        TemporalResolution.MES,
    ),
    (
        Variable.GERACAO_EOLICA,
        SpatialResolution.USINA_EOLICA,
        TemporalResolution.PATAMAR,
    ),
    (
        Variable.GERACAO_EOLICA,
        SpatialResolution.SUBMERCADO,
        TemporalResolution.PATAMAR,
    ),
    (
        Variable.GERACAO_EOLICA,
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

    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ["TMPDIR"] = tmpdirname
        uow = factory(
            "FS",
            Settings().tmpdir,
            Settings().synthesis_dir,
        )
        # Preprocess
        with uow:
            variaveis_sintese = []
            dger = uow.files.get_dger()
            ree = uow.files.get_ree()
            indiv = ree.rees["Mês Fim Individualizado"].isna().sum() == 0
            eolica = dger.considera_geracao_eolica != 0
            Log.log().info(f"Caso com geração de cenários de eólica: {eolica}")
            Log.log().info(f"Caso com modelagem híbrida: {indiv}")
            for v in variaveis:
                if (
                    v[0]
                    in [Variable.VELOCIDADE_VENTO, Variable.GERACAO_EOLICA]
                    and not eolica
                ):
                    continue
                if v[1] == SpatialResolution.USINA_HIDROELETRICA and not indiv:
                    continue
                variaveis_sintese.append(v)
            Log.log().info(f"Variáveis: {variaveis_sintese}")
        for v in variaveis_sintese:
            command = commands.SynthetizeNwlistop(*v)
            handlers.synthetize_nwlistop(command, uow)

    Log.log().info("# Fim da síntese #")


app.add_command(nwlistop)
