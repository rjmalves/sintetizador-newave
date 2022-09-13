import pandas as pd  # type: ignore

from sintetizador.utils.log import Log
import sintetizador.domain.commands as commands
from sintetizador.model.spatialresolution import SpatialResolution
from sintetizador.services.unitofwork import AbstractUnitOfWork


def synthetize_nwlistop(
    command: commands.SynthetizeNwlistop, uow: AbstractUnitOfWork
):
    with uow:
        filename = (
            command.variable.value
            + "_"
            + command.spatialresolution.value
            + "_"
            + command.temporalresolution.value
        )
        Log.log().info(f"Realizando síntese de {filename}")
        if command.spatialresolution == SpatialResolution.SISTEMA_INTERLIGADO:
            Log.log().info(f"Processando arquivo do SIN")
            df = uow.files.get_nwlistop(
                command.variable,
                command.spatialresolution,
                command.temporalresolution,
                "",
            )
        elif command.spatialresolution == SpatialResolution.SUBMERCADO:
            sistema = uow.files.get_sistema()
            sbms_idx = sistema.custo_deficit["Num. Subsistema"]
            sbms_name = sistema.custo_deficit["Nome"]
            df = pd.DataFrame()
            for s, n in zip(sbms_idx, sbms_name):
                Log.log().info(f"Processando arquivo do submercado: {s} - {n}")
                df_sbm = uow.files.get_nwlistop(
                    command.variable,
                    command.spatialresolution,
                    command.temporalresolution,
                    submercado=s,
                )
                cols = df_sbm.columns.tolist()
                df_sbm["Submercado"] = n
                df_sbm = df_sbm[["Submercado"] + cols]
                df = pd.concat(
                    [df, df_sbm],
                    ignore_index=True,
                )
        elif (
            command.spatialresolution
            == SpatialResolution.RESERVATORIO_EQUIVALENTE
        ):
            ree = uow.files.get_ree()
            rees_idx = ree.rees["Número"]
            rees_name = ree.rees["Nome"]
            df = pd.DataFrame()
            for s, n in zip(rees_idx, rees_name):
                Log.log().info(f"Processando arquivo do REE: {s} - {n}")
                df_sbm = uow.files.get_nwlistop(
                    command.variable,
                    command.spatialresolution,
                    command.temporalresolution,
                    ree=s,
                )
                cols = df_sbm.columns.tolist()
                df_sbm["REE"] = n
                df_sbm = df_sbm[["REE"] + cols]
                df = pd.concat(
                    [df, df_sbm],
                    ignore_index=True,
                )
        uow.synthetizer.synthetize_df(df, filename)
