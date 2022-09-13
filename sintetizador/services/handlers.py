import pandas as pd  # type: ignore

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
        if command.spatialresolution == SpatialResolution.SISTEMA_INTERLIGADO:
            df = uow.files.get_nwlistop(
                command.variable,
                command.spatialresolution,
                command.temporalresolution,
            )
        elif command.spatialresolution == SpatialResolution.SUBMERCADO:
            sbms_idx = uow.files.get_sistema().custo_deficit["Num. Subsistema"]
            sbms_name = uow.files.get_sistema().custo_deficit["Nome"]
            df = pd.DataFrame()
            for s, n in zip(sbms_idx, sbms_name):
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
            rees_idx = uow.files.get_ree().rees["Número"]
            rees_name = uow.files.get_ree().rees["Nome"]
            df = pd.DataFrame()
            for s, n in zip(rees_idx, rees_name):
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
