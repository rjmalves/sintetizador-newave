import pandas as pd  # type: ignore
from datetime import datetime
from inewave.config import MESES_DF

from sintetizador.utils.log import Log
import sintetizador.domain.commands as commands
from sintetizador.model.spatialresolution import SpatialResolution
from sintetizador.services.unitofwork import AbstractUnitOfWork


def format_nwlistop_series_df(
    command: commands.FormatNwlistopDataframe,
) -> pd.DataFrame:
    anos = command.df["Ano"].unique().tolist()
    labels = pd.date_range(
        datetime(year=anos[0], month=1, day=1),
        datetime(year=anos[-1], month=12, day=1),
        freq="MS",
    )
    df_series = pd.DataFrame()
    for a in anos:
        df_ano = command.df.loc[command.df["Ano"] == a, MESES_DF].T
        df_ano.columns = list(range(1, df_ano.shape[1] + 1))
        df_series = pd.concat([df_series, df_ano], ignore_index=True)
    cols = df_series.columns.tolist()
    df_series["Data"] = labels
    return df_series[["Data"] + cols]


def process_sin_data(
    command: commands.ProcessSINData, uow: AbstractUnitOfWork
) -> pd.DataFrame:
    with uow:
        Log.log().info(f"Processando arquivo do SIN")
        df = uow.files.get_nwlistop(
            command.variable,
            command.spatialresolution,
            command.temporalresolution,
            "",
        )
        if df is not None:
            return format_nwlistop_series_df(
                commands.FormatNwlistopDataframe(df)
            )
        else:
            return pd.DataFrame()


def process_submercado_data(
    command: commands.ProcessSubmercadoData, uow: AbstractUnitOfWork
) -> pd.DataFrame:
    with uow:
        sistema = uow.files.get_sistema()
        sistemas_reais = sistema.custo_deficit.loc[
            sistema.custo_deficit["Fictício"] == 0, :
        ]
        sbms_idx = sistemas_reais["Num. Subsistema"]
        sbms_name = sistemas_reais["Nome"]
        df = pd.DataFrame()
        for s, n in zip(sbms_idx, sbms_name):
            Log.log().info(f"Processando arquivo do submercado: {s} - {n}")
            df_sbm = format_nwlistop_series_df(
                commands.FormatNwlistopDataframe(
                    uow.files.get_nwlistop(
                        command.variable,
                        command.spatialresolution,
                        command.temporalresolution,
                        submercado=s,
                    )
                )
            )
            cols = df_sbm.columns.tolist()
            df_sbm["Submercado"] = n
            df_sbm = df_sbm[["Submercado"] + cols]
            df = pd.concat(
                [df, df_sbm],
                ignore_index=True,
            )
        return df


def process_ree_data(
    command: commands.ProcessREEData, uow: AbstractUnitOfWork
) -> pd.DataFrame:
    with uow:
        ree = uow.files.get_ree()
        rees_idx = ree.rees["Número"]
        rees_name = ree.rees["Nome"]
        df = pd.DataFrame()
        for s, n in zip(rees_idx, rees_name):
            Log.log().info(f"Processando arquivo do REE: {s} - {n}")
            df_ree = format_nwlistop_series_df(
                commands.FormatNwlistopDataframe(
                    uow.files.get_nwlistop(
                        command.variable,
                        command.spatialresolution,
                        command.temporalresolution,
                        ree=s,
                    )
                )
            )
            cols = df_ree.columns.tolist()
            df_ree["REE"] = n
            df_ree = df_ree[["REE"] + cols]
            df = pd.concat(
                [df, df_ree],
                ignore_index=True,
            )
        return df


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
        df = process_sin_data(
            commands.ProcessSINData(
                command.variable,
                command.spatialresolution,
                command.temporalresolution,
            ),
            uow,
        )
    elif command.spatialresolution == SpatialResolution.SUBMERCADO:
        df = process_submercado_data(
            commands.ProcessSubmercadoData(
                command.variable,
                command.spatialresolution,
                command.temporalresolution,
            ),
            uow,
        )
    elif (
        command.spatialresolution == SpatialResolution.RESERVATORIO_EQUIVALENTE
    ):
        df = process_ree_data(
            commands.ProcessREEData(
                command.variable,
                command.spatialresolution,
                command.temporalresolution,
            ),
            uow,
        )
    with uow:
        uow.synthetizer.synthetize_df(df, filename)
