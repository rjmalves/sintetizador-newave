import pandas as pd  # type: ignore
from typing import Optional, List, Tuple
from datetime import datetime
from inewave.config import MESES_DF
from sintetizador.utils.log import Log
import sintetizador.domain.commands as commands
from sintetizador.model.variable import Variable
from sintetizador.model.temporalresolution import TemporalResolution
from sintetizador.model.spatialresolution import SpatialResolution
from sintetizador.services.unitofwork import AbstractUnitOfWork


def process_nwlistop_variable_arguments(
    command: commands.ProcessVariableArguments,
) -> List[Tuple[Variable, SpatialResolution, TemporalResolution]]:
    args_data = [c.split("_") for c in command.args]
    for a in args_data:
        if len(a) != 3:
            Log.log(f"Erro no argumento fornecido: {a}")
            return []
    return [
        (
            Variable.factory(a[0]),
            SpatialResolution.factory(a[1]),
            TemporalResolution.factory(a[2]),
        )
        for a in args_data
    ]


def format_nwlistop_df(
    command: commands.FormatNwlistopDataframe,
) -> Optional[pd.DataFrame]:
    if command.df is None:
        return None
    if command.temporalresolution == TemporalResolution.MES:
        return format_nwlistop_series_df(
            commands.FormatNwlistopSeriesDataframe(command.df)
        )
    elif command.temporalresolution == TemporalResolution.PATAMAR:
        return format_nwlistop_series_patamar_df(
            commands.FormatNwlistopPatamarDataframe(command.df)
        )


def format_nwlistop_series_df(
    command: commands.FormatNwlistopSeriesDataframe,
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
        df_ano.columns = [str(s) for s in list(range(1, df_ano.shape[1] + 1))]
        df_series = pd.concat([df_series, df_ano], ignore_index=True)
    cols = df_series.columns.tolist()
    df_series["Data"] = labels
    return df_series[["Data"] + cols]


def format_nwlistop_series_patamar_df(
    command: commands.FormatNwlistopPatamarDataframe,
) -> pd.DataFrame:
    anos = command.df["Ano"].unique().tolist()
    patamares = command.df["Patamar"].unique().tolist()
    labels = pd.date_range(
        datetime(year=anos[0], month=1, day=1),
        datetime(year=anos[-1], month=12, day=1),
        freq="MS",
    ).tolist()
    df_series = pd.DataFrame()
    for a in anos:
        for p in patamares:
            df_ano_patamar = command.df.loc[
                (command.df["Ano"] == a) & (command.df["Patamar"] == p),
                MESES_DF,
            ].T
            cols = [
                str(s) for s in list(range(1, df_ano_patamar.shape[1] + 1))
            ]
            df_ano_patamar.columns = cols
            df_ano_patamar["Patamar"] = p
            df_ano_patamar = df_ano_patamar[["Patamar"] + cols]
            df_series = pd.concat(
                [df_series, df_ano_patamar], ignore_index=True
            )
    cols = df_series.columns.tolist()
    df_series["Data"] = labels * len(patamares)
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
            return format_nwlistop_df(
                commands.FormatNwlistopDataframe(
                    df, command.temporalresolution
                )
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
            df_sbm = format_nwlistop_df(
                commands.FormatNwlistopDataframe(
                    uow.files.get_nwlistop(
                        command.variable,
                        command.spatialresolution,
                        command.temporalresolution,
                        submercado=s,
                    ),
                    command.temporalresolution,
                )
            )
            if df_sbm is None:
                continue
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
            df_ree = format_nwlistop_df(
                commands.FormatNwlistopDataframe(
                    uow.files.get_nwlistop(
                        command.variable,
                        command.spatialresolution,
                        command.temporalresolution,
                        ree=s,
                    ),
                    command.temporalresolution,
                )
            )
            if df_ree is None:
                continue
            cols = df_ree.columns.tolist()
            df_ree["REE"] = n
            df_ree = df_ree[["REE"] + cols]
            df = pd.concat(
                [df, df_ree],
                ignore_index=True,
            )
        return df


def process_uhe_data(
    command: commands.ProcessUHEData, uow: AbstractUnitOfWork
) -> pd.DataFrame:
    with uow:
        confhd = uow.files.get_confhd()
        uhes_idx = confhd.usinas["Número"]
        uhes_name = confhd.usinas["Nome"]
        df = pd.DataFrame()
        for s, n in zip(uhes_idx, uhes_name):
            Log.log().info(f"Processando arquivo da UHE: {s} - {n}")
            df_uhe = format_nwlistop_df(
                commands.FormatNwlistopDataframe(
                    uow.files.get_nwlistop(
                        command.variable,
                        command.spatialresolution,
                        command.temporalresolution,
                        ree=s,
                    ),
                    command.temporalresolution,
                )
            )
            if df_uhe is None:
                continue
            cols = df_uhe.columns.tolist()
            df_uhe["UHE"] = n
            df_uhe = df_uhe[["UHE"] + cols]
            df = pd.concat(
                [df, df_uhe],
                ignore_index=True,
            )
        return df


def process_ute_data(
    command: commands.ProcessUTEData, uow: AbstractUnitOfWork
) -> pd.DataFrame:
    with uow:
        conft = uow.files.get_conft()
        utes_idx = conft.usinas["Número"]
        utes_name = conft.usinas["Nome"]
        df = pd.DataFrame()
        for s, n in zip(utes_idx, utes_name):
            Log.log().info(f"Processando arquivo da UTE: {s} - {n}")
            df_ute = format_nwlistop_df(
                commands.FormatNwlistopDataframe(
                    uow.files.get_nwlistop(
                        command.variable,
                        command.spatialresolution,
                        command.temporalresolution,
                        ree=s,
                    ),
                    command.temporalresolution,
                )
            )
            if df_ute is None:
                continue
            cols = df_ute.columns.tolist()
            df_ute["UTE"] = n
            df_ute = df_ute[["UTE"] + cols]
            df = pd.concat(
                [df, df_ute],
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
    elif command.spatialresolution == SpatialResolution.USINA_HIDROELETRICA:
        df = process_uhe_data(
            commands.ProcessUHEData(
                command.variable,
                command.spatialresolution,
                command.temporalresolution,
            ),
            uow,
        )
    elif command.spatialresolution == SpatialResolution.USINA_TERMELETRICA:
        df = process_ute_data(
            commands.ProcessUTEData(
                command.variable,
                command.spatialresolution,
                command.temporalresolution,
            ),
            uow,
        )

    with uow:
        uow.synthetizer.synthetize_df(df, filename)
