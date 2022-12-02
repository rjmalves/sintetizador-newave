from typing import Callable, Dict, List
import pandas as pd
import pathlib
import socket

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.utils.fs import set_directory
from sintetizador.model.execution.variable import Variable
from sintetizador.model.execution.executionsynthesis import ExecutionSynthesis


class ExecutionSynthetizer:

    DEFAULT_EXECUTION_SYNTHESIS_ARGS: List[str] = [
        "PROGRAMA",
        "CONVERGENCIA",
        "TEMPO",
        "CUSTOS",
        "RECURSOS_JOB",
        "RECURSOS_CLUSTER",
    ]

    @classmethod
    def _default_args(cls) -> List[ExecutionSynthesis]:
        return [
            ExecutionSynthesis.factory(a)
            for a in cls.DEFAULT_EXECUTION_SYNTHESIS_ARGS
        ]

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[ExecutionSynthesis]:
        args_data = [ExecutionSynthesis.factory(c) for c in args]
        for i, a in enumerate(args_data):
            if a is None:
                Log.log(f"Erro no argumento fornecido: {args[i]}")
                return []
        return args_data

    @classmethod
    def _resolve(
        cls, synthesis: ExecutionSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        RULES: Dict[Variable, Callable] = {
            Variable.PROGRAMA: cls._resolve_program,
            Variable.CONVERGENCIA: cls._resolve_convergence,
            Variable.COMPOSICAO_CUSTOS: cls._resolve_cost,
            Variable.TEMPO_EXECUCAO: cls._resolve_runtime,
            Variable.RECURSOS_JOB: cls._resolve_job_resources,
            Variable.RECURSOS_CLUSTER: cls._resolve_cluster_resources,
        }
        return RULES[synthesis.variable](uow)

    @classmethod
    def _resolve_program(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return pd.DataFrame(data={"programa": ["NEWAVE"]})

    @classmethod
    def _resolve_convergence(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            pmo = uow.files.get_pmo()
            df = pmo.convergencia
            df_processed = pd.DataFrame(
                data={
                    "iter": df["Iteração"][2::3].to_numpy(),
                    "zinf": df["ZINF"][2::3].to_numpy(),
                    "dZinf": df["Delta ZINF"][2::3].to_numpy(),
                    "zsup": df["ZSUP Iteração"][2::3].to_numpy(),
                    "tempo": df["Tempo"][::3].dt.total_seconds().to_numpy(),
                }
            )
        return df_processed

    @classmethod
    def _resolve_cost(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            pmo = uow.files.get_pmo()
            df = pmo.custo_operacao_series_simuladas
            df_processed = df.rename(
                columns={
                    "Parcela": "parcela",
                    "Valor Esperado": "mean",
                    "Desvio Padrão do VE": "std",
                }
            )
        return df_processed[["parcela", "mean", "std"]]

    @classmethod
    def _resolve_runtime(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            tim = uow.files.get_newavetim()
            df = tim.tempos_etapas
        df = df.rename(columns={"Etapa": "etapa", "Tempo": "tempo"})
        df["tempo"] = df["tempo"].dt.total_seconds()
        return df

    @classmethod
    def _resolve_job_resources(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        # REGRA DE NEGOCIO: arquivos do hpc-job-monitor
        # monitor-job.csv
        with uow:
            file = "monitor-job.csv"
            if pathlib.Path(file).exists():
                df = pd.read_csv("monitor-job.csv")
                return df
            return None

    @classmethod
    def _resolve_cluster_resources(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        # Le o do job para saber tempo inicial e final
        df_job = None
        with uow:
            file = "monitor-job.csv"
            if pathlib.Path(file).exists():
                df_job = pd.read_csv("monitor-job.csv")
        if df_job is None:
            return None
        jobTimeInstants = pd.to_datetime(df_job["timeInstant"]).tolist()
        # REGRA DE NEGOCIO: arquivos do hpc-job-monitor
        # monitor-(hostname).csv
        with set_directory(str(pathlib.Path.home())):
            file = f"monitor-{socket.gethostname()}.csv"
            if pathlib.Path(file).exists():
                df = pd.read_csv(file)
                df["timeInstant"] = pd.to_datetime(df["timeInstant"])
                return df.loc[
                    (df["timeInstant"] >= jobTimeInstants[0])
                    & (df["timeInstant"] <= jobTimeInstants[-1])
                ]
        return None

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        if len(variables) == 0:
            variables = ExecutionSynthetizer._default_args()
        else:
            variables = ExecutionSynthetizer._process_variable_arguments(
                variables
            )
        for s in variables:
            filename = str(s)
            Log.log().info(f"Realizando síntese de {filename}")
            df = cls._resolve(s, uow)
            if df is not None:
                with uow:
                    uow.export.synthetize_df(df, filename)
