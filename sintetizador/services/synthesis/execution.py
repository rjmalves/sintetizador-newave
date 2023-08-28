from typing import Callable, Dict, List, Optional, TypeVar, Type
import pandas as pd  # type: ignore
import pathlib
import socket
import logging
from traceback import print_exc

from sintetizador.services.unitofwork import AbstractUnitOfWork
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

    T = TypeVar("T")

    logger: Optional[logging.Logger] = None

    @classmethod
    def _validate_data(cls, data, type: Type[T], msg: str = "dados") -> T:
        if not isinstance(data, type):
            if cls.logger is not None:
                cls.logger.error(f"Erro na leitura de {msg}")
            raise RuntimeError()
        return data

    @classmethod
    def _default_args(cls) -> List[ExecutionSynthesis]:
        args = [
            ExecutionSynthesis.factory(a)
            for a in cls.DEFAULT_EXECUTION_SYNTHESIS_ARGS
        ]
        return [arg for arg in args if arg is not None]

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[ExecutionSynthesis]:
        args_data = [ExecutionSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        return valid_args

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
            arq_pmo = uow.files.get_pmo()
            if arq_pmo is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do pmo.dat para"
                        + " síntese da execução"
                    )
                raise RuntimeError()
            df = arq_pmo.convergencia
            if df is None:
                return pd.DataFrame()
            df_processed = pd.DataFrame(
                data={
                    "iter": df["iteracao"][2::3].to_numpy(),
                    "zinf": df["zinf"][2::3].to_numpy(),
                    "dZinf": df["delta_zinf"][2::3].to_numpy(),
                    "zsup": df["zsup_iteracao"][2::3].to_numpy(),
                    "tempo": df["tempo"][::3].dt.total_seconds().to_numpy(),
                }
            )
            df_processed = df_processed.astype({"tempo": int})
            return df_processed

    @classmethod
    def _resolve_cost(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            arq_pmo = uow.files.get_pmo()
            if arq_pmo is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do pmo.dat para"
                        + " síntese da execução"
                    )
                raise RuntimeError()
            df = arq_pmo.custo_operacao_series_simuladas
            if df is None:
                return pd.DataFrame()
            df_processed = df.rename(
                columns={
                    "parcela": "parcela",
                    "valor_esperado": "mean",
                    "desvio_padrao": "std",
                }
            )
            return df_processed[["parcela", "mean", "std"]]

    @classmethod
    def _resolve_runtime(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            tim = uow.files.get_newavetim()
            if tim is None:
                return pd.DataFrame()
            df = tim.tempos_etapas
            if df is None:
                return pd.DataFrame()
            df["tempo"] = df["tempo"].dt.total_seconds()
            return df

    @classmethod
    def _resolve_job_resources(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        # REGRA DE NEGOCIO: arquivos do hpc-job-monitor
        # monitor-job.parquet.gzip
        with uow:
            file = "monitor-job.parquet.gzip"
            if pathlib.Path(file).exists():
                try:
                    df = pd.read_parquet(file)
                except Exception as e:
                    if cls.logger is not None:
                        cls.logger.info(
                            f"Erro ao acessar arquivo {file}: {str(e)}"
                        )
                    return None
                return df
            return None

    @classmethod
    def _resolve_cluster_resources(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        # Le o do job para saber tempo inicial e final
        df_job = None
        with uow:
            file = "monitor-job.parquet.gzip"
            if pathlib.Path(file).exists():
                try:
                    df_job = pd.read_parquet(file)
                except Exception as e:
                    if cls.logger is not None:
                        cls.logger.info(
                            f"Erro ao acessar arquivo {file}: {str(e)}"
                        )
                    return None
        if df_job is None:
            return None
        jobTimeInstants = pd.to_datetime(
            df_job["timeInstant"], format="ISO8601"
        ).tolist()
        # REGRA DE NEGOCIO: arquivos do hpc-job-monitor
        # monitor-(hostname).parquet.gzip
        with set_directory(str(pathlib.Path.home())):
            file = f"monitor-{socket.gethostname()}.parquet.gzip"
            if pathlib.Path(file).exists():
                try:
                    df = pd.read_parquet(file)
                except Exception as e:
                    if cls.logger is not None:
                        cls.logger.info(
                            f"Erro ao acessar arquivo {file}: {str(e)}"
                        )
                    return None
                df["timeInstant"] = pd.to_datetime(
                    df["timeInstant"], format="ISO8601"
                )
                return df.loc[
                    (df["timeInstant"] >= jobTimeInstants[0])
                    & (df["timeInstant"] <= jobTimeInstants[-1])
                ]
        return None

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
        try:
            if len(variables) == 0:
                synthesis_variables = ExecutionSynthetizer._default_args()
            else:
                synthesis_variables = (
                    ExecutionSynthetizer._process_variable_arguments(variables)
                )

            for s in synthesis_variables:
                filename = str(s)
                if cls.logger is not None:
                    cls.logger.info(f"Realizando síntese de {filename}")
                df = cls._resolve(s, uow)
                if df is not None:
                    with uow:
                        uow.export.synthetize_df(df, filename)
        except Exception as e:
            print_exc()
            cls.logger.error(str(e))
