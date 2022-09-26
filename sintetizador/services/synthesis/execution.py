from typing import Callable, Dict, List, Tuple
import pandas as pd
from inewave.config import MESES_DF
from datetime import datetime

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.execution.variable import Variable
from sintetizador.model.execution.executionsynthesis import ExecutionSynthesis

# TODO


class ExecutionSynthetizer:

    DEFAULT_EXECUTION_SYNTHESIS_ARGS: List[str] = [
        "CONVERGENCIA",
        "TEMPO",
        "CUSTO",
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
        RULES = Dict[Variable, Callable] = {
            Variable.CONVERGENCIA: cls._resolve_convergence,
            Variable.CUSTO: cls._resolve_cost,
            Variable.TEMPO_EXECUCAO: cls._resolve_runtime,
        }
        return RULES[synthesis.variable](uow)

    @classmethod
    def _resolve_convergence(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            pmo = uow.files.get_pmo()
            df = pmo.convergencia
            df_processed = pd.DataFrame(
                data={
                    "Iteracao": df["Iteração"][2::3].to_numpy(),
                    "Zinf": df["ZINF"][2::3].to_numpy(),
                    "Delta Zinf": df["Delta ZINF"][2::3].to_numpy(),
                    "Zsup": df["ZSUP Iteração"][2::3].to_numpy(),
                    "Tempo": df["Tempo"][::3].to_numpy(),
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
                    "Valor Esperado": "Media",
                    "Desvio Padrão do VE": "Desvio",
                    "(%)": "Percentual",
                }
            )
        return df_processed

    @classmethod
    def _resolve_runtime(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            tim = uow.files.get_newavetim()
            df = tim.tempos_etapas
        return df

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
            with uow:
                uow.export.synthetize_df(df, filename)
