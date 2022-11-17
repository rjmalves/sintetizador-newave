from typing import Callable, Dict, List, Tuple
import pandas as pd
from inewave.config import MESES_DF
from datetime import datetime

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.execution.variable import Variable
from sintetizador.model.execution.executionsynthesis import ExecutionSynthesis


class ExecutionSynthetizer:

    DEFAULT_EXECUTION_SYNTHESIS_ARGS: List[str] = [
        "CONVERGENCIA",
        "TEMPO",
        "CUSTOS",
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
            Variable.CONVERGENCIA: cls._resolve_convergence,
            Variable.COMPOSICAO_CUSTOS: cls._resolve_cost,
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
                    "iter": df["Iteração"][2::3].to_numpy(),
                    "zinf": df["ZINF"][2::3].to_numpy(),
                    "dZinf": df["Delta ZINF"][2::3].to_numpy(),
                    "zsup": df["ZSUP Iteração"][2::3].to_numpy(),
                    "tempo": df["Tempo"][::3].to_numpy(),
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
        # PREPROCESS_STAGES = ["Leitura de Dados", "Calculos Iniciais"]
        # CONVERGENCE_STAGES = ["Calculo da Politica"]
        # SIMULATION_STAGES = ["Simulacao Final"]
        # dfp = pd.DataFrame(columns=["etapa", "tempo"])
        # dfp.loc[dfp.shape[0]] = [
        #     "preprocessamento",
        #     float(df.loc[df["Etapa"].isin(PREPROCESS_STAGES), "Tempo"].sum()),
        # ]
        # dfp.loc[dfp.shape[0]] = [
        #     "convergencia",
        #     float(df.loc[df["Etapa"].isin(CONVERGENCE_STAGES), "Tempo"].sum()),
        # ]
        # dfp.loc[dfp.shape[0]] = [
        #     "simulacao",
        #     float(df.loc[df["Etapa"].isin(SIMULATION_STAGES), "Tempo"].sum()),
        # ]
        # dfp["tempo"] = dfp["tempo"].dt.total_seconds()
        # return dfp

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
