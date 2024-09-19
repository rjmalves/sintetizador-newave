import logging
from logging import ERROR, INFO
from traceback import print_exc
from typing import Callable, Dict, List, Optional, TypeVar

import pandas as pd  # type: ignore

from app.internal.constants import (
    EXECUTION_SYNTHESIS_METADATA_OUTPUT,
    EXECUTION_SYNTHESIS_SUBDIR,
)
from app.model.execution.executionsynthesis import (
    SUPPORTED_SYNTHESIS,
    ExecutionSynthesis,
)
from app.model.execution.variable import Variable
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.regex import match_variables_with_wildcards
from app.utils.timing import time_and_log

# TODO - rever nomes das colunas
# TODO - tirar tempo total


class ExecutionSynthetizer:
    DEFAULT_EXECUTION_SYNTHESIS_ARGS: List[str] = SUPPORTED_SYNTHESIS

    T = TypeVar("T")

    logger: Optional[logging.Logger] = None

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _default_args(cls) -> List[ExecutionSynthesis]:
        args = [
            ExecutionSynthesis.factory(a)
            for a in cls.DEFAULT_EXECUTION_SYNTHESIS_ARGS
        ]
        return [arg for arg in args if arg is not None]

    @classmethod
    def _match_wildcards(cls, variables: List[str]) -> List[str]:
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_EXECUTION_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[ExecutionSynthesis]:
        args_data = [ExecutionSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        return valid_args

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: List[str], uow: AbstractUnitOfWork
    ) -> List[ExecutionSynthesis]:
        """
        Realiza o pré-processamento das variáveis de síntese fornecidas,
        filtrando as válidas para o caso em questão.
        """
        try:
            if len(variables) == 0:
                synthesis_variables = cls._default_args()
            else:
                all_variables = cls._match_wildcards(variables)
                synthesis_variables = cls._process_variable_arguments(
                    all_variables
                )
        except Exception as e:
            print_exc()
            cls._log(str(e), ERROR)
            cls._log("Erro no pré-processamento das variáveis", ERROR)
            synthesis_variables = []
        return synthesis_variables

    @classmethod
    def _resolve(
        cls, synthesis: ExecutionSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        RULES: Dict[Variable, Callable] = {
            Variable.PROGRAMA: cls._resolve_program,
            Variable.CONVERGENCIA: cls._resolve_convergence,
            Variable.COMPOSICAO_CUSTOS: cls._resolve_cost,
            Variable.TEMPO_EXECUCAO: cls._resolve_runtime,
        }
        return RULES[synthesis.variable](uow)

    # TODO - adicionar versao
    @classmethod
    def _resolve_program(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return pd.DataFrame(data={"programa": ["NEWAVE"]})

    @classmethod
    def _resolve_convergence(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.convergence(uow)
        processed_d = pd.DataFrame(
            data={
                "iteracao": df["iteracao"][2::3].to_numpy(),
                "zinf": df["zinf"][2::3].to_numpy(),
                "delta_zinf": df["delta_zinf"][2::3].to_numpy(),
                "zsup": df["zsup_iteracao"][2::3].to_numpy(),
                "tempo": df["tempo"][::3].dt.total_seconds().to_numpy(),
            }
        )
        processed_d = processed_d.astype({"tempo": int})
        return processed_d

    @classmethod
    def _resolve_cost(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.costs(uow)
        return df[["parcela", "valor_esperado", "desvio_padrao"]]

    @classmethod
    def _resolve_runtime(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.runtimes(uow)
        df["tempo"] = df["tempo"].dt.total_seconds()
        df = df.loc[df["etapa"] != "Tempo Total"]
        return df

    @classmethod
    def _export_metadata(
        cls,
        success_synthesis: List[ExecutionSynthesis],
        uow: AbstractUnitOfWork,
    ):
        metadata_df = pd.DataFrame(
            columns=[
                "chave",
                "nome_curto",
                "nome_longo",
            ]
        )
        for s in success_synthesis:
            metadata_df.loc[metadata_df.shape[0]] = [
                str(s),
                s.variable.short_name,
                s.variable.long_name,
            ]
        with uow:
            existing_df = uow.export.read_df(
                EXECUTION_SYNTHESIS_METADATA_OUTPUT
            )
            if existing_df is not None:
                metadata_df = pd.concat(
                    [existing_df, metadata_df], ignore_index=True
                )
                metadata_df = metadata_df.drop_duplicates()
            uow.export.synthetize_df(
                metadata_df, EXECUTION_SYNTHESIS_METADATA_OUTPUT
            )

    @classmethod
    def _synthetize_single_variable(
        cls, s: ExecutionSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[ExecutionSynthesis]:
        """
        Realiza a síntese de execução para uma variável
        fornecida.
        """
        filename = str(s)
        with time_and_log(
            message_root=f"Tempo para sintese de {filename}",
            logger=cls.logger,
        ):
            try:
                cls._log(f"Realizando síntese de {filename}")
                df = cls._resolve(s, uow)
                if df is not None:
                    with uow:
                        uow.export.synthetize_df(df, filename)
                        return s
                return None
            except Exception as e:
                print_exc()
                cls._log(str(e), ERROR)
                return None

    @classmethod
    def enforce_version(cls, uow: AbstractUnitOfWork):
        version = Deck.pmo(uow).versao_modelo
        if version is not None:
            uow.version = version

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
        uow.subdir = EXECUTION_SYNTHESIS_SUBDIR
        with time_and_log(
            message_root="Tempo para sintese da execucao",
            logger=cls.logger,
        ):
            cls.enforce_version(uow)
            synthesis_variables = cls._preprocess_synthesis_variables(
                variables, uow
            )
            success_synthesis: List[ExecutionSynthesis] = []
            for s in synthesis_variables:
                r = cls._synthetize_single_variable(s, uow)
                if r:
                    success_synthesis.append(r)

            cls._export_metadata(success_synthesis, uow)
