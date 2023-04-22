from typing import Callable, Dict, List
import pandas as pd  # type: ignore
import logging

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.policy.variable import Variable
from sintetizador.model.policy.policysynthesis import PolicySynthesis


class PolicySynthetizer:
    DEFAULT_POLICY_SYNTHESIS_ARGS: List[str] = [
        "CORTES",
        "ESTADOS",
    ]

    @classmethod
    def _default_args(cls) -> List[PolicySynthesis]:
        return [
            PolicySynthesis.factory(a)
            for a in cls.DEFAULT_POLICY_SYNTHESIS_ARGS
        ]

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[PolicySynthesis]:
        args_data = [PolicySynthesis.factory(c) for c in args]
        for i, a in enumerate(args_data):
            if a is None:
                Log.log(f"Erro no argumento fornecido: {args[i]}")
                return []
        return args_data

    @classmethod
    def _resolve(
        cls, synthesis: PolicySynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        RULES: Dict[Variable, Callable] = {
            Variable.CORTES: cls._resolve_cortes,
            Variable.ESTADOS: cls._resolve_estados,
        }
        return RULES[synthesis.variable](uow)

    @classmethod
    def _resolve_cortes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            arq_cortes = uow.files.get_nwlistcf_cortes()
            if arq_cortes is None:
                return None
            df = arq_cortes.cortes
            if isinstance(df, pd.DataFrame):
                return df
            else:
                return None

    @classmethod
    def _resolve_estados(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            arq_estados = uow.files.get_nwlistcf_estados()
            if arq_estados is None:
                return None
            df = arq_estados.estados
            if isinstance(df, pd.DataFrame):
                return df
            else:
                return None

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger()
        try:
            if len(variables) == 0:
                variables = PolicySynthetizer._default_args()
            else:
                variables = PolicySynthetizer._process_variable_arguments(
                    variables
                )

            for s in variables:
                filename = str(s)
                cls.logger.info(f"Realizando síntese de {filename}")
                df = cls._resolve(s, uow)
                if df is not None:
                    with uow:
                        uow.export.synthetize_df(df, filename)
        except Exception as e:
            cls.logger.error(str(e))
