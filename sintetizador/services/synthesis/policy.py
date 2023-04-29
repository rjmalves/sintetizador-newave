from typing import Callable, Dict, List, Type, TypeVar, Optional
import pandas as pd  # type: ignore
import logging

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.model.policy.variable import Variable
from sintetizador.model.policy.policysynthesis import PolicySynthesis


class PolicySynthetizer:
    DEFAULT_POLICY_SYNTHESIS_ARGS: List[str] = [
        "CORTES",
        "ESTADOS",
    ]

    T = TypeVar("T")

    logger: Optional[logging.Logger] = None

    @classmethod
    def _default_args(cls) -> List[PolicySynthesis]:
        args = [
            PolicySynthesis.factory(a)
            for a in cls.DEFAULT_POLICY_SYNTHESIS_ARGS
        ]
        return [arg for arg in args if arg is not None]

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[PolicySynthesis]:
        args_data = [PolicySynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        return valid_args

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
    def _validate_data(cls, data, type: Type[T], msg: str = "dados") -> T:
        if not isinstance(data, type):
            if cls.logger is not None:
                cls.logger.error(f"Erro na leitura de {msg}")
            raise RuntimeError()
        return data

    @classmethod
    def _resolve_cortes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            arq_cortes = uow.files.get_nwlistcf_cortes()
            if arq_cortes is None:
                return None
            df = arq_cortes.cortes
            return df

    @classmethod
    def _resolve_estados(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            arq_estados = uow.files.get_nwlistcf_estados()
            if arq_estados is None:
                return None
            df = arq_estados.estados
            return df

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
        try:
            if len(variables) == 0:
                synthesis_variables = PolicySynthetizer._default_args()
            else:
                synthesis_variables = (
                    PolicySynthetizer._process_variable_arguments(variables)
                )

            for s in synthesis_variables:
                filename = str(s)
                cls.logger.info(f"Realizando síntese de {filename}")
                df = cls._resolve(s, uow)
                if df is not None:
                    with uow:
                        uow.export.synthetize_df(df, filename)
        except Exception as e:
            cls.logger.error(str(e))
