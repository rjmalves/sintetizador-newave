import logging
from logging import ERROR, INFO
from traceback import print_exc
from typing import Callable, Dict, List, Optional, TypeVar

import pandas as pd  # type: ignore

from app.internal.constants import (
    POLICY_SYNTHESIS_METADATA_OUTPUT,
    POLICY_SYNTHESIS_SUBDIR,
)
from app.model.policy.policysynthesis import (
    SUPPORTED_SYNTHESIS,
    PolicySynthesis,
)
from app.model.policy.variable import Variable
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.regex import match_variables_with_wildcards
from app.utils.timing import time_and_log


class PolicySynthetizer:
    DEFAULT_POLICY_SYNTHESIS_ARGS: List[str] = SUPPORTED_SYNTHESIS

    T = TypeVar("T")

    logger: Optional[logging.Logger] = None

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _default_args(cls) -> List[PolicySynthesis]:
        args = [
            PolicySynthesis.factory(a)
            for a in cls.DEFAULT_POLICY_SYNTHESIS_ARGS
        ]
        return [arg for arg in args if arg is not None]

    @classmethod
    def _match_wildcards(cls, variables: List[str]) -> List[str]:
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_POLICY_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[PolicySynthesis]:
        args_data = [PolicySynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        return valid_args

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: List[str], uow: AbstractUnitOfWork
    ) -> List[PolicySynthesis]:
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
    def _export_metadata(
        cls,
        success_synthesis: List[PolicySynthesis],
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
            uow.export.synthetize_df(
                metadata_df, POLICY_SYNTHESIS_METADATA_OUTPUT
            )

    @classmethod
    def _synthetize_single_variable(
        cls, s: PolicySynthesis, uow: AbstractUnitOfWork
    ) -> Optional[PolicySynthesis]:
        """
        Realiza a síntese de política para uma variável
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
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
        uow.subdir = POLICY_SYNTHESIS_SUBDIR
        with time_and_log(
            message_root="Tempo para sintese da politica",
            logger=cls.logger,
        ):
            synthesis_variables = cls._preprocess_synthesis_variables(
                variables, uow
            )
            success_synthesis: List[PolicySynthesis] = []
            for s in synthesis_variables:
                r = cls._synthetize_single_variable(s, uow)
                if r:
                    success_synthesis.append(r)

            cls._export_metadata(success_synthesis, uow)
