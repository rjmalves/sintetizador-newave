import logging
from logging import ERROR, INFO
from traceback import print_exc
from typing import Callable, Dict, List, Optional, TypeVar

import pandas as pd  # type: ignore
from dateutil.relativedelta import relativedelta  # type: ignore

from app.internal.constants import (
    END_DATE_COL,
    STAGE_COL,
    STAGE_DURATION_HOURS,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    SYSTEM_SYNTHESIS_METADATA_OUTPUT,
    SYSTEM_SYNTHESIS_SUBDIR,
    VALUE_COL,
)
from app.model.system.systemsynthesis import (
    SUPPORTED_SYNTHESIS,
    SystemSynthesis,
)
from app.model.system.variable import Variable
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.regex import match_variables_with_wildcards
from app.utils.timing import time_and_log

# TODO - rever nomes das colunas


class SystemSynthetizer:
    DEFAULT_SYSTEM_SYNTHESIS_ARGS = SUPPORTED_SYNTHESIS

    T = TypeVar("T")

    logger: Optional[logging.Logger] = None

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _default_args(cls) -> List[SystemSynthesis]:
        args = [
            SystemSynthesis.factory(a)
            for a in cls.DEFAULT_SYSTEM_SYNTHESIS_ARGS
        ]
        return [arg for arg in args if arg is not None]

    @classmethod
    def _match_wildcards(cls, variables: List[str]) -> List[str]:
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_SYSTEM_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[SystemSynthesis]:
        args_data = [SystemSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        return valid_args

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: List[str], uow: AbstractUnitOfWork
    ) -> List[SystemSynthesis]:
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
    def filter_valid_variables(
        cls, variables: List[SystemSynthesis], uow: AbstractUnitOfWork
    ) -> List[SystemSynthesis]:
        valid_variables = variables
        cls._log(f"Variáveis: {valid_variables}")
        return valid_variables

    @classmethod
    def _resolve(
        cls, synthesis: SystemSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        RULES: Dict[Variable, Callable] = {
            Variable.EST: cls.__resolve_EST,
            Variable.PAT: cls.__resolve_PAT,
            Variable.SBM: cls.__resolve_SBM,
            Variable.REE: cls.__resolve_REE,
            Variable.UTE: cls.__resolve_UTE,
            Variable.UHE: cls.__resolve_UHE,
        }
        return RULES[synthesis.variable](uow)

    @classmethod
    def __resolve_EST(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        start_dates = Deck.stages_starting_dates_final_simulation(uow)
        end_dates = [d + relativedelta(months=1) for d in start_dates]
        return pd.DataFrame(
            data={
                STAGE_COL: list(range(1, len(start_dates) + 1)),
                START_DATE_COL: start_dates,
                END_DATE_COL: end_dates,
            }
        )

    @classmethod
    def __resolve_PAT(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.block_lengths(uow)
        df[VALUE_COL] *= STAGE_DURATION_HOURS
        return df

    @classmethod
    def __resolve_SBM(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.submarkets(uow).reset_index()
        return df[[SUBMARKET_CODE_COL, SUBMARKET_NAME_COL]]

    @classmethod
    def __resolve_REE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.eer_submarket_map(uow)
        return df.reset_index()

    @classmethod
    def __resolve_UTE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.thermal_submarket_map(uow)
        return df.reset_index()

    @classmethod
    def __resolve_UHE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.hydro_eer_submarket_map(uow)
        return df.reset_index()

    @classmethod
    def _export_metadata(
        cls,
        success_synthesis: List[SystemSynthesis],
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
            existing_df = uow.export.read_df(SYSTEM_SYNTHESIS_METADATA_OUTPUT)
            if existing_df is not None:
                metadata_df = pd.concat(
                    [existing_df, metadata_df], ignore_index=True
                )
                metadata_df = metadata_df.drop_duplicates()
            uow.export.synthetize_df(
                metadata_df, SYSTEM_SYNTHESIS_METADATA_OUTPUT
            )

    @classmethod
    def _synthetize_single_variable(
        cls, s: SystemSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[SystemSynthesis]:
        """
        Realiza a síntese de sistema para uma variável
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
        uow.subdir = SYSTEM_SYNTHESIS_SUBDIR
        with time_and_log(
            message_root="Tempo para sintese da sistema",
            logger=cls.logger,
        ):
            synthesis_variables = cls._preprocess_synthesis_variables(
                variables, uow
            )
            success_synthesis: List[SystemSynthesis] = []
            for s in synthesis_variables:
                r = cls._synthetize_single_variable(s, uow)
                if r:
                    success_synthesis.append(r)

            cls._export_metadata(success_synthesis, uow)
