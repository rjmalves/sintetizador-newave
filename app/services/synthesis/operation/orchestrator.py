import logging
import sys as _sys
from concurrent.futures import ProcessPoolExecutor
from logging import ERROR, INFO, WARNING
from traceback import print_exc
from typing import Dict, List, Optional

import pandas as pd
import polars as pl

from app.internal.constants import OPERATION_SYNTHESIS_SUBDIR
from app.model.operation.operationsynthesis import (
    SUPPORTED_SYNTHESIS,
    SYNTHESIS_DEPENDENCIES,
    OperationSynthesis,
)
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.model.settings import Settings
from app.services.deck.bounds import OperationVariableBounds
from app.services.deck.context import DeckContext
from app.services.deck.deck import Deck

# Intra-package imports (leaf modules only; spatial/stubs are deferred to
# avoid circular imports during class definition of orchestrator.py itself)
from app.services.synthesis.operation import bounds as _bounds_mod
from app.services.synthesis.operation import cache as _cache_mod
from app.services.synthesis.operation import export as _export_mod
from app.services.synthesis.operation import pipeline as _pipeline_mod
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.regex import match_variables_with_wildcards
from app.utils.timing import time_and_log


def _pkg():
    return _sys.modules[__package__]


_WIND_VARS = frozenset(
    [
        Variable.VELOCIDADE_VENTO,
        Variable.GERACAO_EOLICA,
        Variable.CORTE_GERACAO_EOLICA,
    ]
)
_HYDRO_ONLY_VARS = frozenset(
    [
        Variable.VIOLACAO_FPHA,
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
    ]
)


class OperationSynthetizer:
    logger: Optional[logging.Logger] = None
    DEFAULT_OPERATION_SYNTHESIS_ARGS: List[str] = SUPPORTED_SYNTHESIS
    SYNTHESIS_TO_CACHE: List[OperationSynthesis] = list(
        set([p for pr in SYNTHESIS_DEPENDENCIES.values() for p in pr])
    )
    CACHED_SYNTHESIS: Dict[OperationSynthesis, pl.DataFrame] = {}
    ORDERED_SYNTHESIS_ENTITIES: Dict[OperationSynthesis, Dict[str, list]] = {}
    SYNTHESIS_STATS: Dict[SpatialResolution, List[pl.DataFrame]] = {}

    @classmethod
    def clear_cache(cls) -> None:
        """Limpa o cache de síntese de operação."""
        cls.CACHED_SYNTHESIS.clear()
        cls.ORDERED_SYNTHESIS_ENTITIES.clear()
        cls.SYNTHESIS_STATS.clear()

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _default_args(cls) -> List[OperationSynthesis]:
        """Return all supported synthesis objects."""
        args = [
            OperationSynthesis.factory(a)
            for a in cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        ]
        return [arg for arg in args if arg is not None]

    @classmethod
    def _match_wildcards(cls, variables: List[str]) -> List[str]:
        """Match variables against supported synthesis wildcards."""
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[OperationSynthesis]:
        """Convert variable strings to synthesis objects."""
        args_data = [OperationSynthesis.factory(c) for c in args]
        return [arg for arg in args_data if arg is not None]

    @classmethod
    def _filter_valid_variables(
        cls, variables: List[OperationSynthesis], uow: AbstractUnitOfWork
    ) -> List[OperationSynthesis]:
        """Filter variables by wind/hydro availability."""
        has_wind = Deck.models_wind_generation(uow)
        has_hydro = Deck.final_simulation_aggregation(
            uow
        ) or Deck.hybrid_policy(uow)
        uhe = SpatialResolution.USINA_HIDROELETRICA
        valid_variables: List[OperationSynthesis] = []
        for v in variables:
            if v.variable in _WIND_VARS and not has_wind:
                continue
            if v.spatial_resolution == uhe and not has_hydro:
                continue
            if v.variable in _HYDRO_ONLY_VARS and not has_hydro:
                continue
            if (
                v.variable == Variable.VALOR_AGUA
                and v.spatial_resolution == uhe
                and not has_hydro
            ):
                continue
            valid_variables.append(v)
        cls._log(f"Sinteses: {valid_variables}")
        return valid_variables

    @classmethod
    def _add_synthesis_dependencies(
        cls, synthesis: List[OperationSynthesis]
    ) -> List[OperationSynthesis]:
        """Add dependency objects to each synthesis variable."""

        def _add_dependencies_recursive(
            result: List[OperationSynthesis],
            s: OperationSynthesis,
        ) -> None:
            for dep in SYNTHESIS_DEPENDENCIES.get(s, []):
                _add_dependencies_recursive(result, dep)
            if s not in result:
                result.append(s)

        result: List[OperationSynthesis] = []
        for v in synthesis:
            _add_dependencies_recursive(result, v)
        return result

    @classmethod
    def _get_ordered_entities(cls, s) -> Dict[str, list]:
        """Get ordered entities for a synthesis."""
        return _pipeline_mod.get_ordered_entities(cls, s)

    @staticmethod
    def _resolve_temporal_resolution(df, uow, deck_context=None):
        """Add temporal information to synthesis DataFrame."""
        return _pipeline_mod.resolve_temporal_resolution(df, uow, deck_context)

    @staticmethod
    def _resolve_starting_stage_polars(df, deck_context, uow):
        """Polars variant of resolve_starting_stage."""
        return _pipeline_mod.resolve_starting_stage_polars(
            df, deck_context, uow
        )

    @classmethod
    def _post_resolve_entity(
        cls,
        df,
        s,
        entity_column_values,
        uow,
        internal_stubs=None,
        deck_context=None,
    ):
        """Post-process entity extraction."""
        return _pipeline_mod.post_resolve_entity(
            cls,
            df,
            s,
            entity_column_values,
            uow,
            internal_stubs or {},
            deck_context,
        )

    @classmethod
    def _post_resolve(
        cls, resolve_responses, s, uow, early_hooks=None, late_hooks=None
    ):
        """Post-process all synthesis data."""
        return _pipeline_mod.post_resolve(
            cls, resolve_responses, s, uow, early_hooks or [], late_hooks or []
        )

    @classmethod
    def _get_from_cache(cls, s: OperationSynthesis) -> pl.DataFrame:
        """Extract synthesis result from cache."""
        return _cache_mod.get_from_cache(cls, s)

    @classmethod
    def _resolve_bounds(cls, s, df, uow) -> pl.DataFrame:
        """Compute upper and lower bounds."""
        return _bounds_mod.resolve_bounds(cls, s, df, uow)

    @classmethod
    def _export_metadata(cls, success_synthesis, uow):
        """Export synthesis variable metadata."""
        _export_mod.export_metadata(cls, success_synthesis, uow)

    @classmethod
    def _add_synthesis_stats(cls, s, df):
        """Add synthesis statistics."""
        _export_mod.add_synthesis_stats(cls, s, df)

    @classmethod
    def _export_scenario_synthesis(cls, s, df, uow):
        """Export operation synthesis data."""
        _export_mod.export_scenario_synthesis(cls, s, df, uow)

    @classmethod
    def _export_stats(cls, uow):
        """Export synthesis statistics."""
        _export_mod.export_stats(cls, uow)

    @classmethod
    def _stub_mappings(cls, s):
        from app.services.synthesis.operation import stubs as _stubs_mod

        return _stubs_mod.stub_mappings(cls, s)

    @classmethod
    def _resolve_stub(cls, s, uow):
        from app.services.synthesis.operation import stubs as _stubs_mod

        return _stubs_mod.resolve_stub(cls, s, uow)

    @classmethod
    def _resolve_SBM_entity(
        cls, uow, synthesis, sbm_index, sbm_name, deck_context=None
    ):
        from app.services.synthesis.operation import resolution_sbm as _mod

        return _mod.resolve_SBM_entity(
            cls, uow, synthesis, sbm_index, sbm_name, deck_context
        )

    @classmethod
    def _resolve_SBP_entity(
        cls,
        uow,
        synthesis,
        sbm1_index,
        sbm1_name,
        sbm2_index,
        sbm2_name,
        deck_context=None,
    ):
        from app.services.synthesis.operation import resolution_sbp as _mod

        return _mod.resolve_SBP_entity(
            cls,
            uow,
            synthesis,
            sbm1_index,
            sbm1_name,
            sbm2_index,
            sbm2_name,
            deck_context,
        )

    @classmethod
    def _resolve_REE_entity(
        cls, uow, synthesis, ree_index, ree_name, deck_context=None
    ):
        from app.services.synthesis.operation import resolution_ree as _mod

        return _mod.resolve_REE_entity(
            cls, uow, synthesis, ree_index, ree_name, deck_context
        )

    @classmethod
    def _resolve_UHE_entity(
        cls, uow, synthesis, uhe_index, uhe_name, deck_context=None
    ):
        from app.services.synthesis.operation import resolution_uhe as _mod

        return _mod.resolve_UHE_entity(
            cls, uow, synthesis, uhe_index, uhe_name, deck_context
        )

    @classmethod
    def _resolve_GTER_UTE_entity(
        cls, uow, synthesis, sbm_index, sbm_name, deck_context=None
    ):
        from app.services.synthesis.operation import resolution_ute as _mod

        return _mod.resolve_GTER_UTE_entity(
            cls, uow, synthesis, sbm_index, sbm_name, deck_context
        )

    @classmethod
    def _resolve_SBM_entity_MER_MERL(cls, uow, synthesis, sbm_index, sbm_name):
        from app.services.synthesis.operation import stubs as _stubs_mod

        return _stubs_mod.resolve_SBM_entity_MER_MERL(
            cls, uow, synthesis, sbm_index, sbm_name
        )

    @classmethod
    def _resolve_spatial_resolution(
        cls, synthesis, uow, deck_context=None, executor=None
    ):
        from app.services.synthesis.operation import spatial as _spatial_mod

        return _spatial_mod.resolve_spatial_resolution(
            cls, synthesis, uow, deck_context, executor
        )

    @classmethod
    def _resolve_synthesis(cls, s, uow, deck_context=None, executor=None):
        from app.services.synthesis.operation import spatial as _spatial_mod

        return _spatial_mod.resolve_synthesis(
            cls, s, uow, deck_context, executor
        )

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: List[str], uow: AbstractUnitOfWork
    ) -> List[OperationSynthesis]:
        try:
            if len(variables) == 0:
                synthesis_variables = cls._default_args()
            else:
                all_variables = cls._match_wildcards(variables)
                synthesis_variables = cls._process_variable_arguments(
                    all_variables
                )
            valid_synthesis = cls._filter_valid_variables(
                synthesis_variables, uow
            )
            synthesis_with_dependencies = cls._add_synthesis_dependencies(
                valid_synthesis
            )
        except Exception as e:
            print_exc()
            cls._log(str(e), ERROR)
            cls._log("Erro no pré-processamento das variáveis", ERROR)
            synthesis_with_dependencies = []
        return synthesis_with_dependencies

    @classmethod
    def _synthetize_single_variable(
        cls,
        s: OperationSynthesis,
        uow: AbstractUnitOfWork,
        deck_context: Optional[DeckContext] = None,
        executor: Optional[ProcessPoolExecutor] = None,
    ) -> Optional[OperationSynthesis]:
        """Synthesize a single operation variable."""
        filename = str(s)
        with time_and_log(
            message_root=f"Tempo para sintese de {filename}",
            logger=cls.logger,
        ):
            try:
                found_synthesis = False
                cls._log(f"Realizando sintese de {filename}")
                df = _cache_mod.get_from_cache_if_exists(cls, s)
                is_stub = cls._stub_mappings(s) is not None
                if df.is_empty():
                    df, is_stub = cls._resolve_stub(s, uow)
                    if not is_stub:
                        df = cls._resolve_synthesis(
                            s, uow, deck_context, executor
                        )
                if df is not None:
                    if not df.is_empty():
                        found_synthesis = True
                        cls._export_scenario_synthesis(s, df, uow)
                        return s
                if not found_synthesis:
                    cls._log(
                        "Nao foram encontrados dados"
                        + f" para a sintese de {filename}",
                        WARNING,
                    )
                return None
            except Exception as e:
                print_exc()
                cls._log(str(e), ERROR)
                cls._log(
                    f"Nao foi possível realizar a sintese de: {filename}",
                    ERROR,
                )
                return None

    @classmethod
    def enforce_version(cls, uow: AbstractUnitOfWork) -> None:
        version = Deck.pmo(uow).versao_modelo
        if version is not None:
            uow.version = version

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork) -> None:
        """
        Realiza a síntese de operação para as variáveis operativas
        fornecidas, na agregação desejada. As variáveis são pré-processadas
        para filtrar as válidas para o caso em questão,
        e então são resolvidas de acordo com a síntese.
        """
        cls.logger = logging.getLogger("main")
        Deck.logger = cls.logger
        OperationVariableBounds.logger = cls.logger
        uow.subdir = OPERATION_SYNTHESIS_SUBDIR
        with time_and_log(
            message_root="Tempo para sintese da operacao",
            logger=cls.logger,
        ):
            cls.enforce_version(uow)
            deck_context = DeckContext.from_deck(uow)
            synthesis_with_dependencies = cls._preprocess_synthesis_variables(
                variables, uow
            )
            n_procs = int(Settings().processors)
            success_synthesis: List[OperationSynthesis] = []
            current_resolution: Optional[SpatialResolution] = None
            current_executor: Optional[ProcessPoolExecutor] = None
            for s in synthesis_with_dependencies:
                if s.spatial_resolution != current_resolution:
                    # Resolution boundary: shut down the old executor (if any)
                    # and open a new one for the incoming resolution group.
                    if current_executor is not None:
                        current_executor.shutdown(wait=True)
                    current_resolution = s.spatial_resolution
                    # SIN variables do not use a pool; skip executor creation.
                    if (
                        current_resolution
                        != SpatialResolution.SISTEMA_INTERLIGADO
                    ):
                        current_executor = _pkg().ProcessPoolExecutor(
                            max_workers=n_procs
                        )
                    else:
                        current_executor = None
                r = cls._synthetize_single_variable(
                    s, uow, deck_context, current_executor
                )
                if r:
                    success_synthesis.append(r)
            # Shut down the final group's executor after the loop completes.
            if current_executor is not None:
                current_executor.shutdown(wait=True)

            cls._export_stats(uow)
            cls._export_metadata(success_synthesis, uow)
