from typing import Callable, Dict, List, Optional, Tuple, Type, TypeVar

import numpy as np
import polars as pl
from dateutil.relativedelta import relativedelta  # type: ignore
from inewave.newave import (
    Patamar,
    Sistema,
)

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    EER_CODE_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    HM3_M3S_MONTHLY_FACTOR,
    HYDRO_CODE_COL,
    IDENTIFICATION_COLUMNS,
    LOWER_BOUND_COL,
    SCENARIO_COL,
    STAGE_COL,
    STAGE_DURATION_HOURS,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    THERMAL_CODE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.unit import Unit
from app.model.operation.variable import Variable
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.operations import fast_group_df


class OperationVariableBounds:
    """
    Entidade responsável por calcular os limites das variáveis de operação
    existentes nos arquivos de saída do NEWAVE, que são processadas no
    processo de síntese da operação.
    """

    T = TypeVar("T")

    MAPPINGS: Dict[OperationSynthesis, Callable] = {
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            initial=True,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df, uow, synthesis_unit=Unit.MWmes.value, ordered_entities=entities
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            initial=True,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=None,
            initial=True,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=None,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=None,
            initial=True,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=None,
        ),
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._unbounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._unbounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._turbined_flow_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._turbined_flow_bounds(
            df,
            uow,
            synthesis_unit=Unit.m3s.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, entities: OperationVariableBounds._outflow_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, entities: OperationVariableBounds._outflow_bounds(
            df,
            uow,
            synthesis_unit=Unit.m3s.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._flow_diversion_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._flow_diversion_bounds(
            df,
            uow,
            synthesis_unit=Unit.m3s.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._qdes_vdes_uhe_bounds(
            df, uow, synthesis_unit=Unit.hm3.value
        ),
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._qdes_vdes_uhe_bounds(
            df, uow, synthesis_unit=Unit.m3s.value
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VOLUME_EVAPORADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VOLUME_EVAPORADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_EVAPORADO,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_EVAPORADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VAZAO_EVAPORADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VAZAO_EVAPORADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_EVAPORADA,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_EVAPORADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.INTERCAMBIO,
            SpatialResolution.PAR_SUBMERCADOS,
        ): lambda df, uow, entities: OperationVariableBounds._exchange_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.USINA_TERMELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._thermal_generation_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=THERMAL_CODE_COL,
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._thermal_generation_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._thermal_generation_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=None,
        ),
    }

    @classmethod
    def _get_sistema(cls, uow: AbstractUnitOfWork) -> Sistema:
        with uow:
            sistema = uow.files.get_sistema()
            if sistema is None:
                raise RuntimeError("Erro na leitura do arquivo sistema.dat")
            return sistema

    @classmethod
    def _get_patamar(cls, uow: AbstractUnitOfWork) -> Patamar:
        with uow:
            patamar = uow.files.get_patamar()
            if patamar is None:
                raise RuntimeError("Erro na leitura do arquivo patamar.dat")
            return patamar

    @classmethod
    def _validate_data(cls, data, type: Type[T]) -> T:
        if not isinstance(data, type):
            raise RuntimeError("Erro na validação dos dados.")
        return data

    @classmethod
    def _stored_energy_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list],
        entity_column: Optional[str] = None,
        initial: bool = False,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Energia Armazenada Absoluta (EARM) e Energia
        Armazenada Percentual (EARP).
        """

        def _replace_initial_bounds(
            df: pl.DataFrame, bound: float
        ) -> pl.DataFrame:
            df = df.with_columns(pl.col(START_DATE_COL).dt.offset_by("1mo"))
            stages = Deck.stages_starting_dates_final_simulation(uow)
            first_stage = stages[0]
            last_stage = stages[-1] + relativedelta(months=1)
            df = df.with_columns(
                pl.when(pl.col(START_DATE_COL) == last_stage)
                .then(bound)
                .otherwise(pl.col(VALUE_COL))
                .alias(VALUE_COL)
            )
            df = df.with_columns(
                pl.when(pl.col(START_DATE_COL) == last_stage)
                .then(first_stage)
                .otherwise(pl.col(START_DATE_COL))
                .alias(START_DATE_COL)
            )
            return df

        def _get_group_and_cast_bounds() -> Tuple[np.ndarray, np.ndarray]:
            upper_bound_df = Deck.stored_energy_upper_bounds(uow)
            lower_bound_df = Deck.eer_stored_energy_lower_bounds(uow)
            if initial:
                upper_bound_df = _replace_initial_bounds(
                    upper_bound_df, float("inf")
                )
                lower_bound_df = _replace_initial_bounds(lower_bound_df, 0.0)

            upper_bound_df = upper_bound_df.sort(grouping_columns)
            upper_bounds = (
                upper_bound_df.group_by(grouping_columns, maintain_order=True)
                .sum()[VALUE_COL]
                .to_numpy()
            )
            lower_bound_df = lower_bound_df.sort(grouping_columns)
            lower_bounds = (
                lower_bound_df.group_by(grouping_columns, maintain_order=True)
                .sum()[VALUE_COL]
                .to_numpy()
            )
            if synthesis_unit == Unit.perc_modif.value:
                lower_bounds = lower_bounds / upper_bounds * 100.0
                upper_bounds = 100.0 * np.ones_like(lower_bounds)
            return lower_bounds, upper_bounds

        def _repeat_bounds_by_scenario(
            df: pl.DataFrame,
            lower_bounds: np.ndarray,
            upper_bounds: np.ndarray,
        ) -> pl.DataFrame:
            num_entities = (
                len(ordered_entities[entity_column]) if entity_column else 1
            )
            num_stages = len(ordered_entities[STAGE_COL])
            num_scenarios = len(ordered_entities[SCENARIO_COL])
            num_blocks = len(ordered_entities[BLOCK_COL])
            df = df.with_columns(
                pl.Series(
                    name=LOWER_BOUND_COL,
                    values=cls._repeats_data_by_scenario(
                        lower_bounds,
                        num_entities,
                        num_stages,
                        num_scenarios,
                        num_blocks,
                    ),
                ),
                pl.Series(
                    name=UPPER_BOUND_COL,
                    values=cls._repeats_data_by_scenario(
                        upper_bounds,
                        num_entities,
                        num_stages,
                        num_scenarios,
                        num_blocks,
                    ),
                ),
            )
            return df

        def _sort_and_round_bounds(df: pl.DataFrame) -> pl.DataFrame:
            num_digits = 1 if synthesis_unit == Unit.perc_modif.value else 0
            df = df.with_columns(pl.col(VALUE_COL).round(num_digits))
            df = df.sort(grouping_columns)
            df = df.with_columns(
                pl.col(LOWER_BOUND_COL).round(num_digits),
                pl.col(UPPER_BOUND_COL).round(num_digits),
            )
            return df

        entity_column_list = [entity_column] if entity_column else []
        grouping_columns = entity_column_list + [START_DATE_COL]
        lower_bounds, upper_bounds = _get_group_and_cast_bounds()
        df = _repeat_bounds_by_scenario(df, lower_bounds, upper_bounds)
        df = _sort_and_round_bounds(df)
        return df

    @classmethod
    def _repeats_data_by_scenario(
        cls,
        data: np.ndarray,
        num_entities: int,
        num_stages: int,
        num_scenarios: int,
        num_blocks: int,
    ):
        """
        Expande os dados cadastrais para cada cenário, mantendo a ordem dos
        patamares internamente.
        """
        data_cenarios = np.zeros((len(data) * num_scenarios,), dtype=np.float64)
        for i in range(num_entities):
            for j in range(num_stages):
                i_i = i * num_stages * num_blocks + j * num_blocks
                i_f = i_i + num_blocks
                data_cenarios[i_i * num_scenarios : i_f * num_scenarios] = (
                    np.tile(data[i_i:i_f], num_scenarios)
                )
        return data_cenarios

    @classmethod
    def _repeats_data_by_scenario_and_block(
        cls,
        data: np.ndarray,
        num_entities: int,
        num_stages: int,
        num_scenarios: int,
        num_blocks: int,
    ):
        """
        Expande os dados cadastrais para cada cenário, mantendo a ordem dos
        patamares internamente.
        """
        data_cenarios = np.zeros(
            (len(data) * num_scenarios * num_blocks,), dtype=np.float64
        )
        for i in range(num_entities):
            for j in range(num_stages):
                i_i = i * num_stages + j
                i_f = i_i + 1
                data_cenarios[
                    i_i * num_scenarios * num_blocks : i_f
                    * num_scenarios
                    * num_blocks
                ] = np.tile(np.repeat(data[i_i:i_f], num_blocks), num_scenarios)
        return data_cenarios

    @classmethod
    def _group_hydro_df(
        cls, df: pl.DataFrame, grouping_column: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de UHE
        para uma síntese de REEs, SBMs ou para o SIN. A agregação
        tem como requisito que as variáveis fornecidas sejam em unidades
        cuja agregação seja possível apenas pela soma.
        """
        valid_grouping_columns = [
            HYDRO_CODE_COL,
            EER_CODE_COL,
            SUBMARKET_CODE_COL,
        ]

        grouping_column_map: Dict[str, List[str]] = {
            HYDRO_CODE_COL: [
                HYDRO_CODE_COL,
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            EER_CODE_COL: [
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            SUBMARKET_CODE_COL: [SUBMARKET_CODE_COL],
        }

        mapped_columns = (
            grouping_column_map[grouping_column] if grouping_column else []
        )
        grouping_columns = mapped_columns + [
            c
            for c in df.columns
            if c in IDENTIFICATION_COLUMNS and c not in valid_grouping_columns
        ]

        grouped_df = fast_group_df(
            df,
            grouping_columns,
            [VALUE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL],
            operation="sum",
        )

        return grouped_df

    @classmethod
    def _group_thermal_df(
        cls, df: pl.DataFrame, grouping_column: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de UHE
        para uma síntese de REEs, SBMs ou para o SIN. A agregação
        tem como requisito que as variáveis fornecidas sejam em unidades
        cuja agregação seja possível apenas pela soma.
        """
        valid_grouping_columns = [
            THERMAL_CODE_COL,
            SUBMARKET_CODE_COL,
        ]

        grouping_column_map: Dict[str, List[str]] = {
            THERMAL_CODE_COL: [
                THERMAL_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            SUBMARKET_CODE_COL: [SUBMARKET_CODE_COL],
        }

        mapped_columns = (
            grouping_column_map[grouping_column] if grouping_column else []
        )
        grouping_columns = mapped_columns + [
            c
            for c in df.columns
            if c in IDENTIFICATION_COLUMNS and c not in valid_grouping_columns
        ]

        grouped_df = fast_group_df(
            df,
            grouping_columns,
            [VALUE_COL],
            operation="sum",
        )

        return grouped_df

    @classmethod
    def _group_hydro_df_vol_flow_cast(
        cls, df: pl.DataFrame, grouping_column: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de UHE
        para uma síntese de REEs, SBMs ou para o SIN, convertendo
        a unidade de volume para vazão.

        É usada em casos em que osdados são fornecidos em unidade
        de volume, mas a síntese desejada é em unidade de vazão.
        """
        df_group = cls._group_hydro_df(df, grouping_column)
        for c in [VALUE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL]:
            df_group = df_group.with_columns(
                (
                    pl.col(c)
                    * (STAGE_DURATION_HOURS * HM3_M3S_MONTHLY_FACTOR)
                    / pl.col(BLOCK_DURATION_COL)
                ).alias(c)
            )

        return df_group

    @classmethod
    def _stored_volume_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list],
        entity_column: Optional[str] = None,
        initial: bool = False,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Armazenado Absoluto (VARM) e Volume
        Armazenado Percentual (VARP) para cada UHE.
        """

        def _replace_initial_bounds(
            df: pl.DataFrame, bounds: dict[str, float]
        ) -> pl.DataFrame:
            df = df.with_columns(pl.col(START_DATE_COL).dt.offset_by("1mo"))
            stages = Deck.stages_starting_dates_final_simulation(uow)
            first_stage = stages[0]
            last_stage = stages[-1] + relativedelta(months=1)
            for col, bound in bounds.items():
                df = df.with_columns(
                    pl.when(pl.col(START_DATE_COL) == last_stage)
                    .then(bound)
                    .otherwise(pl.col(col))
                    .alias(col)
                )
            df = df.with_columns(
                pl.when(pl.col(START_DATE_COL) == last_stage)
                .then(first_stage)
                .otherwise(pl.col(START_DATE_COL))
                .alias(START_DATE_COL)
            )
            return df

        def _get_group_and_cast_bounds() -> Tuple[np.ndarray, np.ndarray]:
            volume_bounds_in_stages_df = Deck.hydro_volume_bounds_in_stages(uow)
            synthesis_hydro_codes = df[HYDRO_CODE_COL].unique().to_list()
            volume_bounds_in_stages_df = volume_bounds_in_stages_df.filter(
                pl.col(HYDRO_CODE_COL).is_in(synthesis_hydro_codes)
            )
            if initial:
                volume_bounds_in_stages_df = _replace_initial_bounds(
                    volume_bounds_in_stages_df,
                    {LOWER_BOUND_COL: 0.0, UPPER_BOUND_COL: float("inf")},
                )

            volume_bounds_in_stages_df = volume_bounds_in_stages_df.sort(
                grouping_columns
            )
            grouped_bounds_df = (
                volume_bounds_in_stages_df.group_by(
                    grouping_columns, maintain_order=True
                )
                .sum()[[LOWER_BOUND_COL, UPPER_BOUND_COL]]
                .to_numpy()
            )
            lower_bounds = grouped_bounds_df[:, 0]
            upper_bounds = grouped_bounds_df[:, 1]

            if synthesis_unit == Unit.perc_modif.value:
                lower_bounds = 0.0 * np.ones_like(lower_bounds)
                upper_bounds = 100.0 * np.ones_like(lower_bounds)
            return lower_bounds, upper_bounds

        def _repeat_bounds_by_scenario(
            df: pl.DataFrame,
            lower_bounds: np.ndarray,
            upper_bounds: np.ndarray,
        ) -> pl.DataFrame:
            num_entities = (
                len(ordered_entities[entity_column]) if entity_column else 1
            )
            num_stages = len(ordered_entities[STAGE_COL])
            num_scenarios = len(ordered_entities[SCENARIO_COL])
            num_blocks = len(ordered_entities[BLOCK_COL])
            df = df.sort(grouping_columns)
            df = df.with_columns(
                pl.Series(
                    name=LOWER_BOUND_COL,
                    values=cls._repeats_data_by_scenario(
                        lower_bounds,
                        num_entities,
                        num_stages,
                        num_scenarios,
                        num_blocks,
                    ),
                ),
                pl.Series(
                    name=UPPER_BOUND_COL,
                    values=cls._repeats_data_by_scenario(
                        upper_bounds,
                        num_entities,
                        num_stages,
                        num_scenarios,
                        num_blocks,
                    ),
                ),
            )
            return df

        def _sort_and_round_bounds(
            df: pl.DataFrame,
            already_had_limits: bool,
        ) -> pl.DataFrame:
            num_digits = 2
            df = df.with_columns(pl.col(VALUE_COL).round(num_digits))
            df = df.sort(grouping_columns)
            df = df.with_columns(
                pl.col(LOWER_BOUND_COL).round(num_digits),
                pl.col(UPPER_BOUND_COL).round(num_digits),
            )
            if (
                synthesis_unit == Unit.hm3_modif.value
                and not already_had_limits
            ):
                df = df.with_columns(
                    (pl.col(VALUE_COL) + pl.col(LOWER_BOUND_COL)).alias(
                        VALUE_COL
                    )
                )
            return df

        has_limits = (
            LOWER_BOUND_COL in df.columns and UPPER_BOUND_COL in df.columns
        )
        entity_column_list = [entity_column] if entity_column else []
        grouping_columns = entity_column_list + [START_DATE_COL]
        lower_bounds, upper_bounds = _get_group_and_cast_bounds()
        if entity_column != HYDRO_CODE_COL:
            df = cls._group_hydro_df(df, entity_column)
        df = _repeat_bounds_by_scenario(
            df,
            lower_bounds,
            upper_bounds,
        )
        df = _sort_and_round_bounds(df, has_limits)
        return df

    @classmethod
    def _outflow_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list],
        entity_column: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Defluente (VDEF) e Vazão Defluente (QDEF)
        para cada UHE.
        """

        def _get_group_for_bounds() -> Tuple[np.ndarray, np.ndarray]:
            outflow_bounds = Deck.hydro_outflow_bounds_in_stages(uow)
            synthesis_hydro_codes = df[HYDRO_CODE_COL].unique().to_list()
            outflow_bounds = outflow_bounds.filter(
                pl.col(HYDRO_CODE_COL).is_in(synthesis_hydro_codes)
            )

            grouped_bounds_df = (
                outflow_bounds.group_by(grouping_columns, maintain_order=True)
                .sum()[[LOWER_BOUND_COL, UPPER_BOUND_COL]]
                .to_numpy()
            )
            lower_bounds = grouped_bounds_df[:, 0]
            upper_bounds = grouped_bounds_df[:, 1]

            return lower_bounds, upper_bounds

        def _repeat_bounds_by_scenario_and_cast(
            df: pl.DataFrame,
            lower_bounds: np.ndarray,
            upper_bounds: np.ndarray,
        ) -> pl.DataFrame:
            num_entities = (
                len(ordered_entities[entity_column]) if entity_column else 1
            )
            num_stages = len(ordered_entities[STAGE_COL])
            num_scenarios = len(ordered_entities[SCENARIO_COL])
            num_blocks = len(ordered_entities[BLOCK_COL])
            df = df.sort(grouping_columns)
            df = df.with_columns(
                pl.Series(
                    name=LOWER_BOUND_COL,
                    values=cls._repeats_data_by_scenario_and_block(
                        lower_bounds,
                        num_entities,
                        num_stages,
                        num_scenarios,
                        num_blocks,
                    ),
                ),
                pl.Series(
                    name=UPPER_BOUND_COL,
                    values=cls._repeats_data_by_scenario_and_block(
                        upper_bounds,
                        num_entities,
                        num_stages,
                        num_scenarios,
                        num_blocks,
                    ),
                ),
            )
            if synthesis_unit == Unit.hm3.value:
                df = df.with_columns(
                    (
                        pl.col(LOWER_BOUND_COL)
                        * pl.col(BLOCK_DURATION_COL)
                        / (HM3_M3S_MONTHLY_FACTOR * STAGE_DURATION_HOURS)
                    ).alias(LOWER_BOUND_COL),
                    (
                        pl.col(UPPER_BOUND_COL)
                        * pl.col(BLOCK_DURATION_COL)
                        / (HM3_M3S_MONTHLY_FACTOR * STAGE_DURATION_HOURS)
                    ).alias(UPPER_BOUND_COL),
                )
            return df

        def _sort_and_round_bounds(
            df: pl.DataFrame,
        ) -> pl.DataFrame:
            num_digits = 2
            df = df.sort(sorting_columns)
            df = df.with_columns(
                pl.col(VALUE_COL).round(num_digits),
                pl.col(LOWER_BOUND_COL).round(num_digits),
                pl.col(UPPER_BOUND_COL).round(num_digits),
            )
            return df

        entity_column_list = [entity_column] if entity_column else []
        grouping_columns = entity_column_list + [START_DATE_COL, BLOCK_COL]
        sorting_columns = entity_column_list + [
            START_DATE_COL,
            SCENARIO_COL,
            BLOCK_COL,
        ]
        lower_bounds, upper_bounds = _get_group_for_bounds()
        if entity_column != HYDRO_CODE_COL:
            df = cls._group_hydro_df(df, entity_column)
        df = _repeat_bounds_by_scenario_and_cast(
            df,
            lower_bounds,
            upper_bounds,
        )
        df = _sort_and_round_bounds(df)
        return df

    @classmethod
    def _turbined_flow_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list],
        entity_column: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Turbinado (VTUR) e Vazão Turbinada (QTUR)
        para cada UHE.

        TODO - considerar exph.dat para limites de turbinamento com
        entradas de máquinas no meio do horizonte.

        """

        def _get_group_for_bounds() -> Tuple[np.ndarray, np.ndarray]:
            turbined_flow_bounds = Deck.hydro_turbined_flow_bounds_in_stages(
                uow
            )
            synthesis_hydro_codes = df[HYDRO_CODE_COL].unique().to_list()
            turbined_flow_bounds = turbined_flow_bounds.filter(
                pl.col(HYDRO_CODE_COL).is_in(synthesis_hydro_codes)
            )

            grouped_bounds_df = (
                turbined_flow_bounds.group_by(
                    grouping_columns, maintain_order=True
                )
                .sum()[[LOWER_BOUND_COL, UPPER_BOUND_COL]]
                .to_numpy()
            )

            lower_bounds = grouped_bounds_df[:, 0]
            upper_bounds = grouped_bounds_df[:, 1]

            return lower_bounds, upper_bounds

        def _repeat_bounds_by_scenario_and_cast(
            df: pl.DataFrame,
            lower_bounds: np.ndarray,
            upper_bounds: np.ndarray,
        ) -> pl.DataFrame:
            num_entities = (
                len(ordered_entities[entity_column]) if entity_column else 1
            )
            num_stages = len(ordered_entities[STAGE_COL])
            num_scenarios = len(ordered_entities[SCENARIO_COL])
            num_blocks = len(ordered_entities[BLOCK_COL])
            df = df.sort(grouping_columns)
            df = df.with_columns(
                pl.Series(
                    name=LOWER_BOUND_COL,
                    values=cls._repeats_data_by_scenario(
                        lower_bounds,
                        num_entities,
                        num_stages,
                        num_scenarios,
                        num_blocks,
                    ),
                ),
                pl.Series(
                    name=UPPER_BOUND_COL,
                    values=cls._repeats_data_by_scenario(
                        upper_bounds,
                        num_entities,
                        num_stages,
                        num_scenarios,
                        num_blocks,
                    ),
                ),
            )
            if synthesis_unit == Unit.hm3.value:
                df = df.with_columns(
                    (
                        pl.col(LOWER_BOUND_COL)
                        * pl.col(BLOCK_DURATION_COL)
                        / (HM3_M3S_MONTHLY_FACTOR * STAGE_DURATION_HOURS)
                    ).alias(LOWER_BOUND_COL),
                    (
                        pl.col(UPPER_BOUND_COL)
                        * pl.col(BLOCK_DURATION_COL)
                        / (HM3_M3S_MONTHLY_FACTOR * STAGE_DURATION_HOURS)
                    ).alias(UPPER_BOUND_COL),
                )
            return df

        def _sort_and_round_bounds(
            df: pl.DataFrame,
        ) -> pl.DataFrame:
            num_digits = 2
            df = df.sort(sorting_columns)
            df = df.with_columns(
                pl.col(VALUE_COL).round(num_digits),
                pl.col(LOWER_BOUND_COL).round(num_digits),
                pl.col(UPPER_BOUND_COL).round(num_digits),
            )
            return df

        entity_column_list = [entity_column] if entity_column else []
        grouping_columns = entity_column_list + [START_DATE_COL, BLOCK_COL]
        sorting_columns = entity_column_list + [
            START_DATE_COL,
            SCENARIO_COL,
            BLOCK_COL,
        ]
        lower_bounds, upper_bounds = _get_group_for_bounds()
        if entity_column != HYDRO_CODE_COL:
            df = cls._group_hydro_df(df, entity_column)
        df = _repeat_bounds_by_scenario_and_cast(
            df,
            lower_bounds,
            upper_bounds,
        )
        df = _sort_and_round_bounds(df)
        return df

    @classmethod
    def _flow_diversion_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list],
        entity_column: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Retirado (VRET) e Vazão Retirada (QRET)
        para cada UHE.
        """

        def _get_group_for_bounds() -> Tuple[np.ndarray, np.ndarray]:
            flow_diversion_bounds = Deck.flow_diversion(uow)
            synthesis_hydro_codes = df[HYDRO_CODE_COL].unique().to_list()
            flow_diversion_bounds = flow_diversion_bounds.filter(
                pl.col(HYDRO_CODE_COL).is_in(synthesis_hydro_codes)
            )

            grouped_bounds_df = (
                flow_diversion_bounds.group_by(
                    grouping_columns, maintain_order=True
                )
                .sum()[[LOWER_BOUND_COL, UPPER_BOUND_COL]]
                .to_numpy()
            )
            lower_bounds = grouped_bounds_df[:, 0]
            upper_bounds = grouped_bounds_df[:, 1]

            return lower_bounds, upper_bounds

        def _repeat_bounds_by_scenario_and_cast(
            df: pl.DataFrame,
            lower_bounds: np.ndarray,
            upper_bounds: np.ndarray,
        ) -> pl.DataFrame:
            num_entities = (
                len(ordered_entities[entity_column]) if entity_column else 1
            )
            num_stages = len(ordered_entities[STAGE_COL])
            num_scenarios = len(ordered_entities[SCENARIO_COL])
            num_blocks = len(ordered_entities[BLOCK_COL])
            df = df.sort(grouping_columns)
            df = df.with_columns(
                pl.Series(
                    name=LOWER_BOUND_COL,
                    values=cls._repeats_data_by_scenario(
                        lower_bounds,
                        num_entities,
                        num_stages,
                        num_scenarios,
                        num_blocks,
                    ),
                ),
                pl.Series(
                    name=UPPER_BOUND_COL,
                    values=cls._repeats_data_by_scenario(
                        upper_bounds,
                        num_entities,
                        num_stages,
                        num_scenarios,
                        num_blocks,
                    ),
                ),
            )
            if synthesis_unit == Unit.hm3.value:
                df = df.with_columns(
                    (
                        pl.col(LOWER_BOUND_COL)
                        * pl.col(BLOCK_DURATION_COL)
                        / (HM3_M3S_MONTHLY_FACTOR * STAGE_DURATION_HOURS)
                    ).alias(LOWER_BOUND_COL),
                    (
                        pl.col(UPPER_BOUND_COL)
                        * pl.col(BLOCK_DURATION_COL)
                        / (HM3_M3S_MONTHLY_FACTOR * STAGE_DURATION_HOURS)
                    ).alias(UPPER_BOUND_COL),
                )
            return df

        def _sort_and_round_bounds(
            df: pl.DataFrame,
        ) -> pl.DataFrame:
            num_digits = 2
            df = df.sort(sorting_columns)
            df = df.with_columns(
                pl.col(VALUE_COL).round(num_digits),
                pl.col(LOWER_BOUND_COL).round(num_digits),
                pl.col(UPPER_BOUND_COL).round(num_digits),
            )
            return df

        entity_column_list = [entity_column] if entity_column else []
        grouping_columns = entity_column_list + [START_DATE_COL, BLOCK_COL]
        sorting_columns = entity_column_list + [
            START_DATE_COL,
            SCENARIO_COL,
            BLOCK_COL,
        ]
        lower_bounds, upper_bounds = _get_group_for_bounds()
        if entity_column != HYDRO_CODE_COL:
            df = cls._group_hydro_df(df, entity_column)
        df = _repeat_bounds_by_scenario_and_cast(
            df,
            lower_bounds,
            upper_bounds,
        )
        df = _sort_and_round_bounds(df)
        return df

    @classmethod
    def _lower_bounded_bounds(
        cls, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Afluente (VAFL) e Vazão Afluente (QAFL)
        para cada UHE.
        """
        df = df.with_columns(
            pl.col(VALUE_COL).round(2),
            pl.lit(0.0).alias(LOWER_BOUND_COL),
            pl.lit(float("inf")).alias(UPPER_BOUND_COL),
        )
        return df

    @classmethod
    def _unbounded_bounds(
        cls, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para variáveis ilimitadas.
        """
        df = df.with_columns(
            pl.col(VALUE_COL).round(2),
            pl.lit(float("-inf")).alias(LOWER_BOUND_COL),
            pl.lit(float("inf")).alias(UPPER_BOUND_COL),
        )
        return df

    @classmethod
    def _qdes_vdes_uhe_bounds(
        cls, df: pl.DataFrame, uow: AbstractUnitOfWork, synthesis_unit: str
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Desviado (VDES) e Vazão Desviada (QDES)
        para cada UHE.
        """
        # TODO - Procurar limite superior no modif.dat
        df = df.with_columns(
            pl.lit(0.0).alias(LOWER_BOUND_COL),
            pl.lit(float("inf")).alias(UPPER_BOUND_COL),
        )
        return df

    @classmethod
    def _exchange_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list],
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Intercâmbio (INT) por par de submercados.
        """

        def _apply_exchange_bounds_in_MWmes(
            df: pl.DataFrame, exchange_block_bounds_df: pl.DataFrame
        ) -> pl.DataFrame:
            """
            Cria as colunas de limites de intercâmbio em MWmes no DataFrame
            da síntese que foi recebido.
            Os limites inferior e superior são aplicados de acordo com o
            par de submercados e o sentido do intercâmbio, sendo um deles
            sempre <= 0.
            """
            start_dates = np.array(
                df[START_DATE_COL].unique().to_list(), dtype=np.datetime64
            )
            # Filtra os pares de submercados de limites dentre os
            # que existem no df
            PAIR_TMP_COL = "par_sbm"
            PAIR_TMP_COL_R = "par_sbm_r"
            df = df.with_columns(
                (
                    pl.col(EXCHANGE_SOURCE_CODE_COL).cast(str)
                    + "-"
                    + pl.col(EXCHANGE_TARGET_CODE_COL).cast(str)
                    + "-"
                    + pl.col(START_DATE_COL).cast(str)
                    + "-"
                    + pl.col(BLOCK_COL).cast(str)
                ).alias(PAIR_TMP_COL)
            )
            exchange_block_bounds_df = exchange_block_bounds_df.with_columns(
                (
                    pl.col(EXCHANGE_SOURCE_CODE_COL).cast(str)
                    + "-"
                    + pl.col(EXCHANGE_TARGET_CODE_COL).cast(str)
                    + "-"
                    + pl.col(START_DATE_COL).cast(str)
                    + "-"
                    + pl.col(BLOCK_COL).cast(str)
                ).alias(PAIR_TMP_COL),
                (
                    pl.col(EXCHANGE_TARGET_CODE_COL).cast(str)
                    + "-"
                    + pl.col(EXCHANGE_SOURCE_CODE_COL).cast(str)
                    + "-"
                    + pl.col(START_DATE_COL).cast(str)
                    + "-"
                    + pl.col(BLOCK_COL).cast(str)
                ).alias(PAIR_TMP_COL_R),
            )

            exchange_block_bounds_df = exchange_block_bounds_df.filter(
                pl.col(START_DATE_COL) >= start_dates[0]
            ).sort([
                EXCHANGE_SOURCE_CODE_COL,
                EXCHANGE_TARGET_CODE_COL,
                START_DATE_COL,
            ])

            df = df.join(
                exchange_block_bounds_df[[PAIR_TMP_COL, VALUE_COL]].rename({
                    VALUE_COL: UPPER_BOUND_COL
                }),
                on=PAIR_TMP_COL,
            )
            df = df.join(
                exchange_block_bounds_df[[PAIR_TMP_COL_R, VALUE_COL]].rename({
                    PAIR_TMP_COL_R: PAIR_TMP_COL,
                    VALUE_COL: LOWER_BOUND_COL,
                }),
                on=PAIR_TMP_COL,
            )
            df = df.with_columns(
                (pl.col(LOWER_BOUND_COL) * -1).alias(LOWER_BOUND_COL)
            )
            num_digits = 1
            df = df.with_columns(
                pl.col(LOWER_BOUND_COL)
                .fill_nan(-float("inf"))
                .round(num_digits),
                pl.col(UPPER_BOUND_COL)
                .fill_nan(float("inf"))
                .round(num_digits),
            )

            return df.drop([PAIR_TMP_COL])

        # Lê e converte os limites de intercâmbio para MWmes,
        # pois os arquivos de entrada são em MWmed com P.U e
        # o nwlistop fornece a saída em MWmes.
        exchange_block_bounds_df = Deck.exchange_bounds(uow)
        return _apply_exchange_bounds_in_MWmes(df, exchange_block_bounds_df)

    @classmethod
    def _thermal_generation_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list],
        entity_column: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Realiza o cálculo dos limites de geração térmica para cada
        UTE, SBM e SIN, considerando os limites por usina fornecidos
        no pmo.dat.

        Regra de negócio: os limites de geração térmica são fornecidos
        sempre no pmo.dat apenas por usina térmica, e não por agrupamento.
        Desta forma, o DataFrame de síntese fornecido será já agregado
        de acordo com a síntese desejada, mas os limites deverão ser
        agregados de acordo.
        """

        def _get_group_and_cast_bounds(
            entity_column: Optional[str], entity_list: list
        ) -> Tuple[np.ndarray, np.ndarray]:
            bounds_df = Deck.thermal_generation_bounds(uow)
            dates = Deck.stages_starting_dates_final_simulation(uow)
            bounds_df = bounds_df.filter(pl.col(START_DATE_COL) >= dates[0])
            if entity_column:
                bounds_entities = bounds_df[entity_column].unique().to_list()
                diff = list(set(entity_list).difference(bounds_entities))
                for d in diff:
                    bounds_df_sample = bounds_df.filter(
                        pl.col(entity_column) == bounds_entities[0]
                    )
                    bounds_df_sample = bounds_df_sample.with_columns(
                        pl.lit(d).alias(entity_column),
                        pl.lit(0.0).alias(LOWER_BOUND_COL),
                        pl.lit(0.0).alias(UPPER_BOUND_COL),
                    )
                    bounds_df = pl.concat([bounds_df, bounds_df_sample])
            grouped_bounds_df = (
                bounds_df.group_by(grouping_columns, maintain_order=True)
                .sum()[[LOWER_BOUND_COL, UPPER_BOUND_COL]]
                .to_numpy()
            )
            lower_bounds = grouped_bounds_df[:, 0]
            upper_bounds = grouped_bounds_df[:, 1]

            return lower_bounds, upper_bounds

        def _repeat_bounds_by_scenario_and_block(
            df: pl.DataFrame,
            lower_bounds: np.ndarray,
            upper_bounds: np.ndarray,
        ) -> pl.DataFrame:
            num_entities = (
                len(ordered_entities[entity_column]) if entity_column else 1
            )
            num_stages = len(ordered_entities[STAGE_COL])
            num_scenarios = len(ordered_entities[SCENARIO_COL])
            num_blocks = len(ordered_entities[BLOCK_COL])
            df = df.sort(grouping_columns)
            df = df.with_columns(
                pl.Series(
                    name=LOWER_BOUND_COL,
                    values=cls._repeats_data_by_scenario_and_block(
                        lower_bounds,
                        num_entities,
                        num_stages,
                        num_scenarios,
                        num_blocks,
                    ),
                ),
                pl.Series(
                    name=UPPER_BOUND_COL,
                    values=cls._repeats_data_by_scenario_and_block(
                        upper_bounds,
                        num_entities,
                        num_stages,
                        num_scenarios,
                        num_blocks,
                    ),
                ),
            )
            return df

        def _cast_bounds(df: pl.DataFrame) -> pl.DataFrame:
            df = df.with_columns(
                (
                    pl.col(LOWER_BOUND_COL)
                    * pl.col(BLOCK_DURATION_COL)
                    / STAGE_DURATION_HOURS
                ).alias(LOWER_BOUND_COL),
                (
                    pl.col(UPPER_BOUND_COL)
                    * pl.col(BLOCK_DURATION_COL)
                    / STAGE_DURATION_HOURS
                ).alias(UPPER_BOUND_COL),
            )
            return df

        def _sort_and_round_bounds(
            df: pl.DataFrame,
        ) -> pl.DataFrame:
            num_digits = 2
            df = df.with_columns(
                pl.col(VALUE_COL).round(num_digits),
                pl.col(LOWER_BOUND_COL).round(num_digits),
                pl.col(UPPER_BOUND_COL).round(num_digits),
            )
            return df

        entity_column_list = [entity_column] if entity_column else []
        entity_list = df[entity_column].unique() if entity_column else []
        grouping_columns = entity_column_list + [START_DATE_COL]
        lower_bounds, upper_bounds = _get_group_and_cast_bounds(
            entity_column, entity_list
        )
        if entity_column != THERMAL_CODE_COL:
            df = cls._group_thermal_df(df, entity_column)

        df = _repeat_bounds_by_scenario_and_block(
            df,
            lower_bounds,
            upper_bounds,
        )
        df = _cast_bounds(df)
        df = _sort_and_round_bounds(df)
        return df

    @classmethod
    def is_bounded(cls, s: OperationSynthesis) -> bool:
        """
        Verifica se uma determinada síntese possui limites implementados
        para adição ao DataFrame.
        """
        return s in cls.MAPPINGS

    @classmethod
    def _unbounded(cls, df: pl.DataFrame) -> pl.DataFrame:
        """
        Adiciona os valores padrão para variáveis não limitadas.
        """
        df = df.with_columns(
            pl.lit(-float("inf")).alias(LOWER_BOUND_COL),
            pl.lit(float("inf")).alias(UPPER_BOUND_COL),
        )
        return df

    @classmethod
    def resolve_bounds(
        cls,
        s: OperationSynthesis,
        df: pl.DataFrame,
        ordered_synthesis_entities: Dict[str, list],
        uow: AbstractUnitOfWork,
    ) -> pl.DataFrame:
        """
        Adiciona colunas de limite inferior e superior a um DataFrame,
        calculando os valores necessários caso a variável seja limitada
        ou atribuindo -inf e +inf caso contrário.

        """
        if cls.is_bounded(s):
            return cls.MAPPINGS[s](df, uow, ordered_synthesis_entities)
        else:
            return cls._unbounded(df)
