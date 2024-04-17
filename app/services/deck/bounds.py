import pandas as pd  # type: ignore
import numpy as np
from typing import Dict, List, Tuple, Callable, Type, TypeVar, Optional
from app.model.operation.variable import Variable
from app.model.operation.unit import Unit
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.operationsynthesis import OperationSynthesis
from app.services.unitofwork import AbstractUnitOfWork
from app.services.deck.deck import Deck
from app.internal.constants import (
    START_DATE_COL,
    END_DATE_COL,
    STAGE_COL,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    EER_CODE_COL,
    EER_NAME_COL,
    THERMAL_CODE_COL,
    THERMAL_NAME_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_SOURCE_NAME_COL,
    EXCHANGE_TARGET_CODE_COL,
    EXCHANGE_TARGET_NAME_COL,
    BLOCK_COL,
    BLOCK_DURATION_COL,
    SCENARIO_COL,
    STATS_OR_SCENARIO_COL,
    UPPER_BOUND_COL,
    LOWER_BOUND_COL,
    VALUE_COL,
    SYSTEM_GROUPING_COL,
)
from app.utils.operations import fast_group_df
from inewave.newave import (
    Modif,
    Sistema,
    Patamar,
    Conft,
)
from inewave.newave.modelos.modif import (
    NUMCNJ,
    NUMMAQ,
)


class OperationVariableBounds:
    """
    Entidade responsável por calcular os limites das variáveis de operação
    existentes nos arquivos de saída do NEWAVE, que são processadas no
    processo de síntese da operação.
    """

    IDENTIFICATION_COLUMNS = [
        START_DATE_COL,
        END_DATE_COL,
        STAGE_COL,
        SUBMARKET_CODE_COL,
        SUBMARKET_NAME_COL,
        EXCHANGE_SOURCE_CODE_COL,
        EXCHANGE_SOURCE_NAME_COL,
        EXCHANGE_TARGET_CODE_COL,
        EXCHANGE_TARGET_NAME_COL,
        HYDRO_CODE_COL,
        HYDRO_NAME_COL,
        THERMAL_CODE_COL,
        THERMAL_NAME_COL,
        EER_CODE_COL,
        EER_NAME_COL,
        BLOCK_COL,
        BLOCK_DURATION_COL,
        SCENARIO_COL,
        STATS_OR_SCENARIO_COL,
    ]

    STAGE_DURATION_HOURS = 730.0
    HM3_M3S_FACTOR = 1 / 2.63

    T = TypeVar("T")

    MAPPINGS: Dict[OperationSynthesis, Callable] = {
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_energy_bounds(
            df, uow, synthesis_unit=Unit.MWmes.value, ordered_entities=entities
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_energy_bounds(
            df, uow, synthesis_unit=Unit.MWmes.value, ordered_entities=entities
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=None,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=None,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=None,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: OperationVariableBounds._stored_volume_bounds(
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
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
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
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, entities: OperationVariableBounds._turbined_flow_bounds(
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
        ): lambda df, uow, entities: OperationVariableBounds._turbined_flow_bounds(
            df,
            uow,
            synthesis_unit=Unit.m3s.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
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
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
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
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._unbounded_bounds(
            df, uow
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
        ): lambda df, uow, _: OperationVariableBounds._unbounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, entities: OperationVariableBounds._qdes_vdes_uhe_bounds(
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
            df, grouping_column=SYSTEM_GROUPING_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, entities: OperationVariableBounds._qdes_vdes_uhe_bounds(
            df, uow, synthesis_unit=Unit.m3s.value
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
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
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=EER_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_EVAPORADA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_EVAPORADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._group_hydro_df_vol_flow_cast(
            df, grouping_column=None
        ),
        OperationSynthesis(
            Variable.INTERCAMBIO,
            SpatialResolution.PAR_SUBMERCADOS,
        ): lambda df, uow, entities: OperationVariableBounds._exchange_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.USINA_TERMELETRICA,
        ): lambda df, uow, entities: OperationVariableBounds._thermal_generation_bounds(
            df, uow, THERMAL_NAME_COL
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: OperationVariableBounds._thermal_generation_bounds(
            df, uow, SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: OperationVariableBounds._thermal_generation_bounds(
            df, uow, SYSTEM_GROUPING_COL
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
    def _get_conft(cls, uow: AbstractUnitOfWork) -> Conft:
        with uow:
            conft = uow.files.get_conft()
            if conft is None:
                raise RuntimeError("Erro na leitura do arquivo conft.dat")
            return conft

    @classmethod
    def _validate_data(cls, data, type: Type[T]) -> T:
        if not isinstance(data, type):
            raise RuntimeError("Erro na validação dos dados.")
        return data

    @classmethod
    def _stored_energy_bounds(
        cls,
        df: pd.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list],
        entity_column: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Energia Armazenada Absoluta (EARM) e Energia
        Armazenada Percentual (EARP).
        """

        def _get_group_and_cast_bounds() -> Tuple[np.ndarray, np.ndarray]:
            upper_bound_df = Deck.stored_energy_upper_bounds(uow)
            lower_bound_df = Deck.eer_stored_energy_lower_bounds(uow)
            upper_bounds = (
                upper_bound_df.groupby(grouping_columns, as_index=False)
                .sum(numeric_only=True)[VALUE_COL]
                .to_numpy()
            )
            lower_bounds = (
                lower_bound_df.groupby(grouping_columns, as_index=False)
                .sum(numeric_only=True)[VALUE_COL]
                .to_numpy()
            )
            if synthesis_unit == Unit.perc_modif.value:
                lower_bounds = lower_bounds / upper_bounds * 100.0
                upper_bounds = 100.0 * np.ones_like(lower_bounds)
            return lower_bounds, upper_bounds

        def _repeat_bounds_by_scenario(
            df: pd.DataFrame,
            lower_bounds: np.ndarray,
            upper_bounds: np.ndarray,
        ) -> pd.DataFrame:
            num_entities = len(ordered_entities.get(entity_column, [None]))
            num_stages = len(ordered_entities[STAGE_COL])
            num_scenarios = len(ordered_entities[SCENARIO_COL])
            num_blocks = len(ordered_entities[BLOCK_COL])
            df[LOWER_BOUND_COL] = cls._repeats_data_by_scenario(
                lower_bounds,
                num_entities,
                num_stages,
                num_scenarios,
                num_blocks,
            )
            df[UPPER_BOUND_COL] = cls._repeats_data_by_scenario(
                upper_bounds,
                num_entities,
                num_stages,
                num_scenarios,
                num_blocks,
            )
            return df

        def _sort_and_round_bounds(df: pd.DataFrame) -> pd.DataFrame:
            num_digits = 1 if synthesis_unit == Unit.perc_modif.value else 0
            df[VALUE_COL] = np.round(df[VALUE_COL], num_digits)
            df = df.sort_values(grouping_columns)
            df[LOWER_BOUND_COL] = np.round(df[LOWER_BOUND_COL], num_digits)
            df[UPPER_BOUND_COL] = np.round(df[UPPER_BOUND_COL], num_digits)
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
        data_cenarios = np.zeros(
            (len(data) * num_scenarios,), dtype=np.float64
        )
        for i in range(num_entities):
            for j in range(num_stages):
                i_i = i * num_stages * num_blocks + j * num_blocks
                i_f = i_i + num_blocks
                data_cenarios[i_i * num_scenarios : i_f * num_scenarios] = (
                    np.tile(data[i_i:i_f], num_scenarios)
                )
        return data_cenarios

    @classmethod
    def _group_hydro_df(
        cls, df: pd.DataFrame, grouping_column: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de UHE
        para uma síntese de REEs, SBMs ou para o SIN. A agregação
        tem como requisito que as variáveis fornecidas sejam em unidades
        cuja agregação seja possível apenas pela soma.
        """
        valid_grouping_columns = [
            HYDRO_CODE_COL,
            HYDRO_NAME_COL,
            EER_CODE_COL,
            EER_NAME_COL,
            SUBMARKET_CODE_COL,
            SUBMARKET_NAME_COL,
        ]

        grouping_column_map: Dict[str, List[str]] = {
            HYDRO_CODE_COL: [
                HYDRO_CODE_COL,
                HYDRO_NAME_COL,
                EER_CODE_COL,
                EER_NAME_COL,
                SUBMARKET_CODE_COL,
                SUBMARKET_NAME_COL,
            ],
            EER_CODE_COL: [
                EER_CODE_COL,
                EER_NAME_COL,
                SUBMARKET_CODE_COL,
                SUBMARKET_NAME_COL,
            ],
            SUBMARKET_CODE_COL: [SUBMARKET_CODE_COL, SUBMARKET_NAME_COL],
            None: [],
        }

        grouping_columns = grouping_column_map.get(grouping_column, []) + [
            c
            for c in df.columns
            if c in cls.IDENTIFICATION_COLUMNS
            and c not in valid_grouping_columns
        ]

        grouped_df = fast_group_df(
            df,
            grouping_columns,
            [VALUE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL],
            operation="sum",
        )

        return grouped_df

    @classmethod
    def _group_hydro_df_vol_flow_cast(
        cls, df: pd.DataFrame, grouping_column: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de UHE
        para uma síntese de REEs, SBMs ou para o SIN, convertendo
        a unidade de volume para vazão.

        É usada em casos em que osdados são fornecidos em unidade
        de volume, mas a síntese desejada é em unidade de vazão.
        """
        df_group = cls._group_hydro_df(df, grouping_column)
        for c in [VALUE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL]:
            df_group[c] = (
                df_group[c]
                * (cls.STAGE_DURATION_HOURS * cls.HM3_M3S_FACTOR)
                / df_group[BLOCK_DURATION_COL]
            )

        return df_group

    @classmethod
    def _stored_volume_bounds(
        cls,
        df: pd.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list],
        entity_column: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Armazenado Absoluto (VARM) e Volume
        Armazenado Percentual (VARP) para cada UHE.
        """

        def _get_group_and_cast_bounds() -> Tuple[np.ndarray, np.ndarray]:
            volume_bounds_in_stages_df = Deck.hydro_volume_bounds_in_stages(
                uow
            )
            synthesis_hydro_codes = df[HYDRO_CODE_COL].unique().tolist()
            volume_bounds_in_stages_df = volume_bounds_in_stages_df.loc[
                volume_bounds_in_stages_df[HYDRO_CODE_COL].isin(
                    synthesis_hydro_codes
                )
            ].reset_index()

            grouped_bounds_df = (
                volume_bounds_in_stages_df.groupby(
                    grouping_columns, as_index=False
                )
                .sum(numeric_only=True)[[LOWER_BOUND_COL, UPPER_BOUND_COL]]
                .to_numpy()
            )
            lower_bounds = grouped_bounds_df[:, 0]
            upper_bounds = grouped_bounds_df[:, 1]

            if synthesis_unit == Unit.perc_modif.value:
                lower_bounds = 0.0 * np.ones_like(lower_bounds)
                upper_bounds = 100.0 * np.ones_like(lower_bounds)
            return lower_bounds, upper_bounds

        def _repeat_bounds_by_scenario(
            df: pd.DataFrame,
            lower_bounds: np.ndarray,
            upper_bounds: np.ndarray,
        ) -> pd.DataFrame:
            num_entities = len(ordered_entities.get(entity_column, [None]))
            num_stages = len(ordered_entities[STAGE_COL])
            num_scenarios = len(ordered_entities[SCENARIO_COL])
            num_blocks = len(ordered_entities[BLOCK_COL])
            df = df.sort_values(grouping_columns)
            df[LOWER_BOUND_COL] = cls._repeats_data_by_scenario(
                lower_bounds,
                num_entities,
                num_stages,
                num_scenarios,
                num_blocks,
            )
            df[UPPER_BOUND_COL] = cls._repeats_data_by_scenario(
                upper_bounds,
                num_entities,
                num_stages,
                num_scenarios,
                num_blocks,
            )
            return df

        def _sort_and_round_bounds(
            df: pd.DataFrame,
            already_had_limits: bool,
        ) -> pd.DataFrame:
            num_digits = 2

            df[VALUE_COL] = np.round(df[VALUE_COL], num_digits)
            df[LOWER_BOUND_COL] = np.round(df[LOWER_BOUND_COL], num_digits)
            df[UPPER_BOUND_COL] = np.round(df[UPPER_BOUND_COL], num_digits)
            if (
                synthesis_unit == Unit.hm3_modif.value
                and not already_had_limits
            ):
                df[VALUE_COL] += df[LOWER_BOUND_COL]
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
        df: pd.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list],
        entity_column: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Defluente (VDEF) e Vazão Defluente (QDEF)
        para cada UHE.
        """

        def _get_group_for_bounds() -> Tuple[np.ndarray, np.ndarray]:
            outflow_bounds = Deck.hydro_outflow_bounds_in_stages(uow)
            synthesis_hydro_codes = df[HYDRO_CODE_COL].unique().tolist()
            outflow_bounds = outflow_bounds.loc[
                outflow_bounds[HYDRO_CODE_COL].isin(synthesis_hydro_codes)
            ].reset_index(drop=True)

            grouped_bounds_df = (
                outflow_bounds.groupby(grouping_columns, as_index=False)
                .sum(numeric_only=True)[[LOWER_BOUND_COL, UPPER_BOUND_COL]]
                .to_numpy()
            )
            lower_bounds = grouped_bounds_df[:, 0]
            upper_bounds = grouped_bounds_df[:, 1]

            return lower_bounds, upper_bounds

        def _repeat_bounds_by_scenario_and_cast(
            df: pd.DataFrame,
            lower_bounds: np.ndarray,
            upper_bounds: np.ndarray,
        ) -> pd.DataFrame:
            num_entities = len(ordered_entities.get(entity_column, [None]))
            num_stages = len(ordered_entities[STAGE_COL])
            num_scenarios = len(ordered_entities[SCENARIO_COL])
            num_blocks = len(ordered_entities[BLOCK_COL])
            df = df.sort_values(grouping_columns)
            df[LOWER_BOUND_COL] = cls._repeats_data_by_scenario(
                lower_bounds,
                num_entities,
                num_stages,
                num_scenarios,
                num_blocks,
            )
            df[UPPER_BOUND_COL] = cls._repeats_data_by_scenario(
                upper_bounds,
                num_entities,
                num_stages,
                num_scenarios,
                num_blocks,
            )
            if synthesis_unit == Unit.hm3.value:
                for col in [LOWER_BOUND_COL, UPPER_BOUND_COL]:
                    df[col] = (
                        df[col]
                        * df[BLOCK_DURATION_COL]
                        / (cls.HM3_M3S_FACTOR * cls.STAGE_DURATION_HOURS)
                    )
            return df

        def _sort_and_round_bounds(
            df: pd.DataFrame,
        ) -> pd.DataFrame:
            num_digits = 2
            df = df.sort_values(sorting_columns)
            df[VALUE_COL] = np.round(df[VALUE_COL], num_digits)
            df[LOWER_BOUND_COL] = np.round(df[LOWER_BOUND_COL], num_digits)
            df[UPPER_BOUND_COL] = np.round(df[UPPER_BOUND_COL], num_digits)
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
        df: pd.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list],
        entity_column: Optional[str] = None,
    ) -> pd.DataFrame:
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
            synthesis_hydro_codes = df[HYDRO_CODE_COL].unique().tolist()
            turbined_flow_bounds = turbined_flow_bounds.loc[
                turbined_flow_bounds[HYDRO_CODE_COL].isin(
                    synthesis_hydro_codes
                )
            ].reset_index(drop=True)

            grouped_bounds_df = (
                turbined_flow_bounds.groupby(grouping_columns, as_index=False)
                .sum(numeric_only=True)[[LOWER_BOUND_COL, UPPER_BOUND_COL]]
                .to_numpy()
            )
            lower_bounds = grouped_bounds_df[:, 0]
            upper_bounds = grouped_bounds_df[:, 1]

            return lower_bounds, upper_bounds

        def _repeat_bounds_by_scenario_and_cast(
            df: pd.DataFrame,
            lower_bounds: np.ndarray,
            upper_bounds: np.ndarray,
        ) -> pd.DataFrame:
            num_entities = len(ordered_entities.get(entity_column, [None]))
            num_stages = len(ordered_entities[STAGE_COL])
            num_scenarios = len(ordered_entities[SCENARIO_COL])
            num_blocks = len(ordered_entities[BLOCK_COL])
            df = df.sort_values(grouping_columns)
            df[LOWER_BOUND_COL] = cls._repeats_data_by_scenario(
                lower_bounds,
                num_entities,
                num_stages,
                num_scenarios,
                num_blocks,
            )
            df[UPPER_BOUND_COL] = cls._repeats_data_by_scenario(
                upper_bounds,
                num_entities,
                num_stages,
                num_scenarios,
                num_blocks,
            )
            if synthesis_unit == Unit.hm3.value:
                for col in [LOWER_BOUND_COL, UPPER_BOUND_COL]:
                    df[col] = (
                        df[col]
                        * df[BLOCK_DURATION_COL]
                        / (cls.HM3_M3S_FACTOR * cls.STAGE_DURATION_HOURS)
                    )
            return df

        def _sort_and_round_bounds(
            df: pd.DataFrame,
        ) -> pd.DataFrame:
            num_digits = 2
            df = df.sort_values(sorting_columns)
            df[VALUE_COL] = np.round(df[VALUE_COL], num_digits)
            df[LOWER_BOUND_COL] = np.round(df[LOWER_BOUND_COL], num_digits)
            df[UPPER_BOUND_COL] = np.round(df[UPPER_BOUND_COL], num_digits)
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
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Afluente (VAFL) e Vazão Afluente (QAFL)
        para cada UHE.
        """
        df[VALUE_COL] = np.round(df[VALUE_COL], 2)
        df[LOWER_BOUND_COL] = 0.0
        df[UPPER_BOUND_COL] = float("inf")
        return df

    @classmethod
    def _unbounded_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para variáveis ilimitadas.
        """
        df[VALUE_COL] = np.round(df[VALUE_COL], 2)
        df[LOWER_BOUND_COL] = -float("inf")
        df[UPPER_BOUND_COL] = float("inf")
        return df

    @classmethod
    def _qdes_vdes_uhe_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, synthesis_unit: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Desviado (VDES) e Vazão Desviada (QDES)
        para cada UHE.
        """
        # TODO - Procurar limite superior no modif.dat
        df[LOWER_BOUND_COL] = 0.0
        df[UPPER_BOUND_COL] = float("inf")
        return df

    @classmethod
    def _considera_flag_sentido_limite_intercambio(
        cls, df_limites: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Inverte as colunas submercado_de e submercado_para a
        partir do valor da coluna sentido, aplicada apenas para
        o DataFrame de limites de intercâmbio do sistema.dat.
        """
        filtro = df_limites["sentido"] == 1
        (
            df_limites.loc[filtro, "submercado_de"],
            df_limites.loc[filtro, "submercado_para"],
        ) = (
            df_limites.loc[filtro, "submercado_para"],
            df_limites.loc[filtro, "submercado_de"],
        )
        return df_limites.drop(columns=["sentido"])

    @classmethod
    def _cria_pat0_ficticio(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cria um patamar 0 fictício com 1 p.u. nos dados lidos do
        patamar.dat.
        """
        df_pat0 = df.loc[df["patamar"] == 1].copy()
        df_pat0["patamar"] = 0
        df_pat0["valor"] = 1.0
        df = pd.concat([df, df_pat0], ignore_index=True)
        cols_ordenacao = [c for c in df.columns if c != "valor"]
        return df.sort_values(cols_ordenacao)

    @classmethod
    def _converte_limites_intercambio_MWmes(
        cls,
        df_limites_patamar_pu: pd.DataFrame,
        df_limites_estagios_mwmed: pd.DataFrame,
        df_duracoes: pd.DataFrame,
        df_submercados: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Obtem limites de intercâmbio em MWmes a partir de limites
        em MWmed e P.U. e das durações de cada patamar. Estes limites
        são compatíveis com o visto no nwlistop.
        """
        # Obtem limites por patamar em MWmed
        df_limites_pat = df_limites_patamar_pu.copy()
        df_limites_pat["valor"] = df_limites_pat.apply(
            lambda linha: df_limites_estagios_mwmed.loc[
                (
                    df_limites_estagios_mwmed["submercado_de"]
                    == linha["submercado_de"]
                )
                & (
                    df_limites_estagios_mwmed["submercado_para"]
                    == linha["submercado_para"]
                )
                & (df_limites_estagios_mwmed["data"] == linha["data"]),
                "valor",
            ].iloc[0]
            * linha["valor"],
            axis=1,
        )

        # Converte para MWmes
        df_duracoes = df_duracoes.sort_values(["data", "patamar"])
        n_pares_limites = df_limites_pat.drop_duplicates(
            ["submercado_de", "submercado_para"]
        ).shape[0]
        df_limites_pat["valor"] *= np.tile(
            df_duracoes["valor"].to_numpy(), n_pares_limites
        )
        # Substitui códigos dos submercados pelos nomes
        df_limites_pat = df_limites_pat.astype(
            {"submercado_de": str, "submercado_para": str}
        )
        df_submercados = df_submercados.drop_duplicates(
            ["nome_submercado", SUBMARKET_CODE_COL]
        )
        mapa_nomes_submercados = {
            str(codigo): nome
            for codigo, nome in zip(
                df_submercados[SUBMARKET_CODE_COL],
                df_submercados["nome_submercado"],
            )
        }
        for col in ["submercado_de", "submercado_para"]:
            codigos = df_limites_pat[col].unique()
            for cod in codigos:
                df_limites_pat.loc[df_limites_pat[col] == cod, col] = (
                    mapa_nomes_submercados[cod]
                )
        return df_limites_pat

    @classmethod
    def _aplica_limites_intercambio_mwmes(
        cls, df: pd.DataFrame, df_limites_pat: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Cria as colunas de limites de intercâmbio em MWmes no DataFrame
        da síntese que foi recebido.
        Os limites inferior e superior são aplicados de acordo com o
        par de submercados e o sentido do intercâmbio, sendo um deles
        sempre <= 0.
        """
        datas_inicio = df[START_DATE_COL].unique().tolist()
        n_estagios = len(datas_inicio)
        n_cenarios = len(df[SCENARIO_COL].unique())
        n_patamares = len(df["patamar"].unique())
        # Filtra os pares de submercados de limites dentre os
        # que existem no df
        df["par_sbm"] = (
            df[EXCHANGE_SOURCE_NAME_COL] + "-" + df[EXCHANGE_TARGET_NAME_COL]
        )
        df_limites_pat["par_sbm"] = (
            df_limites_pat["submercado_de"]
            + "-"
            + df_limites_pat["submercado_para"]
        )
        df_limites_pat["par_sbm_r"] = (
            df_limites_pat["submercado_para"]
            + "-"
            + df_limites_pat["submercado_de"]
        )
        df_limites_pat = (
            df_limites_pat.loc[df_limites_pat["data"].isin(datas_inicio)]
            .sort_values(["submercado_de", "submercado_para", "data"])
            .reset_index(drop=True)
        )

        pares_sbm_df = df["par_sbm"].unique().tolist()
        pares_sbm_limites_r = df_limites_pat["par_sbm_r"].unique().tolist()
        pares_sbm_limites = df_limites_pat["par_sbm"].unique().tolist()

        # Inicializa limites com valores default
        df[LOWER_BOUND_COL] = -float("inf")
        df[UPPER_BOUND_COL] = float("inf")
        # Aplica os limites, considerando o par de submercados
        # e o sentido reverso como sinal negativo
        for p in pares_sbm_df:
            if p in pares_sbm_limites_r:
                lims = -cls._repeats_data_by_scenario(
                    df_limites_pat.loc[
                        df_limites_pat["par_sbm_r"] == p, "valor"
                    ].to_numpy(),
                    1,
                    n_estagios,
                    n_cenarios,
                    n_patamares,
                )
                df.loc[df["par_sbm"] == p, LOWER_BOUND_COL] = lims
            if p in pares_sbm_limites:
                lims = cls._repeats_data_by_scenario(
                    df_limites_pat.loc[
                        df_limites_pat["par_sbm"] == p, "valor"
                    ].to_numpy(),
                    1,
                    n_estagios,
                    n_cenarios,
                    n_patamares,
                )
                df.loc[df["par_sbm"] == p, UPPER_BOUND_COL] = lims

        df[LOWER_BOUND_COL] = np.round(df[LOWER_BOUND_COL], 1)
        df[UPPER_BOUND_COL] = np.round(df[UPPER_BOUND_COL], 1)

        return df.drop(columns=["par_sbm"])

    @classmethod
    def _exchange_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Intercâmbio (INT) por par de submercados.
        """
        # Lê e converte os limites de intercâmbio para MWmes,
        # pois os arquivos de entrada são em MWmed com P.U e
        # o nwlistop fornece a saída em MWmes.

        # Lê sistema.dat
        arq_sistema = cls._get_sistema(uow)
        df_submercados = cls._validate_data(
            arq_sistema.custo_deficit, pd.DataFrame
        )
        df_limites = cls._validate_data(
            arq_sistema.limites_intercambio, pd.DataFrame
        )
        # Lê patamar.dat
        arq_patamar = cls._get_patamar(uow)
        df_pu = cls._validate_data(
            arq_patamar.intercambio_patamares, pd.DataFrame
        )
        df_duracoes = cls._validate_data(
            arq_patamar.duracao_mensal_patamares, pd.DataFrame
        )
        # Formata os dados lidos
        df_limites = cls._considera_flag_sentido_limite_intercambio(df_limites)
        df_pu = cls._cria_pat0_ficticio(df_pu)
        df_duracoes = cls._cria_pat0_ficticio(df_duracoes)

        df_limites_pat = cls._converte_limites_intercambio_MWmes(
            df_pu, df_limites, df_duracoes, df_submercados
        )
        return cls._aplica_limites_intercambio_mwmes(df, df_limites_pat)

    @classmethod
    def _adiciona_submercado_limites_gter(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        map = Deck.thermal_submarket_map(uow)
        df = df.rename(
            columns={
                "nome_usina": THERMAL_NAME_COL,
                "codigo_usina": THERMAL_CODE_COL,
            }
        )
        df = df.join(
            map[[SUBMARKET_CODE_COL, SUBMARKET_NAME_COL]], on=THERMAL_CODE_COL
        )
        return df

    @classmethod
    def _agrega_variaveis_limites_ute(
        cls,
        df: pd.DataFrame,
        grouping_col: Optional[str] = None,
        ordem_sintese: Optional[list] = None,
        datas_sintese: Optional[list] = None,
    ) -> pd.DataFrame:
        """
        Realiza a agregação de limites de geração de usinas
        térmicas.
        """
        cols_grp_validas = [
            THERMAL_CODE_COL,
            THERMAL_NAME_COL,
            SUBMARKET_CODE_COL,
            SUBMARKET_NAME_COL,
            SYSTEM_GROUPING_COL,
        ]
        if grouping_col is None:
            return df

        if grouping_col == SYSTEM_GROUPING_COL:
            df["group"] = 1
        elif grouping_col in cols_grp_validas:
            df["group"] = df[grouping_col]
        else:
            raise RuntimeError(
                f"Coluna de agrupamento inválida: {grouping_col}"
            )

        if datas_sintese:
            df = df.loc[df["data"].isin(datas_sintese)].reset_index(drop=True)

        cols_group = ["group", "data"]
        df_group = (
            df.groupby(cols_group)[["valor_MWmed"]]
            .sum(engine="numba")
            .reset_index()
        )

        if ordem_sintese:
            df_group = df_group.loc[
                df_group["group"].isin(ordem_sintese)
            ].copy()
            df_group["group"] = pd.Categorical(
                df_group["group"], categories=ordem_sintese, ordered=True
            )
        df_group = df_group.sort_values(["group", "data"])
        df_group["group"] = df_group["group"].astype(str)

        if grouping_col:
            df_group = df_group.rename(columns={"group": grouping_col})
        else:
            df_group = df_group.drop(columns=["group"])

        return df_group

    @classmethod
    def _expande_dados_cenarios_gter(
        cls,
        df: pd.DataFrame,
        df_gtmin: pd.DataFrame,
        df_gtmax: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Expande os dados da síntese de geração térmica
        para o número de cenários e patamares existentes.

        É um wrapper para a chamada da _repeats_data_by_scenario
        pois existe o comportamento particular dos limites de geração
        serem fornecidos apenas em MWmed por estágio, e por
        questões de desempenho este são repetidos
        (n_patamares * n_cenarios) vezes como se existisse apenas 1
        patamar e a conversão para MWmes é feita posterioremente.
        """
        n_cenarios = len(df[SCENARIO_COL].unique())
        n_patamares = len(df["patamar"].unique())
        lim_inf = np.repeat(
            df_gtmin["valor_MWmed"].to_numpy(), n_cenarios * n_patamares
        )
        lim_sup = np.repeat(
            df_gtmax["valor_MWmed"].to_numpy(), n_cenarios * n_patamares
        )
        df[LOWER_BOUND_COL] = lim_inf
        df[UPPER_BOUND_COL] = lim_sup
        return df

    @classmethod
    def _thermal_generation_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, grouping_col: str
    ) -> pd.DataFrame:
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
        # Lê os limites em MWmed do pmo.dat
        arq_pmo = Deck.pmo(uow)
        df_gtmin = cls._validate_data(
            arq_pmo.geracao_minima_usinas_termicas, pd.DataFrame
        )
        df_gtmax = cls._validate_data(
            arq_pmo.geracao_maxima_usinas_termicas, pd.DataFrame
        )
        # Adiciona informações do submercado de cada UTE
        df_gtmin = cls._adiciona_submercado_limites_gter(df_gtmin, uow)
        df_gtmax = cls._adiciona_submercado_limites_gter(df_gtmax, uow)
        # Agrupa os limites, se necessário
        datas_sintese = df[START_DATE_COL].unique().tolist()
        ordem_sintese = (
            df[grouping_col].unique().tolist()
            if grouping_col != SYSTEM_GROUPING_COL
            else None
        )
        df_gtmin = cls._agrega_variaveis_limites_ute(
            df_gtmin, grouping_col, ordem_sintese, datas_sintese
        )
        df_gtmax = cls._agrega_variaveis_limites_ute(
            df_gtmax, grouping_col, ordem_sintese, datas_sintese
        )
        # Repete os limites para todos os estágios e cenarios
        df = cls._expande_dados_cenarios_gter(df, df_gtmin, df_gtmax)
        # Converte os limites para MWmes
        df[LOWER_BOUND_COL] *= (
            df[BLOCK_DURATION_COL] / cls.STAGE_DURATION_HOURS
        )
        df[UPPER_BOUND_COL] *= (
            df[BLOCK_DURATION_COL] / cls.STAGE_DURATION_HOURS
        )

        df[LOWER_BOUND_COL] = np.round(df[LOWER_BOUND_COL], 1)
        df[UPPER_BOUND_COL] = np.round(df[UPPER_BOUND_COL], 1)

        return df

    # TODO intercambios - também tem que pensar em alguma lógica para plotar os limites
    # de intercâmbio considerando os agrupamentos? Ou criar uma nova síntese
    # de agrupamentos de intercâmbio?

    # TODO ghid UHE, REE, SBM e SIN - como obter limites para geração hidráulica?
    # pode congelar as demais variáveis e usar a FPHA para obter limites de geração
    # variando somente o turbinamento.. é justo? Conferir se mais coisas
    # podem estar envolvidas e limitar o quanto a usina poderia gerar naquele ponto
    # de operação (efeito do polinjus...)

    @classmethod
    def is_bounded(cls, s: OperationSynthesis) -> bool:
        """
        Verifica se uma determinada síntese possui limites implementados
        para adição ao DataFrame.
        """
        return s in cls.MAPPINGS

    @classmethod
    def _unbounded(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona os valores padrão para variáveis não limitadas.
        """
        df[LOWER_BOUND_COL] = -float("inf")
        df[UPPER_BOUND_COL] = float("inf")
        return df

    @classmethod
    def resolve_bounds(
        cls,
        s: OperationSynthesis,
        df: pd.DataFrame,
        ordered_synthesis_entities: Dict[str, list],
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        """
        Adiciona colunas de limite inferior e superior a um DataFrame,
        calculando os valores necessários caso a variável seja limitada
        ou atribuindo -inf e +inf caso contrário.

        """
        if cls.is_bounded(s):
            return cls.MAPPINGS[s](df, uow, ordered_synthesis_entities)
        else:
            return cls._unbounded(df)
