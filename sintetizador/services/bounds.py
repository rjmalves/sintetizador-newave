import pandas as pd  # type: ignore
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Callable, Type, TypeVar, Optional, Union
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.operationsynthesis import OperationSynthesis
from sintetizador.services.unitofwork import AbstractUnitOfWork

from inewave.newave import (
    Dger,
    Hidr,
    Modif,
    Pmo,
    Sistema,
    Ree,
    Curva,
    Patamar,
    Conft,
    Confhd,
)
from inewave.newave.modelos.modif import (
    VOLMIN,
    VOLMAX,
    VMINT,
    VMAXT,
    VAZMIN,
    VAZMINT,
    VAZMAXT,
    TURBMINT,
    TURBMAXT,
    NUMCNJ,
    NUMMAQ,
)
from cfinterface.components.register import Register


class OperationVariableBounds:
    """
    Entidade responsável por calcular os limites das variáveis de operação
    existentes nos arquivos de saída do NEWAVE, que são processadas no
    processo de síntese da operação.
    """

    IDENTIFICATION_COLUMNS = [
        "dataInicio",
        "dataFim",
        "estagio",
        "submercado",
        "submercadoDe",
        "submercadoPara",
        "ree",
        "pee",
        "usina",
        "patamar",
        "duracaoPatamar",
        "serie",
    ]

    STAGE_DURATION_HOURS = 730.0
    HM3_M3S_FACTOR = 1 / 2.63

    T = TypeVar("T")

    MAPPINGS: Dict[OperationSynthesis, Callable] = {
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow: OperationVariableBounds._earm_earp_ree_bounds(
            df, uow, unidade_sintese="mwmes"
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow: OperationVariableBounds._earm_earp_ree_bounds(
            df, uow, unidade_sintese="mwmes"
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow: OperationVariableBounds._earm_earp_ree_bounds(
            df, uow, unidade_sintese="'%'"
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow: OperationVariableBounds._earm_earp_ree_bounds(
            df, uow, unidade_sintese="'%'"
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow: OperationVariableBounds._earm_earp_sbm_bounds(
            df, uow, unidade_sintese="mwmes"
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow: OperationVariableBounds._earm_earp_sbm_bounds(
            df, uow, unidade_sintese="mwmes"
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow: OperationVariableBounds._earm_earp_sbm_bounds(
            df, uow, unidade_sintese="'%'"
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow: OperationVariableBounds._earm_earp_sbm_bounds(
            df, uow, unidade_sintese="'%'"
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow: OperationVariableBounds._earm_earp_sin_bounds(
            df, uow, unidade_sintese="mwmes"
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow: OperationVariableBounds._earm_earp_sin_bounds(
            df, uow, unidade_sintese="mwmes"
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow: OperationVariableBounds._earm_earp_sin_bounds(
            df, uow, unidade_sintese="'%'"
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow: OperationVariableBounds._earm_earp_sin_bounds(
            df, uow, unidade_sintese="'%'"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._varm_varp_uhe_bounds(
            df, uow, unidade_sintese="'h'"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._varm_varp_uhe_bounds(
            df, uow, unidade_sintese="'h'"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._varm_varp_uhe_bounds(
            df, uow, unidade_sintese="'%'"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_absoluto_percentual(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_absoluto_percentual(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_absoluto_percentual(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._varm_varp_uhe_bounds(
            df, uow, unidade_sintese="'%'"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_absoluto_percentual(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_absoluto_percentual(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_absoluto_percentual(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qafl_vafl_uhe_bounds(
            df, uow, unidade_sintese="hm3"
        ),
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qafl_vafl_uhe_bounds(
            df, uow, unidade_sintese="m3/s"
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qinc_vinc_uhe_bounds(
            df, uow, unidade_sintese="hm3"
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qinc_vinc_uhe_bounds(
            df, uow, unidade_sintese="m3/s"
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qtur_vtur_uhe_bounds(
            df, uow, unidade_sintese="hm3"
        ),
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qtur_vtur_uhe_bounds(
            df, uow, unidade_sintese="m3/s"
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qver_vver_uhe_bounds(
            df, uow, unidade_sintese="hm3"
        ),
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qver_vver_uhe_bounds(
            df, uow, unidade_sintese="m3/s"
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qdef_vdef_uhe_bounds(
            df, uow, unidade_sintese="hm3"
        ),
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qdef_vdef_uhe_bounds(
            df, uow, unidade_sintese="m3/s"
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qret_vret_uhe_bounds(
            df, uow, unidade_sintese="hm3"
        ),
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qret_vret_uhe_bounds(
            df, uow, unidade_sintese="m3/s"
        ),
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qdes_vdes_uhe_bounds(
            df, uow, unidade_sintese="hm3"
        ),
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow: OperationVariableBounds._qdes_vdes_uhe_bounds(
            df, uow, unidade_sintese="m3/s"
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="ree"
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="submercado"
        ),
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, _: OperationVariableBounds._agrega_variaveis_uhe_volume_vazao(
            df, col_grp="sin"
        ),
        OperationSynthesis(
            Variable.INTERCAMBIO,
            SpatialResolution.PAR_SUBMERCADOS,
        ): lambda df, uow: OperationVariableBounds._int_sbp_bounds(df, uow),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.USINA_TERMELETRICA,
        ): lambda df, uow: OperationVariableBounds._gter_bounds(
            df, uow, "usina"
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow: OperationVariableBounds._gter_bounds(
            df, uow, "submercado"
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow: OperationVariableBounds._gter_bounds(
            df, uow, "sin"
        ),
    }

    @classmethod
    def _get_hidr(cls, uow: AbstractUnitOfWork) -> Hidr:
        with uow:
            hidr = uow.files.get_hidr()
            if hidr is None:
                raise RuntimeError("Erro na leitura do arquivo hidr.dat")
            return hidr

    @classmethod
    def _get_modif(cls, uow: AbstractUnitOfWork) -> Modif:
        with uow:
            modif = uow.files.get_modif()
            if modif is None:
                raise RuntimeError("Erro na leitura do arquivo modif.dat")
            return modif

    @classmethod
    def _get_ree(cls, uow: AbstractUnitOfWork) -> Ree:
        with uow:
            ree = uow.files.get_ree()
            if ree is None:
                raise RuntimeError("Erro na leitura do arquivo ree.dat")
            return ree

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
    def _get_curva(cls, uow: AbstractUnitOfWork) -> Curva:
        with uow:
            curva = uow.files.get_curva()
            if curva is None:
                raise RuntimeError("Erro na leitura do arquivo curva.dat")
            return curva

    @classmethod
    def _get_confhd(cls, uow: AbstractUnitOfWork) -> Confhd:
        with uow:
            confhd = uow.files.get_confhd()
            if confhd is None:
                raise RuntimeError("Erro na leitura do arquivo confhd.dat")
            return confhd

    @classmethod
    def _get_conft(cls, uow: AbstractUnitOfWork) -> Conft:
        with uow:
            conft = uow.files.get_conft()
            if conft is None:
                raise RuntimeError("Erro na leitura do arquivo conft.dat")
            return conft

    @classmethod
    def _get_pmo(cls, uow: AbstractUnitOfWork) -> Pmo:
        with uow:
            pmo = uow.files.get_pmo()
            if pmo is None:
                raise RuntimeError("Erro na leitura do arquivo pmo.dat")
            return pmo

    @classmethod
    def _get_dger(cls, uow: AbstractUnitOfWork) -> Dger:
        with uow:
            dger = uow.files.get_dger()
            if dger is None:
                raise RuntimeError("Erro na leitura do arquivo dger.dat")
            return dger

    @classmethod
    def _validate_data(cls, data, type: Type[T]) -> T:
        if not isinstance(data, type):
            raise RuntimeError("Erro na validação dos dados.")
        return data

    @classmethod
    def _codigos_rees(
        cls,
        df: pd.DataFrame,
        df_rees: pd.DataFrame,
        n_estagios: int,
        n_patamares: int,
    ) -> np.ndarray:
        """
        Retorna os códigos únicos das usinas, na ordem em que aparecem
        no DataFrame da síntese em processamento.
        """
        rees_df = df["ree"].unique().tolist()
        codigos_rees = []
        for u in rees_df:
            codigos_rees.append(df_rees.loc[df_rees["nome"] == u].index[0])
        return np.repeat(codigos_rees, n_estagios * n_patamares)

    @classmethod
    def _filtra_datas_df(
        cls, df: pd.DataFrame, col: str, inicio: datetime, fim: datetime
    ) -> pd.DataFrame:
        """
        Filtra um DataFrame por um intervalo de datas, segundo uma coluna.
        """
        return df.loc[df[col].between(inicio, fim)]

    @classmethod
    def _adiciona_data_configuracoes(
        cls, df: pd.DataFrame, arq_pmo: Pmo, arq_dger: Dger
    ) -> pd.DataFrame:
        """
        Adiciona a informação da data associada a cada configuração
        em um DataFrame que contenha a configuração.
        """
        data_inicio, data_fim = cls._datas_inicio_fim_estudo(arq_dger)

        df_configs = cls._validate_data(
            arq_pmo.configuracoes_qualquer_modificacao, pd.DataFrame
        )
        df_configs = cls._filtra_datas_df(
            df_configs, "data", data_inicio, data_fim
        )
        # Adiciona informação da data de cada configuração e reordena
        # pela ordem que aparece no dataframe da síntese
        df["data"] = df["configuracao"].apply(
            lambda c: df_configs.loc[df_configs["valor"] == c, "data"].iloc[0]
        )
        return df

    @classmethod
    def _ordena_rees(
        cls, df: pd.DataFrame, rees_df: List[str]
    ) -> pd.DataFrame:
        """
        Ordena o DataFrame fornecido pela ordem em que os REEs aparecem no
        DataFrame da síntese.
        """
        df["nome_ree"] = pd.Categorical(
            df["nome_ree"], categories=rees_df, ordered=True
        )
        df = df.sort_values(["nome_ree", "data"])
        return df

    @classmethod
    def _adiciona_codigos_rees(
        cls, df: pd.DataFrame, col: str, arq_ree: Ree
    ) -> pd.DataFrame:
        """
        Adiciona a informação do código de cada REE em um DataFrame que
        contenha os nomes.
        """
        df_rees = cls._validate_data(arq_ree.rees, pd.DataFrame)
        # Adiciona informação do código de cada REE
        df["codigo_ree"] = df[col].apply(
            lambda r: df_rees.loc[df_rees["nome"] == r, "codigo"].iloc[0]
        )
        return df

    @classmethod
    def _adiciona_nomes_rees(
        cls, df: pd.DataFrame, col: str, arq_ree: Ree
    ) -> pd.DataFrame:
        """
        Adiciona a informação do nome de cada REE em um DataFrame que
        contenha os códigos.
        """
        df_rees = cls._validate_data(arq_ree.rees, pd.DataFrame)
        # Adiciona informação do nome de cada REE
        df["nome_ree"] = df[col].apply(
            lambda r: df_rees.loc[df_rees["codigo"] == r, "nome"].iloc[0]
        )
        return df

    @classmethod
    def _adiciona_nome_submercados(
        cls, df: pd.DataFrame, col: str, arq_ree: Ree, arq_sistema: Sistema
    ) -> pd.DataFrame:
        """
        Adiciona a informação do nome de cada submercado em um DataFrame que
        contenha a informação do nome de cada REE.
        """
        df_rees = cls._validate_data(arq_ree.rees, pd.DataFrame)
        df_sbms = cls._validate_data(arq_sistema.custo_deficit, pd.DataFrame)
        # Adiciona informação do nome do submercado de cada REE
        df["nome_submercado"] = df[col].apply(
            lambda r: df_sbms.loc[
                df_sbms["codigo_submercado"]
                == df_rees.loc[df_rees["nome"] == r, "submercado"].iloc[0],
                "nome_submercado",
            ].iloc[0]
        )
        return df

    @classmethod
    def _ordena_submercados(
        cls, df: pd.DataFrame, sbms_df: List[str]
    ) -> pd.DataFrame:
        """
        Ordena o DataFrame fornecido pela ordem em que os Submercados aparecem
        no DataFrame da síntese.
        """
        df["nome_submercado"] = pd.Categorical(
            df["nome_submercado"], categories=sbms_df, ordered=True
        )
        df = df.sort_values(["nome_submercado", "data"])
        return df

    @classmethod
    def _datas_inicio_fim_estudo(
        cls, arq_dger: Dger
    ) -> Tuple[datetime, datetime]:
        """
        Obtem as datas do primeiro e último estágios do período
        de estudo do modelo, considerando anos do pós-estudo utilizados para
        simulacao final.
        """
        mes_inicio_estudo = cls._validate_data(arq_dger.mes_inicio_estudo, int)
        ano_inicio_estudo = cls._validate_data(arq_dger.ano_inicio_estudo, int)
        mes_fim_estudo = 12
        n_anos_estudo = cls._validate_data(arq_dger.num_anos_estudo, int)
        n_anos_pos_estudo = cls._validate_data(
            arq_dger.num_anos_pos_sim_final, int
        )
        ano_fim_estudo = (
            ano_inicio_estudo + n_anos_estudo + n_anos_pos_estudo - 1
        )
        data_inicio_estudo = datetime(ano_inicio_estudo, mes_inicio_estudo, 1)
        data_fim_estudo = datetime(ano_fim_estudo, mes_fim_estudo, 1)
        return data_inicio_estudo, data_fim_estudo

    @classmethod
    def _limites_superiores_ear_pmo(
        cls,
        cols_agrupar: List[str],
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        """
        Obtém os limites superiores de energia armazenada a partir do arquivo
        `pmo.dat` e agrupa segundo uma lista de colunas fornecida.
        """
        arq_dger = cls._get_dger(uow)
        # Lê os limites superiores do pmo.dat
        arq_pmo = cls._get_pmo(uow)
        df_earmax = cls._validate_data(
            arq_pmo.energia_armazenada_maxima, pd.DataFrame
        )
        # Adiciona informações do submercado de cada REE
        arq_ree = cls._get_ree(uow)
        arq_sistema = cls._get_sistema(uow)
        df_earmax = cls._adiciona_data_configuracoes(
            df_earmax, arq_pmo, arq_dger
        )
        df_earmax = cls._completa_entradas_meses_pre_estudo(df_earmax, "data")
        df_earmax = cls._adiciona_codigos_rees(df_earmax, "nome_ree", arq_ree)
        df_earmax = cls._adiciona_nome_submercados(
            df_earmax, "nome_ree", arq_ree, arq_sistema
        )
        return df_earmax.groupby(cols_agrupar, as_index=False).sum(
            numeric_only=True
        )

    @classmethod
    def _completa_entradas_meses_pre_estudo(
        cls, df: pd.DataFrame, col: str
    ) -> pd.DataFrame:
        """
        Completa o DataFrame fornecido com valores do primeiro mês do estudo,
        para todos os meses do primeiro ano que antecedem o período de estudo.
        """
        # Obtem os meses do estudo
        datas_estudo: List[datetime] = df[col].unique().tolist()
        primeiro_estagio = min(datas_estudo)
        mes_inicio = primeiro_estagio.month
        if mes_inicio == 1:
            return df

        # Repete o valores pro primeiro estágio, começando de janeiro
        dfs_pre: List[pd.DataFrame] = []
        for m in range(1, mes_inicio):
            df_m = df.loc[df[col] == primeiro_estagio].copy()
            df_m[col] = datetime(primeiro_estagio.year, m, 1)
            dfs_pre.append(df_m)
        df = pd.concat(dfs_pre + [df], ignore_index=True)
        return df

    @classmethod
    def _limites_inferiores_ear_curva(
        cls,
        cols_agrupar: List[str],
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        """
        Obtém os limites inferiores de energia armazenada a partir do arquivo
        `curva.dat`, converte para valores absolutos em MWmes e agrupa
        segundo uma lista de colunas fornecida.
        """
        arq_dger = cls._get_dger(uow)
        # Obtem outros dados necessários
        arq_ree = cls._get_ree(uow)
        arq_sistema = cls._get_sistema(uow)
        # Obtem os limites máximos do pmo.dat
        arq_pmo = cls._get_pmo(uow)
        df_earmax = cls._validate_data(
            arq_pmo.energia_armazenada_maxima, pd.DataFrame
        )
        # Adiciona informações do submercado de cada REE
        arq_ree = cls._get_ree(uow)
        arq_sistema = cls._get_sistema(uow)
        df_earmax = cls._adiciona_data_configuracoes(
            df_earmax, arq_pmo, arq_dger
        )
        df_earmax = cls._adiciona_codigos_rees(df_earmax, "nome_ree", arq_ree)
        df_earmax = cls._completa_entradas_meses_pre_estudo(df_earmax, "data")
        # Lê os limites inferiores do curva.dat
        arq_curva = cls._get_curva(uow)
        df_curva = cls._validate_data(arq_curva.curva_seguranca, pd.DataFrame)
        data_inicio, data_fim = cls._datas_inicio_fim_estudo(arq_dger)
        df_curva = cls._filtra_datas_df(
            df_curva, "data", data_inicio, data_fim
        )
        df_curva = cls._completa_entradas_meses_pre_estudo(df_curva, "data")
        df_curva = cls._adiciona_nomes_rees(df_curva, "codigo_ree", arq_ree)
        df_curva = cls._adiciona_nome_submercados(
            df_curva, "nome_ree", arq_ree, arq_sistema
        )
        # Adiciona os earmax de cada ree no curva e calcula
        # o limite inferior absoluto
        df_curva["earmax"] = df_curva.apply(
            lambda linha: df_earmax.loc[
                (df_earmax["codigo_ree"] == linha["codigo_ree"])
                & (df_earmax["data"] == linha["data"]),
                "valor_MWmes",
            ].iloc[0],
            axis=1,
        )
        df_curva["valor"] = df_curva["valor"] * df_curva["earmax"] / 100.0
        # Agrupa segundo as colunas desejadas e retorna
        return df_curva.groupby(cols_agrupar, as_index=False).sum(
            numeric_only=True
        )

    @classmethod
    def _completa_entidades_faltantes_curva(
        cls,
        df_curva: pd.DataFrame,
        entidades: List[str],
        col: str,
        df_earmax: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Completa o DataFrame fornecido com as entidades que faltam para
        que todas as existentes nos dados de síntese possuam um valor válido
        de limite inferior.
        """
        # Obtem os submercados que não possuem curva e complementa com 0.0
        entidades_curva = df_curva[col].unique().tolist()
        entidades_sem_curva = list(set(entidades).difference(entidades_curva))
        dfs_limites_inferiores = [df_curva]
        for r in entidades_sem_curva:
            df_r = df_curva.loc[df_curva[col] == entidades_curva[0]].copy()
            df_r[col] = r
            df_r["valor"] = 0.0
            df_r["earmax"] = df_earmax.loc[
                df_earmax[col] == r, "valor_MWmes"
            ].to_numpy()
            dfs_limites_inferiores.append(df_r)
        return pd.concat(dfs_limites_inferiores, ignore_index=True)

    @classmethod
    def _earm_earp_ree_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, unidade_sintese: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Energia Armazenada Absoluta (EARM) e Energia
        Armazenada Percentual (EARP) para cada REE.
        """
        # Obtem REEs do DF na ordem em que aparecem
        datas_inicio = df["dataInicio"].unique().tolist()
        rees_df = df["ree"].unique().tolist()
        n_rees = len(rees_df)
        n_estagios = len(datas_inicio)
        n_cenarios = len(df["serie"].unique())
        n_patamares = len(df["patamar"].unique())

        cols_agrupar = ["nome_ree", "data"]
        df_earmax_ree = cls._limites_superiores_ear_pmo(cols_agrupar, uow)
        df_earmax_ree = cls._ordena_rees(df_earmax_ree, rees_df)

        df_curva_ree = cls._limites_inferiores_ear_curva(cols_agrupar, uow)
        df_limite_inferior = cls._completa_entidades_faltantes_curva(
            df_curva_ree, rees_df, "nome_ree", df_earmax_ree
        )
        # Reordena pela ordem que aparece no dataframe da síntese
        df_limite_inferior = cls._ordena_rees(df_limite_inferior, rees_df)

        # Converte para a unidade desejada
        limites_superiores = df_earmax_ree["valor_MWmes"].to_numpy()
        limites_inferiores = df_limite_inferior["valor"].to_numpy()
        if unidade_sintese == "'%'":
            limites_inferiores = (
                limites_inferiores / limites_superiores * 100.0
            )
            limites_superiores = 100.0 * np.ones_like(limites_inferiores)
        # Expande os valores pelo número de cenários
        limites_inferiores_cenarios = cls._expande_dados_para_cenarios(
            limites_inferiores, n_rees, n_estagios, n_cenarios, n_patamares
        )
        limites_superiores_cenarios = cls._expande_dados_para_cenarios(
            limites_superiores, n_rees, n_estagios, n_cenarios, n_patamares
        )
        df["limiteInferior"] = np.round(limites_inferiores_cenarios, 2)
        df["limiteSuperior"] = np.round(limites_superiores_cenarios, 2)
        return df

    @classmethod
    def _earm_earp_sbm_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, unidade_sintese: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Energia Armazenada Absoluta (EARM) para cada SBM.
        """
        # Obtem SBMs do DF na ordem em que aparecem
        datas_inicio = df["dataInicio"].unique().tolist()
        sbms_df = df["submercado"].unique().tolist()
        n_sbms = len(sbms_df)
        n_estagios = len(datas_inicio)
        n_cenarios = len(df["serie"].unique())
        n_patamares = len(df["patamar"].unique())

        cols_agrupar = ["nome_submercado", "data"]
        df_earmax_sbm = cls._limites_superiores_ear_pmo(cols_agrupar, uow)
        df_earmax_sbm = cls._ordena_submercados(df_earmax_sbm, sbms_df)

        df_curva_sbm = cls._limites_inferiores_ear_curva(cols_agrupar, uow)
        df_limite_inferior = cls._completa_entidades_faltantes_curva(
            df_curva_sbm, sbms_df, "nome_submercado", df_earmax_sbm
        )
        # Reordena pela ordem que aparece no dataframe da síntese
        df_limite_inferior = cls._ordena_submercados(
            df_limite_inferior, sbms_df
        )

        # Converte para a unidade desejada
        limites_superiores = df_earmax_sbm["valor_MWmes"].to_numpy()
        limites_inferiores = df_limite_inferior["valor"].to_numpy()
        if unidade_sintese == "'%'":
            limites_inferiores = (
                limites_inferiores / limites_superiores * 100.0
            )
            limites_superiores = 100.0 * np.ones_like(limites_inferiores)
        # Expande os valores pelo número de cenários
        limites_inferiores_cenarios = cls._expande_dados_para_cenarios(
            limites_inferiores, n_sbms, n_estagios, n_cenarios, n_patamares
        )
        limites_superiores_cenarios = cls._expande_dados_para_cenarios(
            limites_superiores, n_sbms, n_estagios, n_cenarios, n_patamares
        )
        df["limiteInferior"] = np.round(limites_inferiores_cenarios, 2)
        df["limiteSuperior"] = np.round(limites_superiores_cenarios, 2)
        return df

    @classmethod
    def _earm_earp_sin_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, unidade_sintese: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Energia Armazenada Absoluta (EARM) e Energia
        Armazeada Percetual (EARP) para o SIN.
        """
        # Obtem REEs do DF na ordem em que aparecem
        datas_inicio = df["dataInicio"].unique().tolist()
        n_estagios = len(datas_inicio)
        n_cenarios = len(df["serie"].unique())
        n_patamares = len(df["patamar"].unique())

        cols_agrupar = ["data"]
        df_earmax_sin = cls._limites_superiores_ear_pmo(cols_agrupar, uow)
        df_curva_sin = cls._limites_inferiores_ear_curva(cols_agrupar, uow)
        # Converte para a unidade desejada
        limites_superiores = df_earmax_sin["valor_MWmes"].to_numpy()
        limites_inferiores = df_curva_sin["valor"].to_numpy()
        if unidade_sintese == "'%'":
            limites_inferiores = (
                limites_inferiores / limites_superiores * 100.0
            )
            limites_superiores = 100.0 * np.ones_like(limites_inferiores)
        # Expande os valores pelo número de cenários
        limites_inferiores_cenarios = cls._expande_dados_para_cenarios(
            limites_inferiores, 1, n_estagios, n_cenarios, n_patamares
        )
        limites_superiores_cenarios = cls._expande_dados_para_cenarios(
            limites_superiores, 1, n_estagios, n_cenarios, n_patamares
        )
        df["limiteInferior"] = np.round(limites_inferiores_cenarios, 2)
        df["limiteSuperior"] = np.round(limites_superiores_cenarios, 2)
        return df

    @classmethod
    def _codigos_usinas_unicas(
        cls,
        df: pd.DataFrame,
        df_hidr: pd.DataFrame,
    ) -> np.ndarray:
        """
        Retorna os códigos únicos das usinas, na ordem em que aparecem
        no DataFrame da síntese em processamento.
        """
        usinas_df = df["usina"].unique().tolist()
        codigos_usinas = []
        for u in usinas_df:
            codigos_usinas.append(
                df_hidr.loc[df_hidr["nome_usina"] == u].index[0]
            )
        return np.array(codigos_usinas)

    @classmethod
    def _codigos_usinas(
        cls,
        df: pd.DataFrame,
        df_hidr: pd.DataFrame,
        n_estagios: int,
        n_patamares: int,
    ) -> np.ndarray:
        """
        Retorna os códigos únicos das usinas, na ordem em que aparecem
        no DataFrame da síntese em processamento.
        """
        usinas_df = df["usina"].unique().tolist()
        codigos_usinas = []
        for u in usinas_df:
            codigos_usinas.append(
                df_hidr.loc[df_hidr["nome_usina"] == u].index[0]
            )
        return np.repeat(codigos_usinas, n_estagios * n_patamares)

    @classmethod
    def _duracoes_patamares_estagios(cls, df: pd.DataFrame) -> np.ndarray:
        """
        Retorna as durações dos patamares em cada estágio, na ordem em que aparecem
        no DataFrame da síntese em processamento.
        """
        ex = df["serie"].unique().tolist()[0]
        return df.loc[df["serie"] == ex, "duracaoPatamar"].to_numpy()

    @classmethod
    def _dado_cadastral_hidr_uhes(
        cls,
        df_hidr: pd.DataFrame,
        codigos_usinas: np.ndarray,
        coluna: str,
    ) -> np.ndarray:
        """
        Constroi um array com um dado cadastral `coluna` das usinas
        hidrelétricas extraído do `df_hidr`, na mesma ordem em que foram
        fornecidos os códigos `codigos_usinas`.
        """
        dados = np.zeros_like(codigos_usinas, dtype=np.float64)
        for i, u in enumerate(codigos_usinas):
            dados[i] = df_hidr.at[u, coluna]
        return dados

    @classmethod
    def _converte_volume_percentual_hm3(
        cls,
        volume_minimo_atual: float,
        volume_maximo_atual: float,
        volume_modif: float,
    ) -> float:
        """
        Realiza a conversão de um volume fornecido em percentual para
        um volume em hm3, considerando os limites inferior e superior.
        """
        return volume_minimo_atual + volume_modif / 100.0 * (
            volume_maximo_atual - volume_minimo_atual
        )

    @classmethod
    def _converte_volume_hm3_percentual(
        cls,
        volume_minimo_atual: float,
        volume_maximo_atual: float,
        volume_modif: float,
    ) -> float:
        """
        Realiza a conversão de um volume fornecido em hm3 para
        um volume em percentual, considerando os limites inferior e superior.
        """
        vol_c = (
            (volume_modif - volume_minimo_atual)
            * 100
            / (volume_maximo_atual - volume_minimo_atual)
        )
        return vol_c if not np.isnan(vol_c) else 0.0

    @classmethod
    def _converte_volume_vazao(
        cls,
        duracao_patamar_horas: float,
        volume_modif: float,
    ) -> float:
        """
        Realiza a conversão de um volume fornecido em hm3 para
        uma vazão em m3/s, considerando um período de horas para cálculo.
        """
        return (
            volume_modif
            * (cls.STAGE_DURATION_HOURS * cls.HM3_M3S_FACTOR)
            / duracao_patamar_horas
        )

    @classmethod
    def _converte_vazao_volume(
        cls,
        duracao_patamar_horas: float,
        vazao_modif: float,
    ) -> float:
        """
        Realiza a conversão de uma vazão em m3/s para um volume fornecido
        em hm3, considerando um período de horas para cálculo.
        """
        return (
            vazao_modif
            * duracao_patamar_horas
            / (cls.STAGE_DURATION_HOURS * cls.HM3_M3S_FACTOR)
        )

    @classmethod
    def _converte_unidade_cadastro_unidade_sintese(
        cls,
        valor_cadastro: Optional[float],
        unidade_cadastro: str,
        unidade_sintese: str,
        *args,
        **kwargs,
    ) -> Optional[float]:
        """
        Converte a unidade de um valor de alteração cadastral fornecido
        em um cadastro para a unidade de síntese da variável.

        Dependendo da conversão que é feita, são esperados argumentos adicionais:

        - Para conversão de volume percentual para hm3, espera-se os limites
            inferior e superior do volume cadastral (`limite_inferior_cadastral`
             e `limite_superior_cadastral`).
        - Para conversão de volume em hm3 para percentual, espera-se os limites
            inferior e superior do volume cadastral (`limite_inferior_cadastral`
             e `limite_superior_cadastral`).
        - Para conversão de vazão em m3/s para hm3, espera-se a duração do
            patamar em horas (`duracao_patamar_horas`).
        - Para conversão de volume em hm3 para vazão em m3/s, espera-se a duração
            do patamar em horas (`duracao_patamar_horas`).

        """
        if valor_cadastro is None:
            return valor_cadastro
        limite_inferior = kwargs.get("limite_inferior_cadastral", 0)
        limite_superior = kwargs.get("limite_superior_cadastral", float("inf"))
        duracao_patamar_horas = kwargs.get(
            "duracao_patamar_horas", cls.STAGE_DURATION_HOURS
        )
        if unidade_cadastro == "'%'" and unidade_sintese == "hm3":
            return cls._converte_volume_percentual_hm3(
                limite_inferior,
                limite_superior,
                valor_cadastro,
            )
        elif unidade_cadastro == "'h'" and unidade_sintese == "'%'":
            return cls._converte_volume_hm3_percentual(
                limite_inferior,
                limite_superior,
                valor_cadastro,
            )
        elif unidade_cadastro == "hm3" and unidade_sintese == "m3/s":
            return cls._converte_volume_vazao(
                duracao_patamar_horas, valor_cadastro
            )
        elif unidade_cadastro == "m3/s" and unidade_sintese == "hm3":
            return cls._converte_vazao_volume(
                duracao_patamar_horas, valor_cadastro
            )
        else:
            return valor_cadastro

    @classmethod
    def _converte_unidades_cadastro_unidades_sintese(
        cls,
        df: pd.DataFrame,
        dados_cadastrais: np.ndarray,
        unidades_cadastrais: np.ndarray,
        unidade_sintese: str,
        *args,
        **kwargs,
    ) -> np.ndarray:
        duracoes_patamares_horas = cls._duracoes_patamares_estagios(df)
        limites_inferiores = kwargs.get("limites_inferiores", None)
        limites_superiores = kwargs.get("limites_superiores", None)
        for i in range(len(dados_cadastrais)):
            dados_cadastrais[i] = (
                cls._converte_unidade_cadastro_unidade_sintese(
                    dados_cadastrais[i],
                    unidades_cadastrais[i],
                    unidade_sintese,
                    duracao_patamar_horas=duracoes_patamares_horas[i],
                    limite_inferior_cadastral=(
                        limites_inferiores[i]
                        if limites_inferiores is not None
                        else 0.0
                    ),
                    limite_superior_cadastral=(
                        limites_superiores[i]
                        if limites_superiores is not None
                        else float("inf")
                    ),
                )
            )
        return dados_cadastrais

    @classmethod
    def _extrai_dados_modif_uhe(
        cls, registro: Register
    ) -> Tuple[Optional[float], Optional[str]]:
        """
        Extrai um dado de um registro do modif.dat com a sua unidade.
        """
        if isinstance(registro, VOLMIN):
            return registro.volume, registro.unidade
        elif isinstance(registro, VMINT):
            return registro.volume, registro.unidade
        elif isinstance(registro, VOLMAX):
            return registro.volume, registro.unidade
        elif isinstance(registro, VMAXT):
            return registro.volume, registro.unidade
        elif isinstance(registro, VAZMIN):
            return registro.vazao, "m3/s"
        elif isinstance(registro, VAZMINT):
            return registro.vazao, "m3/s"
        elif isinstance(registro, VAZMAXT):
            return registro.vazao, "m3/s"
        elif isinstance(registro, TURBMINT):
            return registro.turbinamento, "m3/s"
        elif isinstance(registro, TURBMAXT):
            return registro.turbinamento, "m3/s"
        return None, None

    @classmethod
    def _modificacoes_cadastro_uhes(
        cls,
        dados_cadastrais: np.ndarray,
        unidades: np.ndarray,
        arq_modif: Modif,
        tipo_registro_modif: Type[Union[VAZMIN, VOLMIN, VOLMAX]],
        codigos_usinas: np.ndarray,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Realiza a extração de modificações cadastrais de volumes de usinas
        hidrelétricas a partir do arquivo modif.dat, atualizando os cadastros
        conforme as declarações de modificações são encontradas.
        """
        for i, u in enumerate(codigos_usinas):
            modificacoes_usina = arq_modif.modificacoes_usina(u)
            if modificacoes_usina is not None:
                regs_usina = [
                    r
                    for r in modificacoes_usina
                    if isinstance(r, tipo_registro_modif)
                ]
                if len(regs_usina) > 0:
                    registro = regs_usina[-1]
                    valor, unidade = cls._extrai_dados_modif_uhe(registro)
                    if valor is not None:
                        dados_cadastrais[i] = valor
                    if unidade is not None:
                        unidades[i] = unidade.lower()
        return dados_cadastrais, unidades

    @classmethod
    def _modificacoes_cadastro_temporais_uhes(
        cls,
        dados_cadastrais: np.ndarray,
        unidades: np.ndarray,
        datas: List[datetime],
        n_estagios: int,
        n_patamares: int,
        arq_modif: Modif,
        tipo_registro_modif: Type[
            Union[VMINT, VMAXT, VAZMINT, TURBMINT, TURBMAXT]
        ],
        codigos_usinas: np.ndarray,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Realiza a extração de modificações cadastrais de volumes de usinas
        hidrelétricas a partir do arquivo modif.dat, considerando também
        modificações cadastrais com relação temporal. Os cadastros são
        expandidos para um valor por usina e estágio e são atualizados
        conforme as declarações de modificações são encontradas.
        """
        dados_cadastrais_modificados = dados_cadastrais.copy()
        unidades_modificadas = unidades.copy()
        for i, u in enumerate(codigos_usinas):
            modificacoes_usina = arq_modif.modificacoes_usina(u)
            i_i = i * n_estagios * n_patamares
            i_f = i_i + n_estagios * n_patamares
            if modificacoes_usina is not None:
                registros_usina = [
                    r
                    for r in modificacoes_usina
                    if isinstance(r, tipo_registro_modif)
                ]
                for reg in registros_usina:
                    idx_data = datas.index(reg.data_inicio)
                    valor, unidade = cls._extrai_dados_modif_uhe(reg)
                    dados_cadastrais_modificados[i_i + idx_data : i_f] = valor
                    unidades_modificadas[i_i + idx_data : i_f] = unidade
        return dados_cadastrais_modificados, unidades_modificadas

    @classmethod
    def _expande_dados_para_cenarios(
        cls,
        dados_cadastrais: np.ndarray,
        n_entidades: int,
        n_estagios: int,
        n_cenarios: int,
        n_patamares: int,
    ):
        """
        Expande os dados cadastrais para cada cenário, mantendo a ordem dos
        patamares internamente.
        """
        dados_cadastrais_cenarios = np.zeros(
            (len(dados_cadastrais) * n_cenarios,), dtype=np.float64
        )
        for i in range(n_entidades):
            for j in range(n_estagios):
                i_i = i * n_estagios * n_patamares + j * n_patamares
                i_f = i_i + n_patamares
                dados_cadastrais_cenarios[
                    i_i * n_cenarios : i_f * n_cenarios
                ] = np.tile(dados_cadastrais[i_i:i_f], n_cenarios)
        return dados_cadastrais_cenarios

    @classmethod
    def _agrega_variaveis_uhe(
        cls, df: pd.DataFrame, col_grp: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de UHE
        para uma síntese de REEs, SBMs ou para o SIN. A agregação
        tem como requisito que as variáveis fornecidas sejam em unidades
        cuja agregação seja possível apenas pela soma.
        """
        cols_grp_validas = ["usina", "ree", "submercado", "sin"]

        if col_grp is None:
            return df

        if col_grp == "sin":
            df["group"] = 1
        elif col_grp in cols_grp_validas:
            df["group"] = df[col_grp]
        else:
            raise RuntimeError(f"Coluna de agrupamento inválida: {col_grp}")

        cols_group = ["group"] + [
            c
            for c in df.columns
            if c in cls.IDENTIFICATION_COLUMNS and c not in cols_grp_validas
        ]
        df = df.astype({"serie": int})
        df_group = df.groupby(cols_group).sum(numeric_only=True).reset_index()
        if col_grp == "sin" or col_grp is None:
            df_group = df_group.drop(columns=["group"])
        else:
            df_group = df_group.rename(columns={"group": col_grp})
        return df_group

    @classmethod
    def _agrega_variaveis_uhe_volume_vazao(
        cls, df: pd.DataFrame, col_grp: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de UHE
        para uma síntese de REEs, SBMs ou para o SIN, convertendo
        a unidade de volume para vazão.

        É usada em casos em que osdados são fornecidos em unidade
        de volume, mas a síntese desejada é em unidade de vazão.
        """
        cols_grp_validas = ["usina", "ree", "submercado", "sin"]

        if col_grp is None:
            return df

        if col_grp == "sin":
            df["group"] = 1
        elif col_grp in cols_grp_validas:
            df["group"] = df[col_grp]
        else:
            raise RuntimeError(f"Coluna de agrupamento inválida: {col_grp}")

        cols_group = ["group"] + [
            c
            for c in df.columns
            if c in cls.IDENTIFICATION_COLUMNS and c not in cols_grp_validas
        ]
        df = df.astype({"serie": int})
        df_group = df.groupby(cols_group).sum(numeric_only=True).reset_index()
        if col_grp == "sin" or col_grp is None:
            df_group = df_group.drop(columns=["group"])
        else:
            df_group = df_group.rename(columns={"group": col_grp})

        # Converte volume para vazão
        for c in ["valor", "limiteInferior", "limiteSuperior"]:
            df_group[c] = (
                df_group[c]
                * (cls.STAGE_DURATION_HOURS * cls.HM3_M3S_FACTOR)
                / df_group["duracaoPatamar"]
            )

        return df_group

    @classmethod
    def _agrega_variaveis_uhe_volume_absoluto_percentual(
        cls, df: pd.DataFrame, col_grp: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de UHE
        para uma síntese de REEs, SBMs ou para o SIN, convertendo
        a unidade de volume de absoluto para percentual do útil.

        É usada em casos em que os dados são fornecidos em unidade
        de volume total (hm3), mas a síntese desejada é em percentual.
        """
        cols_grp_validas = ["usina", "ree", "submercado", "sin"]

        if col_grp is None:
            return df

        if col_grp == "sin":
            df["group"] = 1
        elif col_grp in cols_grp_validas:
            df["group"] = df[col_grp]
        else:
            raise RuntimeError(f"Coluna de agrupamento inválida: {col_grp}")

        cols_group = ["group"] + [
            c
            for c in df.columns
            if c in cls.IDENTIFICATION_COLUMNS and c not in cols_grp_validas
        ]
        df = df.astype({"serie": int})
        df_group = df.groupby(cols_group).sum(numeric_only=True).reset_index()
        if col_grp:
            df_group = df_group.rename(columns={"group": col_grp})
        else:
            df_group = df_group.drop(columns=["group"])

        # Obter dados cadastrais para cada UHE, desconsiderando modif.dat

        # Converte volume de absoluto para percentual
        df_group["valor"] -= df_group["limiteInferior"]
        df_group["valor_util"] = (
            df_group["limiteSuperior"] - df_group["limiteInferior"]
        )
        df_group["valor"] = 100 * df_group["valor"] / df_group["valor_util"]
        df_group["valor"] = df_group["valor"].fillna(0.0)
        df_group["limiteInferior"] = 0.0
        df_group["limiteSuperior"] = 100.0

        return df_group

    @classmethod
    def _varm_varp_uhe_bounds(
        cls,
        df: pd.DataFrame,
        uow: AbstractUnitOfWork,
        unidade_sintese: str,
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Armazenado Absoluto (VARM) e Volume
        Armazenado Percentual (VARP) para cada UHE.
        """
        datas_inicio = df["dataInicio"].unique().tolist()
        n_usinas = len(df["usina"].unique())
        n_estagios = len(datas_inicio)
        n_cenarios = len(df["serie"].unique())
        n_patamares = len(df["patamar"].unique())
        # Lê hidr.dat
        arq_hidr = cls._get_hidr(uow)
        df_hidr = cls._validate_data(arq_hidr.cadastro, pd.DataFrame)
        # Lê modif.dat
        arq_modif = cls._get_modif(uow)

        # Obtem usinas do df na ordem em que aparecem e durações dos patamares
        codigos_usinas = cls._codigos_usinas_unicas(df, df_hidr)

        def _modificacoes_cadastro_uhes(
            df_hidr: pd.DataFrame,
            arq_modif: Modif,
            codigos_usinas: np.ndarray,
        ) -> pd.DataFrame:
            """
            Realiza a extração de modificações cadastrais de volumes de usinas
            hidrelétricas a partir do arquivo modif.dat, atualizando os cadastros
            conforme as declarações de modificações são encontradas.
            """
            for u in codigos_usinas:
                modificacoes_usina = arq_modif.modificacoes_usina(u)
                if modificacoes_usina is not None:
                    regs_volmin = [
                        r for r in modificacoes_usina if isinstance(r, VOLMIN)
                    ]
                    if len(regs_volmin) > 0:
                        reg_volmin = regs_volmin[-1]
                        if reg_volmin.unidade == "'%'":
                            reg_volmin.volume = (
                                cls._converte_volume_percentual_hm3(
                                    df_hidr.at[u, "volume_minimo"],
                                    df_hidr.at[u, "volume_maximo"],
                                    reg_volmin.volume,
                                )
                            )
                        df_hidr.at[u, "volume_minimo"] = reg_volmin.volume
                    regs_volmax = [
                        r for r in modificacoes_usina if isinstance(r, VOLMAX)
                    ]
                    if len(regs_volmax) > 0:
                        reg_volmax = regs_volmax[-1]
                        if reg_volmax.unidade == "'%'":
                            reg_volmax.volume = (
                                cls._converte_volume_percentual_hm3(
                                    df_hidr.at[u, "volume_minimo"],
                                    df_hidr.at[u, "volume_maximo"],
                                    reg_volmax.volume,
                                )
                            )
                        df_hidr.at[u, "volume_maximo"] = reg_volmax.volume
            return df_hidr

        # Modifica o hidr.dat considerando apenas as UHEs do caso
        df_hidr = _modificacoes_cadastro_uhes(
            df_hidr, arq_modif, codigos_usinas
        )

        # Inicializa limites com valores do hidr.dat
        limites_inferiores_cadastrais = cls._dado_cadastral_hidr_uhes(
            df_hidr, codigos_usinas, "volume_minimo"
        )
        limites_superiores_cadastrais = cls._dado_cadastral_hidr_uhes(
            df_hidr, codigos_usinas, "volume_maximo"
        )

        # Repete para todos os estagios e patamares
        limites_inferiores_cadastrais = np.repeat(
            limites_inferiores_cadastrais, n_estagios * n_patamares
        )
        unidades_limites_inferiores_cadastrais = np.array(
            ["'h'"] * len(limites_inferiores_cadastrais)
        )
        limites_superiores_cadastrais = np.repeat(
            limites_superiores_cadastrais, n_estagios * n_patamares
        )
        unidades_limites_superiores_cadastrais = np.array(
            ["'h'"] * len(limites_superiores_cadastrais)
        )

        # Atualiza limites com valores de VMINT e VMAXT do modif.dat,
        # só no caso de ser VARM. Caso seja VARP, uma usina com certo
        # VMAXT especificando volume de espera de 5% deve necessariamente
        # ter seu limite superior em 95%, pois o % do Volume Util
        # é sempre calculado em relação ao dado cadastral.
        limites_inferiores, unidades_limites_inferiores = (
            cls._modificacoes_cadastro_temporais_uhes(
                limites_inferiores_cadastrais,
                unidades_limites_inferiores_cadastrais,
                datas_inicio,
                n_estagios,
                n_patamares,
                arq_modif,
                VMINT,
                codigos_usinas,
            )
        )
        limites_superiores, unidades_limites_superiores = (
            cls._modificacoes_cadastro_temporais_uhes(
                limites_superiores_cadastrais,
                unidades_limites_superiores_cadastrais,
                datas_inicio,
                n_estagios,
                n_patamares,
                arq_modif,
                VMAXT,
                codigos_usinas,
            )
        )

        # Converte limites para a unidade de síntese
        limites_inferiores = cls._converte_unidades_cadastro_unidades_sintese(
            df,
            limites_inferiores,
            unidades_limites_inferiores,
            unidade_sintese,
            limites_inferiores=limites_inferiores_cadastrais,
            limites_superiores=limites_superiores_cadastrais,
        )
        limites_superiores = cls._converte_unidades_cadastro_unidades_sintese(
            df,
            limites_superiores,
            unidades_limites_superiores,
            unidade_sintese,
            limites_inferiores=limites_inferiores_cadastrais,
            limites_superiores=limites_superiores_cadastrais,
        )

        # Constroi limites para cada estágio e cenario
        limites_inferiores_cenarios = cls._expande_dados_para_cenarios(
            limites_inferiores, n_usinas, n_estagios, n_cenarios, n_patamares
        )
        limites_superiores_cenarios = cls._expande_dados_para_cenarios(
            limites_superiores, n_usinas, n_estagios, n_cenarios, n_patamares
        )
        # Adiciona ao df e retorna
        df["limiteInferior"] = np.round(limites_inferiores_cenarios, 2)
        df["limiteSuperior"] = np.round(limites_superiores_cenarios, 2)
        df["valor"] = np.round(df["valor"], 2)
        # Específico do VARM: soma o limite inferior cadastral, pois o representado
        # nos arquivos de saída é em volume útil (hm3).
        if unidade_sintese == "'h'":
            df["valor"] += limites_inferiores_cenarios

        return df

    @classmethod
    def _qdef_vdef_uhe_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, unidade_sintese: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Defluente (VDEF) e Vazão Defluente (QDEF)
        para cada UHE.
        """
        datas_inicio = df["dataInicio"].unique().tolist()
        n_usinas = len(df["usina"].unique())
        n_estagios = len(datas_inicio)
        n_cenarios = len(df["serie"].unique())
        n_patamares = len(df["patamar"].unique())
        # Lê hidr.dat
        arq_hidr = cls._get_hidr(uow)
        df_hidr = cls._validate_data(arq_hidr.cadastro, pd.DataFrame)
        # Lê modif.dat
        arq_modif = cls._get_modif(uow)

        # Obtem usinas do df na ordem em que aparecem e durações dos patamares
        codigos_usinas = cls._codigos_usinas_unicas(df, df_hidr)

        def _modificacoes_cadastro_uhes(
            df_hidr: pd.DataFrame,
            arq_modif: Modif,
            codigos_usinas: np.ndarray,
        ) -> pd.DataFrame:
            """
            Realiza a extração de modificações cadastrais de volumes de usinas
            hidrelétricas a partir do arquivo modif.dat, atualizando os cadastros
            conforme as declarações de modificações são encontradas.
            """
            for u in codigos_usinas:
                modificacoes_usina = arq_modif.modificacoes_usina(u)
                if modificacoes_usina is not None:
                    regs_vazmin = [
                        r for r in modificacoes_usina if isinstance(r, VAZMIN)
                    ]
                    if len(regs_vazmin) > 0:
                        reg_vazmin = regs_vazmin[-1]
                        df_hidr.at[u, "vazao_minima_historica"] = (
                            reg_vazmin.vazao
                        )
            return df_hidr

        # Modifica o hidr.dat considerando apenas as UHEs do caso
        df_hidr = _modificacoes_cadastro_uhes(
            df_hidr, arq_modif, codigos_usinas
        )
        # Inicializa limites com valores do hidr.dat modificado
        limites_inferiores = cls._dado_cadastral_hidr_uhes(
            df_hidr, codigos_usinas, "vazao_minima_historica"
        )

        # Repete para todos os estagios e patamares
        limites_inferiores = np.repeat(
            limites_inferiores, n_estagios * n_patamares
        )
        unidades_limites_inferiores = np.array(
            ["m3/s"] * len(limites_inferiores)
        )
        limites_superiores = np.ones_like(limites_inferiores) * float("inf")
        unidades_limites_superiores = np.array(
            ["m3/s"] * len(limites_superiores)
        )

        # Atualiza limites com valores de VAZMINT do modif.dat
        limites_inferiores, unidades_limites_inferiores = (
            cls._modificacoes_cadastro_temporais_uhes(
                limites_inferiores,
                unidades_limites_inferiores,
                datas_inicio,
                n_estagios,
                n_patamares,
                arq_modif,
                VAZMINT,
                codigos_usinas,
            )
        )
        # Atualiza limites com valores de VAZMAXT do modif.dat
        limites_superiores, unidades_limites_superiores = (
            cls._modificacoes_cadastro_temporais_uhes(
                limites_superiores,
                unidades_limites_superiores,
                datas_inicio,
                n_estagios,
                n_patamares,
                arq_modif,
                VAZMAXT,
                codigos_usinas,
            )
        )
        # Converte limites para a unidade de síntese
        limites_inferiores = cls._converte_unidades_cadastro_unidades_sintese(
            df,
            limites_inferiores,
            unidades_limites_inferiores,
            unidade_sintese,
        )
        limites_superiores = cls._converte_unidades_cadastro_unidades_sintese(
            df,
            limites_superiores,
            unidades_limites_superiores,
            unidade_sintese,
        )
        # Constroi limites para cada estágio e cenario
        limites_inferiores_cenarios = cls._expande_dados_para_cenarios(
            limites_inferiores, n_usinas, n_estagios, n_cenarios, n_patamares
        )
        limites_superiores_cenarios = cls._expande_dados_para_cenarios(
            limites_superiores, n_usinas, n_estagios, n_cenarios, n_patamares
        )
        # Adiciona ao df e retorna
        df["valor"] = np.round(df["valor"], 2)
        df["limiteInferior"] = np.round(limites_inferiores_cenarios, 2)
        df["limiteSuperior"] = np.round(limites_superiores_cenarios, 2)
        return df

    @classmethod
    def _qtur_vtur_uhe_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, unidade_sintese: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Turbinado (VTUR) e Vazão Turbinada (QTUR)
        para cada UHE.

        TODO - considerar exph.dat para limites de turbinamento com
        entradas de máquinas no meio do horizonte.

        """
        datas_inicio = df["dataInicio"].unique().tolist()
        n_usinas = len(df["usina"].unique())
        n_estagios = len(datas_inicio)
        n_cenarios = len(df["serie"].unique())
        n_patamares = len(df["patamar"].unique())
        # Lê hidr.dat
        arq_hidr = cls._get_hidr(uow)
        df_hidr = cls._validate_data(arq_hidr.cadastro, pd.DataFrame)
        # Lê modif.dat
        arq_modif = cls._get_modif(uow)

        def _modificacoes_cadastro_uhes(
            df_hidr: pd.DataFrame,
            arq_modif: Modif,
            codigos_usinas: np.ndarray,
        ) -> pd.DataFrame:
            """
            Realiza a extração de modificações cadastrais de volumes de usinas
            hidrelétricas a partir do arquivo modif.dat, atualizando os cadastros
            conforme as declarações de modificações são encontradas.
            """
            for u in codigos_usinas:
                modificacoes_usina = arq_modif.modificacoes_usina(u)
                if modificacoes_usina is not None:
                    regs_numcnj = [
                        r for r in modificacoes_usina if isinstance(r, NUMCNJ)
                    ]
                    if len(regs_numcnj) > 0:
                        reg_numcnj = regs_numcnj[-1]
                        df_hidr.at[u, "numero_conjuntos_maquinas"] = (
                            reg_numcnj.numero
                        )
                    regs_nummaq = [
                        r for r in modificacoes_usina if isinstance(r, NUMMAQ)
                    ]
                    for reg_nummaq in regs_nummaq:
                        df_hidr.at[
                            u, f"maquinas_conjunto_{reg_nummaq.conjunto}"
                        ] = reg_nummaq.numero_maquinas
                    df_hidr = df_hidr.copy()

            return df_hidr

        # Obtem usinas do df na ordem em que aparecem e durações dos patamares
        codigos_usinas = cls._codigos_usinas_unicas(df, df_hidr)

        # Modifica o hidr.dat considerando apenas as UHEs do caso
        df_hidr = _modificacoes_cadastro_uhes(
            df_hidr, arq_modif, codigos_usinas
        )

        def calcula_engolimento_uhe(
            codigo_usina: int, df_hidr: pd.DataFrame
        ) -> float:
            n_conjuntos = df_hidr.at[codigo_usina, "numero_conjuntos_maquinas"]
            colunas_cadastro_maquinas = [
                f"maquinas_conjunto_{i}" for i in range(1, n_conjuntos + 1)
            ]
            colunas_cadastro_engolimento = [
                f"vazao_nominal_conjunto_{i}"
                for i in range(1, n_conjuntos + 1)
            ]
            numero_maquinas = (
                df_hidr.loc[codigo_usina, colunas_cadastro_maquinas]
                .to_numpy()
                .flatten()
            )
            vazoes_maquinas = (
                df_hidr.loc[codigo_usina, colunas_cadastro_engolimento]
                .to_numpy()
                .flatten()
            )
            return np.sum(numero_maquinas * vazoes_maquinas)

        # Inicializa limites com valores do hidr.dat modificado
        limites_superiores = np.array(
            [calcula_engolimento_uhe(c, df_hidr) for c in codigos_usinas]
        )

        # Repete para todos os estagios e patamares
        limites_superiores = np.repeat(
            limites_superiores, n_estagios * n_patamares
        )
        unidades_limites_superiores = np.array(
            ["m3/s"] * len(limites_superiores)
        )
        limites_inferiores = np.zeros_like(limites_superiores)
        unidades_limites_inferiores = np.array(
            ["m3/s"] * len(limites_inferiores)
        )

        # Atualiza limites superiores com valores de TURBMMAXT do modif.dat
        limites_superiores, unidades_limites_superiores = (
            cls._modificacoes_cadastro_temporais_uhes(
                limites_superiores,
                unidades_limites_superiores,
                datas_inicio,
                n_estagios,
                n_patamares,
                arq_modif,
                TURBMAXT,
                codigos_usinas,
            )
        )
        # Atualiza limites inferiores com valores de TURBMINT do modif.dat
        limites_inferiores, unidades_limites_inferiores = (
            cls._modificacoes_cadastro_temporais_uhes(
                limites_inferiores,
                unidades_limites_inferiores,
                datas_inicio,
                n_estagios,
                n_patamares,
                arq_modif,
                TURBMINT,
                codigos_usinas,
            )
        )
        # Converte limites para a unidade de síntese
        limites_superiores = cls._converte_unidades_cadastro_unidades_sintese(
            df,
            limites_superiores,
            unidades_limites_superiores,
            unidade_sintese,
        )
        limites_inferiores = cls._converte_unidades_cadastro_unidades_sintese(
            df,
            limites_inferiores,
            unidades_limites_inferiores,
            unidade_sintese,
        )
        # Constroi limites para cada estágio e cenario
        limites_superiores_cenarios = cls._expande_dados_para_cenarios(
            limites_superiores, n_usinas, n_estagios, n_cenarios, n_patamares
        )
        limites_inferiores_cenarios = cls._expande_dados_para_cenarios(
            limites_inferiores, n_usinas, n_estagios, n_cenarios, n_patamares
        )
        # Adiciona ao df e retorna
        df["valor"] = np.round(df["valor"], 2)
        df["limiteInferior"] = np.round(limites_inferiores_cenarios, 2)
        df["limiteSuperior"] = np.round(limites_superiores_cenarios, 2)
        return df

    @classmethod
    def _qver_vver_uhe_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, unidade_sintese: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Vertido (VVER) e Vazão Vertida (QVER)
        para cada UHE.
        """
        df["valor"] = np.round(df["valor"], 2)
        df["limiteInferior"] = 0.0
        df["limiteSuperior"] = float("inf")
        return df

    @classmethod
    def _qafl_vafl_uhe_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, unidade_sintese: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Afluente (VAFL) e Vazão Afluente (QAFL)
        para cada UHE.
        """
        df["valor"] = np.round(df["valor"], 2)
        df["limiteInferior"] = 0.0
        df["limiteSuperior"] = float("inf")
        return df

    @classmethod
    def _qinc_vinc_uhe_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, unidade_sintese: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Incremental (VINC) e Vazão Incremental (QINC)
        para cada UHE.
        """
        df["valor"] = np.round(df["valor"], 2)
        df["limiteInferior"] = -float("inf")
        df["limiteSuperior"] = float("inf")
        return df

    @classmethod
    def _qret_vret_uhe_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, unidade_sintese: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Retirado (VRET) e Vazão Retirada (QRET)
        para cada UHE.
        """
        df["valor"] = np.round(df["valor"], 2)
        df["limiteInferior"] = -float("inf")
        df["limiteSuperior"] = float("inf")
        return df

    @classmethod
    def _qdes_vdes_uhe_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, unidade_sintese: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Desviado (VDES) e Vazão Desviada (QDES)
        para cada UHE.
        """
        # TODO - Procurar limite superior no modif.dat
        df["limiteInferior"] = 0.0
        df["limiteSuperior"] = float("inf")
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
            ["nome_submercado", "codigo_submercado"]
        )
        mapa_nomes_submercados = {
            str(codigo): nome
            for codigo, nome in zip(
                df_submercados["codigo_submercado"],
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
        datas_inicio = df["dataInicio"].unique().tolist()
        n_estagios = len(datas_inicio)
        n_cenarios = len(df["serie"].unique())
        n_patamares = len(df["patamar"].unique())
        # Filtra os pares de submercados de limites dentre os
        # que existem no df
        df["par_sbm"] = df["submercadoDe"] + "-" + df["submercadoPara"]
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

        pares_sbm_df = df["par_sbm"].unique().tolist()
        pares_sbm_limites_r = df_limites_pat["par_sbm_r"].unique().tolist()
        pares_sbm_limites = df_limites_pat["par_sbm"].unique().tolist()

        # Inicializa limites com valores default
        df["limiteInferior"] = -float("inf")
        df["limiteSuperior"] = float("inf")
        # Aplica os limites, considerando o par de submercados
        # e o sentido reverso como sinal negativo
        for p in pares_sbm_df:
            if p in pares_sbm_limites_r:
                lims = -cls._expande_dados_para_cenarios(
                    df_limites_pat.loc[
                        df_limites_pat["par_sbm_r"] == p, "valor"
                    ].to_numpy(),
                    1,
                    n_estagios,
                    n_cenarios,
                    n_patamares,
                )
                df.loc[df["par_sbm"] == p, "limiteInferior"] = lims
            if p in pares_sbm_limites:
                lims = cls._expande_dados_para_cenarios(
                    df_limites_pat.loc[
                        df_limites_pat["par_sbm"] == p, "valor"
                    ].to_numpy(),
                    1,
                    n_estagios,
                    n_cenarios,
                    n_patamares,
                )
                df.loc[df["par_sbm"] == p, "limiteSuperior"] = lims

        df["limiteInferior"] = np.round(df["limiteInferior"], 1)
        df["limiteSuperior"] = np.round(df["limiteSuperior"], 1)

        return df.drop(columns=["par_sbm"])

    @classmethod
    def _int_sbp_bounds(
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
        cls, df: pd.DataFrame, arq_conft: Conft, arq_sistema: Sistema
    ) -> pd.DataFrame:
        df_usinas = cls._validate_data(arq_conft.usinas, pd.DataFrame)
        df_submercados = cls._validate_data(
            arq_sistema.custo_deficit, pd.DataFrame
        )
        # Adiciona informação do submercado de cada UTE
        df["submercado"] = df["codigo_usina"].apply(
            lambda c: df_usinas.loc[
                df_usinas["codigo_usina"] == c, "submercado"
            ].iloc[0]
        )
        df["submercado"] = df["submercado"].apply(
            lambda c: df_submercados.loc[
                df_submercados["codigo_submercado"] == c, "nome_submercado"
            ].iloc[0]
        )
        df = df.rename(columns={"nome_usina": "usina"})
        return df.drop(columns=["codigo_usina"])

    @classmethod
    def _agrega_variaveis_limites_ute(
        cls,
        df: pd.DataFrame,
        col_grp: Optional[str] = None,
        ordem_sintese: Optional[list] = None,
    ) -> pd.DataFrame:
        """
        Realiza a agregação de limites de geração de usinas
        térmicas.
        """
        cols_grp_validas = ["usina", "ree", "submercado", "sin"]

        if col_grp is None:
            return df

        if col_grp == "sin":
            df["group"] = 1
        elif col_grp in cols_grp_validas:
            df["group"] = df[col_grp]
        else:
            raise RuntimeError(f"Coluna de agrupamento inválida: {col_grp}")

        cols_group = ["group", "data"]
        df_group = df.groupby(cols_group).sum(numeric_only=True).reset_index()

        if ordem_sintese:
            df_group["group"] = pd.Categorical(
                df_group["group"], categories=ordem_sintese, ordered=True
            )
        df_group = df_group.sort_values(["group", "data"])

        df_group["group"] = df_group["group"].astype(str)

        if col_grp:
            df_group = df_group.rename(columns={"group": col_grp})
        else:
            df_group = df_group.drop(columns=["group"])
        return df_group

    @classmethod
    def _expande_dados_cenarios_gter(
        cls,
        df: pd.DataFrame,
        df_gtmin: pd.DataFrame,
        df_gtmax: pd.DataFrame,
        col_grp: str,
        ordem_sintese: list,
    ) -> pd.DataFrame:
        """
        Expande os dados da síntese de geração térmica
        para o número de cenários e patamares existentes.

        É um wrapper para a chamada da _expande_dados_para_cenarios
        pois existe o comportamento particular dos limites de geração
        serem fornecidos apenas em MWmed por estágio, e por
        questões de desempenho este são repetidos
        (n_patamares * n_cenarios) vezes como se existisse apenas 1
        patamar e a conversão para MWmes é feita posterioremente.
        """
        datas_inicio = df["dataInicio"].unique().tolist()
        n_estagios = len(datas_inicio)
        n_cenarios = len(df["serie"].unique())
        n_patamares = len(df["patamar"].unique())
        if col_grp != "sin":
            for u in ordem_sintese:
                lim_inf = cls._expande_dados_para_cenarios(
                    df_gtmin.loc[
                        df_gtmin[col_grp] == u, "valor_MWmed"
                    ].to_numpy(),
                    1,
                    n_estagios,
                    n_cenarios * n_patamares,
                    1,
                )
                df.loc[df[col_grp] == u, "limiteInferior"] = lim_inf
                lim_sup = cls._expande_dados_para_cenarios(
                    df_gtmax.loc[
                        df_gtmax[col_grp] == u, "valor_MWmed"
                    ].to_numpy(),
                    1,
                    n_estagios,
                    n_cenarios * n_patamares,
                    1,
                )
                df.loc[df[col_grp] == u, "limiteSuperior"] = lim_sup
        else:
            lim_inf = cls._expande_dados_para_cenarios(
                df_gtmin["valor_MWmed"].to_numpy(),
                1,
                n_estagios,
                n_cenarios * n_patamares,
                1,
            )
            df["limiteInferior"] = lim_inf
            lim_sup = cls._expande_dados_para_cenarios(
                df_gtmax["valor_MWmed"].to_numpy(),
                1,
                n_estagios,
                n_cenarios * n_patamares,
                1,
            )
            df["limiteSuperior"] = lim_sup
        return df

    @classmethod
    def _gter_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, col_grp: str
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
        arq_pmo = cls._get_pmo(uow)
        df_gtmin = cls._validate_data(
            arq_pmo.geracao_minima_usinas_termicas, pd.DataFrame
        )
        df_gtmax = cls._validate_data(
            arq_pmo.geracao_maxima_usinas_termicas, pd.DataFrame
        )
        # Adiciona informações do submercado de cada UTE
        arq_conft = cls._get_conft(uow)
        arq_sistema = cls._get_sistema(uow)
        df_gtmin = cls._adiciona_submercado_limites_gter(
            df_gtmin, arq_conft, arq_sistema
        )
        df_gtmax = cls._adiciona_submercado_limites_gter(
            df_gtmax, arq_conft, arq_sistema
        )
        # Agrupa os limites, se necessário
        ordem_sintese = (
            df[col_grp].unique().tolist() if col_grp != "sin" else None
        )
        df_gtmin = cls._agrega_variaveis_limites_ute(
            df_gtmin, col_grp, ordem_sintese
        )
        df_gtmax = cls._agrega_variaveis_limites_ute(
            df_gtmax, col_grp, ordem_sintese
        )
        # Repete os limites para todos os estágios e cenarios
        df = cls._expande_dados_cenarios_gter(
            df, df_gtmin, df_gtmax, col_grp, ordem_sintese
        )

        # Converte os limites para MWmes
        df["limiteInferior"] *= df["duracaoPatamar"] / cls.STAGE_DURATION_HOURS
        df["limiteSuperior"] *= df["duracaoPatamar"] / cls.STAGE_DURATION_HOURS

        df["limiteInferior"] = np.round(df["limiteInferior"], 1)
        df["limiteSuperior"] = np.round(df["limiteSuperior"], 1)

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
    def resolve_bounds(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        if s in cls.MAPPINGS:
            return cls.MAPPINGS[s](df, uow)
        return df