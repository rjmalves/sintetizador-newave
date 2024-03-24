from typing import Callable, Dict, List, Tuple, Optional, Type, TypeVar
import pandas as pd  # type: ignore
import numpy as np
import logging
from time import time
import traceback
from multiprocessing import Pool
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta  # type: ignore
from inewave.newave import Dger, Ree, Confhd, Conft, Sistema, Clast, Hidr
from sintetizador.utils.log import Log
from sintetizador.utils.regex import match_variables_with_wildcards
from sintetizador.model.settings import Settings
from sintetizador.services.bounds import OperationVariableBounds
from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.operationsynthesis import OperationSynthesis
from sintetizador.model.operation.unit import Unit


FATOR_HM3_M3S_MES = 1.0 / 2.63


class OperationSynthetizer:
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

    STAGE_DURATION_HOURS = 730

    DEFAULT_OPERATION_SYNTHESIS_ARGS: List[str] = [
        "CMO_SBM",
        "VAGUA_REE",
        "VAGUA_UHE",
        "VAGUAI_UHE",
        "CTER_SBM",
        "CTER_SIN",
        "COP_SIN",
        "ENAA_REE",
        "ENAA_SBM",
        "ENAA_SIN",
        "ENAAR_REE",
        "ENAAR_SBM",
        "ENAAR_SIN",
        "ENAAF_REE",
        "ENAAF_SBM",
        "ENAAF_SIN",
        "EARPI_REE",
        "EARPI_SBM",
        "EARPI_SIN",
        "EARPF_REE",
        "EARPF_SBM",
        "EARPF_SIN",
        "EARMI_REE",
        "EARMI_SBM",
        "EARMI_SIN",
        "EARMF_REE",
        "EARMF_SBM",
        "EARMF_SIN",
        "GHIDR_REE",
        "GHIDR_SBM",
        "GHIDR_SIN",
        "GHID_REE",
        "GHID_SBM",
        "GHID_SIN",
        "GHIDF_REE",
        "GHIDF_SBM",
        "GHIDF_SIN",
        "GTER_SBM",
        "GTER_SIN",
        "GTER_UTE",
        "EVER_REE",
        "EVER_SBM",
        "EVER_SIN",
        "EVERR_REE",
        "EVERR_SBM",
        "EVERR_SIN",
        "EVERF_REE",
        "EVERF_SBM",
        "EVERF_SIN",
        "EVERFT_REE",
        "EVERFT_SBM",
        "EVERFT_SIN",
        "EDESR_REE",
        "EDESR_SBM",
        "EDESR_SIN",
        "EDESF_REE",
        "EDESF_SBM",
        "EDESF_SIN",
        "EVMIN_REE",
        "EVMIN_SBM",
        "EVMIN_SIN",
        "EVMOR_REE",
        "EVMOR_SBM",
        "EVMOR_SIN",
        "EEVAP_REE",
        "EEVAP_SBM",
        "EEVAP_SIN",
        "VGHMIN_REE",
        "VGHMIN_SBM",
        "VGHMIN_SIN",
        "VTUR_UHE",
        "VVER_UHE",
        "QTUR_UHE",
        "QVER_UHE",
        "QAFL_UHE",
        "QINC_UHE",
        "VAFL_UHE",
        "VINC_UHE",
        "QDEF_UHE",
        "VDEF_UHE",
        "VRET_UHE",
        "VDES_UHE",
        "QRET_UHE",
        "QDES_UHE",
        "VTUR_REE",
        "VVER_REE",
        "QTUR_REE",
        "QVER_REE",
        "QAFL_REE",
        "QINC_REE",
        "VAFL_REE",
        "VINC_REE",
        "QDEF_REE",
        "VDEF_REE",
        "VRET_REE",
        "VDES_REE",
        "QRET_REE",
        "QDES_REE",
        "VTUR_SBM",
        "VVER_SBM",
        "QTUR_SBM",
        "QVER_SBM",
        "QAFL_SBM",
        "QINC_SBM",
        "VAFL_SBM",
        "VINC_SBM",
        "QDEF_SBM",
        "VDEF_SBM",
        "VRET_SBM",
        "VDES_SBM",
        "QRET_SBM",
        "QDES_SBM",
        "VTUR_SIN",
        "VVER_SIN",
        "QTUR_SIN",
        "QVER_SIN",
        "QAFL_SIN",
        "QINC_SIN",
        "VAFL_SIN",
        "VINC_SIN",
        "QDEF_SIN",
        "VDEF_SIN",
        "VRET_SIN",
        "VDES_SIN",
        "QRET_SIN",
        "QDES_SIN",
        "VARMI_UHE",
        "VARMI_REE",
        "VARMI_SBM",
        "VARMI_SIN",
        "VARMF_UHE",
        "VARMF_REE",
        "VARMF_SBM",
        "VARMF_SIN",
        "VARPI_UHE",
        "VARPF_UHE",
        "GHID_UHE",
        "VENTO_PEE",
        "GEOL_PEE",
        "GEOL_SBM",
        "GEOL_SIN",
        "INT_SBP",
        "DEF_SBM",
        "DEF_SIN",
        "EXC_SBM",
        "EXC_SIN",
        "CDEF_SBM",
        "CDEF_SIN",
        "MERL_SBM",
        "MERL_SIN",
        "VEOL_SBM",
        "VGHMIN_UHE",
        "VFPHA_UHE",
        "VFPHA_REE",
        "VFPHA_SBM",
        "VFPHA_SIN",
        "HMON_UHE",
        "HJUS_UHE",
        "HLIQ_UHE",
        "VEVP_UHE",
        "VEVP_REE",
        "VEVP_SBM",
        "VEVP_SIN",
        "VEVAP_UHE",
        "VEVAP_REE",
        "VEVAP_SBM",
        "VEVAP_SIN",
    ]

    CACHED_SYNTHESIS: Dict[OperationSynthesis, pd.DataFrame] = {}

    # Declaração de dependência entre uma síntese e outras,
    # para o caso de variáveis que não são explicitamente fornecidas
    # como saída pelo modelo, e sim calculadas pelo sintetizador.
    PREREQ_SYNTHESIS: Dict[OperationSynthesis, List[OperationSynthesis]] = {
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.ENERGIA_VERTIDA_RESERV,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            OperationSynthesis(
                Variable.ENERGIA_VERTIDA_FIO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
        ],
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.ENERGIA_VERTIDA_RESERV,
                SpatialResolution.SUBMERCADO,
            ),
            OperationSynthesis(
                Variable.ENERGIA_VERTIDA_FIO,
                SpatialResolution.SUBMERCADO,
            ),
        ],
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.ENERGIA_VERTIDA_RESERV,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            OperationSynthesis(
                Variable.ENERGIA_VERTIDA_FIO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
        ],
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            )
        ],
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            )
        ],
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SUBMERCADO,
            )
        ],
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SUBMERCADO,
            )
        ],
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            )
        ],
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            )
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            OperationSynthesis(
                Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
        ],
        OperationSynthesis(
            Variable.ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SUBMERCADO,
            ),
            OperationSynthesis(
                Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SUBMERCADO,
            ),
        ],
        OperationSynthesis(
            Variable.ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            OperationSynthesis(
                Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.VOLUME_VERTIDO,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.VOLUME_TURBINADO,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.VOLUME_DESVIADO,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.VOLUME_RETIRADO,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            )
        ],
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.VAZAO_TURBINADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
            OperationSynthesis(
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.VOLUME_TURBINADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
            OperationSynthesis(
                Variable.VOLUME_VERTIDO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_FPHA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_FPHA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_FPHA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VOLUME_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VOLUME_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VOLUME_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VOLUME_VERTIDO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_VERTIDO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_VERTIDO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VOLUME_TURBINADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_TURBINADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_TURBINADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VOLUME_RETIRADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_RETIRADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_RETIRADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VOLUME_DESVIADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_DESVIADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VOLUME_DESVIADO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
            OperationSynthesis(
                Variable.VOLUME_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
            OperationSynthesis(
                Variable.VOLUME_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
            OperationSynthesis(
                Variable.VOLUME_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
            OperationSynthesis(
                Variable.VOLUME_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
            OperationSynthesis(
                Variable.VOLUME_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
            OperationSynthesis(
                Variable.VOLUME_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VAZAO_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VAZAO_TURBINADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_TURBINADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_TURBINADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VAZAO_RETIRADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_RETIRADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_RETIRADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VAZAO_DESVIADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_DESVIADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VAZAO_DESVIADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_POSITIVA_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
            OperationSynthesis(
                Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_EVAPORACAO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_EVAPORACAO,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_EVAPORACAO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_POSITIVA_EVAPORACAO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_POSITIVA_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_POSITIVA_EVAPORACAO,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_POSITIVA_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_POSITIVA_EVAPORACAO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_POSITIVA_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
            SpatialResolution.SUBMERCADO,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): [
            OperationSynthesis(
                Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.QUEDA_LIQUIDA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): [
            OperationSynthesis(
                Variable.QUEDA_LIQUIDA,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
            OperationSynthesis(
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ),
        ],
    }

    SYNTHESIS_TO_CACHE: List[OperationSynthesis] = list(
        set([p for pr in PREREQ_SYNTHESIS.values() for p in pr])
    )

    UNITS: Dict[OperationSynthesis, Unit] = {
        OperationSynthesis(
            Variable.CUSTO_MARGINAL_OPERACAO, SpatialResolution.SUBMERCADO
        ): Unit.RS_MWh,
        OperationSynthesis(
            Variable.VALOR_AGUA, SpatialResolution.RESERVATORIO_EQUIVALENTE
        ): Unit.RS_MWh,
        OperationSynthesis(
            Variable.VALOR_AGUA, SpatialResolution.USINA_HIDROELETRICA
        ): Unit.RS_hm3,
        OperationSynthesis(
            Variable.VALOR_AGUA_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.RS_hm3,
        OperationSynthesis(
            Variable.CUSTO_GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
        ): Unit.MiRS,
        OperationSynthesis(
            Variable.CUSTO_GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MiRS,
        OperationSynthesis(
            Variable.CUSTO_OPERACAO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MiRS,
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.perc,
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): Unit.perc,
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.perc,
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.perc,
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        ): Unit.perc,
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.perc,
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA_RESERVATORIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA_RESERVATORIO,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA_RESERVATORIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA_FIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA_FIO,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA_FIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.USINA_TERMELETRICA,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_DESVIO_RESERVATORIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_DESVIO_RESERVATORIO,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_DESVIO_RESERVATORIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_DESVIO_FIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_DESVIO_FIO,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_DESVIO_FIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VOLUME_MORTO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VOLUME_MORTO,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_VOLUME_MORTO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_EVAPORACAO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_EVAPORACAO,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.ENERGIA_EVAPORACAO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.SUBMERCADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.SUBMERCADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.SUBMERCADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.SUBMERCADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.SUBMERCADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.SUBMERCADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.SUBMERCADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.SUBMERCADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.SUBMERCADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.SUBMERCADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.m3s,
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.SUBMERCADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SUBMERCADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.perc,
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.perc,
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.VELOCIDADE_VENTO,
            SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
        ): Unit.ms,
        OperationSynthesis(
            Variable.GERACAO_EOLICA,
            SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_EOLICA,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.GERACAO_EOLICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.INTERCAMBIO,
            SpatialResolution.PAR_SUBMERCADOS,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.DEFICIT,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.DEFICIT,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.EXCESSO,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.EXCESSO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.CUSTO_DEFICIT,
            SpatialResolution.SUBMERCADO,
        ): Unit.MiRS,
        OperationSynthesis(
            Variable.CUSTO_DEFICIT,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MiRS,
        OperationSynthesis(
            Variable.MERCADO_LIQUIDO,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.MERCADO_LIQUIDO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.CORTE_GERACAO_EOLICA,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.SUBMERCADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.MWmes,
        OperationSynthesis(
            Variable.COTA_MONTANTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.m,
        OperationSynthesis(
            Variable.COTA_JUSANTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.m,
        OperationSynthesis(
            Variable.QUEDA_LIQUIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.m,
        OperationSynthesis(
            Variable.VOLUME_EVAPORADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_EVAPORADO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_EVAPORADO,
            SpatialResolution.SUBMERCADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VOLUME_EVAPORADO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VIOLACAO_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VIOLACAO_EVAPORACAO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VIOLACAO_EVAPORACAO,
            SpatialResolution.SUBMERCADO,
        ): Unit.hm3,
        OperationSynthesis(
            Variable.VIOLACAO_EVAPORACAO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): Unit.hm3,
    }

    T = TypeVar("T")

    logger: Optional[logging.Logger] = None

    @classmethod
    def _get_dger(cls, uow: AbstractUnitOfWork) -> Dger:
        with uow:
            dger = uow.files.get_dger()
            if dger is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do dger.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return dger

    @classmethod
    def _get_ree(cls, uow: AbstractUnitOfWork) -> Ree:
        with uow:
            ree = uow.files.get_ree()
            if ree is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do ree.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return ree

    @classmethod
    def _get_confhd(cls, uow: AbstractUnitOfWork) -> Confhd:
        with uow:
            confhd = uow.files.get_confhd()
            if confhd is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do confhd.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return confhd

    @classmethod
    def _get_hidr(cls, uow: AbstractUnitOfWork) -> Hidr:
        with uow:
            hidr = uow.files.get_hidr()
            if hidr is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do hidr.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return hidr

    @classmethod
    def _get_conft(cls, uow: AbstractUnitOfWork) -> Conft:
        with uow:
            conft = uow.files.get_conft()
            if conft is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do conft.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return conft

    @classmethod
    def _get_sistema(cls, uow: AbstractUnitOfWork) -> Sistema:
        with uow:
            sist = uow.files.get_sistema()
            if sist is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do sistema.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return sist

    @classmethod
    def _get_clast(cls, uow: AbstractUnitOfWork) -> Clast:
        with uow:
            sist = uow.files.get_clast()
            if sist is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do clast.dat para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return sist

    @classmethod
    def _validate_data(cls, data, type: Type[T], msg: str = "dados") -> T:
        if not isinstance(data, type):
            if cls.logger is not None:
                cls.logger.error(f"Erro na leitura de {msg}")
            raise RuntimeError()
        return data

    @classmethod
    def _default_args(cls) -> List[OperationSynthesis]:
        args = [
            OperationSynthesis.factory(a)
            for a in cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        ]
        return [arg for arg in args if arg is not None]

    @classmethod
    def _match_wildcards(cls, variables: List[str]) -> List[str]:
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[OperationSynthesis]:
        args_data = [OperationSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        return valid_args

    @classmethod
    def _validate_spatial_resolution_request(
        cls, spatial_resolution: SpatialResolution, *args, **kwargs
    ) -> bool:
        RESOLUTION_ARGS_MAP: Dict[SpatialResolution, List[str]] = {
            SpatialResolution.SISTEMA_INTERLIGADO: [],
            SpatialResolution.SUBMERCADO: ["submercado"],
            SpatialResolution.PAR_SUBMERCADOS: ["submercados"],
            SpatialResolution.RESERVATORIO_EQUIVALENTE: ["ree"],
            SpatialResolution.USINA_HIDROELETRICA: ["uhe"],
            SpatialResolution.USINA_TERMELETRICA: ["ute"],
            SpatialResolution.PARQUE_EOLICO_EQUIVALENTE: ["pee"],
        }

        mandatory = RESOLUTION_ARGS_MAP[spatial_resolution]
        valid = all([a in kwargs.keys() for a in mandatory])
        if not valid:
            if cls.logger is not None:
                cls.logger.error(
                    f"Erro no processamento da informação por {spatial_resolution}"
                )
        return valid

    @classmethod
    def filter_valid_variables(
        cls, variables: List[OperationSynthesis], uow: AbstractUnitOfWork
    ) -> List[OperationSynthesis]:
        dger = cls._get_dger(uow)
        rees = cls._validate_data(cls._get_ree(uow).rees, pd.DataFrame, "REE")
        valid_variables: List[OperationSynthesis] = []
        sf_indiv = dger.agregacao_simulacao_final == 1
        politica_indiv = rees["ano_fim_individualizado"].isna().sum() == 0
        indiv = sf_indiv or politica_indiv
        geracao_eolica = cls._validate_data(
            dger.considera_geracao_eolica, int, "dger"
        )
        eolica = geracao_eolica != 0
        if cls.logger is not None:
            cls.logger.info(
                f"Caso com geração de cenários de eólica: {eolica}"
            )
            cls.logger.info(f"Caso com modelagem híbrida: {indiv}")
        for v in variables:
            if (
                v.variable
                in [
                    Variable.VELOCIDADE_VENTO,
                    Variable.GERACAO_EOLICA,
                    Variable.CORTE_GERACAO_EOLICA,
                ]
                and not eolica
            ):
                continue
            if (
                v.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA
                and not indiv
            ):
                continue
            if (
                v.variable
                in [
                    Variable.VIOLACAO_FPHA,
                    Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                    Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                ]
                and not indiv
            ):
                continue
            if all(
                [
                    v.variable == Variable.VALOR_AGUA,
                    v.spatial_resolution
                    == SpatialResolution.USINA_HIDROELETRICA,
                    not indiv,
                ]
            ):
                continue
            valid_variables.append(v)
        if cls.logger is not None:
            cls.logger.info(f"Sinteses: {valid_variables}")
        return valid_variables

    @classmethod
    def _add_prereq_synthesis_recursive(
        cls,
        current_synthesis: List[OperationSynthesis],
        todo_synthesis: OperationSynthesis,
    ):
        if todo_synthesis in cls.PREREQ_SYNTHESIS.keys():
            for prereq in cls.PREREQ_SYNTHESIS[todo_synthesis]:
                cls._add_prereq_synthesis_recursive(current_synthesis, prereq)
        if todo_synthesis not in current_synthesis:
            current_synthesis.append(todo_synthesis)

    @classmethod
    def _add_prereq_synthesis(
        cls, synthesis: List[OperationSynthesis]
    ) -> List[OperationSynthesis]:
        result_synthesis: List[OperationSynthesis] = []
        for v in synthesis:
            cls._add_prereq_synthesis_recursive(result_synthesis, v)
        return result_synthesis

    @classmethod
    def __obtem_duracoes_patamares(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        arq_patamares = uow.files.get_patamar()
        if arq_patamares:
            df_pat = arq_patamares.duracao_mensal_patamares
            if df_pat is not None:
                df_pat_0 = df_pat.groupby("data", as_index=False).sum(
                    numeric_only=True
                )
                df_pat_0["patamar"] = 0
                df_pat = pd.concat([df_pat, df_pat_0], ignore_index=True)
                df_pat.sort_values(["data", "patamar"], inplace=True)
                return df_pat
            else:
                if cls.logger:
                    cls.logger.error("Erro na leitura dos patamares")
                raise RuntimeError()
        else:
            if cls.logger:
                cls.logger.error("Erro na abertura do arquivo de patamares")
            raise RuntimeError()

    @classmethod
    def __add_temporal_info(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        df = df.copy()
        df.sort_values(["data", "serie", "patamar"], inplace=True)
        cols = df.columns.tolist()
        datas = df["data"].unique().tolist()
        datas.sort()
        n_datas = len(datas)
        patamares = df["patamar"].unique().tolist()
        n_patamares = len(patamares)
        n_series = int(df.shape[0] / (n_datas * n_patamares))
        df["serie"] = np.tile(
            np.repeat(np.arange(1, n_series + 1), n_patamares), n_datas
        )
        # Atribui estagio e dataFim de forma posicional
        estagios = list(range(1, n_datas + 1))
        estagios_df = np.repeat(estagios, n_series * n_patamares)
        datasFim = [d + relativedelta(months=1) for d in datas]
        datasFim_df = np.repeat(datasFim, n_series * n_patamares)
        df = df.rename(columns={"data": "dataInicio"})
        df["estagio"] = estagios_df
        df["dataFim"] = datasFim_df
        # Atribui durações dos patamares de forma posicional
        df_pat = cls.__obtem_duracoes_patamares(uow)
        df_pat = df_pat.loc[df_pat["patamar"].isin(patamares)]
        duracoes_patamares = np.array((), dtype=np.float64)
        for d in datas:
            duracoes_data = df_pat.loc[df_pat["data"] == d, "valor"].to_numpy()
            duracoes_patamares = np.concatenate(
                (duracoes_patamares, np.tile(duracoes_data, n_series))
            )
        df["duracaoPatamar"] = cls.STAGE_DURATION_HOURS * duracoes_patamares
        return df[
            ["estagio", "dataInicio", "dataFim", "patamar", "duracaoPatamar"]
            + [c for c in cols if c not in ["patamar", "data"]]
        ]

    @classmethod
    def _resolve_temporal_resolution(
        cls,
        df: pd.DataFrame,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        if df is None:
            return None
        return cls.__add_temporal_info(df, uow)

    @classmethod
    def __resolve_SIN(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            if cls.logger is not None:
                cls.logger.info("Processando arquivo do SIN")
            df = uow.files.get_nwlistop(
                synthesis.variable,
                synthesis.spatial_resolution,
                "",
            )
            if df is not None:
                return cls._resolve_temporal_resolution(df, uow)
            else:
                return pd.DataFrame()

    @classmethod
    def _resolve_SBM_submercado(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        sbm_index: int,
        sbm_name: str,
    ) -> pd.DataFrame:
        logger_name = f"{synthesis.variable.value}_{sbm_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, sbm_index
        )
        with uow:
            logger.info(
                f"Processando arquivo do submercado: {sbm_index} - {sbm_name}"
            )
            df_sbm = cls._resolve_temporal_resolution(
                uow.files.get_nwlistop(
                    synthesis.variable,
                    synthesis.spatial_resolution,
                    submercado=sbm_index,
                ),
                uow,
            )
            if df_sbm is None:
                return None
            cols = df_sbm.columns.tolist()
            df_sbm["submercado"] = sbm_name
            df_sbm = df_sbm[["submercado"] + cols]
            return df_sbm

    @classmethod
    def __resolve_SBM(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sistema = cls._validate_data(
            cls._get_sistema(uow).custo_deficit,
            pd.DataFrame,
            "submercados",
        )
        sistemas_reais = sistema.loc[sistema["ficticio"] == 0, :]
        sbms_idx = sistemas_reais["codigo_submercado"].unique()
        sbms_name = [
            sistemas_reais.loc[
                sistemas_reais["codigo_submercado"] == s, "nome_submercado"
            ].iloc[0]
            for s in sbms_idx
        ]
        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                idx: pool.apply_async(
                    cls._resolve_SBM_submercado, (uow, synthesis, idx, name)
                )
                for idx, name in zip(sbms_idx, sbms_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        dfs_validos = [d for d in dfs.values() if d is not None]
        if len(dfs_validos) > 0:
            df_completo = pd.concat(dfs_validos, ignore_index=True)

        return df_completo

    @classmethod
    def _resolve_SBP_par_submercados(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        sbm1_index: int,
        sbm1_name: str,
        sbm2_index: int,
        sbm2_name: str,
    ) -> pd.DataFrame:
        if sbm1_index >= sbm2_index:
            return pd.DataFrame()
        logger_name = f"{synthesis.variable.value}_{sbm1_name}_{sbm2_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, sbm1_index + 10 * sbm2_index
        )
        with uow:
            logger.info(
                "Processando arquivo do par de submercados:"
                + f" {sbm1_index}[{sbm1_name}] - {sbm2_index}[{sbm2_name}]"
            )
            df_sbp = cls._resolve_temporal_resolution(
                uow.files.get_nwlistop(
                    synthesis.variable,
                    synthesis.spatial_resolution,
                    submercados=(sbm1_index, sbm2_index),
                ),
                uow,
            )
            if df_sbp is None:
                return None
            cols = df_sbp.columns.tolist()
            df_sbp["submercadoDe"] = sbm1_name
            df_sbp["submercadoPara"] = sbm2_name
            df_sbp = df_sbp[["submercadoDe", "submercadoPara"] + cols]
            return df_sbp

    @classmethod
    def __resolve_SBP(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sistema = cls._validate_data(
            cls._get_sistema(uow).custo_deficit,
            pd.DataFrame,
            "submercados",
        )
        sbms_idx = sistema["codigo_submercado"].unique()
        sbms_name = [
            sistema.loc[
                sistema["codigo_submercado"] == s, "nome_submercado"
            ].iloc[0]
            for s in sbms_idx
        ]
        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                f"{idx1}-{idx2}": pool.apply_async(
                    cls._resolve_SBP_par_submercados,
                    (uow, synthesis, idx1, name1, idx2, name2),
                )
                for idx1, name1 in zip(sbms_idx, sbms_name)
                for idx2, name2 in zip(sbms_idx, sbms_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        dfs_validos = [d for d in dfs.values() if d is not None]
        if len(dfs_validos) > 0:
            df_completo = pd.concat(dfs_validos, ignore_index=True)

        return df_completo

    @classmethod
    def _resolve_REE_ree(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        ree_index: int,
        ree_name: str,
    ) -> pd.DataFrame:
        logger_name = f"{synthesis.variable.value}_{ree_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, ree_index
        )
        with uow:
            logger.info(
                f"Processando arquivo do REE: {ree_index} - {ree_name}"
            )
            df_sbm = cls._resolve_temporal_resolution(
                uow.files.get_nwlistop(
                    synthesis.variable,
                    synthesis.spatial_resolution,
                    ree=ree_index,
                ),
                uow,
            )
            if df_sbm is None:
                return None
            cols = df_sbm.columns.tolist()
            df_sbm["ree"] = ree_name
            df_sbm = df_sbm[["ree"] + cols]
            return df_sbm

    @classmethod
    def __resolve_REE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        rees = cls._validate_data(cls._get_ree(uow).rees, pd.DataFrame, "REEs")
        rees_idx = rees["codigo"]
        rees_name = rees["nome"]
        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                idx: pool.apply_async(
                    cls._resolve_REE_ree, (uow, synthesis, idx, name)
                )
                for idx, name in zip(rees_idx, rees_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        dfs_validos = [d for d in dfs.values() if d is not None]
        if len(dfs_validos) > 0:
            df_completo = pd.concat(dfs_validos, ignore_index=True)

        df_completo = cls._add_submercado_rees(df_completo, uow)

        return df_completo

    @classmethod
    def _add_submercado_rees(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona a informação do SBM a uma síntese feita para REEs.
        """
        if "submercado" in df.columns:
            return df
        else:
            ti = time()
            cols = df.columns.tolist()
            rees = cls._validate_data(
                cls._get_ree(uow).rees, pd.DataFrame, "REEs"
            ).set_index("nome")
            sistema = cls._validate_data(
                cls._get_sistema(uow).custo_deficit, pd.DataFrame, "SBMs"
            )
            sistema = sistema.drop_duplicates(
                ["codigo_submercado", "nome_submercado"]
            ).set_index("codigo_submercado")
            # Obtem os nomes dos SBMs na mesma ordem em que aparecem os REEs
            rees_df = np.array(df["ree"].unique().tolist())
            codigos_sbms_df = [rees.at[r, "submercado"] for r in rees_df]
            nomes_sbms_df = [
                sistema.at[c, "nome_submercado"] for c in codigos_sbms_df
            ]
            # Aplica de modo posicional por desempenho
            n_patamares = len(df["patamar"].unique())
            n_estagios = len(df["estagio"].unique())
            n_series = len(df["serie"].unique())
            df["submercado"] = np.repeat(
                nomes_sbms_df, n_series * n_estagios * n_patamares
            )
            # Reordena as colunas e retorna
            tf = time()
            if cls.logger:
                cls.logger.info(
                    f"Tempo para adicionar SBM dos REE: {tf - ti:.2f} s"
                )
            return df[["ree", "submercado"] + [c for c in cols if c != "ree"]]

    @classmethod
    def _resolve_UHE_usina(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        uhe_index: int,
        uhe_name: str,
    ) -> pd.DataFrame:
        logger_name = f"{synthesis.variable.value}_{uhe_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, uhe_index
        )
        with uow:
            logger.info(
                f"Processando arquivo da UHE: {uhe_index} - {uhe_name}"
            )
            df_uhe = cls._resolve_temporal_resolution(
                uow.files.get_nwlistop(
                    synthesis.variable,
                    synthesis.spatial_resolution,
                    uhe=uhe_index,
                ),
                uow,
            )
            if df_uhe is None:
                return None

            cols = df_uhe.columns.tolist()
            df_uhe["usina"] = uhe_name
            df_uhe = df_uhe[["usina"] + cols]
            return df_uhe

    @classmethod
    def _resolve_gtert_temporal(
        cls, df: Optional[pd.DataFrame], uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        if df is None:
            return df
        df.sort_values(["classe", "data", "serie", "patamar"], inplace=True)
        classes = df["classe"].unique().tolist()
        n_classes = len(classes)
        datas = df["data"].unique().tolist()
        datas.sort()
        n_datas = len(datas)
        patamares = df["patamar"].unique().tolist()
        n_patamares = len(patamares)
        n_series = int(df.shape[0] / (n_datas * n_patamares * n_classes))
        # Atribui estagio e dataFim de forma posicional
        estagios = list(range(1, n_datas + 1))
        estagios_df = np.tile(
            np.repeat(estagios, n_series * n_patamares), n_classes
        )
        datasFim = [d + relativedelta(months=1) for d in datas]
        datasFim_df = np.tile(
            np.repeat(datasFim, n_series * n_patamares), n_classes
        )
        df = df.copy()
        df["estagio"] = estagios_df
        df["dataFim"] = datasFim_df
        df = df.rename(columns={"data": "dataInicio"})
        # Atribui duração dos patamares de forma posicional
        df_pat = cls.__obtem_duracoes_patamares(uow)
        df_pat = df_pat.loc[df_pat["patamar"].isin(patamares)]
        duracoes_patamares = np.array((), dtype=np.float64)
        for d in datas:
            duracoes_data = df_pat.loc[df_pat["data"] == d, "valor"].to_numpy()
            duracoes_patamares = np.concatenate(
                (duracoes_patamares, np.tile(duracoes_data, n_series))
            )
        duracoes_patamares = np.tile(duracoes_patamares, n_classes)
        df["duracaoPatamar"] = cls.STAGE_DURATION_HOURS * duracoes_patamares
        return df

    @classmethod
    def _resolve_gtert(
        cls,
        uow: AbstractUnitOfWork,
        synthesis: OperationSynthesis,
        sbm_index: int,
        sbm_name: str,
    ) -> pd.DataFrame:
        logger_name = f"{synthesis.variable.value}_{sbm_name}"
        logger = Log.configure_process_logger(
            uow.queue, logger_name, sbm_index
        )
        with uow:
            logger.info(
                f"Processando arquivo do submercado: {sbm_index} - {sbm_name}"
            )
            df_gtert = cls._resolve_gtert_temporal(
                uow.files.get_nwlistop(
                    synthesis.variable,
                    synthesis.spatial_resolution,
                    submercado=sbm_index,
                ),
                uow,
            )
            return df_gtert

    @classmethod
    def __stub_converte_volume_em_vazao(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        variable_map = {
            Variable.VAZAO_VERTIDA: Variable.VOLUME_VERTIDO,
            Variable.VAZAO_TURBINADA: Variable.VOLUME_TURBINADO,
            Variable.VAZAO_DESVIADA: Variable.VOLUME_DESVIADO,
            Variable.VAZAO_RETIRADA: Variable.VOLUME_RETIRADO,
        }
        synt_vol = OperationSynthesis(
            variable_map[synthesis.variable],
            synthesis.spatial_resolution,
        )
        df_completo = cls._get_from_cache(synt_vol)
        df_completo.loc[:, "valor"] = (
            df_completo["valor"]
            * FATOR_HM3_M3S_MES
            * cls.STAGE_DURATION_HOURS
            / df_completo["duracaoPatamar"]
        )

        return df_completo

    @classmethod
    def __stub_converte_vazao_em_volume(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        variable_map = {
            Variable.VOLUME_AFLUENTE: Variable.VAZAO_AFLUENTE,
            Variable.VOLUME_INCREMENTAL: Variable.VAZAO_INCREMENTAL,
        }
        synt_vaz = OperationSynthesis(
            variable_map[synthesis.variable],
            synthesis.spatial_resolution,
        )
        df_completo = cls._get_from_cache(synt_vaz)
        df_completo.loc[:, "valor"] = (
            df_completo["valor"]
            * df_completo["duracaoPatamar"]
            / (FATOR_HM3_M3S_MES * cls.STAGE_DURATION_HOURS)
        )
        return df_completo

    @classmethod
    def __stub_QDEF(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sintese_tur = OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            synthesis.spatial_resolution,
        )
        sintese_ver = OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            synthesis.spatial_resolution,
        )
        df_tur = cls._get_from_cache(sintese_tur)
        df_ver = cls._get_from_cache(sintese_ver)

        df_ver.loc[:, "valor"] = (
            df_tur["valor"].to_numpy() + df_ver["valor"].to_numpy()
        )
        return df_ver

    @classmethod
    def __stub_VDEF(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sintese_tur = OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            synthesis.spatial_resolution,
        )
        sintese_ver = OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            synthesis.spatial_resolution,
        )
        df_tur = cls._get_from_cache(sintese_tur)
        df_ver = cls._get_from_cache(sintese_ver)

        df_ver.loc[:, "valor"] = (
            df_tur["valor"].to_numpy() + df_ver["valor"].to_numpy()
        )
        return df_ver

    @classmethod
    def __stub_VEVAP(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sintese_pos = OperationSynthesis(
            Variable.VIOLACAO_POSITIVA_EVAPORACAO,
            synthesis.spatial_resolution,
        )
        sintese_neg = OperationSynthesis(
            Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
            synthesis.spatial_resolution,
        )
        df_pos = cls._get_from_cache(sintese_pos)
        df_neg = cls._get_from_cache(sintese_neg)

        df_pos.loc[:, "valor"] = (
            df_neg["valor"].to_numpy() + df_pos["valor"].to_numpy()
        )
        return df_pos

    @classmethod
    def __stub_EVER(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sintese_reserv = OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            synthesis.spatial_resolution,
        )
        sintese_fio = OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            synthesis.spatial_resolution,
        )
        cache_reserv = cls._get_from_cache(sintese_reserv)
        cache_fio = cls._get_from_cache(sintese_fio)

        cache_reserv.loc[:, "valor"] = (
            cache_fio["valor"].to_numpy() + cache_reserv["valor"].to_numpy()
        )
        return cache_reserv

    @classmethod
    def __stub_mapa_variaveis_agregacao_simples_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o mapeamento de síntese de uma variável calculada
        a partir de uma agregação simples de variáveis de UHE.
        """

        s = OperationSynthesis(
            variable=synthesis.variable,
            spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
        )
        df_uhe = cls._get_from_cache(s)

        return df_uhe

    @classmethod
    def __stub_mapa_variaveis_vazao_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o mapeamento de uma síntese solicitada para variável
        em vazão para a variável correspondente em volume, obtendo os
        dados já processados e armazenados em cache.

        Regra de negócio: o df passado para resolução dos bounds é
        em unidade de volume e sempre a nível de UHE. As agregações
        e conversões de unidade são feitas na resolução dos bounds.
        O OperationSynthesis passado para o bounds é o original.
        """

        variable_map = {
            Variable.VAZAO_AFLUENTE: Variable.VOLUME_AFLUENTE,
            Variable.VAZAO_INCREMENTAL: Variable.VOLUME_INCREMENTAL,
            Variable.VAZAO_DEFLUENTE: Variable.VOLUME_DEFLUENTE,
            Variable.VAZAO_VERTIDA: Variable.VOLUME_VERTIDO,
            Variable.VAZAO_TURBINADA: Variable.VOLUME_TURBINADO,
            Variable.VAZAO_RETIRADA: Variable.VOLUME_RETIRADO,
            Variable.VAZAO_DESVIADA: Variable.VOLUME_DESVIADO,
        }

        s = OperationSynthesis(
            variable=variable_map[synthesis.variable],
            spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
        )
        df_uhe = cls._get_from_cache(s)

        return df_uhe

    @classmethod
    def __stub_mapa_variaveis_volumes_percentuais_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o mapeamento de uma síntese solicitada para variável
        em volume percentuais para a em volume absoluto, obtendo os
        dados já processados e armazenados em cache.

        Regra de negócio: o df passado para resolução dos bounds é
        em unidade de volume e sempre a nível de UHE. As agregações
        e conversões de unidade são feitas na resolução dos bounds.
        O OperationSynthesis passado para o bounds é o original.
        """

        variable_map = {
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL: Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,  # noqa
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL: Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,  # noqa
        }

        s = OperationSynthesis(
            variable=variable_map[synthesis.variable],
            spatial_resolution=SpatialResolution.USINA_HIDROELETRICA,
        )
        df_uhe = cls._get_from_cache(s)

        return df_uhe

    @classmethod
    def __stub_resolve_energias_iniciais_ree(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        earmi = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL
        earmf = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL
        earpi = Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL
        earpf = Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL
        variable_map = {
            earmi: earmf,
            earpi: earpf,
        }
        sintese_final = OperationSynthesis(
            variable=variable_map[synthesis.variable],
            spatial_resolution=synthesis.spatial_resolution,
        )
        df_final = cls._get_from_cache(sintese_final)
        # Contém as duas colunas: absoluta e percentual
        earmi_pmo = cls._earmi_percentual(synthesis, uow)
        col_earmi_pmo = (
            "valor_MWmes"
            if synthesis.variable
            == Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL
            else "valor_percentual"
        )
        df_inicial = df_final.copy()
        col_group_map = {
            SpatialResolution.RESERVATORIO_EQUIVALENTE: "ree",
            SpatialResolution.SUBMERCADO: "submercado",
            SpatialResolution.SISTEMA_INTERLIGADO: None,
        }
        col_group = col_group_map[synthesis.spatial_resolution]
        if col_group is not None:
            groups = df_inicial[col_group].unique().tolist()
        else:
            groups = [1]
        n_groups = len(groups)
        series = df_inicial["serie"].unique().tolist()
        n_series = len(series)
        estagios = df_inicial["estagio"].unique().tolist()
        n_estagios = len(estagios)
        # Faz uma atribuição posicional. A maneira mais pythonica é lenta.
        offset_meses = cls._offset_meses_inicio(df_inicial, uow)
        offsets_groups = [i * n_series * n_estagios for i in range(n_groups)]
        indices_primeiros_estagios = offset_meses * n_series + np.tile(
            np.arange(n_series), n_groups
        )
        indices_primeiros_estagios += np.repeat(offsets_groups, n_series)
        earmi_pmo = earmi_pmo.loc[earmi_pmo["group"].isin(groups)]
        valores_earmi = (
            earmi_pmo.set_index("group").loc[groups, col_earmi_pmo].to_numpy()
        )
        valores_iniciais = df_inicial["valor"].to_numpy()
        valores_iniciais[n_series:] = valores_iniciais[:-n_series]
        valores_iniciais[indices_primeiros_estagios] = np.repeat(
            valores_earmi, n_series
        )
        df_inicial["valor"] = valores_iniciais
        df_inicial["valor"] = df_inicial["valor"].fillna(0.0)
        return df_inicial

    @classmethod
    def __stub_resolve_volumes_iniciais_uhe(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        varmi = Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL
        varmf = Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL
        varpi = Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL
        varpf = Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL
        variable_map = {
            varmi: varmf,
            varpi: varpf,
        }
        sintese_final = OperationSynthesis(
            variable=variable_map[synthesis.variable],
            spatial_resolution=synthesis.spatial_resolution,
        )
        df_final = cls._get_from_cache(sintese_final)
        with uow:
            arq_pmo = uow.files.get_pmo()
            if arq_pmo is not None:
                varmi_pmo = cls._validate_data(
                    arq_pmo.volume_armazenado_inicial, pd.DataFrame, "VARMI"
                )
            else:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro na leitura do VARM inicial do pmo.dat"
                    )
                raise RuntimeError()
        col_varmi_pmo = (
            "valor_hm3"
            if synthesis.variable
            == Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL
            else "valor_percentual"
        )
        df_inicial = df_final.copy()
        uhes = df_inicial["usina"].unique().tolist()
        n_uhes = len(uhes)
        series = df_inicial["serie"].unique().tolist()
        n_series = len(series)
        estagios = df_inicial["estagio"].unique().tolist()
        n_estagios = len(estagios)
        # Faz uma atribuição posicional. A maneira mais pythonica é lenta.
        offset_meses = cls._offset_meses_inicio(df_inicial, uow)
        offsets_uhes = [i * n_series * n_estagios for i in range(n_uhes)]
        indices_primeiros_estagios = offset_meses * n_series + np.tile(
            np.arange(n_series), n_uhes
        )
        indices_primeiros_estagios += np.repeat(offsets_uhes, n_series)
        varmi_pmo = varmi_pmo.loc[varmi_pmo["nome_usina"].isin(uhes)]
        valores_varmi = (
            varmi_pmo.set_index("nome_usina")
            .loc[uhes, col_varmi_pmo]
            .to_numpy()
        )
        valores_iniciais = df_inicial["valor"].to_numpy()
        valores_iniciais[n_series:] = valores_iniciais[:-n_series]
        valores_iniciais[indices_primeiros_estagios] = np.repeat(
            valores_varmi, n_series
        )
        df_inicial["valor"] = valores_iniciais
        df_inicial["valor"] = df_inicial["valor"].fillna(0.0)
        return df_inicial

    @classmethod
    def _obtem_usinas_jusante(
        cls, nome_usina: str, df_uhes: pd.DataFrame
    ) -> List[str]:
        uhes_jusante: List[str] = []
        codigo_uhe_jusante = df_uhes.loc[
            df_uhes["nome_usina"] == nome_usina, "codigo_usina_jusante"
        ].iloc[0]
        while codigo_uhe_jusante != 0:
            nome_uhe_jusante = df_uhes.loc[
                df_uhes["codigo_usina"] == codigo_uhe_jusante, "nome_usina"
            ].iloc[0]
            uhes_jusante.append(nome_uhe_jusante)
            codigo_uhe_jusante = df_uhes.loc[
                df_uhes["codigo_usina"] == codigo_uhe_jusante,
                "codigo_usina_jusante",
            ].iloc[0]

        return uhes_jusante

    @classmethod
    def _acumula_produtibilidades_reservatorios(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        df_uhes = cls._validate_data(
            cls._get_confhd(uow).usinas, pd.DataFrame, "UHEs"
        )
        cadastro_uhes = cls._validate_data(
            cls._get_hidr(uow).cadastro, pd.DataFrame, "hidr"
        )
        # Para cada usina, obtem a lista de usina a jusante até
        # o mar e acumula as produtibilidades
        df["prod_acum"] = df["prod_ponto"]
        uhes = df["usina"].unique().tolist()
        for uhe in uhes:
            if cls.logger:
                cls.logger.info(f"Calculando prodt. acumulada para {uhe}...")
            regulacao_uhe = cadastro_uhes.loc[
                cadastro_uhes["nome_usina"] == uhe, "tipo_regulacao"
            ].iloc[0]
            if regulacao_uhe == "M":
                uhes_jusante = cls._obtem_usinas_jusante(uhe, df_uhes)
                for uhe_jusante in uhes_jusante:
                    prod_jusante = df.loc[
                        df["usina"] == uhe_jusante, "prod_ponto"
                    ].to_numpy()
                    if len(prod_jusante) > 0:
                        df.loc[df["usina"] == uhe, "prod_acum"] += prod_jusante
            else:
                df.loc[df["usina"] == uhe, "prod_acum"] = 0.0
        return df

    @classmethod
    def __stub_EARM_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        ti = time()
        sintese_hliq = OperationSynthesis(
            Variable.QUEDA_LIQUIDA,
            synthesis.spatial_resolution,
        )
        mapa_earm_varm = {
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL: Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,  # noqa
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL: Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,  # noqa
        }
        sintese_varm = OperationSynthesis(
            mapa_earm_varm[synthesis.variable],
            synthesis.spatial_resolution,
        )
        df_hliq = cls._get_from_cache(sintese_hliq)
        df_hliq = df_hliq.loc[df_hliq["patamar"] == 0].copy()
        df_varm = cls._get_from_cache(sintese_varm)
        # Converte queda líquida em produtividade usando a
        # produtibilidade específica
        hidr = cls._validate_data(
            cls._get_hidr(uow).cadastro, pd.DataFrame, "UHEs"
        )
        nomes_uhes = df_hliq["usina"].unique().tolist()
        n_entradas_uhe = df_hliq.loc[df_hliq["usina"] == nomes_uhes[0]].shape[
            0
        ]
        hidr_uhes = hidr.loc[hidr["nome_usina"].isin(nomes_uhes)].set_index(
            "nome_usina"
        )
        # Produtibilidades em MW / ( (m3/s) / m)
        prod_esp = np.repeat(
            hidr_uhes.loc[nomes_uhes, "produtibilidade_especifica"].to_numpy(),
            n_entradas_uhe,
        )
        # Converte para MW / ( (hm3 / mes) / m )
        prod_esp /= 2.63

        # Adiciona ao df e acumula as produtibilidades nos reservatórios
        df_hliq["prod_esp"] = prod_esp
        df_hliq["prod_ponto"] = df_hliq["prod_esp"] * df_hliq["valor"]
        df_hliq = cls._acumula_produtibilidades_reservatorios(df_hliq, uow)

        uhes_varm = df_varm["usina"].unique()
        df_hliq = df_hliq.loc[df_hliq["usina"].isin(uhes_varm)].copy()

        # Multiplica o volume (útil) armazenado em cada UHE pela
        # produtibilidade acumulada nos pontos de operação.
        df_hliq = df_hliq.sort_values(["usina", "estagio", "patamar"])
        df_varm = df_varm.sort_values(["usina", "estagio", "patamar"])

        df_varm["valor"] = (
            df_varm["valor"] - df_varm["limiteInferior"]
        ) * df_hliq["prod_acum"].to_numpy()
        df_varm["limiteInferior"] = 0.0
        df_varm["limiteSuperior"] = (
            df_varm["limiteSuperior"] - df_varm["limiteInferior"]
        ) * df_hliq["prod_acum"].to_numpy()

        tf = time()
        if cls.logger:
            cls.logger.info(
                f"Tempo para conversão do VARM em EARM: {tf - ti:.2f} s"
            )

        return df_varm

    @classmethod
    def __stub_energia_defluencia_minima(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sintese_meta = OperationSynthesis(
            Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            synthesis.spatial_resolution,
        )
        sintese_violacao = OperationSynthesis(
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            synthesis.spatial_resolution,
        )
        df_meta = cls._get_from_cache(sintese_meta)
        df_violacao = cls._get_from_cache(sintese_violacao)

        df_meta.loc[:, "valor"] = (
            df_meta["valor"].to_numpy() - df_violacao["valor"].to_numpy()
        )
        return df_meta

    @classmethod
    def __stub_calc_pat_0_weighted_mean(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        df = cls._resolve_spatial_resolution(synthesis, uow)
        ti = time()
        df_pat0 = df.copy()
        df_pat0["patamar"] = 0
        df_pat0["valor"] = (
            df_pat0["valor"] * df_pat0["duracaoPatamar"]
        ) / cls.STAGE_DURATION_HOURS
        cols_group = [
            c
            for c in df.columns
            if c
            not in [
                "patamar",
                "duracaoPatamar",
                "valor",
            ]
        ]
        df_pat0 = df_pat0.groupby(cols_group, as_index=False).sum(
            numeric_only=True
        )
        df_pat0 = pd.concat([df, df_pat0], ignore_index=True)
        df_pat0 = df_pat0.sort_values(cols_group + ["patamar"])
        tf = time()
        if cls.logger:
            cls.logger.info(
                f"Tempo para cálculo do patamar médio: {tf - ti:.2f} s"
            )
        return df_pat0

    @classmethod
    def __resolve_UHE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        confhd = cls._validate_data(
            cls._get_confhd(uow).usinas, pd.DataFrame, "UHEs"
        )
        rees = cls._validate_data(cls._get_ree(uow).rees, pd.DataFrame, "REEs")
        dger = cls._get_dger(uow)
        ano_inicio = cls._validate_data(dger.ano_inicio_estudo, int, "dger")
        agregacao_sim_final = dger.agregacao_simulacao_final
        anos_estudo = cls._validate_data(dger.num_anos_estudo, int, "dger")
        anos_pos_sim_final = cls._validate_data(
            dger.num_anos_pos_sim_final, int, "dger"
        )

        # Obtem o fim do periodo individualizado
        if agregacao_sim_final == 1:
            fim = datetime(
                year=ano_inicio + anos_estudo + anos_pos_sim_final,
                month=1,
                day=1,
            )
        elif rees["ano_fim_individualizado"].isna().sum() > 0:
            fim = datetime(
                year=ano_inicio + anos_estudo + anos_pos_sim_final,
                month=1,
                day=1,
            )
        else:
            fim = datetime(
                year=int(rees["ano_fim_individualizado"].iloc[0]),
                month=int(rees["mes_fim_individualizado"].iloc[0]),
                day=1,
            )
        uhes_idx = confhd["codigo_usina"]
        uhes_name = confhd["nome_usina"]

        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                idx: pool.apply_async(
                    cls._resolve_UHE_usina, (uow, synthesis, idx, name)
                )
                for idx, name in zip(uhes_idx, uhes_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        ti = time()
        dfs_validos = [d for d in dfs.values() if d is not None]
        if len(dfs_validos) > 0:
            df_completo = pd.concat(dfs_validos, ignore_index=True)

        if not df_completo.empty:
            df_completo = df_completo.loc[df_completo["dataInicio"] < fim, :]

        tf = time()
        if cls.logger is not None:
            cls.logger.info(
                f"Tempo para compactação dos dados: {tf - ti:.2f} s"
            )

        df_completo = cls._add_ree_submercado_uhes(df_completo, uow)
        return df_completo

    @classmethod
    def _add_ree_submercado_uhes(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona a informação do REE e SBM a uma síntese feita para UHEs.
        """
        if "ree" in df.columns and "submercado" in df.columns:
            return df
        else:
            ti = time()
            cols = df.columns.tolist()
            confhd = cls._validate_data(
                cls._get_confhd(uow).usinas, pd.DataFrame, "UHEs"
            ).set_index("nome_usina")
            rees = cls._validate_data(
                cls._get_ree(uow).rees, pd.DataFrame, "REEs"
            ).set_index("codigo")
            sistema = cls._validate_data(
                cls._get_sistema(uow).custo_deficit, pd.DataFrame, "SBMs"
            )
            sistema = sistema.drop_duplicates(
                ["codigo_submercado", "nome_submercado"]
            ).set_index("codigo_submercado")
            # Obtem os nomes dos REEs e SBMs na mesma ordem em que aparecem as UHEs
            uhes_df = np.array(df["usina"].unique().tolist())
            rees_df = [rees.at[confhd.at[u, "ree"], "nome"] for u in uhes_df]
            codigos_sbms_df = [
                rees.at[confhd.at[u, "ree"], "submercado"] for u in uhes_df
            ]
            nomes_sbms_df = [
                sistema.at[c, "nome_submercado"] for c in codigos_sbms_df
            ]
            # Aplica de modo posicional por desempenho
            n_patamares = len(df["patamar"].unique())
            n_estagios = len(df["estagio"].unique())
            n_series = len(df["serie"].unique())
            df["ree"] = np.repeat(rees_df, n_series * n_estagios * n_patamares)
            df["submercado"] = np.repeat(
                nomes_sbms_df, n_series * n_estagios * n_patamares
            )
            tf = time()
            if cls.logger:
                cls.logger.info(
                    f"Tempo para adicionar REE e SBM das UHE: {tf - ti:.2f} s"
                )
            # Reordena as colunas e retorna
            return df[
                ["usina", "ree", "submercado"]
                + [c for c in cols if c != "usina"]
            ]

    @classmethod
    def __resolve_UTE_normal(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        conft = cls._validate_data(
            cls._get_conft(uow).usinas, pd.DataFrame, "UTEs"
        )
        utes_idx = conft["codigo_usina"]
        utes_name = conft["nome_usina"]
        df = pd.DataFrame()
        with uow:
            for s, n in zip(utes_idx, utes_name):
                if cls.logger is not None:
                    cls.logger.info(f"Processando arquivo da UTE: {s} - {n}")
                df_ute = cls._resolve_temporal_resolution(
                    uow.files.get_nwlistop(
                        synthesis.variable,
                        synthesis.spatial_resolution,
                        ute=s,
                    ),
                    uow,
                )
                if df_ute is None:
                    continue
                cols = df_ute.columns.tolist()
                df_ute["usina"] = n
                df_ute = df_ute[["usina"] + cols]
                df = pd.concat(
                    [df, df_ute],
                    ignore_index=True,
                )
            return df

    @classmethod
    def __stub_GTER_UTE_patamar(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        sistema = cls._validate_data(
            cls._get_sistema(uow).custo_deficit,
            pd.DataFrame,
            "submercados",
        )
        sistemas_reais = sistema.loc[sistema["ficticio"] == 0, :]
        sbms_idx = sistemas_reais["codigo_submercado"].unique()
        sbms_name = [
            sistemas_reais.loc[
                sistemas_reais["codigo_submercado"] == s, "nome_submercado"
            ].iloc[0]
            for s in sbms_idx
        ]
        df_completo = pd.DataFrame()
        n_procs = int(Settings().processors)
        synthesis = OperationSynthesis(
            variable=synthesis.variable,
            spatial_resolution=synthesis.spatial_resolution,
        )
        with Pool(processes=n_procs) as pool:
            if n_procs > 1:
                if cls.logger is not None:
                    cls.logger.info("Paralelizando...")
            async_res = {
                idx: pool.apply_async(
                    cls._resolve_gtert, (uow, synthesis, idx, name)
                )
                for idx, name in zip(sbms_idx, sbms_name)
            }
            dfs = {ir: r.get(timeout=3600) for ir, r in async_res.items()}
        if cls.logger is not None:
            cls.logger.info("Compactando dados...")
        dfs_validos = [d for d in dfs.values() if d is not None]
        if len(dfs_validos) > 0:
            df_completo = pd.concat(dfs_validos, ignore_index=True)

        clast = cls._validate_data(
            cls._get_clast(uow).usinas,
            pd.DataFrame,
            "usinas termelétricas",
        )[["codigo_usina", "nome_usina"]].drop_duplicates()
        usinas_arquivo = df_completo["classe"].unique().tolist()
        nomes_usinas_arquivo = [
            clast.loc[clast["codigo_usina"] == c, "nome_usina"].iloc[0]
            for c in usinas_arquivo
        ]
        linhas_por_usina = df_completo.loc[
            df_completo["classe"] == usinas_arquivo[0]
        ].shape[0]
        df_completo["classe"] = np.repeat(
            nomes_usinas_arquivo, linhas_por_usina
        )
        return df_completo.rename(columns={"classe": "usina"})[
            [
                "usina",
                "estagio",
                "dataInicio",
                "dataFim",
                "patamar",
                "duracaoPatamar",
                "serie",
                "valor",
            ]
        ]

    @classmethod
    def __resolve_UTE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        if synthesis.variable == Variable.GERACAO_TERMICA:
            df = cls.__stub_GTER_UTE_patamar(synthesis, uow)
        else:
            df = cls.__resolve_UTE_normal(synthesis, uow)

        return cls._add_submercado_utes(df, uow)

    @classmethod
    def _add_submercado_utes(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona a informação do SBM a uma síntese feita para UTEs.
        """
        if "submercado" in df.columns:
            return df
        else:
            ti = time()
            cols = df.columns.tolist()
            utes = cls._validate_data(
                cls._get_conft(uow).usinas, pd.DataFrame, "UTEs"
            ).set_index("nome_usina")
            sistema = cls._validate_data(
                cls._get_sistema(uow).custo_deficit, pd.DataFrame, "SBMs"
            )
            sistema = sistema.drop_duplicates(
                ["codigo_submercado", "nome_submercado"]
            ).set_index("codigo_submercado")
            # Obtem os nomes dos SBMs na mesma ordem em que aparecem as UTEs
            utes_df = np.array(df["usina"].unique().tolist())
            codigos_sbms_df = [utes.at[r, "submercado"] for r in utes_df]
            nomes_sbms_df = [
                sistema.at[c, "nome_submercado"] for c in codigos_sbms_df
            ]
            # Aplica de modo posicional por desempenho
            n_patamares = len(df["patamar"].unique())
            n_estagios = len(df["estagio"].unique())
            n_series = len(df["serie"].unique())
            df["submercado"] = np.repeat(
                nomes_sbms_df, n_series * n_estagios * n_patamares
            )
            tf = time()
            if cls.logger:
                cls.logger.info(
                    f"Tempo para adicionar SBM das UTE: {tf - ti:.2f} s"
                )
            # Reordena as colunas e retorna
            return df[
                ["usina", "submercado"] + [c for c in cols if c != "usina"]
            ]

    @classmethod
    def __resolve_PEE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            eolica = uow.files.get_eolica()
            if eolica is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do eolica-cadastro.csv para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            uees_idx = []
            uees_name = []
            regs = eolica.pee_cad()
            df = pd.DataFrame()
            if regs is None:
                return df
            elif isinstance(regs, list):
                for r in regs:
                    uees_idx.append(r.codigo_pee)
                    uees_name.append(r.nome_pee)
            for s, n in zip(uees_idx, uees_name):
                if cls.logger is not None:
                    cls.logger.info(f"Processando arquivo da UEE: {s} - {n}")
                df_uee = cls._resolve_temporal_resolution(
                    uow.files.get_nwlistop(
                        synthesis.variable,
                        synthesis.spatial_resolution,
                        uee=s,
                    ),
                    uow,
                )
                if df_uee is None:
                    continue
                cols = df_uee.columns.tolist()
                df_uee["pee"] = n
                df_uee = df_uee[["pee"] + cols]
                df = pd.concat(
                    [df, df_uee],
                    ignore_index=True,
                )
            return df

    @classmethod
    def _resolve_spatial_resolution(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        RESOLUTION_FUNCTION_MAP: Dict[SpatialResolution, Callable] = {
            SpatialResolution.SISTEMA_INTERLIGADO: cls.__resolve_SIN,
            SpatialResolution.SUBMERCADO: cls.__resolve_SBM,
            SpatialResolution.PAR_SUBMERCADOS: cls.__resolve_SBP,
            SpatialResolution.RESERVATORIO_EQUIVALENTE: cls.__resolve_REE,
            SpatialResolution.USINA_HIDROELETRICA: cls.__resolve_UHE,
            SpatialResolution.USINA_TERMELETRICA: cls.__resolve_UTE,
            SpatialResolution.PARQUE_EOLICO_EQUIVALENTE: cls.__resolve_PEE,
        }
        solver = RESOLUTION_FUNCTION_MAP[synthesis.spatial_resolution]
        return solver(synthesis, uow)

    @classmethod
    def _offset_meses_inicio(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> int:
        dger = cls._get_dger(uow)
        ano_inicio = cls._validate_data(dger.ano_inicio_estudo, int, "dger")
        mes_inicio = cls._validate_data(dger.mes_inicio_estudo, int, "dger")
        starting_date = datetime(ano_inicio, mes_inicio, 1)
        data_starting_date = df["dataInicio"].min().to_pydatetime()
        month_difference = int(
            (starting_date - data_starting_date) / timedelta(days=30)
        )
        return month_difference

    @classmethod
    def _resolve_starting_stage(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ):
        ti = time()
        month_difference = cls._offset_meses_inicio(df, uow)
        starting_df = df.copy()
        starting_df.loc[:, "estagio"] -= month_difference
        # Considera somente estágios do período de estudo em diante
        starting_df = starting_df.loc[starting_df["estagio"] > 0]
        starting_df = starting_df.rename(columns={"serie": "cenario"})
        tf = time()
        if cls.logger:
            cls.logger.info(
                f"Tempo para consideração do estágio inicial: {tf - ti:.2f} s"
            )
        return starting_df.copy()

    @classmethod
    def _processa_media(cls, df: pd.DataFrame) -> pd.DataFrame:
        cols_valores = ["cenario", "valor"]
        cols_agrupamento = [c for c in df.columns if c not in cols_valores]
        df_mean = (
            df.groupby(cols_agrupamento).mean(numeric_only=True).reset_index()
        )
        df_mean["cenario"] = "mean"
        df_std = (
            df.groupby(cols_agrupamento).std(numeric_only=True).reset_index()
        )
        df_std["cenario"] = "std"
        df = pd.concat([df, df_mean, df_std], ignore_index=True)
        return df

    @classmethod
    def _processa_quantis(
        cls, df: pd.DataFrame, quantiles: List[float]
    ) -> pd.DataFrame:
        cols_valores = ["cenario", "valor"]
        cols_agrupamento = [c for c in df.columns if c not in cols_valores]
        df_q = (
            df.groupby(cols_agrupamento)
            .quantile(quantiles, numeric_only=True)
            .reset_index()
        )

        def quantile_map(q: float) -> str:
            if q == 0:
                label = "min"
            elif q == 1:
                label = "max"
            elif q == 0.5:
                label = "median"
            else:
                label = f"p{int(100 * q)}"
            return label

        level_column = [c for c in df_q.columns if "level_" in c]
        if len(level_column) != 1:
            if cls.logger is not None:
                cls.logger.error("Erro no cálculo dos quantis")
                raise RuntimeError()

        df_q = df_q.drop(columns=["cenario"]).rename(
            columns={level_column[0]: "cenario"}
        )
        df_q["cenario"] = df_q["cenario"].apply(quantile_map)
        return pd.concat([df, df_q], ignore_index=True)

    @classmethod
    def _postprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        ti = time()
        df = cls._processa_quantis(df, [0.05 * i for i in range(21)])
        df = cls._processa_media(df)
        df = df.astype({"cenario": str})
        tf = time()
        if cls.logger:
            cls.logger.info(f"Tempo para pos-processamento: {tf - ti:.2f} s")
        return df

    @classmethod
    def _earmi_percentual(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with uow:
            arq_pmo = uow.files.get_pmo()
            if arq_pmo is not None:
                earmi_pmo = cls._validate_data(
                    arq_pmo.energia_armazenada_inicial, pd.DataFrame, "EARMI"
                )
            else:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro na leitura do EARM inicial do pmo.dat"
                    )
                raise RuntimeError()
        if s.spatial_resolution == SpatialResolution.RESERVATORIO_EQUIVALENTE:
            return earmi_pmo.rename(columns={"nome_ree": "group"})
        earmi_pmo["earmax"] = (
            100 * earmi_pmo["valor_MWmes"] / earmi_pmo["valor_percentual"]
        )
        if s.spatial_resolution == SpatialResolution.SUBMERCADO:
            sistema = cls._validate_data(
                cls._get_sistema(uow).custo_deficit,
                pd.DataFrame,
                "submercados",
            )
            rees = cls._validate_data(
                cls._get_ree(uow).rees, pd.DataFrame, "REEs"
            )
            nomes_rees = rees["nome"].unique().tolist()
            rees_submercados = {
                r: str(
                    sistema.loc[
                        sistema["codigo_submercado"]
                        == int(
                            rees.loc[rees["nome"] == r, "submercado"].iloc[0]
                        ),
                        "nome_submercado",
                    ].tolist()[0]
                )
                for r in nomes_rees
            }
            earmi_pmo.dropna(inplace=True)
            earmi_pmo["group"] = earmi_pmo.apply(
                lambda linha: rees_submercados[linha["nome_ree"].strip()],
                axis=1,
            )
        else:
            earmi_pmo["group"] = 1
        earmi_pmo = (
            earmi_pmo.groupby("group").sum(numeric_only=True).reset_index()
        )
        earmi_pmo["valor_percentual"] = (
            100 * earmi_pmo["valor_MWmes"] / (earmi_pmo["earmax"])
        )
        return earmi_pmo

    @classmethod
    def _get_from_cache(cls, s: OperationSynthesis) -> pd.DataFrame:
        if s in cls.CACHED_SYNTHESIS.keys():
            if cls.logger:
                cls.logger.info(f"Lendo do cache - {str(s)}")
            val = cls.CACHED_SYNTHESIS.get(s)
            if val is None:
                if cls.logger:
                    cls.logger.error(f"Erro na leitura do cache - {str(s)}")
                raise RuntimeError()
            return val.copy()
        else:
            if cls.logger:
                cls.logger.error(f"Erro na leitura do cache - {str(s)}")
            raise RuntimeError()

    @classmethod
    def _stub_mappings(  # noqa
        cls, s: OperationSynthesis
    ) -> Optional[Callable]:
        f = None
        if s.variable == Variable.ENERGIA_VERTIDA:
            f = cls.__stub_EVER
        elif all(
            [
                s.variable
                in [
                    Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                    Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                ],
                s.spatial_resolution
                in [
                    SpatialResolution.RESERVATORIO_EQUIVALENTE,
                    SpatialResolution.SUBMERCADO,
                    SpatialResolution.SISTEMA_INTERLIGADO,
                ],
            ]
        ):
            f = cls.__stub_resolve_energias_iniciais_ree
        elif all(
            [
                s.variable
                in [
                    Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                    Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
                ],
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_resolve_volumes_iniciais_uhe
        elif all(
            # TODO - eliminar todas as sínteses de violações que
            # não sejam de slacks propositais das metodologias,
            # que tornam o problema RCR.
            # Mapeadas: fpha, evap, deficit, excesso, ...
            [
                s.variable
                in [
                    Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                    Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                    Variable.VOLUME_AFLUENTE,
                    Variable.VOLUME_INCREMENTAL,
                    Variable.VOLUME_DEFLUENTE,
                    Variable.VOLUME_VERTIDO,
                    Variable.VOLUME_TURBINADO,
                    Variable.VOLUME_RETIRADO,
                    Variable.VOLUME_DESVIADO,
                    Variable.VOLUME_EVAPORADO,
                    Variable.VIOLACAO_EVAPORACAO,
                    Variable.VIOLACAO_FPHA,
                    Variable.VIOLACAO_POSITIVA_EVAPORACAO,
                    Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
                ],
                s.spatial_resolution != SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_mapa_variaveis_agregacao_simples_UHE
        elif all(
            [
                s.variable
                in [
                    Variable.VAZAO_AFLUENTE,
                    Variable.VAZAO_INCREMENTAL,
                    Variable.VAZAO_DEFLUENTE,
                    Variable.VAZAO_VERTIDA,
                    Variable.VAZAO_TURBINADA,
                    Variable.VAZAO_RETIRADA,
                    Variable.VAZAO_DESVIADA,
                ],
                s.spatial_resolution != SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_mapa_variaveis_vazao_UHE
        elif all(
            [
                s.variable
                in [
                    Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
                    Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                ],
                s.spatial_resolution != SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_mapa_variaveis_volumes_percentuais_UHE
        elif s.variable in [Variable.ENERGIA_DEFLUENCIA_MINIMA]:
            f = cls.__stub_energia_defluencia_minima
        elif s.variable in [Variable.COTA_JUSANTE, Variable.QUEDA_LIQUIDA]:
            f = cls.__stub_calc_pat_0_weighted_mean
        elif all(
            [
                s.variable
                in [
                    Variable.VAZAO_TURBINADA,
                    Variable.VAZAO_VERTIDA,
                    Variable.VAZAO_RETIRADA,
                    Variable.VAZAO_DESVIADA,
                ],
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_converte_volume_em_vazao
        elif all(
            [
                s.variable
                in [
                    Variable.VOLUME_AFLUENTE,
                    Variable.VOLUME_INCREMENTAL,
                ],
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_converte_vazao_em_volume
        elif all(
            [
                s.variable == Variable.VAZAO_DEFLUENTE,
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_QDEF
        elif all(
            [
                s.variable == Variable.VOLUME_DEFLUENTE,
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_VDEF
        elif all(
            [
                s.variable == Variable.VIOLACAO_EVAPORACAO,
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_VEVAP
        elif all(
            [
                s.variable
                in [
                    Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                    Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                ],
                s.spatial_resolution == SpatialResolution.USINA_HIDROELETRICA,
            ]
        ):
            f = cls.__stub_EARM_UHE
        return f

    @classmethod
    def _resolve_stub(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Tuple[pd.DataFrame, bool]:
        f = cls._stub_mappings(s)
        if f:
            df, is_stub = f(s, uow), True
        else:
            df, is_stub = pd.DataFrame(), False
        if is_stub:
            df = cls._resolve_bounds(s, df, uow)
        return df, is_stub

    @classmethod
    def __get_from_cache_if_exists(cls, s: OperationSynthesis) -> pd.DataFrame:
        if s in cls.CACHED_SYNTHESIS.keys():
            return cls._get_from_cache(s)
        else:
            return pd.DataFrame()

    @classmethod
    def __store_in_cache_if_needed(
        cls, s: OperationSynthesis, df: pd.DataFrame
    ):
        if s in cls.SYNTHESIS_TO_CACHE:
            ti = time()
            cls.CACHED_SYNTHESIS[s] = df.copy()
            tf = time()
            if cls.logger:
                cls.logger.info(
                    f"Tempo para armazenamento na cache: {tf - ti:.2f} s"
                )

    @classmethod
    def _resolve_bounds(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        ti = time()
        df = OperationVariableBounds.resolve_bounds(s, df, uow)
        tf = time()
        if cls.logger:
            cls.logger.info(f"Tempo para cálculo dos limites: {tf - ti:.2f} s")
        return df

    @classmethod
    def _resolve_synthesis(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        df = cls._resolve_spatial_resolution(s, uow)
        if df is not None:
            df = cls._resolve_bounds(s, df, uow)
        return df

    @classmethod
    def _export_metadata(
        cls,
        success_synthesis: List[Tuple[OperationSynthesis, bool]],
        uow: AbstractUnitOfWork,
    ):
        metadata_df = pd.DataFrame(
            columns=[
                "chave",
                "nome_curto_variavel",
                "nome_longo_variavel",
                "nome_curto_agregacao",
                "nome_longo_agregacao",
                "unidade",
                "calculado",
                "limitado",
            ]
        )
        for s, calculated in success_synthesis:
            metadata_df.loc[metadata_df.shape[0]] = [
                str(s),
                s.variable.short_name,
                s.variable.long_name,
                s.spatial_resolution.value,
                s.spatial_resolution.long_name,
                cls.UNITS[s].value if s in cls.UNITS else "",
                calculated,
                OperationVariableBounds.is_bounded(s),
            ]
        with uow:
            uow.export.synthetize_df(metadata_df, "METADADOS_OPERACAO")

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
        ti = time()
        if len(variables) == 0:
            synthesis_variables = cls._default_args()
        else:
            all_variables = cls._match_wildcards(variables)
            synthesis_variables = cls._process_variable_arguments(
                all_variables
            )
        valid_synthesis = cls.filter_valid_variables(synthesis_variables, uow)
        synthesis_with_prereqs = cls._add_prereq_synthesis(valid_synthesis)
        success_synthesis: List[Tuple[OperationSynthesis, bool]] = []
        for s in synthesis_with_prereqs:
            ti_s = time()
            try:
                filename = str(s)
                found_synthesis = False
                cls.logger.info(f"Realizando sintese de {filename}")
                df = cls.__get_from_cache_if_exists(s)
                is_stub = cls._stub_mappings(s) is not None
                if df.empty:
                    df, is_stub = cls._resolve_stub(s, uow)
                    if not is_stub:
                        df = cls._resolve_synthesis(s, uow)
                    cls.__store_in_cache_if_needed(s, df)
                if df is not None:
                    if not df.empty:
                        found_synthesis = True
                        df = cls._resolve_starting_stage(df, uow)
                        df = cls._postprocess(df)
                        with uow:
                            uow.export.synthetize_df(df, filename)
                        success_synthesis.append((s, is_stub))
                        tf_s = time()
                        cls.logger.info(
                            f"Tempo para síntese de {str(s)}: {tf_s - ti_s:.2f} s"
                        )
                if not found_synthesis:
                    cls.logger.warning(
                        "Nao foram encontrados dados"
                        + f" para a sintese de {str(s)}"
                    )
            except Exception as e:
                traceback.print_exc()
                cls.logger.error(str(e))
                cls.logger.error(
                    f"Nao foi possível realizar a sintese de: {str(s)}"
                )

        cls._export_metadata(success_synthesis, uow)
        tf = time()
        cls.logger.info(f"Tempo para síntese da operação: {tf - ti:.2f} s")
