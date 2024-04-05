STAGE_DURATION_HOURS = 730

HM3_M3S_MONTHLY_FACTOR = 1.0 / 2.63

HYDRO_COL = "usina"
THERMAL_COL = "usina"
EER_COL = "ree"
EEP_COL = "pee"
SUBMARKET_COL = "submercado"
EXCHANGE_SOURCE_COL = "submercadoDe"
EXCHANGE_TARGET_COL = "submercadoPara"
STAGE_COL = "estagio"
START_DATE_COL = "dataInicio"
END_DATE_COL = "dataFim"
SCENARIO_COL = "cenario"
BLOCK_COL = "patamar"
BLOCK_DURATION_COL = "duracaoPatamar"
VALUE_COL = "valor"
LOWER_BOUND_COL = "limiteInferior"
UPPER_BOUND_COL = "limiteSuperior"
VARIABLE_COL = "variavel"
GROUPING_TMP_COL = "group"
PRODUCTIVITY_TMP_COL = "prod"

SYNTHESIS_IDENTIFICATION_COLUMNS = [
    START_DATE_COL,
    END_DATE_COL,
    STAGE_COL,
    SUBMARKET_COL,
    EXCHANGE_SOURCE_COL,
    EXCHANGE_TARGET_COL,
    EER_COL,
    EEP_COL,
    HYDRO_COL,
    THERMAL_COL,
    BLOCK_COL,
    BLOCK_DURATION_COL,
    SCENARIO_COL,
]

OPERATION_SYNTHESIS_COMMON_COLUMNS = [
    STAGE_COL,
    START_DATE_COL,
    END_DATE_COL,
    SCENARIO_COL,
    BLOCK_COL,
    BLOCK_DURATION_COL,
    VALUE_COL,
]

OPERATION_SYNTHESIS_METADATA_OUTPUT = "METADADOS_OPERACAO"
OPERATION_SYNTHESIS_STATS_ROOT = "OPERACAO"

import pandas  # type: ignore # noqa: E402

__has_numba = True
try:
    import numba  # type: ignore # noqa: E402
except ImportError:
    __has_numba = False
if pandas.__version__ >= "2.2.0" and __has_numba:
    PANDAS_GROUPING_ENGINE = "numba"
else:
    PANDAS_GROUPING_ENGINE = "cython"

STRING_DF_TYPE = pandas.StringDtype(storage="pyarrow")
