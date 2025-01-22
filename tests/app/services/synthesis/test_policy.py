from os.path import join
from typing import Optional, Tuple
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from inewave.nwlistcf import Estados, Nwlistcfrel

from app.internal.constants import (
    BLOCK_COL,
    COEF_TYPE_COL,
    COEF_VALUE_COL,
    CUT_INDEX_COL,
    EARM_COEF_CODE,
    ENA_COEF_CODE,
    GTER_COEF_CODE,
    LAG_COL,
    MAXVIOL_COEF_CODE,
    POLICY_SYNTHESIS_METADATA_OUTPUT,
    RHS_COEF_CODE,
    STATE_VALUE_COL,
)
from app.model.policy.policysynthesis import PolicySynthesis
from app.services.synthesis.policy import PolicySynthetizer
from app.services.unitofwork import factory
from tests.conftest import DECK_TEST_DIR, q

uow = factory("FS", DECK_TEST_DIR, q)


def __sintetiza_com_mock(synthesis_str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        PolicySynthetizer.synthetize([synthesis_str], uow)
    m.assert_called()
    df = __obtem_dados_sintese_mock(synthesis_str, m)
    df_meta = __obtem_dados_sintese_mock(POLICY_SYNTHESIS_METADATA_OUTPUT, m)
    assert df is not None
    assert df_meta is not None
    return df, df_meta


def __obtem_dados_sintese_mock(
    chave: str, mock: MagicMock
) -> Optional[pd.DataFrame]:
    for c in mock.mock_calls:
        if c.args[1] == chave:
            return c.args[0]
    return None


def __valida_metadata(chave: str, df_metadata: pd.DataFrame):
    s = PolicySynthesis.factory(chave)
    assert s is not None
    assert str(s) in df_metadata["chave"].tolist()
    assert s.variable.short_name in df_metadata["nome_curto"].tolist()
    assert s.variable.long_name in df_metadata["nome_longo"].tolist()


def test_sintese_cortes_variaveis(test_settings):
    synthesis_str = "CORTES_VARIAVEIS"
    _, df_meta = __sintetiza_com_mock(synthesis_str)
    __valida_metadata(synthesis_str, df_meta)


def _valida_estados_cortes(df_sintese: pd.DataFrame, df_estados: pd.DataFrame):
    cut_indices = df_sintese[CUT_INDEX_COL].unique().tolist()
    for cut_index in cut_indices:
        # RHS
        synthesis_coefs = df_sintese.loc[
            (df_sintese[COEF_TYPE_COL] == RHS_COEF_CODE)
            & (df_sintese[CUT_INDEX_COL] == cut_index),
            STATE_VALUE_COL,
        ].to_numpy()
        file_coefs = df_estados.loc[
            (df_estados["IREG"] == RHS_COEF_CODE)
            & (df_estados["FUNC.OBJ."] > 0),
            "FUNC.OBJ.",
        ].to_numpy()
        assert np.allclose(synthesis_coefs, file_coefs, rtol=0.01)
        # EARM
        synthesis_coefs = df_sintese.loc[
            (df_sintese[COEF_TYPE_COL] == EARM_COEF_CODE)
            & (df_sintese[CUT_INDEX_COL] == cut_index),
            STATE_VALUE_COL,
        ].to_numpy()
        file_coefs = df_estados.loc[
            df_estados["IREG"] == cut_index,
            "EARM",
        ].to_numpy()
        assert np.allclose(synthesis_coefs, file_coefs, rtol=0.01)
        # EAF(1)
        synthesis_coefs = df_sintese.loc[
            (df_sintese[COEF_TYPE_COL] == ENA_COEF_CODE)
            & (df_sintese[CUT_INDEX_COL] == cut_index)
            & (df_sintese[LAG_COL] == 1),
            STATE_VALUE_COL,
        ].to_numpy()
        file_coefs = df_estados.loc[
            df_estados["IREG"] == cut_index,
            "EAF(1)",
        ].to_numpy()
        assert np.allclose(synthesis_coefs, file_coefs, rtol=0.01)
        # PIGTAD(P1L1)
        synthesis_coefs = df_sintese.loc[
            (df_sintese[COEF_TYPE_COL] == GTER_COEF_CODE)
            & (df_sintese[CUT_INDEX_COL] == cut_index)
            & (df_sintese[LAG_COL] == 1)
            & (df_sintese[BLOCK_COL] == 1),
            STATE_VALUE_COL,
        ].to_numpy()
        file_coefs = df_estados.loc[
            (df_estados["IREG"] == cut_index)
            & (df_estados["REE"].isin([1, 2, 3, 4])),
            "SGT(P1E1)",
        ].to_numpy()
        assert np.allclose(synthesis_coefs, file_coefs, rtol=0.01)
        # PIMX_VMN
        synthesis_coefs = df_sintese.loc[
            (df_sintese[COEF_TYPE_COL] == MAXVIOL_COEF_CODE)
            & (df_sintese[CUT_INDEX_COL] == cut_index),
            STATE_VALUE_COL,
        ].to_numpy()
        file_coefs = df_estados.loc[
            (df_estados["IREG"] == cut_index),
            "MX_CURVA",
        ].to_numpy()
        assert np.allclose(synthesis_coefs, file_coefs, rtol=0.01)


def _valida_coefs_cortes(df_sintese: pd.DataFrame, df_nwlistcf: pd.DataFrame):
    cut_indices = df_sintese[CUT_INDEX_COL].unique().tolist()
    for cut_index in cut_indices:
        # RHS
        synthesis_coefs = df_sintese.loc[
            (df_sintese[COEF_TYPE_COL] == RHS_COEF_CODE)
            & (df_sintese[CUT_INDEX_COL] == cut_index),
            COEF_VALUE_COL,
        ].to_numpy()
        file_coefs = df_nwlistcf.loc[
            (df_nwlistcf["IREG"] == RHS_COEF_CODE) & (df_nwlistcf["RHS"] > 0),
            "RHS",
        ].to_numpy()
        assert np.allclose(synthesis_coefs, file_coefs, rtol=0.01)
        # EARM
        synthesis_coefs = df_sintese.loc[
            (df_sintese[COEF_TYPE_COL] == EARM_COEF_CODE)
            & (df_sintese[CUT_INDEX_COL] == cut_index),
            COEF_VALUE_COL,
        ].to_numpy()
        file_coefs = df_nwlistcf.loc[
            df_nwlistcf["IREG"] == cut_index,
            "PIEARM",
        ].to_numpy()
        assert np.allclose(synthesis_coefs, file_coefs, rtol=0.01)
        # EAF(1)
        synthesis_coefs = df_sintese.loc[
            (df_sintese[COEF_TYPE_COL] == ENA_COEF_CODE)
            & (df_sintese[CUT_INDEX_COL] == cut_index)
            & (df_sintese[LAG_COL] == 1),
            COEF_VALUE_COL,
        ].to_numpy()
        file_coefs = df_nwlistcf.loc[
            df_nwlistcf["IREG"] == cut_index,
            "PIH(1)",
        ].to_numpy()
        assert np.allclose(synthesis_coefs, file_coefs, rtol=0.01)
        # PIGTAD(P1L1)
        synthesis_coefs = df_sintese.loc[
            (df_sintese[COEF_TYPE_COL] == GTER_COEF_CODE)
            & (df_sintese[CUT_INDEX_COL] == cut_index)
            & (df_sintese[LAG_COL] == 1)
            & (df_sintese[BLOCK_COL] == 1),
            COEF_VALUE_COL,
        ].to_numpy()
        file_coefs = df_nwlistcf.loc[
            (df_nwlistcf["IREG"] == cut_index)
            & (df_nwlistcf["REE"].isin([1, 2, 3, 4])),
            "PIGTAD(P1L1)",
        ].to_numpy()
        assert np.allclose(synthesis_coefs, file_coefs, rtol=0.01)
        # PIMX_VMN
        synthesis_coefs = df_sintese.loc[
            (df_sintese[COEF_TYPE_COL] == MAXVIOL_COEF_CODE)
            & (df_sintese[CUT_INDEX_COL] == cut_index),
            COEF_VALUE_COL,
        ].to_numpy()
        file_coefs = df_nwlistcf.loc[
            (df_nwlistcf["IREG"] == cut_index),
            "PIMX_VMN",
        ].to_numpy()
        assert np.allclose(synthesis_coefs, file_coefs, rtol=0.01)


def test_sintese_cortes_coeficientes(test_settings):
    synthesis_str = "CORTES_COEFICIENTES"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    __valida_metadata(synthesis_str, df_meta)
    coefs_df = Nwlistcfrel.read(join(DECK_TEST_DIR, "nwlistcf.rel")).cortes
    _valida_coefs_cortes(df, coefs_df)
    states_df = Estados.read(join(DECK_TEST_DIR, "estados.rel")).estados
    _valida_estados_cortes(df, states_df)
