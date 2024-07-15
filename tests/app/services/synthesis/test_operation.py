from os.path import join
import numpy as np
import pandas as pd
from datetime import datetime
from unittest.mock import patch, MagicMock
from app.services.unitofwork import factory
from app.model.operation.operationsynthesis import OperationSynthesis, UNITS
from app.services.synthesis.operation import OperationSynthetizer
from app.services.deck.bounds import OperationVariableBounds
from app.internal.constants import (
    HM3_M3S_MONTHLY_FACTOR,
    OPERATION_SYNTHESIS_METADATA_OUTPUT,
)
from typing import Optional, Tuple
from inewave.newave import Ree, Confhd, Patamar

from inewave.nwlistop import (
    Cmarg,
    Cmargmed,
    Vagua,
    Pivarm,
    Pivarmincr,
    Cterm,
    Ctermsin,
    Coper,
    Eafb,
    Eafbm,
    Eafbsin,
    Eaf,
    Eafm,
    # Eafsin, # TODO - substituir quando existir na inewave
    Earmfp,
    Earmfpm,
    Earmfpsin,
    Earmf,
    Earmfm,
    Earmfsin,
    Ghidr,
    Ghidrm,
    Ghidrsin,
    # Gfiol, # TODO - substituir quando existir na inewave
    # Gfiolm, # TODO - substituir quando existir na inewave
    # Gfiolsin, # TODO - substituir quando existir na inewave
    Evert,
    Evertm,
    Evertsin,
    Ghtot,
    Ghtotm,
    Ghtotsin,
    Gtert,
    Gttot,
    Gttotsin,
    Perdf,
    Perdfm,
    Perdfsin,
    Verturb,
    Verturbm,
    Verturbsin,
    Edesvc,
    Edesvcm,
    Edesvcsin,
    # Edesvf, # TODO - substituir quando existir na inewave
    # Edesvfm, # TODO - substituir quando existir na inewave
    # Edesvfsin, # TODO - substituir quando existir na inewave
    Mevmin,
    Mevminm,
    Mevminsin,
    Vmort,
    Vmortm,
    Vmortsin,
    Evapo,
    Evapom,
    Evaporsin,
    Qafluh,
    Qincruh,
    Vturuh,
    Vertuh,
    Varmuh,
    Varmpuh,
    Ghiduh,
    Def,
    Defsin,
    Exces,
    Excessin,
    Intercambio,
    Cdef,
    Cdefsin,
    Mercl,
    Merclsin,
    Dfphauh,
    Vevmin,
    Vevminm,
    Vevminsin,
    Desvuh,
    Vdesviouh,
    Vghminuh,
    Vghmin,
    Vghminm,
    Vghminsin,
    Hmont,
    Hjus,
    Hliq,
    Vevapuh,
    Dposevap,
    Dnegevap,
)

from tests.conftest import DECK_TEST_DIR, q

uow = factory("FS", DECK_TEST_DIR, q)


def __compara_sintese_nwlistop(
    df_sintese: pd.DataFrame, df_nwlistop: pd.DataFrame, *args, **kwargs
):
    data = kwargs.get("data_inicio", datetime(2023, 10, 1))
    cenario = kwargs.get("cenario", 1)
    filtros_sintese = (df_sintese["data_inicio"] == data) & (
        df_sintese["cenario"] == cenario
    )
    filtros_nwlistop = (df_nwlistop["data"] == data) & (
        df_nwlistop["serie"] == cenario
    )
    if "patamar" in df_nwlistop.columns:
        df_nwlistop = df_nwlistop.astype({"patamar": str})
    # Processa argumentos adicionais
    for col, val in kwargs.items():
        if col in df_sintese.columns:
            if col not in ["data_inicio", "cenario"]:
                filtros_sintese = filtros_sintese & (df_sintese[col].isin(val))
        if col in df_nwlistop.columns:
            if col == "patamar":
                val_nwlistop = {0: "TOTAL", 1: "1", 2: "2", 3: "3"}
                filtros_nwlistop = filtros_nwlistop & (
                    df_nwlistop[col].isin([val_nwlistop[v] for v in val])
                )
            else:
                filtros_nwlistop = filtros_nwlistop & (
                    df_nwlistop[col].isin(val)
                )

    dados_sintese = df_sintese.loc[filtros_sintese, "valor"].to_numpy()
    dados_nwlistop = df_nwlistop.loc[filtros_nwlistop, "valor"].to_numpy()

    assert len(dados_sintese) > 0
    assert len(dados_nwlistop) > 0

    try:
        assert np.allclose(
            dados_sintese,
            dados_nwlistop,
        )
    except AssertionError:
        print("Síntese:")
        print(df_sintese.loc[filtros_sintese])
        print("NWLISTOP:")
        print(df_nwlistop.loc[filtros_nwlistop])
        raise


def __valida_limites(
    df: pd.DataFrame, tol: float = 0.2, lower=True, upper=True
):
    num_amostras = df.shape[0]
    if upper:
        try:
            assert (
                df["valor"] <= (df["limite_superior"] + tol)
            ).sum() == num_amostras
        except AssertionError:
            print(df.loc[df["valor"] > (df["limite_superior"] + tol)])
            raise
    if lower:
        try:
            assert (
                df["valor"] >= (df["limite_inferior"] - tol)
            ).sum() == num_amostras
        except AssertionError:
            print(df.loc[df["valor"] < (df["limite_inferior"] - tol)])
            raise


def __valida_metadata(chave: str, df_metadata: pd.DataFrame, calculated: bool):
    s = OperationSynthesis.factory(chave)
    assert s is not None
    assert str(s) in df_metadata["chave"].tolist()
    assert s.variable.short_name in df_metadata["nome_curto_variavel"].tolist()
    assert s.variable.long_name in df_metadata["nome_longo_variavel"].tolist()
    assert (
        s.spatial_resolution.value
        in df_metadata["nome_curto_agregacao"].tolist()
    )
    assert (
        s.spatial_resolution.long_name
        in df_metadata["nome_longo_agregacao"].tolist()
    )
    unit_str = UNITS[s].value if s in UNITS else ""
    assert unit_str in df_metadata["unidade"].tolist()
    assert calculated in df_metadata["calculado"].tolist()
    assert (
        OperationVariableBounds.is_bounded(s)
        in df_metadata["limitado"].tolist()
    )


def __sintetiza_com_mock(synthesis_str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize([synthesis_str], uow)
        OperationSynthetizer.clear_cache()
    m.assert_called()
    df = __obtem_dados_sintese_mock(synthesis_str, m)
    df_meta = __obtem_dados_sintese_mock(OPERATION_SYNTHESIS_METADATA_OUTPUT, m)
    assert df is not None
    assert df_meta is not None
    return df, df_meta


def __sintetiza_com_mock_wildcard(synthesis_str) -> pd.DataFrame:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize([synthesis_str], uow)
        OperationSynthetizer.clear_cache()
    m.assert_called()
    df_meta = __obtem_dados_sintese_mock(OPERATION_SYNTHESIS_METADATA_OUTPUT, m)
    assert df_meta is not None
    return df_meta


def __obtem_dados_sintese_mock(
    chave: str, mock: MagicMock
) -> Optional[pd.DataFrame]:
    for c in mock.mock_calls:
        if c.args[1] == chave:
            return c.args[0]
    return None


# -----------------------------------------------------------------------------
# Valida funções que calculam colunas novas a partir de dados dos arquivos
# do NWLISTOP


def test_calcula_patamar_medio_soma(test_settings):
    def __calcula_patamar_medio_soma(df: pd.DataFrame) -> pd.DataFrame:
        df_pat0 = df.copy()
        df_pat0 = df_pat0.groupby(["data", "serie"], as_index=False).sum(
            numeric_only=True
        )
        df_pat0["patamar"] = "TOTAL"
        df_pat0 = pd.concat([df, df_pat0], ignore_index=True)
        return df_pat0.sort_values(["data", "serie", "patamar"])

    synthesis_str = "VTUR_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = __calcula_patamar_medio_soma(
        Vturuh.read(join(DECK_TEST_DIR, "vturuh006.out")).valores
    )
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_usina=[6],
    )
    __valida_limites(df)
    __valida_metadata(synthesis_str, df_meta, False)


def test_calcula_patamar_medio_soma_gter_ute(test_settings):
    def __calcula_patamar_medio_soma_gter_ute(
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        df_pat0 = df.copy()
        df_pat0 = df_pat0.groupby(
            ["classe", "data", "serie"], as_index=False
        ).sum(numeric_only=True)
        df_pat0["patamar"] = "TOTAL"
        df_pat0 = pd.concat([df, df_pat0], ignore_index=True)
        return df_pat0.sort_values(["classe", "data", "serie", "patamar"])

    synthesis_str = "GTER_UTE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    df_arq = __calcula_patamar_medio_soma_gter_ute(
        Gtert.read(join(DECK_TEST_DIR, "gtert001.out")).valores
    )

    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_usina=[1],
        classe=[1],
    )
    __valida_limites(df)
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_cmo_sbm(test_settings):
    synthesis_str = "CMO_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq_pat = Cmarg.read(join(DECK_TEST_DIR, "cmarg001.out")).valores
    df_arq_med = Cmargmed.read(join(DECK_TEST_DIR, "cmarg001-med.out")).valores
    # Compara CMOs por patamar
    __compara_sintese_nwlistop(
        df,
        df_arq_pat,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[1],
        patamar=[1, 2, 3],
    )
    # Compara CMO médio do estágio
    __compara_sintese_nwlistop(
        df,
        df_arq_med,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[1],
        patamar=[0],
    )
    __valida_metadata(synthesis_str, df_meta, False)


# -----------------------------------------------------------------------------
# Valida sínteses que são calculadas envolvendo informações de outros arquivos,
# além do próprio arquivo do NWLISTOP.

# EVER para REE, SBM e SIN (soma VERT.TURB com VERT.NTURB)


def test_sintese_ever_ree(test_settings):
    synthesis_str = "EVER_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_evert = Evert.read(join(DECK_TEST_DIR, "evert010.out")).valores
    df_evernt = Perdf.read(join(DECK_TEST_DIR, "perdf010.out")).valores
    df_evert["valor"] += df_evernt["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_evert,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[10],
        patamar=[0],
    )
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_ever_sbm(test_settings):
    synthesis_str = "EVER_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_evert = Evertm.read(join(DECK_TEST_DIR, "evertm001.out")).valores
    df_evernt = Perdfm.read(join(DECK_TEST_DIR, "perdfm001.out")).valores
    df_evert["valor"] += df_evernt["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_evert,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[1],
        patamar=[0],
    )
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_ever_sin(test_settings):
    synthesis_str = "EVER_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_evert = Evertsin.read(join(DECK_TEST_DIR, "evertsin.out")).valores
    df_evernt = Perdfsin.read(join(DECK_TEST_DIR, "perdfsin.out")).valores
    df_evert["valor"] += df_evernt["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_evert,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_earmi_ree(test_settings):
    synthesis_str = "EARMI_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - implementar validação
    __valida_limites(df, lower=False, tol=2.0)
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_earpi_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    synthesis_str = "EARPI_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - implementar validação
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_earmi_sbm(test_settings):
    synthesis_str = "EARMI_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - implementar validação
    __valida_limites(df, lower=False, tol=2.0)
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_earpi_sbm(test_settings):
    synthesis_str = "EARPI_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - implementar validação
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_earmi_sin(test_settings):
    synthesis_str = "EARMI_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - implementar validação
    __valida_limites(df, lower=False, tol=2.0)
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_earpi_sin(test_settings):
    synthesis_str = "EARPI_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - implementar validação
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_varmi_uhe(test_settings):
    synthesis_str = "VARMI_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - implementar validação
    __valida_limites(df)
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_varpi_uhe(test_settings):
    synthesis_str = "VARPI_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - implementar validação
    __valida_metadata(synthesis_str, df_meta, True)


# EARM para UHE: calcula produtibilidades acumulando


def test_sintese_earmi_uhe(test_settings):
    synthesis_str = "EARMI_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - implementar validação
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_earmf_uhe(test_settings):
    synthesis_str = "EARMF_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - implementar validação
    __valida_metadata(synthesis_str, df_meta, True)


# VFPHA, VRET, VDES, QRET, QDES para REE, SBM e SIN (soma valores das UHEs)


# --------------------------------------------------------------


def test_sintese_varmf_ree(test_settings):
    synthesis_str = "VARMF_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_ree = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[df_uhes["ree"] == 2, "codigo_usina"].unique()
    with uow:
        df_hidr = uow.files.get_hidr().cadastro
    for uhe in codigos_uhes:
        df_uhe = Varmuh.read(
            join(DECK_TEST_DIR, f"varmuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_ree is None:
            df_ree = df_uhe
        else:
            df_ree["valor"] += df_uhe["valor"].to_numpy()
        # Somente para VARM: soma volume mínimo para comparação com síntese,
        # que imprime volume total.
        df_ree["valor"] += df_hidr.at[uhe, "volume_minimo"]

    __compara_sintese_nwlistop(
        df,
        df_ree,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[2],
        patamar=[0],
    )
    __valida_limites(df)
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_varmf_sbm(test_settings):
    synthesis_str = "VARMF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sbm = None
    df_rees = Ree.read(join(DECK_TEST_DIR, "ree.dat")).rees
    rees_sbm = df_rees.loc[df_rees["submercado"] == 2, "codigo"].unique()
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[
        df_uhes["ree"].isin(rees_sbm), "codigo_usina"
    ].unique()
    with uow:
        df_hidr = uow.files.get_hidr().cadastro
    for uhe in codigos_uhes:
        df_uhe = Varmuh.read(
            join(DECK_TEST_DIR, f"varmuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sbm is None:
            df_sbm = df_uhe
        else:
            df_sbm["valor"] += df_uhe["valor"].to_numpy()
        # Somente para VARM: soma volume mínimo para comparação com síntese,
        # que imprime volume total.
        df_sbm["valor"] += df_hidr.at[uhe, "volume_minimo"]

    __compara_sintese_nwlistop(
        df,
        df_sbm,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[2],
        patamar=[0],
    )
    __valida_limites(df)
    __valida_metadata(synthesis_str, df_meta, True)


def test_sintese_varmf_sin(test_settings):
    synthesis_str = "VARMF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sin = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes["codigo_usina"].unique()
    with uow:
        df_hidr = uow.files.get_hidr().cadastro
    for uhe in codigos_uhes:
        df_uhe = Varmuh.read(
            join(DECK_TEST_DIR, f"varmuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sin is None:
            df_sin = df_uhe
        else:
            df_sin["valor"] += df_uhe["valor"].to_numpy()
        # Somente para VARM: soma volume mínimo para comparação com síntese,
        # que imprime volume total.
        df_sin["valor"] += df_hidr.at[uhe, "volume_minimo"]

    __compara_sintese_nwlistop(
        df,
        df_sin,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_limites(df)
    __valida_metadata("VARMF_SIN", df_meta, True)


def test_sintese_vafl_ree(test_settings):
    synthesis_str = "VAFL_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_ree = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[df_uhes["ree"] == 2, "codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Qafluh.read(
            join(DECK_TEST_DIR, f"qafluh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_ree is None:
            df_ree = df_uhe
        else:
            df_ree["valor"] += df_uhe["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_ree["valor"] /= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_ree,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[2],
        patamar=[0],
    )

    __valida_metadata("VAFL_REE", df_meta, True)


def test_sintese_vafl_sbm(test_settings):
    synthesis_str = "VAFL_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sbm = None
    df_rees = Ree.read(join(DECK_TEST_DIR, "ree.dat")).rees
    rees_sbm = df_rees.loc[df_rees["submercado"] == 2, "codigo"].unique()
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[
        df_uhes["ree"].isin(rees_sbm), "codigo_usina"
    ].unique()

    for uhe in codigos_uhes:
        df_uhe = Qafluh.read(
            join(DECK_TEST_DIR, f"qafluh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sbm is None:
            df_sbm = df_uhe
        else:
            df_sbm["valor"] += df_uhe["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_sbm["valor"] /= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_sbm,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[2],
        patamar=[0],
    )

    __valida_metadata("VAFL_SBM", df_meta, True)


def test_sintese_vafl_sin(test_settings):
    synthesis_str = "VAFL_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sin = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes["codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Qafluh.read(
            join(DECK_TEST_DIR, f"qafluh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sin is None:
            df_sin = df_uhe
        else:
            df_sin["valor"] += df_uhe["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_sin["valor"] /= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_sin,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )

    __valida_metadata("VAFL_SIN", df_meta, True)


def test_sintese_qafl_ree(test_settings):
    synthesis_str = "QAFL_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_ree = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[df_uhes["ree"] == 2, "codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Qafluh.read(
            join(DECK_TEST_DIR, f"qafluh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_ree is None:
            df_ree = df_uhe
        else:
            df_ree["valor"] += df_uhe["valor"].to_numpy()

    __compara_sintese_nwlistop(
        df,
        df_ree,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[2],
        patamar=[0],
    )
    __valida_metadata("QAFL_REE", df_meta, True)


def test_sintese_qafl_sbm(test_settings):
    synthesis_str = "QAFL_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sbm = None
    df_rees = Ree.read(join(DECK_TEST_DIR, "ree.dat")).rees
    rees_sbm = df_rees.loc[df_rees["submercado"] == 2, "codigo"].unique()
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[
        df_uhes["ree"].isin(rees_sbm), "codigo_usina"
    ].unique()

    for uhe in codigos_uhes:
        df_uhe = Qafluh.read(
            join(DECK_TEST_DIR, f"qafluh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sbm is None:
            df_sbm = df_uhe
        else:
            df_sbm["valor"] += df_uhe["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_sbm,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[2],
        patamar=[0],
    )
    __valida_metadata("QAFL_SBM", df_meta, True)


def test_sintese_qafl_sin(test_settings):
    synthesis_str = "QAFL_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sin = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes["codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Qafluh.read(
            join(DECK_TEST_DIR, f"qafluh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sin is None:
            df_sin = df_uhe
        else:
            df_sin["valor"] += df_uhe["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_sin,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("QAFL_SIN", df_meta, True)


def test_sintese_vinc_ree(test_settings):
    synthesis_str = "VINC_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_ree = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[df_uhes["ree"] == 2, "codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Qincruh.read(
            join(DECK_TEST_DIR, f"qincruh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_ree is None:
            df_ree = df_uhe
        else:
            df_ree["valor"] += df_uhe["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_ree["valor"] /= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_ree,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[2],
        patamar=[0],
    )
    __valida_metadata("VINC_REE", df_meta, True)


def test_sintese_vinc_sbm(test_settings):
    synthesis_str = "VINC_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sbm = None
    df_rees = Ree.read(join(DECK_TEST_DIR, "ree.dat")).rees
    rees_sbm = df_rees.loc[df_rees["submercado"] == 2, "codigo"].unique()
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[
        df_uhes["ree"].isin(rees_sbm), "codigo_usina"
    ].unique()

    for uhe in codigos_uhes:
        df_uhe = Qincruh.read(
            join(DECK_TEST_DIR, f"qincruh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sbm is None:
            df_sbm = df_uhe
        else:
            df_sbm["valor"] += df_uhe["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_sbm["valor"] /= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_sbm,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[2],
        patamar=[0],
    )
    __valida_metadata("VINC_SBM", df_meta, True)


def test_sintese_vinc_sin(test_settings):
    synthesis_str = "VINC_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sin = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes["codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Qincruh.read(
            join(DECK_TEST_DIR, f"qincruh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sin is None:
            df_sin = df_uhe
        else:
            df_sin["valor"] += df_uhe["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_sin["valor"] /= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_sin,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("VINC_SIN", df_meta, True)


def test_sintese_qinc_ree(test_settings):
    synthesis_str = "QINC_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_ree = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[df_uhes["ree"] == 2, "codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Qincruh.read(
            join(DECK_TEST_DIR, f"qincruh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_ree is None:
            df_ree = df_uhe
        else:
            df_ree["valor"] += df_uhe["valor"].to_numpy()

    __compara_sintese_nwlistop(
        df,
        df_ree,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[2],
        patamar=[0],
    )
    __valida_metadata("QINC_REE", df_meta, True)


def test_sintese_qinc_sbm(test_settings):
    synthesis_str = "QINC_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sbm = None
    df_rees = Ree.read(join(DECK_TEST_DIR, "ree.dat")).rees
    rees_sbm = df_rees.loc[df_rees["submercado"] == 2, "codigo"].unique()
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[
        df_uhes["ree"].isin(rees_sbm), "codigo_usina"
    ].unique()

    for uhe in codigos_uhes:
        df_uhe = Qincruh.read(
            join(DECK_TEST_DIR, f"qincruh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sbm is None:
            df_sbm = df_uhe
        else:
            df_sbm["valor"] += df_uhe["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_sbm,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[2],
        patamar=[0],
    )
    __valida_metadata("QINC_SBM", df_meta, True)


def test_sintese_qinc_sin(test_settings):
    synthesis_str = "QINC_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sin = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes["codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Qincruh.read(
            join(DECK_TEST_DIR, f"qincruh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sin is None:
            df_sin = df_uhe
        else:
            df_sin["valor"] += df_uhe["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_sin,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("QINC_SIN", df_meta, True)


def test_sintese_vtur_ree(test_settings):
    synthesis_str = "VTUR_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_ree = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[df_uhes["ree"] == 2, "codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Vturuh.read(
            join(DECK_TEST_DIR, f"vturuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_ree is None:
            df_ree = df_uhe
        else:
            df_ree["valor"] += df_uhe["valor"].to_numpy()

    __compara_sintese_nwlistop(
        df,
        df_ree,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[2],
        patamar=[1],
    )
    __valida_metadata("VTUR_REE", df_meta, True)


def test_sintese_vtur_sbm(test_settings):
    synthesis_str = "VTUR_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sbm = None
    df_rees = Ree.read(join(DECK_TEST_DIR, "ree.dat")).rees
    rees_sbm = df_rees.loc[df_rees["submercado"] == 2, "codigo"].unique()
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[
        df_uhes["ree"].isin(rees_sbm), "codigo_usina"
    ].unique()

    for uhe in codigos_uhes:
        df_uhe = Vturuh.read(
            join(DECK_TEST_DIR, f"vturuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sbm is None:
            df_sbm = df_uhe
        else:
            df_sbm["valor"] += df_uhe["valor"].to_numpy()

    __compara_sintese_nwlistop(
        df,
        df_sbm,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[2],
        patamar=[1],
    )
    __valida_metadata("VTUR_SBM", df_meta, True)


def test_sintese_vtur_sin(test_settings):
    synthesis_str = "VTUR_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sin = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes["codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Vturuh.read(
            join(DECK_TEST_DIR, f"vturuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sin is None:
            df_sin = df_uhe
        else:
            df_sin["valor"] += df_uhe["valor"].to_numpy()

    __compara_sintese_nwlistop(
        df,
        df_sin,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[1],
    )
    __valida_metadata("VTUR_SIN", df_meta, True)


def test_sintese_qtur_ree(test_settings):
    synthesis_str = "QTUR_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_ree = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[df_uhes["ree"] == 2, "codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Vturuh.read(
            join(DECK_TEST_DIR, f"vturuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_ree is None:
            df_ree = df_uhe
        else:
            df_ree["valor"] += df_uhe["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_ree = (
        df_ree.groupby(["data", "serie"]).sum(numeric_only=True).reset_index()
    )
    df_ree["patamar"] = "TOTAL"
    df_ree["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_ree,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[2],
        patamar=[0],
    )
    __valida_metadata("QTUR_REE", df_meta, True)


def test_sintese_qtur_sbm(test_settings):
    synthesis_str = "QTUR_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sbm = None
    df_rees = Ree.read(join(DECK_TEST_DIR, "ree.dat")).rees
    rees_sbm = df_rees.loc[df_rees["submercado"] == 2, "codigo"].unique()
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[
        df_uhes["ree"].isin(rees_sbm), "codigo_usina"
    ].unique()

    for uhe in codigos_uhes:
        df_uhe = Vturuh.read(
            join(DECK_TEST_DIR, f"vturuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sbm is None:
            df_sbm = df_uhe
        else:
            df_sbm["valor"] += df_uhe["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_sbm = (
        df_sbm.groupby(["data", "serie"]).sum(numeric_only=True).reset_index()
    )
    df_sbm["patamar"] = "TOTAL"
    df_sbm["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_sbm,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[2],
        patamar=[0],
    )
    __valida_metadata("QTUR_SBM", df_meta, True)


def test_sintese_qtur_sin(test_settings):
    synthesis_str = "QTUR_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sin = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes["codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Vturuh.read(
            join(DECK_TEST_DIR, f"vturuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sin is None:
            df_sin = df_uhe
        else:
            df_sin["valor"] += df_uhe["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_sin = (
        df_sin.groupby(["data", "serie"]).sum(numeric_only=True).reset_index()
    )
    df_sin["patamar"] = "TOTAL"
    df_sin["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_sin,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("QTUR_SIN", df_meta, True)


def test_sintese_vver_ree(test_settings):
    synthesis_str = "VVER_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_ree = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[df_uhes["ree"] == 2, "codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Vertuh.read(
            join(DECK_TEST_DIR, f"vertuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_ree is None:
            df_ree = df_uhe
        else:
            df_ree["valor"] += df_uhe["valor"].to_numpy()

    __compara_sintese_nwlistop(
        df,
        df_ree,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[2],
        patamar=[1],
    )
    __valida_metadata("VVER_REE", df_meta, True)


def test_sintese_vver_sbm(test_settings):
    synthesis_str = "VVER_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sbm = None
    df_rees = Ree.read(join(DECK_TEST_DIR, "ree.dat")).rees
    rees_sbm = df_rees.loc[df_rees["submercado"] == 2, "codigo"].unique()
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[
        df_uhes["ree"].isin(rees_sbm), "codigo_usina"
    ].unique()

    for uhe in codigos_uhes:
        df_uhe = Vertuh.read(
            join(DECK_TEST_DIR, f"vertuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sbm is None:
            df_sbm = df_uhe
        else:
            df_sbm["valor"] += df_uhe["valor"].to_numpy()

    __compara_sintese_nwlistop(
        df,
        df_sbm,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[2],
        patamar=[1],
    )
    __valida_metadata("VVER_SBM", df_meta, True)


def test_sintese_vver_sin(test_settings):
    synthesis_str = "VVER_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sin = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes["codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Vertuh.read(
            join(DECK_TEST_DIR, f"vertuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sin is None:
            df_sin = df_uhe
        else:
            df_sin["valor"] += df_uhe["valor"].to_numpy()

    __compara_sintese_nwlistop(
        df,
        df_sin,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[1],
    )
    __valida_metadata("VVER_SIN", df_meta, True)


def test_sintese_qver_ree(test_settings):
    synthesis_str = "QVER_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_ree = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[df_uhes["ree"] == 2, "codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Vertuh.read(
            join(DECK_TEST_DIR, f"vertuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_ree is None:
            df_ree = df_uhe
        else:
            df_ree["valor"] += df_uhe["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_ree = (
        df_ree.groupby(["data", "serie"]).sum(numeric_only=True).reset_index()
    )
    df_ree["patamar"] = "TOTAL"
    df_ree["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_ree,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[2],
        patamar=[0],
    )
    __valida_metadata("QVER_REE", df_meta, True)


def test_sintese_qver_sbm(test_settings):
    synthesis_str = "QVER_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sbm = None
    df_rees = Ree.read(join(DECK_TEST_DIR, "ree.dat")).rees
    rees_sbm = df_rees.loc[df_rees["submercado"] == 2, "codigo"].unique()
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes.loc[
        df_uhes["ree"].isin(rees_sbm), "codigo_usina"
    ].unique()

    for uhe in codigos_uhes:
        df_uhe = Vertuh.read(
            join(DECK_TEST_DIR, f"vertuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sbm is None:
            df_sbm = df_uhe
        else:
            df_sbm["valor"] += df_uhe["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_sbm = (
        df_sbm.groupby(["data", "serie"]).sum(numeric_only=True).reset_index()
    )
    df_sbm["patamar"] = "TOTAL"
    df_sbm["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_sbm,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[2],
        patamar=[0],
    )
    __valida_metadata("QVER_SBM", df_meta, True)


def test_sintese_qver_sin(test_settings):
    synthesis_str = "QVER_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_sin = None
    df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    codigos_uhes = df_uhes["codigo_usina"].unique()
    for uhe in codigos_uhes:
        df_uhe = Vertuh.read(
            join(DECK_TEST_DIR, f"vertuh{str(uhe).zfill(3)}.out")
        ).valores
        if df_uhe is None:
            continue
        if df_sin is None:
            df_sin = df_uhe
        else:
            df_sin["valor"] += df_uhe["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_sin = (
        df_sin.groupby(["data", "serie"]).sum(numeric_only=True).reset_index()
    )
    df_sin["patamar"] = "TOTAL"
    df_sin["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_sin,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("QVER_SIN", df_meta, True)


## EVMIN para REE, SBM e SIN (soma meta de EVMIN com violacao de EVMIN)


def test_sintese_evmin_ree(test_settings):
    synthesis_str = "EVMIN_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_mevmin = Mevminm.read(join(DECK_TEST_DIR, "mevmin010.out")).valores
    df_vevmin = Vevminm.read(join(DECK_TEST_DIR, "vevmin010.out")).valores
    df_mevmin["valor"] += df_vevmin["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_mevmin,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[10],
        patamar=[0],
    )
    __valida_metadata("EVMIN_REE", df_meta, True)


def test_sintese_evmin_sbm(test_settings):
    synthesis_str = "EVMIN_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_mevmin = Mevminm.read(join(DECK_TEST_DIR, "mevminm001.out")).valores
    df_vevmin = Vevminm.read(join(DECK_TEST_DIR, "vevminm001.out")).valores
    df_mevmin["valor"] += df_vevmin["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_mevmin,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[1],
        patamar=[0],
    )
    __valida_metadata("EVMIN_SBM", df_meta, True)


def test_sintese_evmin_sin(test_settings):
    synthesis_str = "EVMIN_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_mevmin = Mevminsin.read(join(DECK_TEST_DIR, "mevminsin.out")).valores
    df_vevmin = Vevminsin.read(join(DECK_TEST_DIR, "vevminsin.out")).valores
    df_mevmin["valor"] += df_vevmin["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_mevmin,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("EVMIN_SIN", df_meta, True)


# HJUS e HLIQ para o patamar MEDIO (média ponderada das durações dos patamares)


def test_sintese_hjus_uhe(test_settings):
    def __adiciona_duracoes_patamares(df: pd.DataFrame) -> pd.DataFrame:
        arq_pat = Patamar.read(join(DECK_TEST_DIR, "patamar.dat"))
        df_pat = arq_pat.duracao_mensal_patamares
        df = df.astype({"patamar": int})
        df["duracaoPatamar"] = df.apply(
            lambda linha: df_pat.loc[
                (df_pat["data"] == linha["data"])
                & (df_pat["patamar"] == linha["patamar"]),
                "valor",
            ].iloc[0],
            axis=1,
        )
        df = df.astype({"patamar": str})
        return df

    def __calcula_media_ponderada(df: pd.DataFrame) -> pd.DataFrame:
        df_pat0 = df.copy()
        df_pat0["valor"] = df_pat0["valor"] * df_pat0["duracaoPatamar"]
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
        df_group = (
            df_pat0.groupby(cols_group).sum(numeric_only=True).reset_index()
        )
        df_group["patamar"] = "TOTAL"
        df_pat0 = pd.concat([df_pat0, df_group], ignore_index=True)
        return df_pat0.sort_values(cols_group + ["patamar"]).reset_index(
            drop=True
        )

    synthesis_str = "HJUS_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = __calcula_media_ponderada(
        __adiciona_duracoes_patamares(
            Hjus.read(join(DECK_TEST_DIR, "hjus006.out")).valores
        )
    )

    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("HJUS_UHE", df_meta, False)


def test_sintese_hliq_uhe(test_settings):
    def __adiciona_duracoes_patamares(df: pd.DataFrame) -> pd.DataFrame:
        arq_pat = Patamar.read(join(DECK_TEST_DIR, "patamar.dat"))
        df_pat = arq_pat.duracao_mensal_patamares
        df = df.astype({"patamar": int})
        df["duracaoPatamar"] = df.apply(
            lambda linha: df_pat.loc[
                (df_pat["data"] == linha["data"])
                & (df_pat["patamar"] == linha["patamar"]),
                "valor",
            ].iloc[0],
            axis=1,
        )
        df = df.astype({"patamar": str})
        return df

    def __calcula_media_ponderada(df: pd.DataFrame) -> pd.DataFrame:
        df_pat0 = df.copy()
        df_pat0["valor"] = df_pat0["valor"] * df_pat0["duracaoPatamar"]
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
        df_group = (
            df_pat0.groupby(cols_group).sum(numeric_only=True).reset_index()
        )
        df_group["patamar"] = "TOTAL"
        df_pat0 = pd.concat([df_pat0, df_group], ignore_index=True)
        return df_pat0.sort_values(cols_group + ["patamar"]).reset_index(
            drop=True
        )

    synthesis_str = "HLIQ_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = __calcula_media_ponderada(
        __adiciona_duracoes_patamares(
            Hliq.read(join(DECK_TEST_DIR, "hliq006.out")).valores
        )
    )
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("HLIQ_UHE", df_meta, False)


# QTUR, QVER, QRET, QDES para UHE (converter para m3/s)


def test_sintese_qtur_uhe(test_settings):
    synthesis_str = "QTUR_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vturuh.read(join(DECK_TEST_DIR, "vturuh006.out")).valores
    # Conversão simples para conferência apenas do pat. 0
    df_arq = (
        df_arq.groupby(["data", "serie"]).sum(numeric_only=True).reset_index()
    )
    df_arq["patamar"] = "TOTAL"
    df_arq["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("QTUR_UHE", df_meta, True)


def test_sintese_qver_uhe(test_settings):
    synthesis_str = "QVER_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vertuh.read(join(DECK_TEST_DIR, "vertuh006.out")).valores
    # Conversão simples para conferência apenas do pat. 0
    df_arq = (
        df_arq.groupby(["data", "serie"]).sum(numeric_only=True).reset_index()
    )
    df_arq["patamar"] = "TOTAL"
    df_arq["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("QVER_UHE", df_meta, True)


def test_sintese_qret_uhe(test_settings):
    synthesis_str = "QRET_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Desvuh.read(join(DECK_TEST_DIR, "desvuh006.out")).valores
    # Conversão simples para conferência apenas do pat. 0
    df_arq["valor"] *= HM3_M3S_MONTHLY_FACTOR
    df_arq["valor"] = np.round(df_arq["valor"], 2)
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("QRET_UHE", df_meta, True)


def test_sintese_qdes_uhe(test_settings):
    synthesis_str = "QDES_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vdesviouh.read(join(DECK_TEST_DIR, "vdesviouh006.out")).valores
    # Conversão simples para conferência apenas do pat. 0
    df_arq = (
        df_arq.groupby(["data", "serie"]).sum(numeric_only=True).reset_index()
    )
    df_arq["patamar"] = "TOTAL"
    df_arq["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("QDES_UHE", df_meta, True)


# VAFL, VINC para UHE (converter para hm3)


def test_sintese_vafl_uhe(test_settings):
    synthesis_str = "VAFL_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Qafluh.read(join(DECK_TEST_DIR, "qafluh006.out")).valores
    # Conversão simples pois só tem valores por estágio (pat. 0)
    df_arq["valor"] /= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("VAFL_UHE", df_meta, True)


def test_sintese_vinc_uhe(test_settings):
    synthesis_str = "VINC_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Qincruh.read(join(DECK_TEST_DIR, "qincruh006.out")).valores
    # Conversão simples pois só tem valores por estágio (pat. 0)
    df_arq["valor"] /= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("VINC_UHE", df_meta, True)


# QDEF e VDEF para UHE (somar TUR com VER)


def test_sintese_qdef_uhe(test_settings):
    def __calcula_patamar_medio_soma(df: pd.DataFrame) -> pd.DataFrame:
        df_pat0 = df.copy()
        df_pat0 = df_pat0.groupby(["data", "serie"], as_index=False).sum(
            numeric_only=True
        )
        df_pat0["patamar"] = "TOTAL"
        df_pat0 = pd.concat([df, df_pat0], ignore_index=True)
        return df_pat0.sort_values(["data", "serie", "patamar"])

    synthesis_str = "QDEF_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_tur = __calcula_patamar_medio_soma(
        Vturuh.read(join(DECK_TEST_DIR, "vturuh006.out")).valores
    )
    df_ver = __calcula_patamar_medio_soma(
        Vertuh.read(join(DECK_TEST_DIR, "vertuh006.out")).valores
    )
    df_tur["valor"] += df_ver["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0 (média do estágio)
    df_tur["valor"] *= HM3_M3S_MONTHLY_FACTOR

    __compara_sintese_nwlistop(
        df,
        df_tur,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("QDEF_UHE", df_meta, True)


def test_sintese_vdef_uhe(test_settings):
    synthesis_str = "VDEF_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_tur = Vturuh.read(join(DECK_TEST_DIR, "vturuh006.out")).valores
    df_ver = Vertuh.read(join(DECK_TEST_DIR, "vertuh006.out")).valores
    df_tur["valor"] += df_ver["valor"].to_numpy()
    # Conversão simples para conferência apenas do pat. 0
    df_tur = (
        df_tur.groupby(["data", "serie"]).sum(numeric_only=True).reset_index()
    )
    df_tur["patamar"] = "TOTAL"
    __compara_sintese_nwlistop(
        df,
        df_tur,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("VDEF_UHE", df_meta, True)


# VEVAP para UHE (somar VPOSEVAP com VNEGEVAP)


def test_sintese_vevap_uhe(test_settings):
    synthesis_str = "VEVAP_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq_pos = Dposevap.read(
        join(DECK_TEST_DIR, "dpos_evap006.out")
    ).valores.fillna(0.0)
    df_arq_neg = Dnegevap.read(
        join(DECK_TEST_DIR, "dneg_evap006.out")
    ).valores.fillna(0.0)
    df_arq_pos["valor"] += df_arq_neg["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_arq_pos,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("VEVAP_UHE", df_meta, True)


# -----------------------------------------------------------------------------
# Sínteses diretas, sem cálculo de novas variáveis


def test_sintese_vagua_ree(test_settings):
    synthesis_str = "VAGUA_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vagua.read(join(DECK_TEST_DIR, "vagua010.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[10],
        patamar=[0],
    )
    __valida_metadata("VAGUA_REE", df_meta, False)


def test_sintese_vagua_uhe(test_settings):
    synthesis_str = "VAGUA_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Pivarm.read(join(DECK_TEST_DIR, "pivarm006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("VAGUA_UHE", df_meta, False)


def test_sintese_vaguai_uhe(test_settings):
    synthesis_str = "VAGUAI_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Pivarmincr.read(join(DECK_TEST_DIR, "pivarmincr006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("VAGUAI_UHE", df_meta, False)


def test_sintese_cter_sbm(test_settings):
    synthesis_str = "CTER_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Cterm.read(join(DECK_TEST_DIR, "cterm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[1],
        patamar=[0],
    )
    __valida_metadata("CTER_SBM", df_meta, False)


def test_sintese_cter_sin(test_settings):
    synthesis_str = "CTER_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Ctermsin.read(join(DECK_TEST_DIR, "ctermsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("CTER_SIN", df_meta, False)


def test_sintese_coper_sin(test_settings):
    synthesis_str = "COP_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Coper.read(join(DECK_TEST_DIR, "coper.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("COP_SIN", df_meta, False)


def test_sintese_enaa_ree(test_settings):
    synthesis_str = "ENAA_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Eafb.read(join(DECK_TEST_DIR, "eafb001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("ENAA_REE", df_meta, False)


def test_sintese_enaa_sbm(test_settings):
    synthesis_str = "ENAA_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Eafbm.read(join(DECK_TEST_DIR, "eafbm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("ENAA_SBM", df_meta, False)


def test_sintese_enaa_sin(test_settings):
    synthesis_str = "ENAA_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Eafbsin.read(join(DECK_TEST_DIR, "eafbsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("ENAA_SIN", df_meta, False)


def test_sintese_enaar_ree(test_settings):
    synthesis_str = "ENAAR_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Eaf.read(join(DECK_TEST_DIR, "eaf001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("ENAAR_REE", df_meta, False)


def test_sintese_enaar_sbm(test_settings):
    synthesis_str = "ENAAR_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Eafm.read(join(DECK_TEST_DIR, "eafm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("ENAAR_SBM", df_meta, False)


def test_sintese_enaar_sin(test_settings):
    synthesis_str = "ENAAR_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Eafbsin.read(join(DECK_TEST_DIR, "eafmsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("ENAAR_SIN", df_meta, False)


def test_sintese_enaaf_ree(test_settings):
    synthesis_str = "ENAAF_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Eaf.read(join(DECK_TEST_DIR, "efdf001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("ENAAF_REE", df_meta, False)


def test_sintese_enaaf_sbm(test_settings):
    synthesis_str = "ENAAF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Eafm.read(join(DECK_TEST_DIR, "efdfm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("ENAAF_SBM", df_meta, False)


def test_sintese_enaaf_sin(test_settings):
    synthesis_str = "ENAAF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Eafbsin.read(join(DECK_TEST_DIR, "efdfsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("ENAAF_SIN", df_meta, False)


def test_sintese_earpf_ree(test_settings):
    synthesis_str = "EARPF_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Earmfp.read(join(DECK_TEST_DIR, "earmfp001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("EARPF_REE", df_meta, False)


def test_sintese_earpf_sbm(test_settings):
    synthesis_str = "EARPF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Earmfpm.read(join(DECK_TEST_DIR, "earmfpm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("EARPF_SBM", df_meta, False)


def test_sintese_earpf_sin(test_settings):
    synthesis_str = "EARPF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Earmfpsin.read(join(DECK_TEST_DIR, "earmfpsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("EARPF_SIN", df_meta, False)


def test_sintese_earmf_ree(test_settings):
    synthesis_str = "EARMF_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Earmf.read(join(DECK_TEST_DIR, "earmf001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_limites(df, tol=2.0, lower=False)
    __valida_metadata("EARMF_REE", df_meta, False)


def test_sintese_earmf_sbm(test_settings):
    synthesis_str = "EARMF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Earmfm.read(join(DECK_TEST_DIR, "earmfm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_limites(df, tol=2.0, lower=False)
    __valida_metadata("EARMF_SBM", df_meta, False)


def test_sintese_earmf_sin(test_settings):
    synthesis_str = "EARMF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Earmfsin.read(join(DECK_TEST_DIR, "earmfsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_limites(df, tol=2.0, lower=False)
    __valida_metadata("EARMF_SIN", df_meta, False)


def test_sintese_ghidr_ree(test_settings):
    synthesis_str = "GHIDR_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Ghidr.read(join(DECK_TEST_DIR, "ghidr001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("GHIDR_REE", df_meta, False)


def test_sintese_ghidr_sbm(test_settings):
    synthesis_str = "GHIDR_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Ghidrm.read(join(DECK_TEST_DIR, "ghidrm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("GHIDR_SBM", df_meta, False)


def test_sintese_ghidr_sin(test_settings):
    synthesis_str = "GHIDR_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Ghidrsin.read(join(DECK_TEST_DIR, "ghidrsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("GHIDR_SIN", df_meta, False)


def test_sintese_ghidf_ree(test_settings):
    synthesis_str = "GHIDF_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Evert.read(join(DECK_TEST_DIR, "gfiol001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("GHIDF_REE", df_meta, False)


def test_sintese_ghidf_sbm(test_settings):
    synthesis_str = "GHIDF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Evertm.read(join(DECK_TEST_DIR, "gfiolm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("GHIDF_SBM", df_meta, False)


def test_sintese_ghidf_sin(test_settings):
    synthesis_str = "GHIDF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Evertsin.read(join(DECK_TEST_DIR, "gfiolsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("GHIDF_SIN", df_meta, False)


def test_sintese_ghid_ree(test_settings):
    synthesis_str = "GHID_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Ghtot.read(join(DECK_TEST_DIR, "ghtot001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("GHID_REE", df_meta, False)


def test_sintese_ghid_sbm(test_settings):
    synthesis_str = "GHID_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Ghtotm.read(join(DECK_TEST_DIR, "ghtotm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("GHID_SBM", df_meta, False)


def test_sintese_ghid_sin(test_settings):
    synthesis_str = "GHID_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Ghtotsin.read(join(DECK_TEST_DIR, "ghtotsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("GHID_SIN", df_meta, False)


def test_sintese_gter_ute(test_settings):
    synthesis_str = "GTER_UTE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Gtert.read(join(DECK_TEST_DIR, "gtert001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[1],
        classe=[1],
        codigo_usina=[1],
    )
    __valida_limites(df)
    __valida_metadata("GTER_UTE", df_meta, False)


def test_sintese_gter_sbm(test_settings):
    synthesis_str = "GTER_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Gttot.read(join(DECK_TEST_DIR, "gttot001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("GTER_SBM", df_meta, False)


def test_sintese_gter_sin(test_settings):
    synthesis_str = "GTER_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Gttotsin.read(join(DECK_TEST_DIR, "gttotsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_everr_ree(test_settings):
    synthesis_str = "EVERR_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Evert.read(join(DECK_TEST_DIR, "evert001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("EVERR_REE", df_meta, False)


def test_sintese_everr_sbm(test_settings):
    synthesis_str = "EVERR_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Evertm.read(join(DECK_TEST_DIR, "evertm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("EVERR_SBM", df_meta, False)


def test_sintese_everr_sin(test_settings):
    synthesis_str = "EVERR_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Evertsin.read(join(DECK_TEST_DIR, "evertsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("EVERR_SIN", df_meta, False)


def test_sintese_everf_ree(test_settings):
    synthesis_str = "EVERF_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Perdf.read(join(DECK_TEST_DIR, "perdf001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("EVERF_REE", df_meta, False)


def test_sintese_everf_sbm(test_settings):
    synthesis_str = "EVERF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Perdfm.read(join(DECK_TEST_DIR, "perdfm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("EVERF_SBM", df_meta, False)


def test_sintese_everf_sin(test_settings):
    synthesis_str = "EVERF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Perdfsin.read(join(DECK_TEST_DIR, "perdfsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("EVERF_SIN", df_meta, False)


def test_sintese_everft_ree(test_settings):
    synthesis_str = "EVERFT_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Verturb.read(join(DECK_TEST_DIR, "verturb001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("EVERFT_REE", df_meta, False)


def test_sintese_everft_sbm(test_settings):
    synthesis_str = "EVERFT_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Verturbm.read(join(DECK_TEST_DIR, "verturbm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("EVERFT_SBM", df_meta, False)


def test_sintese_everft_sin(test_settings):
    synthesis_str = "EVERFT_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Verturbsin.read(join(DECK_TEST_DIR, "verturbsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("EVERFT_SIN", df_meta, False)


def test_sintese_edesr_ree(test_settings):
    synthesis_str = "EDESR_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Edesvc.read(join(DECK_TEST_DIR, "edesvc001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("EDESR_REE", df_meta, False)


def test_sintese_edesr_sbm(test_settings):
    synthesis_str = "EDESR_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Edesvcm.read(join(DECK_TEST_DIR, "edesvcm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("EDESR_SBM", df_meta, False)


def test_sintese_edesr_sin(test_settings):
    synthesis_str = "EDESR_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Edesvcsin.read(join(DECK_TEST_DIR, "edesvcsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("EDESR_SIN", df_meta, False)


def test_sintese_edesf_ree(test_settings):
    synthesis_str = "EDESF_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Edesvc.read(join(DECK_TEST_DIR, "edesvf001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("EDESF_REE", df_meta, False)


def test_sintese_edesf_sbm(test_settings):
    synthesis_str = "EDESF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Edesvcm.read(join(DECK_TEST_DIR, "edesvfm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("EDESF_SBM", df_meta, False)


def test_sintese_edesf_sin(test_settings):
    synthesis_str = "EDESF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Edesvcsin.read(join(DECK_TEST_DIR, "edesvfsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("EDESF_SIN", df_meta, False)


def test_sintese_mevmin_ree(test_settings):
    synthesis_str = "MEVMIN_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Mevmin.read(join(DECK_TEST_DIR, "mevmin001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("MEVMIN_REE", df_meta, False)


def test_sintese_mevmin_sbm(test_settings):
    synthesis_str = "MEVMIN_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Mevminm.read(join(DECK_TEST_DIR, "mevminm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("MEVMIN_SBM", df_meta, False)


def test_sintese_mevmin_sin(test_settings):
    synthesis_str = "MEVMIN_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Mevminsin.read(join(DECK_TEST_DIR, "mevminsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("MEVMIN_SIN", df_meta, False)


def test_sintese_evmor_ree(test_settings):
    synthesis_str = "EVMOR_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vmort.read(join(DECK_TEST_DIR, "vmort001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("EVMOR_REE", df_meta, False)


def test_sintese_evmor_sbm(test_settings):
    synthesis_str = "EVMOR_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vmortm.read(join(DECK_TEST_DIR, "vmortm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("EVMOR_SBM", df_meta, False)


def test_sintese_evmor_sin(test_settings):
    synthesis_str = "EVMOR_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vmortsin.read(join(DECK_TEST_DIR, "vmortsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("EVMOR_SIN", df_meta, False)


def test_sintese_eevap_ree(test_settings):
    synthesis_str = "EEVAP_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Evapo.read(join(DECK_TEST_DIR, "evapo001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_ree=[1],
    )
    __valida_metadata("EEVAP_REE", df_meta, False)


def test_sintese_eevap_sbm(test_settings):
    synthesis_str = "EEVAP_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Evapom.read(join(DECK_TEST_DIR, "evapom001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("EEVAP_SBM", df_meta, False)


def test_sintese_eevap_sin(test_settings):
    synthesis_str = "EEVAP_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Evaporsin.read(join(DECK_TEST_DIR, "evaporsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("EEVAP_SIN", df_meta, False)


def test_sintese_qafl_uhe(test_settings):
    synthesis_str = "QAFL_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Qafluh.read(join(DECK_TEST_DIR, "qafluh088.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_usina=[88],
    )
    __valida_metadata("QAFL_UHE", df_meta, False)


def test_sintese_qinc_uhe(test_settings):
    synthesis_str = "QINC_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Qincruh.read(join(DECK_TEST_DIR, "qincruh001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_usina=[1],
    )
    __valida_metadata("QINC_UHE", df_meta, False)


def test_sintese_vtur_uhe(test_settings):
    synthesis_str = "VTUR_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vturuh.read(join(DECK_TEST_DIR, "vturuh001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[1],
        codigo_usina=[1],
    )
    __valida_metadata("VTUR_UHE", df_meta, False)


def test_sintese_vver_uhe(test_settings):
    synthesis_str = "VVER_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vertuh.read(join(DECK_TEST_DIR, "vertuh001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[1],
        codigo_usina=[1],
    )
    __valida_metadata("VVER_UHE", df_meta, False)


def test_sintese_varmf_uhe(test_settings):
    synthesis_str = "VARMF_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    with uow:
        df_hidr = uow.files.get_hidr().cadastro
    df_arq = Varmuh.read(join(DECK_TEST_DIR, "varmuh001.out")).valores
    __valida_limites(df)
    # Somente para VARM: subtrai volume mínimo para comparação com nwlistop,
    # que imprime somente volume útil.
    df["valor"] -= df_hidr.at[1, "volume_minimo"]
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_usina=[1],
    )
    __valida_metadata("VARMF_UHE", df_meta, False)


def test_sintese_varpf_uhe(test_settings):
    synthesis_str = "VARPF_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Varmpuh.read(join(DECK_TEST_DIR, "varmpuh001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_usina=[1],
    )
    __valida_metadata("VARPF_UHE", df_meta, False)


def test_sintese_ghid_uhe(test_settings):
    synthesis_str = "GHID_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Ghiduh.read(join(DECK_TEST_DIR, "ghiduh001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[1],
        codigo_usina=[1],
    )
    __valida_metadata("GHID_UHE", df_meta, False)


# TODO - adicionar testes de geração eólica / vento


def test_sintese_def_sbm(test_settings):
    synthesis_str = "DEF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Def.read(join(DECK_TEST_DIR, "def001p001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("DEF_SBM", df_meta, False)


def test_sintese_def_sin(test_settings):
    synthesis_str = "DEF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Defsin.read(join(DECK_TEST_DIR, "defsinp001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("DEF_SIN", df_meta, False)


def test_sintese_exc_sbm(test_settings):
    synthesis_str = "EXC_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Exces.read(join(DECK_TEST_DIR, "exces001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("EXC_SBM", df_meta, False)


def test_sintese_exc_sin(test_settings):
    synthesis_str = "EXC_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Excessin.read(join(DECK_TEST_DIR, "excessin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("EXC_SIN", df_meta, False)


def test_sintese_int_sbp(test_settings):
    synthesis_str = "INT_SBP"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Intercambio.read(join(DECK_TEST_DIR, "int001002.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado_de=[1],
        codigo_submercado_para=[2],
    )
    __valida_limites(df)
    __valida_metadata("INT_SBP", df_meta, False)


def test_sintese_cdef_sbm(test_settings):
    synthesis_str = "CDEF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Cdef.read(join(DECK_TEST_DIR, "cdef001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("CDEF_SBM", df_meta, False)


def test_sintese_cdef_sin(test_settings):
    synthesis_str = "CDEF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Cdefsin.read(join(DECK_TEST_DIR, "cdefsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("CDEF_SIN", df_meta, False)


def test_sintese_merl_sbm(test_settings):
    synthesis_str = "MERL_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Mercl.read(join(DECK_TEST_DIR, "mercl001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        codigo_submercado=[1],
    )
    __valida_metadata("MERL_SBM", df_meta, False)


def test_sintese_merl_sin(test_settings):
    synthesis_str = "MERL_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Merclsin.read(join(DECK_TEST_DIR, "merclsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("MERL_SIN", df_meta, False)


def test_sintese_vfpha_uhe(test_settings):
    synthesis_str = "VFPHA_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Dfphauh.read(join(DECK_TEST_DIR, "dfphauh006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[1],
    )
    __valida_metadata("VFPHA_UHE", df_meta, False)


def test_sintese_vevmin_ree(test_settings):
    synthesis_str = "VEVMIN_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vevmin.read(join(DECK_TEST_DIR, "vevmin001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[1],
        patamar=[0],
    )
    __valida_metadata("VEVMIN_REE", df_meta, False)


def test_sintese_vevmin_sbm(test_settings):
    synthesis_str = "VEVMIN_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vevminm.read(join(DECK_TEST_DIR, "vevminm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[1],
        patamar=[0],
    )
    __valida_metadata("VEVMIN_SBM", df_meta, False)


def test_sintese_vevmin_sin(test_settings):
    synthesis_str = "VEVMIN_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vevminsin.read(join(DECK_TEST_DIR, "vevminsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("VEVMIN_SIN", df_meta, False)


def test_sintese_vret_uhe(test_settings):
    synthesis_str = "VRET_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Desvuh.read(join(DECK_TEST_DIR, "desvuh006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("VRET_UHE", df_meta, False)


def test_sintese_vdes_uhe(test_settings):
    synthesis_str = "VDES_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vdesviouh.read(join(DECK_TEST_DIR, "vdesviouh006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[1],
    )
    __valida_metadata("VDES_UHE", df_meta, False)


def test_sintese_vghmin_uhe(test_settings):
    synthesis_str = "VGHMIN_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vghminuh.read(join(DECK_TEST_DIR, "vghminuh006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[1],
    )
    __valida_metadata("VGHMIN_UHE", df_meta, False)


def test_sintese_vghmin_ree(test_settings):
    synthesis_str = "VGHMIN_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vghmin.read(join(DECK_TEST_DIR, "vghmin001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_ree=[1],
        patamar=[0],
    )
    __valida_metadata("VGHMIN_REE", df_meta, False)


def test_sintese_vghmin_sbm(test_settings):
    synthesis_str = "VGHMIN_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vghminm.read(join(DECK_TEST_DIR, "vghminm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_submercado=[1],
        patamar=[0],
    )
    __valida_metadata("VGHMIN_SBM", df_meta, False)


def test_sintese_vghmin_sin(test_settings):
    synthesis_str = "VGHMIN_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vghminsin.read(join(DECK_TEST_DIR, "vghminsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    __valida_metadata("VGHMIN_SIN", df_meta, False)


def test_sintese_hmon_uhe(test_settings):
    synthesis_str = "HMON_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Hmont.read(join(DECK_TEST_DIR, "hmont006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("HMON_UHE", df_meta, False)


def test_sintese_vevp_uhe(test_settings):
    synthesis_str = "VEVP_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Vevapuh.read(join(DECK_TEST_DIR, "vevapuh006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("VEVP_UHE", df_meta, False)


def test_sintese_vevp_ree(test_settings):
    synthesis_str = "VEVP_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    __valida_metadata("VEVP_REE", df_meta, False)


def test_sintese_vevp_sbm(test_settings):
    synthesis_str = "VEVP_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    __valida_metadata("VEVP_SBM", df_meta, False)


def test_sintese_vevp_sin(test_settings):
    synthesis_str = "VEVP_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    __valida_metadata("VEVP_SIN", df_meta, False)


def test_sintese_vposevap_uhe(test_settings):
    synthesis_str = "VPOSEVAP_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Dposevap.read(join(DECK_TEST_DIR, "dpos_evap006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("VPOSEVAP_UHE", df_meta, False)


def test_sintese_vnegevap_uhe(test_settings):
    synthesis_str = "VNEGEVAP_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_arq = Dnegevap.read(
        join(DECK_TEST_DIR, "dneg_evap006.out")
    ).valores.fillna(0.0)
    __compara_sintese_nwlistop(
        df,
        df_arq,
        data_inicio=datetime(2023, 10, 1),
        cenario=1,
        codigo_usina=[6],
        patamar=[0],
    )
    __valida_metadata("VNEGEVAP_UHE", df_meta, False)


def test_sintese_wildcard_1match(test_settings):
    synthesis_str = "CMO_*"
    df_meta = __sintetiza_com_mock_wildcard(synthesis_str)
    __valida_metadata("CMO_SBM", df_meta, False)


def test_sintese_wildcard_Nmatches(test_settings):
    synthesis_str = "GTER_*"
    df_meta = __sintetiza_com_mock_wildcard(synthesis_str)
    __valida_metadata("GTER_UTE", df_meta, False)
    __valida_metadata("GTER_SBM", df_meta, False)
    __valida_metadata("GTER_SIN", df_meta, False)
    assert df_meta.shape[0] == 3
