from os.path import join
import numpy as np
import pandas as pd
from datetime import datetime
from unittest.mock import patch, MagicMock
from app.services.unitofwork import factory
from app.model.operation.operationsynthesis import OperationSynthesis, UNITS
from app.services.synthesis.operation import OperationSynthetizer
from app.services.deck.bounds import OperationVariableBounds
from app.internal.constants import HM3_M3S_MONTHLY_FACTOR

from inewave.newave import Sistema, Ree, Confhd, Patamar

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
    data = kwargs.get("dataInicio", datetime(2023, 10, 1))
    cenario = kwargs.get("cenario", 1)
    filtros_sintese = (df_sintese["dataInicio"] == data) & (
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
            if col not in ["dataInicio", "cenario"]:
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

    # print(df_sintese.loc[filtros_sintese])
    # print(df_nwlistop.loc[filtros_nwlistop])

    assert np.allclose(
        df_sintese.loc[filtros_sintese, "valor"].to_numpy(),
        df_nwlistop.loc[filtros_nwlistop, "valor"].to_numpy(),
    )


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

    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VTUR_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = __calcula_patamar_medio_soma(
        Vturuh.read(join(DECK_TEST_DIR, "vturuh001.out")).valores
    )

    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        usina=["CAMARGOS"],
    )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VTUR_UHE", df_meta, False)


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

    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GTER_UTE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]

    df_arq = __calcula_patamar_medio_soma_gter_ute(
        Gtert.read(join(DECK_TEST_DIR, "gtert001.out")).valores
    )

    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        usina=["ANGRA 1"],
        classe=[1],
    )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GTER_UTE", df_meta, False)


def test_sintese_cmo_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["CMO_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq_pat = Cmarg.read(join(DECK_TEST_DIR, "cmarg001.out")).valores
    df_arq_med = Cmargmed.read(join(DECK_TEST_DIR, "cmarg001-med.out")).valores
    # Compara CMOs por patamar
    __compara_sintese_nwlistop(
        df,
        df_arq_pat,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUDESTE"],
        patamar=[1, 2, 3],
    )
    # Compara CMO médio do estágio
    __compara_sintese_nwlistop(
        df,
        df_arq_med,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUDESTE"],
        patamar=[0],
    )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("CMO_SBM", df_meta, False)


# -----------------------------------------------------------------------------
# Valida sínteses que são calculadas envolvendo informações de outros arquivos,
# além do próprio arquivo do NWLISTOP.

# EVER para REE, SBM e SIN (soma VERT.TURB com VERT.NTURB)


def test_sintese_ever_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVER_REE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_evert = Evert.read(join(DECK_TEST_DIR, "evert010.out")).valores
    df_evernt = Perdf.read(join(DECK_TEST_DIR, "perdf010.out")).valores
    df_evert["valor"] += df_evernt["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_evert,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["PARANA"],
        patamar=[0],
    )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVER_REE", df_meta, True)


def test_sintese_ever_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVER_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_evert = Evertm.read(join(DECK_TEST_DIR, "evertm001.out")).valores
    df_evernt = Perdfm.read(join(DECK_TEST_DIR, "perdfm001.out")).valores
    df_evert["valor"] += df_evernt["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_evert,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUDESTE"],
        patamar=[0],
    )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVER_SBM", df_meta, True)


def test_sintese_ever_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVER_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[-3].args[0]
    df_evert = Evertsin.read(join(DECK_TEST_DIR, "evertsin.out")).valores
    df_evernt = Perdfsin.read(join(DECK_TEST_DIR, "perdfsin.out")).valores
    df_evert["valor"] += df_evernt["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_evert,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVER_SIN", df_meta, True)


# TODO - VARMI, VARMPI para UHE (consulta pmo.dat e valores de VARMF e VARPF)

# EARM para UHE: calcula produtibilidades acumulando


def test_sintese_earmf_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARMF_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[-3].args[0]
    df.to_csv("teste.csv")
    # raise RuntimeError
    # df_ree = None
    # df_uhes = Confhd.read(join(DECK_TEST_DIR, "confhd.dat")).usinas
    # codigos_uhes = df_uhes.loc[df_uhes["ree"] == 2, "codigo_usina"].unique()
    # with uow:
    #     df_hidr = uow.files.get_hidr().cadastro
    # for uhe in codigos_uhes:
    #     df_uhe = Varmuh.read(
    #         join(DECK_TEST_DIR, f"varmuh{str(uhe).zfill(3)}.out")
    #     ).valores
    #     if df_uhe is None:
    #         continue
    #     if df_ree is None:
    #         df_ree = df_uhe
    #     else:
    #         df_ree["valor"] += df_uhe["valor"].to_numpy()
    #     # Somente para VARM: soma volume mínimo para comparação com síntese,
    #     # que imprime volume total.
    #     df_ree["valor"] += df_hidr.at[uhe, "volume_minimo"]

    # __compara_sintese_nwlistop(
    #     df,
    #     df_ree,
    #     dataInicio=datetime(2023, 10, 1),
    #     cenario=1,
    #     ree=["SUL"],
    #     patamar=[0],
    # )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EARMF_UHE", df_meta, True)


# VFPHA, VRET, VDES, QRET, QDES para REE, SBM e SIN (soma valores das UHEs)


def test_sintese_varmf_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VARMF_REE"], uow)
    m.assert_called()
    df = m.mock_calls[-4].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["SUL"],
        patamar=[0],
    )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VARMF_REE", df_meta, True)


def test_sintese_varmf_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VARMF_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[-4].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUL"],
        patamar=[0],
    )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VARMF_SBM", df_meta, True)


def test_sintese_varmf_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VARMF_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[-4].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VARMF_SIN", df_meta, True)


def test_sintese_vafl_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VAFL_REE"], uow)
    m.assert_called()
    df = m.mock_calls[-4].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["SUL"],
        patamar=[0],
    )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VAFL_REE", df_meta, True)


def test_sintese_vafl_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VAFL_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[-4].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUL"],
        patamar=[0],
    )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VAFL_SBM", df_meta, True)


def test_sintese_vafl_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VAFL_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )

    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VAFL_SIN", df_meta, True)


def test_sintese_qafl_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QAFL_REE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QAFL_REE", df_meta, True)


def test_sintese_qafl_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QAFL_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QAFL_SBM", df_meta, True)


def test_sintese_qafl_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QAFL_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QAFL_SIN", df_meta, True)


def test_sintese_vinc_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VINC_REE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VINC_REE", df_meta, True)


def test_sintese_vinc_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VINC_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VINC_SBM", df_meta, True)


def test_sintese_vinc_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VINC_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VINC_SIN", df_meta, True)


def test_sintese_qinc_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QINC_REE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_REE", df_meta, True)


def test_sintese_qinc_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QINC_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_SBM", df_meta, True)


def test_sintese_qinc_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QINC_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_SIN", df_meta, True)


def test_sintese_vtur_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VTUR_REE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VTUR_REE", df_meta, True)


def test_sintese_vtur_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VTUR_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VTUR_SBM", df_meta, True)


def test_sintese_vtur_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VTUR_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VTUR_SIN", df_meta, True)


def test_sintese_qtur_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QTUR_REE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
    df_ree["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_ree,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QTUR_REE", df_meta, True)


def test_sintese_qtur_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QTUR_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
    df_sbm["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_sbm,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QTUR_SBM", df_meta, True)


def test_sintese_qtur_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QTUR_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
    df_sin["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_sin,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QTUR_SIN", df_meta, True)


def test_sintese_vver_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VVER_REE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VVER_REE", df_meta, True)


def test_sintese_vver_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VVER_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VVER_SBM", df_meta, True)


def test_sintese_vver_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VVER_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VVER_SIN", df_meta, True)


def test_sintese_qver_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QVER_REE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
    df_ree["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_ree,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QVER_REE", df_meta, True)


def test_sintese_qver_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QVER_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
    df_sbm["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_sbm,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUL"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QVER_SBM", df_meta, True)


def test_sintese_qver_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QVER_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
    df_sin["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_sin,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QVER_SIN", df_meta, True)


## EVMIN para REE, SBM e SIN (soma meta de EVMIN com violacao de EVMIN)


def test_sintese_evmin_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVMIN_REE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_mevmin = Mevminm.read(join(DECK_TEST_DIR, "mevmin010.out")).valores
    df_vevmin = Vevminm.read(join(DECK_TEST_DIR, "vevmin010.out")).valores
    df_mevmin["valor"] += df_vevmin["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_mevmin,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["PARANA"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVMIN_REE", df_meta, True)


def test_sintese_evmin_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVMIN_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_mevmin = Mevminm.read(join(DECK_TEST_DIR, "mevminm001.out")).valores
    df_vevmin = Vevminm.read(join(DECK_TEST_DIR, "vevminm001.out")).valores
    df_mevmin["valor"] += df_vevmin["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_mevmin,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUDESTE"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVMIN_SBM", df_meta, True)


def test_sintese_evmin_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVMIN_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_mevmin = Mevminsin.read(join(DECK_TEST_DIR, "mevminsin.out")).valores
    df_vevmin = Vevminsin.read(join(DECK_TEST_DIR, "vevminsin.out")).valores
    df_mevmin["valor"] += df_vevmin["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_mevmin,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVMIN_SIN", df_meta, True)


# HJUS e HLIQ para o patamar MEDIO (média ponderada das durações dos patamares)


def test_sintese_hjus_uhe(test_settings):

    def __adiciona_duracoes_patamares(df: pd.DataFrame) -> pd.DataFrame:
        arq_pat = Patamar.read(join(DECK_TEST_DIR, "patamar.dat"))
        df_pat = arq_pat.duracao_mensal_patamares
        df["duracaoPatamar"] = df.apply(
            lambda linha: df_pat.loc[
                df_pat["data"] == linha["data"], "valor"
            ].iloc[0],
            axis=1,
        )
        return df

    def __calcula_media_ponderada(df: pd.DataFrame) -> pd.DataFrame:
        df_pat0 = df.copy()
        df_pat0["patamar"] = 0
        df_pat0["valor"] = (df_pat0["valor"] * df_pat0["duracaoPatamar"]) / 730
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
        return df_pat0.sort_values(cols_group + ["patamar"])

    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["HJUS_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_arq = __calcula_media_ponderada(
        __adiciona_duracoes_patamares(
            Hjus.read(join(DECK_TEST_DIR, "hjus006.out")).valores
        )
    )
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("HJUS_UHE", df_meta, False)


def test_sintese_hliq_uhe(test_settings):

    def __adiciona_duracoes_patamares(df: pd.DataFrame) -> pd.DataFrame:
        arq_pat = Patamar.read(join(DECK_TEST_DIR, "patamar.dat"))
        df_pat = arq_pat.duracao_mensal_patamares
        df["duracaoPatamar"] = df.apply(
            lambda linha: df_pat.loc[
                df_pat["data"] == linha["data"], "valor"
            ].iloc[0],
            axis=1,
        )
        return df

    def __calcula_media_ponderada(df: pd.DataFrame) -> pd.DataFrame:
        df_pat0 = df.copy()
        df_pat0["patamar"] = 0
        df_pat0["valor"] = (df_pat0["valor"] * df_pat0["duracaoPatamar"]) / 730
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
        return df_pat0.sort_values(cols_group + ["patamar"])

    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["HLIQ_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[-3].args[0]
    df_arq = __calcula_media_ponderada(
        __adiciona_duracoes_patamares(
            Hliq.read(join(DECK_TEST_DIR, "hliq006.out")).valores
        )
    )
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("HLIQ_UHE", df_meta, False)


# QTUR, QVER, QRET, QDES para UHE (converter para m3/s)


def test_sintese_qtur_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QTUR_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_arq = Vturuh.read(join(DECK_TEST_DIR, "vturuh006.out")).valores
    # Conversão simples para conferência apenas do pat. 0
    df_arq["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QTUR_UHE", df_meta, True)


def test_sintese_qver_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QVER_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_arq = Vertuh.read(join(DECK_TEST_DIR, "vertuh006.out")).valores
    # Conversão simples para conferência apenas do pat. 0
    df_arq["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QVER_UHE", df_meta, True)


def test_sintese_qret_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QRET_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_arq = Desvuh.read(join(DECK_TEST_DIR, "desvuh006.out")).valores
    # Conversão simples para conferência apenas do pat. 0
    df_arq["valor"] *= HM3_M3S_MONTHLY_FACTOR
    df_arq["valor"] = np.round(df_arq["valor"], 2)
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QRET_UHE", df_meta, True)


def test_sintese_qdes_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QDES_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_arq = Vdesviouh.read(join(DECK_TEST_DIR, "vdesviouh006.out")).valores
    # Conversão simples para conferência apenas do pat. 0
    df_arq["valor"] *= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QDES_UHE", df_meta, True)


# VAFL, VINC para UHE (converter para hm3)


def test_sintese_vafl_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VAFL_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_arq = Qafluh.read(join(DECK_TEST_DIR, "qafluh006.out")).valores
    # Conversão simples pois só tem valores por estágio (pat. 0)
    df_arq["valor"] /= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VAFL_UHE", df_meta, True)


def test_sintese_vinc_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VINC_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_arq = Qincruh.read(join(DECK_TEST_DIR, "qincruh006.out")).valores
    # Conversão simples pois só tem valores por estágio (pat. 0)
    df_arq["valor"] /= HM3_M3S_MONTHLY_FACTOR
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
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

    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QDEF_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QDEF_UHE", df_meta, True)


def test_sintese_vdef_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VDEF_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[-2].args[0]
    df_tur = Vturuh.read(join(DECK_TEST_DIR, "vturuh006.out")).valores
    df_ver = Vertuh.read(join(DECK_TEST_DIR, "vertuh006.out")).valores
    df_tur["valor"] += df_ver["valor"].to_numpy()
    __compara_sintese_nwlistop(
        df,
        df_tur,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VDEF_UHE", df_meta, True)


# VEVAP para UHE (somar VPOSEVAP com VNEGEVAP)


def test_sintese_vevap_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VEVAP_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
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
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VEVAP_UHE", df_meta, True)


# -----------------------------------------------------------------------------
# Sínteses diretas, sem cálculo de novas variáveis


def test_sintese_vagua_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VAGUA_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vagua.read(join(DECK_TEST_DIR, "vagua010.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["PARANA"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VAGUA_REE", df_meta, False)


def test_sintese_vagua_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VAGUA_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Pivarm.read(join(DECK_TEST_DIR, "pivarm006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VAGUA_UHE", df_meta, False)


def test_sintese_vaguai_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VAGUAI_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Pivarmincr.read(join(DECK_TEST_DIR, "pivarmincr006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VAGUAI_UHE", df_meta, False)


def test_sintese_cter_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["CTER_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Cterm.read(join(DECK_TEST_DIR, "cterm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUDESTE"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("CTER_SBM", df_meta, False)


def test_sintese_cter_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["CTER_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Ctermsin.read(join(DECK_TEST_DIR, "ctermsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("CTER_SIN", df_meta, False)


def test_sintese_coper_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["COP_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Coper.read(join(DECK_TEST_DIR, "coper.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("COP_SIN", df_meta, False)


def test_sintese_enaa_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["ENAA_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Eafb.read(join(DECK_TEST_DIR, "eafb001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAA_REE", df_meta, False)


def test_sintese_enaa_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["ENAA_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Eafbm.read(join(DECK_TEST_DIR, "eafbm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAA_SBM", df_meta, False)


def test_sintese_enaa_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["ENAA_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Eafbsin.read(join(DECK_TEST_DIR, "eafbsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAA_SIN", df_meta, False)


def test_sintese_enaar_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["ENAAR_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Eaf.read(join(DECK_TEST_DIR, "eaf001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAAR_REE", df_meta, False)


def test_sintese_enaar_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["ENAAR_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Eafm.read(join(DECK_TEST_DIR, "eafm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAAR_SBM", df_meta, False)


def test_sintese_enaar_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["ENAAR_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Eafbsin.read(join(DECK_TEST_DIR, "eafmsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAAR_SIN", df_meta, False)


def test_sintese_enaaf_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["ENAAF_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Eaf.read(join(DECK_TEST_DIR, "efdf001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAAF_REE", df_meta, False)


def test_sintese_enaaf_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["ENAAF_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Eafm.read(join(DECK_TEST_DIR, "efdfm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAAF_SBM", df_meta, False)


def test_sintese_enaaf_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["ENAAF_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Eafbsin.read(join(DECK_TEST_DIR, "efdfsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("ENAAF_SIN", df_meta, False)


def test_sintese_earpf_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARPF_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Earmfp.read(join(DECK_TEST_DIR, "earmfp001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EARPF_REE", df_meta, False)


def test_sintese_earpf_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARPF_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Earmfpm.read(join(DECK_TEST_DIR, "earmfpm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EARPF_SBM", df_meta, False)


def test_sintese_earpf_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARPF_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Earmfpsin.read(join(DECK_TEST_DIR, "earmfpsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EARPF_SIN", df_meta, False)


def test_sintese_earmf_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARMF_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Earmf.read(join(DECK_TEST_DIR, "earmf001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EARMF_REE", df_meta, False)


def test_sintese_earmf_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARMF_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Earmfm.read(join(DECK_TEST_DIR, "earmfm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EARMF_SBM", df_meta, False)


def test_sintese_earmf_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EARMF_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Earmfsin.read(join(DECK_TEST_DIR, "earmfsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EARMF_SIN", df_meta, False)


def test_sintese_ghidr_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHIDR_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Ghidr.read(join(DECK_TEST_DIR, "ghidr001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GHIDR_REE", df_meta, False)


def test_sintese_ghidr_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHIDR_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Ghidrm.read(join(DECK_TEST_DIR, "ghidrm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GHIDR_SBM", df_meta, False)


def test_sintese_ghidr_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHIDR_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Ghidrsin.read(join(DECK_TEST_DIR, "ghidrsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GHIDR_SIN", df_meta, False)


def test_sintese_ghidf_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHIDF_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Evert.read(join(DECK_TEST_DIR, "gfiol001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GHIDF_REE", df_meta, False)


def test_sintese_ghidf_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHIDF_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Evertm.read(join(DECK_TEST_DIR, "gfiolm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GHIDF_SBM", df_meta, False)


def test_sintese_ghidf_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHIDF_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Evertsin.read(join(DECK_TEST_DIR, "gfiolsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GHIDF_SIN", df_meta, False)


def test_sintese_ghid_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHID_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Ghtot.read(join(DECK_TEST_DIR, "ghtot001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GHID_REE", df_meta, False)


def test_sintese_ghid_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHID_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Ghtotm.read(join(DECK_TEST_DIR, "ghtotm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GHID_SBM", df_meta, False)


def test_sintese_ghid_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHID_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Ghtotsin.read(join(DECK_TEST_DIR, "ghtotsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GHID_SIN", df_meta, False)


def test_sintese_gter_ute(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GTER_UTE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    print(df)
    df_arq = Gtert.read(join(DECK_TEST_DIR, "gtert001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[1],
        classe=[1],
        usina=["ANGRA 1"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GTER_UTE", df_meta, False)


def test_sintese_gter_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GTER_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Gttot.read(join(DECK_TEST_DIR, "gttot001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GTER_SBM", df_meta, False)


def test_sintese_gter_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GTER_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Gttotsin.read(join(DECK_TEST_DIR, "gttotsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GTER_SIN", df_meta, False)


def test_sintese_everr_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVERR_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Evert.read(join(DECK_TEST_DIR, "evert001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVERR_REE", df_meta, False)


def test_sintese_everr_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVERR_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Evertm.read(join(DECK_TEST_DIR, "evertm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVERR_SBM", df_meta, False)


def test_sintese_everr_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVERR_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Evertsin.read(join(DECK_TEST_DIR, "evertsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVERR_SIN", df_meta, False)


def test_sintese_everf_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVERF_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Perdf.read(join(DECK_TEST_DIR, "perdf001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVERF_REE", df_meta, False)


def test_sintese_everf_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVERF_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Perdfm.read(join(DECK_TEST_DIR, "perdfm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVERF_SBM", df_meta, False)


def test_sintese_everf_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVERF_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Perdfsin.read(join(DECK_TEST_DIR, "perdfsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVERF_SIN", df_meta, False)


def test_sintese_everft_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVERFT_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Verturb.read(join(DECK_TEST_DIR, "verturb001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVERFT_REE", df_meta, False)


def test_sintese_everft_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVERFT_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Verturbm.read(join(DECK_TEST_DIR, "verturbm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVERFT_SBM", df_meta, False)


def test_sintese_everft_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVERFT_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Verturbsin.read(join(DECK_TEST_DIR, "verturbsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVERFT_SIN", df_meta, False)


def test_sintese_edesr_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EDESR_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Edesvc.read(join(DECK_TEST_DIR, "edesvc001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EDESR_REE", df_meta, False)


def test_sintese_edesr_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EDESR_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Edesvcm.read(join(DECK_TEST_DIR, "edesvcm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EDESR_SBM", df_meta, False)


def test_sintese_edesr_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EDESR_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Edesvcsin.read(join(DECK_TEST_DIR, "edesvcsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EDESR_SIN", df_meta, False)


def test_sintese_edesf_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EDESF_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Edesvc.read(join(DECK_TEST_DIR, "edesvf001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EDESF_REE", df_meta, False)


def test_sintese_edesf_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EDESF_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Edesvcm.read(join(DECK_TEST_DIR, "edesvfm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EDESF_SBM", df_meta, False)


def test_sintese_edesf_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EDESF_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Edesvcsin.read(join(DECK_TEST_DIR, "edesvfsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EDESF_SIN", df_meta, False)


def test_sintese_mevmin_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["MEVMIN_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Mevmin.read(join(DECK_TEST_DIR, "mevmin001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("MEVMIN_REE", df_meta, False)


def test_sintese_mevmin_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["MEVMIN_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Mevminm.read(join(DECK_TEST_DIR, "mevminm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("MEVMIN_SBM", df_meta, False)


def test_sintese_mevmin_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["MEVMIN_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Mevminsin.read(join(DECK_TEST_DIR, "mevminsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("MEVMIN_SIN", df_meta, False)


def test_sintese_evmor_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVMOR_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vmort.read(join(DECK_TEST_DIR, "vmort001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVMOR_REE", df_meta, False)


def test_sintese_evmor_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVMOR_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vmortm.read(join(DECK_TEST_DIR, "vmortm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVMOR_SBM", df_meta, False)


def test_sintese_evmor_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EVMOR_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vmortsin.read(join(DECK_TEST_DIR, "vmortsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EVMOR_SIN", df_meta, False)


def test_sintese_eevap_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EEVAP_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Evapo.read(join(DECK_TEST_DIR, "evapo001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        ree=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EEVAP_REE", df_meta, False)


def test_sintese_eevap_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EEVAP_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Evapom.read(join(DECK_TEST_DIR, "evapom001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EEVAP_SBM", df_meta, False)


def test_sintese_eevap_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EEVAP_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Evaporsin.read(join(DECK_TEST_DIR, "evaporsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EEVAP_SIN", df_meta, False)


def test_sintese_qafl_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QAFL_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Qafluh.read(join(DECK_TEST_DIR, "qafluh001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        usina=["CAMARGOS"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QAFL_UHE", df_meta, False)


def test_sintese_qinc_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["QINC_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Qincruh.read(join(DECK_TEST_DIR, "qincruh001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        usina=["CAMARGOS"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("QINC_UHE", df_meta, False)


def test_sintese_vtur_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VTUR_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vturuh.read(join(DECK_TEST_DIR, "vturuh001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[1],
        usina=["CAMARGOS"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VTUR_UHE", df_meta, False)


def test_sintese_vver_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VVER_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vertuh.read(join(DECK_TEST_DIR, "vertuh001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[1],
        usina=["CAMARGOS"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VVER_UHE", df_meta, False)


def test_sintese_varmf_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VARMF_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]

    # Somente para VARM: subtrai volume mínimo para comparação com nwlistop,
    # que imprime somente volume útil.
    with uow:
        df_hidr = uow.files.get_hidr().cadastro
    df["valor"] -= df_hidr.at[1, "volume_minimo"]
    df_arq = Varmuh.read(join(DECK_TEST_DIR, "varmuh001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        usina=["CAMARGOS"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VARMF_UHE", df_meta, False)


def test_sintese_varpf_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VARPF_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Varmpuh.read(join(DECK_TEST_DIR, "varmpuh001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        usina=["CAMARGOS"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VARPF_UHE", df_meta, False)


def test_sintese_ghid_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GHID_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Ghiduh.read(join(DECK_TEST_DIR, "ghiduh001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[1],
        usina=["CAMARGOS"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GHID_UHE", df_meta, False)


# TODO - adicionar testes de geração eólica / vento


def test_sintese_def_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["DEF_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Def.read(join(DECK_TEST_DIR, "def001p001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("DEF_SBM", df_meta, False)


def test_sintese_def_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["DEF_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Defsin.read(join(DECK_TEST_DIR, "defsinp001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("DEF_SIN", df_meta, False)


def test_sintese_exc_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EXC_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Exces.read(join(DECK_TEST_DIR, "exces001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EXC_SBM", df_meta, False)


def test_sintese_exc_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["EXC_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Excessin.read(join(DECK_TEST_DIR, "excessin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("EXC_SIN", df_meta, False)


def test_sintese_int_sbp(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["INT_SBP"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Intercambio.read(join(DECK_TEST_DIR, "int001002.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercadoDe=["SUDESTE"],
        submercadoPara=["SUL"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("INT_SBP", df_meta, False)


def test_sintese_cdef_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["CDEF_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Cdef.read(join(DECK_TEST_DIR, "cdef001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("CDEF_SBM", df_meta, False)


def test_sintese_cdef_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["CDEF_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Cdefsin.read(join(DECK_TEST_DIR, "cdefsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("CDEF_SIN", df_meta, False)


def test_sintese_merl_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["MERL_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Mercl.read(join(DECK_TEST_DIR, "mercl001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
        submercado=["SUDESTE"],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("MERL_SBM", df_meta, False)


def test_sintese_merl_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["MERL_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Merclsin.read(join(DECK_TEST_DIR, "merclsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("MERL_SIN", df_meta, False)


def test_sintese_vfpha_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VFPHA_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Dfphauh.read(join(DECK_TEST_DIR, "dfphauh006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[1],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VFPHA_UHE", df_meta, False)


def test_sintese_vevmin_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VEVMIN_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vevmin.read(join(DECK_TEST_DIR, "vevmin001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["SUDESTE"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VEVMIN_REE", df_meta, False)


def test_sintese_vevmin_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VEVMIN_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vevminm.read(join(DECK_TEST_DIR, "vevminm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUDESTE"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VEVMIN_SBM", df_meta, False)


def test_sintese_vevmin_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VEVMIN_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vevminsin.read(join(DECK_TEST_DIR, "vevminsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VEVMIN_SIN", df_meta, False)


def test_sintese_vret_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VRET_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Desvuh.read(join(DECK_TEST_DIR, "desvuh006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VRET_UHE", df_meta, False)


def test_sintese_vdes_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VDES_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vdesviouh.read(join(DECK_TEST_DIR, "vdesviouh006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[1],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VDES_UHE", df_meta, False)


def test_sintese_vghmin_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VGHMIN_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vghminuh.read(join(DECK_TEST_DIR, "vghminuh006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[1],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VGHMIN_UHE", df_meta, False)


def test_sintese_vghmin_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VGHMIN_REE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vghmin.read(join(DECK_TEST_DIR, "vghmin001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        ree=["SUDESTE"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VGHMIN_REE", df_meta, False)


def test_sintese_vghmin_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VGHMIN_SBM"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vghminm.read(join(DECK_TEST_DIR, "vghminm001.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        submercado=["SUDESTE"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VGHMIN_SBM", df_meta, False)


def test_sintese_vghmin_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VGHMIN_SIN"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vghminsin.read(join(DECK_TEST_DIR, "vghminsin.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VGHMIN_SIN", df_meta, False)


def test_sintese_hmon_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["HMON_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Hmont.read(join(DECK_TEST_DIR, "hmont006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("HMON_UHE", df_meta, False)


def test_sintese_vevp_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VEVP_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Vevapuh.read(join(DECK_TEST_DIR, "vevapuh006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VEVP_UHE", df_meta, False)


def test_sintese_vposevap_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VPOSEVAP_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Dposevap.read(join(DECK_TEST_DIR, "dpos_evap006.out")).valores
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VPOSEVAP_UHE", df_meta, False)


def test_sintese_vnegevap_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["VNEGEVAP_UHE"], uow)
    m.assert_called()
    df = m.mock_calls[0].args[0]
    df_arq = Dnegevap.read(
        join(DECK_TEST_DIR, "dneg_evap006.out")
    ).valores.fillna(0.0)
    __compara_sintese_nwlistop(
        df,
        df_arq,
        dataInicio=datetime(2023, 10, 1),
        cenario=1,
        usina=["FURNAS"],
        patamar=[0],
    )
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("VNEGEVAP_UHE", df_meta, False)


def test_sintese_wildcard_1match(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["CMO_*"], uow)
    m.assert_called()
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("CMO_SBM", df_meta, False)


def test_sintese_wildcard_Nmatches(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize(["GTER_*"], uow)
    m.assert_called()
    df_meta = m.mock_calls[-1].args[0]
    __valida_metadata("GTER_UTE", df_meta, False)
    __valida_metadata("GTER_SBM", df_meta, False)
    __valida_metadata("GTER_SIN", df_meta, False)
    assert df_meta.shape[0] == 3