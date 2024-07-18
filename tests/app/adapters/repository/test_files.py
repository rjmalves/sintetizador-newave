from app.adapters.repository.files import factory
from app.model.operation import variable as operationvariable
from app.model.operation import (
    spatialresolution as operationspatialresolution,
)


import pandas as pd

from tests.conftest import DECK_TEST_DIR


def test_get_dger(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    dger = repo.get_dger()
    assert dger.nome_caso == "Caso Teste"


def test_get_clast(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    clast = repo.get_clast()
    assert isinstance(clast.usinas, pd.DataFrame)


def test_get_conft(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    conft = repo.get_conft()
    assert isinstance(conft.usinas, pd.DataFrame)


def test_get_confhd(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    confhd = repo.get_confhd()
    assert isinstance(confhd.usinas, pd.DataFrame)


def test_get_ree(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    ree = repo.get_ree()
    assert isinstance(ree.rees, pd.DataFrame)


def test_get_sistema(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    sistema = repo.get_sistema()
    assert isinstance(sistema.geracao_usinas_nao_simuladas, pd.DataFrame)


def test_get_patamar(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    patamar = repo.get_patamar()
    assert isinstance(patamar.usinas_nao_simuladas, pd.DataFrame)


def test_get_newavetim(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    newavetim = repo.get_newavetim()
    assert isinstance(newavetim.tempos_etapas, pd.DataFrame)


# TODO - retornar com os testes de eólica
# def test_get_eolica(test_settings):
#     repo = factory("FS", DECK_TEST_DIR)
#     eol = repo.get_eolica()
#     assert isinstance(eol.pee_cad(), list)


def test_get_pmo(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    pmo = repo.get_pmo()
    assert isinstance(pmo.convergencia, pd.DataFrame)
    assert isinstance(pmo.energia_armazenada_inicial, pd.DataFrame)
    assert isinstance(pmo.volume_armazenado_inicial, pd.DataFrame)
    assert isinstance(
        pmo.custo_operacao_referenciado_primeiro_mes, pd.DataFrame
    )


def test_get_hidr(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    hidr = repo.get_hidr()
    assert isinstance(hidr.cadastro, pd.DataFrame)


def test_get_cortes(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    cortes = repo.get_nwlistcf_cortes()
    assert isinstance(cortes.cortes, pd.DataFrame)


def test_get_estados(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    estados = repo.get_nwlistcf_estados()
    assert isinstance(estados.estados, pd.DataFrame)


def test_get_energiaf(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    energiaf = repo.get_energiaf(1)
    assert isinstance(energiaf.series, pd.DataFrame)


def test_get_energiab(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    energiab = repo.get_energiab(1)
    assert isinstance(energiab.series, pd.DataFrame)


def test_get_energias(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    energias = repo.get_energias()
    assert isinstance(energias.series, pd.DataFrame)


def test_get_enavazf(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    enavazf = repo.get_enavazf(1)
    assert isinstance(enavazf.series, pd.DataFrame)


def test_get_enavazb(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    enavazb = repo.get_enavazb(1)
    assert isinstance(enavazb.series, pd.DataFrame)


def test_get_enavazs(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    enavazs = repo.get_enavazs()
    assert isinstance(enavazs.series, pd.DataFrame)


def test_get_vazaof(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    vazaof = repo.get_vazaof(1)
    assert isinstance(vazaof.series, pd.DataFrame)


def test_get_vazaob(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    vazaob = repo.get_vazaob(1)
    assert isinstance(vazaob.series, pd.DataFrame)


def test_get_vazaos(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    vazaos = repo.get_vazaos()
    assert isinstance(vazaos.series, pd.DataFrame)


def test_get_vazoes(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    vazoes = repo.get_vazoes()
    assert isinstance(vazoes.vazoes, pd.DataFrame)


def test_get_engnat(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    engnat = repo.get_engnat()
    assert isinstance(engnat.series, pd.DataFrame)


def test_get_nwlistop(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.CUSTO_MARGINAL_OPERACAO,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VALOR_AGUA,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VALOR_AGUA,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VALOR_AGUA_INCREMENTAL,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.CUSTO_GERACAO_TERMICA,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.CUSTO_GERACAO_TERMICA,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.CUSTO_OPERACAO,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_HIDRAULICA,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_HIDRAULICA,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_HIDRAULICA,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_HIDRAULICA_RESERVATORIO,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_HIDRAULICA_RESERVATORIO,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_HIDRAULICA_RESERVATORIO,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_HIDRAULICA_FIO,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_HIDRAULICA_FIO,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_HIDRAULICA_FIO,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_TERMICA,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_TERMICA,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_TERMICA,
            operationspatialresolution.SpatialResolution.USINA_TERMELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_VERTIDA_RESERV,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_VERTIDA_RESERV,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_VERTIDA_RESERV,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_VERTIDA_FIO,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_VERTIDA_FIO,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_VERTIDA_FIO,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_DESVIO_RESERVATORIO,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_DESVIO_RESERVATORIO,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_DESVIO_RESERVATORIO,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_DESVIO_FIO,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_DESVIO_FIO,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_DESVIO_FIO,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_VOLUME_MORTO,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_VOLUME_MORTO,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_VOLUME_MORTO,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_EVAPORACAO,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_EVAPORACAO,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.ENERGIA_EVAPORACAO,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VAZAO_AFLUENTE,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VAZAO_INCREMENTAL,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VAZAO_TURBINADA,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VAZAO_VERTIDA,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.GERACAO_HIDRAULICA,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    # TODO - retornar com testes de vento
    # assert isinstance(
    #     repo.get_nwlistop(
    #         operationvariable.Variable.VELOCIDADE_VENTO,
    #         operationspatialresolution.SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
    #     ),
    #     pd.DataFrame,
    # )
    # assert isinstance(
    #     repo.get_nwlistop(
    #         operationvariable.Variable.GERACAO_EOLICA,
    #         operationspatialresolution.SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
    #     ),
    #     pd.DataFrame,
    # )
    # assert isinstance(
    #     repo.get_nwlistop(
    #         operationvariable.Variable.GERACAO_EOLICA,
    #         operationspatialresolution.SpatialResolution.SUBMERCADO,
    #     ),
    #     pd.DataFrame,
    # )
    # assert isinstance(
    #     repo.get_nwlistop(
    #         operationvariable.Variable.GERACAO_EOLICA,
    #         operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
    #         None,
    #     ),
    #     pd.DataFrame,
    # )
    # assert isinstance(
    #     repo.get_nwlistop(
    #         operationvariable.Variable.GERACAO_EOLICA,
    #         operationspatialresolution.SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
    #     ),
    #     pd.DataFrame,
    # )
    # assert isinstance(
    #     repo.get_nwlistop(
    #         operationvariable.Variable.GERACAO_EOLICA,
    #         operationspatialresolution.SpatialResolution.SUBMERCADO,
    #     ),
    #     pd.DataFrame,
    # )
    # assert isinstance(
    #     repo.get_nwlistop(
    #         operationvariable.Variable.GERACAO_EOLICA,
    #         operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
    #         None,
    #     ),
    #     pd.DataFrame,
    # )
    # assert isinstance(
    #     repo.get_nwlistop(
    #         operationvariable.Variable.CORTE_GERACAO_EOLICA,
    #         operationspatialresolution.SpatialResolution.SUBMERCADO,
    #     ),
    #     pd.DataFrame,
    # )
    # assert isinstance(
    #     repo.get_nwlistop(
    #         operationvariable.Variable.CORTE_GERACAO_EOLICA,
    #         operationspatialresolution.SpatialResolution.SUBMERCADO,
    #     ),
    #     pd.DataFrame,
    # )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.DEFICIT,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.DEFICIT,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.EXCESSO,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.EXCESSO,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.INTERCAMBIO,
            operationspatialresolution.SpatialResolution.PAR_SUBMERCADOS,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.CUSTO_DEFICIT,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.CUSTO_DEFICIT,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.MERCADO_LIQUIDO,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.MERCADO_LIQUIDO,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VIOLACAO_FPHA,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VOLUME_RETIRADO,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VAZAO_DESVIADA,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
            operationspatialresolution.SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
            operationspatialresolution.SpatialResolution.SUBMERCADO,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
            operationspatialresolution.SpatialResolution.SISTEMA_INTERLIGADO,
            None,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.COTA_MONTANTE,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.COTA_JUSANTE,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
    assert isinstance(
        repo.get_nwlistop(
            operationvariable.Variable.QUEDA_LIQUIDA,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
        pd.DataFrame,
    )
