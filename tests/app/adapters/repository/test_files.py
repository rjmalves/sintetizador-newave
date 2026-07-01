import itertools
from datetime import datetime
from unittest import mock

import numpy as np
import pandas as pd

from app.adapters.repository.files import (
    _num_scenarios_in_first_year_block,
    factory,
)
from app.model.operation import (
    spatialresolution as operationspatialresolution,
)
from app.model.operation import variable as operationvariable
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


def _build_valores(
    labels_per_year,
    *,
    num_patamares=0,
    entities=(),
    null_months=(),
):
    patamares = list(range(1, num_patamares + 1)) if num_patamares else [None]
    ents = list(entities) if entities else [None]
    records = []
    for year_offset, labels in enumerate(labels_per_year):
        year = 2027 + year_offset
        for month, serie, pat, ent in itertools.product(
            range(1, 13), labels, patamares, ents
        ):
            rec = {
                "data": datetime(year, month, 1),
                "serie": serie,
                "valor": np.nan if month in null_months else 1.0,
            }
            if pat is not None:
                rec["patamar"] = pat
            if ent is not None:
                rec["classe"] = ent
            records.append(rec)
    return pd.DataFrame.from_records(records)


def test_num_scenarios_first_block_synthetic_contiguous():
    df = _build_valores([[1, 2, 3, 4, 5]] * 3, num_patamares=2)
    assert _num_scenarios_in_first_year_block(df) == 5


def test_num_scenarios_first_block_ignores_ring_buffer_labels():
    # Per-block count is 4, but the union of shifting history-year labels is
    # 5 (the trap): the count must not be derived from distinct serie labels.
    df = _build_valores(
        [[1942, 1943, 2023, 1931], [1943, 2023, 1931, 1932]],
        num_patamares=3,
    )
    assert df["serie"].nunique() == 5
    assert _num_scenarios_in_first_year_block(df) == 4


def test_num_scenarios_first_block_midyear_nulls_kept():
    df = _build_valores(
        [[1, 2, 3]] * 2, num_patamares=2, null_months=range(1, 9)
    )
    assert _num_scenarios_in_first_year_block(df) == 3


def test_num_scenarios_first_block_midyear_nulls_dropped():
    df = _build_valores(
        [[1, 2, 3]] * 2, num_patamares=2, null_months=range(1, 9)
    )
    df = df.dropna(subset=["valor"]).reset_index(drop=True)
    assert _num_scenarios_in_first_year_block(df) == 3


def test_num_scenarios_first_block_by_plant_entity():
    df = _build_valores(
        [[1, 2, 3, 4]] * 2, num_patamares=2, entities=(10, 20, 30)
    )
    assert _num_scenarios_in_first_year_block(df) == 4


def test_num_scenarios_first_block_no_patamar():
    df = _build_valores([[1, 2, 3, 4, 5, 6]] * 2)
    assert _num_scenarios_in_first_year_block(df) == 6


def test_get_num_scenarios_from_output(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    assert repo.get_num_scenarios_from_output() == 7


def test_get_nwlistop_captures_scenario_count(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    # Reading any output caches the count, so no dedicated re-read is needed.
    repo.get_nwlistop(
        operationvariable.Variable.CUSTO_MARGINAL_OPERACAO,
        operationspatialresolution.SpatialResolution.SUBMERCADO,
    )
    assert repo.get_num_scenarios_from_output() == 7


def test_probe_call_args_by_resolution(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    sr = operationspatialresolution.SpatialResolution
    assert list(repo._probe_call_args(sr.SISTEMA_INTERLIGADO)) == [("",)]
    assert list(repo._probe_call_args(sr.SUBMERCADO)) == [()]
    assert list(repo._probe_call_args(sr.RESERVATORIO_EQUIVALENTE)) == [()]
    uhe_args = list(repo._probe_call_args(sr.USINA_HIDROELETRICA))
    assert len(uhe_args) == len(repo._probe_hydro_codes())
    assert uhe_args and all(len(a) == 1 for a in uhe_args)


def test_get_num_scenarios_from_output_uhe_fallback(test_settings):
    import app.adapters.repository.files as files_mod

    # Force the per-plant (lowest-priority) branch: it must still find the
    # count by iterating actual hydro codes to a plant that has output.
    only_uhe = [
        (
            operationvariable.Variable.GERACAO_HIDRAULICA,
            operationspatialresolution.SpatialResolution.USINA_HIDROELETRICA,
        ),
    ]
    repo = factory("FS", DECK_TEST_DIR)
    with mock.patch.object(
        files_mod, "_SCENARIO_COUNT_PROBE_CANDIDATES", only_uhe
    ):
        assert repo.get_num_scenarios_from_output() == 7
