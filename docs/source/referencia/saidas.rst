.. _comandos:

Saídas
=========


Arquivos de Saída
-----------------------

Os arquivos de saída das sínteses são armazenados na pasta `sintese` do diretório de trabalho. Para cada síntese realizada, é configurados
um arquivo com metadados e um conjunto de arquivos com os dados sintetizados. Para as sínteses da operação e de cenários, além dos arquivos
com os dados brutos sintetizados, são criados arquivos com estatísticas pré-calculadas sobre os dados brutos,
permitindo análises mais rápidas.

No caso de uma síntese do sistema, são esperados os arquivos::

    $ ls sintese
    >>> CVU.parquet
    >>> EST.parquet
    >>> METADADOS_SISTEMA.parquet
    >>> PAT.parquet
    >>> REE.parquet
    >>> SBM.parquet
    >>> UHE.parquet
    >>> UTE.parquet

Para a síntese da execução::

    $ ls sintese
    >>> CONVERGENCIA.parquet
    >>> CUSTOS.parquet
    >>> METADADOS_EXECUCAO.parquet
    >>> PROGRAMA.parquet
    >>> TEMPO.parquet

Para a síntese da política::

    $ ls sintese
    >>> CORTES.parquet
    >>> ESTADOS.parquet
    >>> METADADOS_POLITICA.parquet

Alguns dos arquivos esperados na síntese de cenários::

    $ ls sintese
    >>> ENAA_REE_BKW.parquet
    >>> ENAA_REE_FOR.parquet
    >>> ENAA_REE_SF.parquet
    >>> ...
    >>> ESTATISTICAS_CENARIOS_REE_BKW.parquet
    >>> ESTATISTICAS_CENARIOS_REE_FOR.parquet
    >>> ESTATISTICAS_CENARIOS_REE_SF.parquet
    >>> ESTATISTICAS_CENARIOS_UHE_BKW.parquet
    >>> ESTATISTICAS_CENARIOS_UHE_FOR.parquet
    >>> ...
    >>> METADADOS_CENARIOS.parquet
    >>> QINC_UHE_BKW.parquet
    >>> QINC_UHE_FOR.parquet
    >>> ...

Alguns dos arquivos esperados na síntese da operação::

    $ ls sintese
    >>> CDEF_SBM.parquet
    >>> CDEF_SIN.parquet
    >>> CMO_SBM.parquet
    >>> COP_SIN.parquet
    >>> CTER_SBM.parquet
    >>> CTER_SIN.parquet
    >>> ...
    >>> ESTATISTICAS_OPERACAO_REE.parquet
    >>> ESTATISTICAS_OPERACAO_SBM.parquet
    >>> ESTATISTICAS_OPERACAO_SBP.parquet
    >>> ESTATISTICAS_OPERACAO_SIN.parquet
    >>> ESTATISTICAS_OPERACAO_UHE.parquet
    >>> ESTATISTICAS_OPERACAO_UTE.parquet
    >>> EVERFT_REE.parquet
    >>> EVERFT_SBM.parquet
    >>> ...
    >>> GHID_REE.parquet
    >>> GHID_SBM.parquet
    >>> GHID_SIN.parquet
    >>> GHID_UHE.parquet
    >>> GTER_SBM.parquet
    >>> GTER_SIN.parquet
    >>> GTER_UTE.parquet
    >>> HJUS_UHE.parquet
    >>> HLIQ_UHE.parquet
    >>> HMON_UHE.parquet
    >>> INT_SBP.parquet
    >>> MERL_SBM.parquet
    >>> MERL_SIN.parquet
    >>> ...
    >>> METADADOS_OPERACAO.parquet
    >>> QAFL_UHE.parquet
    >>> QDEF_REE.parquet
    >>> QDEF_SBM.parquet
    >>> ...
    >>> VARMF_UHE.parquet
    >>> VARMI_REE.parquet
    >>> VARMI_SBM.parquet
    >>> ...


Formato dos Metadados
-----------------------

As sínteses realizadas são armazenadas em arquivos de metadados, que também são DataFrames, no mesmo formato que foi solicitado para a saída da síntese (por padrão é utilizado o `parquet`).

Os metadados são armazenados em arquivos com o prefixo `METADADOS_` e o nome da síntese. Por exemplo, para a síntese do sistema, os metadados são armazenados em `METADADOS_SISTEMA.parquet`.

Por exemplo, em uma síntese da operação, os metadados podem ser acessados como:


.. code-block:: python

    import polars as pl
    meta_df = pl.read_parquet("sintese/METADADOS_OPERACAO.parquet")
    print(meta_df)

.. code-block:: none

    shape: (169, 9)
    ┌────────────┬────────────┬───────────┬───────────┬───┬─────────┬───────────┬──────────┬───────────┐
    │ chave      ┆ nome_curto ┆ nome_long ┆ nome_curt ┆ … ┆ unidade ┆ calculado ┆ limitado ┆ __index_l │
    │ ---        ┆ _variavel  ┆ o_variave ┆ o_agregac ┆   ┆ ---     ┆ ---       ┆ ---      ┆ evel_0__  │
    │ str        ┆ ---        ┆ l         ┆ ao        ┆   ┆ str     ┆ bool      ┆ bool     ┆ ---       │
    │            ┆ str        ┆ ---       ┆ ---       ┆   ┆         ┆           ┆          ┆ i64       │
    │            ┆            ┆ str       ┆ str       ┆   ┆         ┆           ┆          ┆           │
    ╞════════════╪════════════╪═══════════╪═══════════╪═══╪═════════╪═══════════╪══════════╪═══════════╡
    │ CMO_SBM    ┆ CMO        ┆ Custo     ┆ SBM       ┆ … ┆ R$/MWh  ┆ false     ┆ false    ┆ 0         │
    │            ┆            ┆ Marginal  ┆           ┆   ┆         ┆           ┆          ┆           │
    │            ┆            ┆ de        ┆           ┆   ┆         ┆           ┆          ┆           │
    │            ┆            ┆ Operação  ┆           ┆   ┆         ┆           ┆          ┆           │
    │ VAGUA_REE  ┆ VAGUA      ┆ Valor da  ┆ REE       ┆ … ┆ R$/MWh  ┆ false     ┆ false    ┆ 1         │
    │            ┆            ┆ Água      ┆           ┆   ┆         ┆           ┆          ┆           │
    │ VAGUA_UHE  ┆ VAGUA      ┆ Valor da  ┆ UHE       ┆ … ┆ R$/hm3  ┆ false     ┆ false    ┆ 2         │
    │            ┆            ┆ Água      ┆           ┆   ┆         ┆           ┆          ┆           │
    │ VAGUAI_UHE ┆ VAGUA Incr ┆ Valor da  ┆ UHE       ┆ … ┆ R$/hm3  ┆ false     ┆ false    ┆ 3         │
    │            ┆ emental    ┆ Água Incr ┆           ┆   ┆         ┆           ┆          ┆           │
    │            ┆            ┆ emental   ┆           ┆   ┆         ┆           ┆          ┆           │
    │ CTER_SBM   ┆ Custo de   ┆ Custo de  ┆ SBM       ┆ … ┆ 10^6 R$ ┆ false     ┆ false    ┆ 4         │
    │            ┆ GT         ┆ Geração   ┆           ┆   ┆         ┆           ┆          ┆           │
    │            ┆            ┆ Térmica   ┆           ┆   ┆         ┆           ┆          ┆           │
    │ …          ┆ …          ┆ …         ┆ …         ┆ … ┆ …       ┆ …         ┆ …        ┆ …         │
    │ VEVAP_UHE  ┆ Violação   ┆ Violação  ┆ UHE       ┆ … ┆ hm3     ┆ true      ┆ false    ┆ 165       │
    │            ┆ EVAP       ┆ de Evapor ┆           ┆   ┆         ┆           ┆          ┆           │
    │            ┆            ┆ ação      ┆           ┆   ┆         ┆           ┆          ┆           │
    │ VEVAP_REE  ┆ Violação   ┆ Violação  ┆ REE       ┆ … ┆ hm3     ┆ true      ┆ false    ┆ 166       │
    │            ┆ EVAP       ┆ de Evapor ┆           ┆   ┆         ┆           ┆          ┆           │
    │            ┆            ┆ ação      ┆           ┆   ┆         ┆           ┆          ┆           │
    │ VEVAP_SBM  ┆ Violação   ┆ Violação  ┆ SBM       ┆ … ┆ hm3     ┆ true      ┆ false    ┆ 167       │
    │            ┆ EVAP       ┆ de Evapor ┆           ┆   ┆         ┆           ┆          ┆           │
    │            ┆            ┆ ação      ┆           ┆   ┆         ┆           ┆          ┆           │
    │ VEVAP_SIN  ┆ Violação   ┆ Violação  ┆ SIN       ┆ … ┆ hm3     ┆ true      ┆ false    ┆ 168       │
    │            ┆ EVAP       ┆ de Evapor ┆           ┆   ┆         ┆           ┆          ┆           │
    │            ┆            ┆ ação      ┆           ┆   ┆         ┆           ┆          ┆           │
    └────────────┴────────────┴───────────┴───────────┴───┴─────────┴───────────┴──────────┴───────────┘


Formato das Estatísticas
--------------------------

As sínteses da operação e dos cenários gerados também produzem estatísticas dos dados envolvidos. Em cada uma das sínteses, as estatísticas são armazenadas segundo diferentes premissas, dependendo geralmente
da agregação espacial dos dados. No caso da síntese dos cenários, além da agregação espacial, também é considerada a etapa da execução do modelo para a qual os cenários foram gerados (forward, backward ou simulação final).

As estatísticas são armazenadas em arquivos com o prefixo `ESTATISTICAS_` e o nome da síntese. Por exemplo, para a síntese da operação, as estatísticas são armazenadas em arquivos com prefixo `ESTATISTICAS_OPERACAO_`, sendo um arquivo por agregação espacial.

Por exemplo, em uma síntese da operação, as estatísticas podem ser acessadas como:


.. code-block:: python

    import polars as pl
    hydro_df = pl.read_parquet("sintese/ESTATISTICAS_OPERACAO_UHE.parquet")
    print(hydro_df)

.. code-block:: none

    shape: (2_466_819, 13)
    ┌──────────┬─────────┬────────────┬────────────┬───┬───────────┬───────────┬───────────┬───────────┐
    │ variavel ┆ estagio ┆ data_inici ┆ data_fim   ┆ … ┆ codigo_re ┆ codigo_su ┆ limite_in ┆ limite_su │
    │ ---      ┆ ---     ┆ o          ┆ ---        ┆   ┆ e         ┆ bmercado  ┆ ferior    ┆ perior    │
    │ str      ┆ i64     ┆ ---        ┆ datetime[n ┆   ┆ ---       ┆ ---       ┆ ---       ┆ ---       │
    │          ┆         ┆ datetime[n ┆ s, UTC]    ┆   ┆ i64       ┆ i64       ┆ f64       ┆ f64       │
    │          ┆         ┆ s, UTC]    ┆            ┆   ┆           ┆           ┆           ┆           │
    ╞══════════╪═════════╪════════════╪════════════╪═══╪═══════════╪═══════════╪═══════════╪═══════════╡
    │ VAGUA    ┆ 1       ┆ 2023-10-01 ┆ 2023-11-01 ┆ … ┆ 10        ┆ 1         ┆ -inf      ┆ inf       │
    │          ┆         ┆ 00:00:00   ┆ 00:00:00   ┆   ┆           ┆           ┆           ┆           │
    │          ┆         ┆ UTC        ┆ UTC        ┆   ┆           ┆           ┆           ┆           │
    │ VAGUAI   ┆ 1       ┆ 2023-10-01 ┆ 2023-11-01 ┆ … ┆ 10        ┆ 1         ┆ -inf      ┆ inf       │
    │          ┆         ┆ 00:00:00   ┆ 00:00:00   ┆   ┆           ┆           ┆           ┆           │
    │          ┆         ┆ UTC        ┆ UTC        ┆   ┆           ┆           ┆           ┆           │
    │ VTUR     ┆ 1       ┆ 2023-10-01 ┆ 2023-11-01 ┆ … ┆ 10        ┆ 1         ┆ 0.0       ┆ 562.82    │
    │          ┆         ┆ 00:00:00   ┆ 00:00:00   ┆   ┆           ┆           ┆           ┆           │
    │          ┆         ┆ UTC        ┆ UTC        ┆   ┆           ┆           ┆           ┆           │
    │ VVER     ┆ 1       ┆ 2023-10-01 ┆ 2023-11-01 ┆ … ┆ 10        ┆ 1         ┆ 0.0       ┆ inf       │
    │          ┆         ┆ 00:00:00   ┆ 00:00:00   ┆   ┆           ┆           ┆           ┆           │
    │          ┆         ┆ UTC        ┆ UTC        ┆   ┆           ┆           ┆           ┆           │
    │ QTUR     ┆ 1       ┆ 2023-10-01 ┆ 2023-11-01 ┆ … ┆ 10        ┆ 1         ┆ 0.0       ┆ 214.0     │
    │          ┆         ┆ 00:00:00   ┆ 00:00:00   ┆   ┆           ┆           ┆           ┆           │
    │          ┆         ┆ UTC        ┆ UTC        ┆   ┆           ┆           ┆           ┆           │
    │ …        ┆ …       ┆ …          ┆ …          ┆ … ┆ …         ┆ …         ┆ …         ┆ …         │
    │ GHID     ┆ 51      ┆ 2027-12-01 ┆ 2028-01-01 ┆ … ┆ 8         ┆ 4         ┆ -inf      ┆ inf       │
    │          ┆         ┆ 00:00:00   ┆ 00:00:00   ┆   ┆           ┆           ┆           ┆           │
    │          ┆         ┆ UTC        ┆ UTC        ┆   ┆           ┆           ┆           ┆           │
    │ VGHMIN   ┆ 51      ┆ 2027-12-01 ┆ 2028-01-01 ┆ … ┆ 8         ┆ 4         ┆ -inf      ┆ inf       │
    │          ┆         ┆ 00:00:00   ┆ 00:00:00   ┆   ┆           ┆           ┆           ┆           │
    │          ┆         ┆ UTC        ┆ UTC        ┆   ┆           ┆           ┆           ┆           │
    │ VFPHA    ┆ 51      ┆ 2027-12-01 ┆ 2028-01-01 ┆ … ┆ 8         ┆ 4         ┆ -inf      ┆ inf       │
    │          ┆         ┆ 00:00:00   ┆ 00:00:00   ┆   ┆           ┆           ┆           ┆           │
    │          ┆         ┆ UTC        ┆ UTC        ┆   ┆           ┆           ┆           ┆           │
    │ HJUS     ┆ 51      ┆ 2027-12-01 ┆ 2028-01-01 ┆ … ┆ 8         ┆ 4         ┆ -inf      ┆ inf       │
    │          ┆         ┆ 00:00:00   ┆ 00:00:00   ┆   ┆           ┆           ┆           ┆           │
    │          ┆         ┆ UTC        ┆ UTC        ┆   ┆           ┆           ┆           ┆           │
    │ HLIQ     ┆ 51      ┆ 2027-12-01 ┆ 2028-01-01 ┆ … ┆ 8         ┆ 4         ┆ -inf      ┆ inf       │
    │          ┆         ┆ 00:00:00   ┆ 00:00:00   ┆   ┆           ┆           ┆           ┆           │
    │          ┆         ┆ UTC        ┆ UTC        ┆   ┆           ┆           ┆           ┆           │
    └──────────┴─────────┴────────────┴────────────┴───┴───────────┴───────────┴───────────┴───────────┘


No arquivo de estatísticas, ao invés dos dados associados aos `N` cenários da etapa de simulação final, são armazenadas as estatísticas dos dados associados a cada entidade, em cada estágio / patamar, calculadas nos cenários.
Nestes arquivos, a coluna `cenario` possui tipo `str`, assumindo valores `mean`, `std` e percentis de 5 em 5 (`min`, `p5`, ..., `p45`, `median`, `p55`, ..., `p95`, `max`).


Formato dos Dados Brutos
--------------------------

Os dados brutos também são armazenados em arquivos de mesma extensão dos demais produzidos pela síntese. Por exemplo, para a síntese da operação, os dados são armazenados em arquivos que possuem os nomes da chave identificadora da variável e da agregação espacial,
como `CMO_SBM` e `EARMF_REE`. Para uma mesma entidade, os arquivos de todas as variáveis possuem as mesmas colunas:


.. code-block:: python

    import polars as pl
    eer_df = pl.read_parquet("sintese/EARMF_REE.parquet")
    print(eer_df)

.. code-block:: none

    shape: (18_337, 11)
    ┌────────────┬────────────┬─────────┬────────────┬───┬─────────┬────────────┬────────────┬───────────┐
    │ codigo_ree ┆ codigo_sub ┆ estagio ┆ data_inici ┆ … ┆ valor   ┆ limite_inf ┆ limite_sup ┆ __index_l │
    │ ---        ┆ mercado    ┆ ---     ┆ o          ┆   ┆ ---     ┆ erior      ┆ erior      ┆ evel_0__  │
    │ i64        ┆ ---        ┆ i64     ┆ ---        ┆   ┆ f64     ┆ ---        ┆ ---        ┆ ---       │
    │            ┆ i64        ┆         ┆ datetime[n ┆   ┆         ┆ f64        ┆ f64        ┆ i64       │
    │            ┆            ┆         ┆ s, UTC]    ┆   ┆         ┆            ┆            ┆           │
    ╞════════════╪════════════╪═════════╪════════════╪═══╪═════════╪════════════╪════════════╪═══════════╡
    │ 1          ┆ 1          ┆ 1       ┆ 2023-10-01 ┆ … ┆ 30647.0 ┆ 10194.0    ┆ 50969.0    ┆ 0         │
    │            ┆            ┆         ┆ 00:00:00   ┆   ┆         ┆            ┆            ┆           │
    │            ┆            ┆         ┆ UTC        ┆   ┆         ┆            ┆            ┆           │
    │ 1          ┆ 1          ┆ 1       ┆ 2023-10-01 ┆ … ┆ 30494.0 ┆ 10194.0    ┆ 50969.0    ┆ 1         │
    │            ┆            ┆         ┆ 00:00:00   ┆   ┆         ┆            ┆            ┆           │
    │            ┆            ┆         ┆ UTC        ┆   ┆         ┆            ┆            ┆           │
    │ 1          ┆ 1          ┆ 1       ┆ 2023-10-01 ┆ … ┆ 31585.0 ┆ 10194.0    ┆ 50969.0    ┆ 2         │
    │            ┆            ┆         ┆ 00:00:00   ┆   ┆         ┆            ┆            ┆           │
    │            ┆            ┆         ┆ UTC        ┆   ┆         ┆            ┆            ┆           │
    │ 1          ┆ 1          ┆ 1       ┆ 2023-10-01 ┆ … ┆ 30273.0 ┆ 10194.0    ┆ 50969.0    ┆ 3         │
    │            ┆            ┆         ┆ 00:00:00   ┆   ┆         ┆            ┆            ┆           │
    │            ┆            ┆         ┆ UTC        ┆   ┆         ┆            ┆            ┆           │
    │ 1          ┆ 1          ┆ 1       ┆ 2023-10-01 ┆ … ┆ 31046.0 ┆ 10194.0    ┆ 50969.0    ┆ 4         │
    │            ┆            ┆         ┆ 00:00:00   ┆   ┆         ┆            ┆            ┆           │
    │            ┆            ┆         ┆ UTC        ┆   ┆         ┆            ┆            ┆           │
    │ …          ┆ …          ┆ …       ┆ …          ┆ … ┆ …       ┆ …          ┆ …          ┆ …         │
    │ 12         ┆ 1          ┆ 51      ┆ 2027-12-01 ┆ … ┆ 10132.0 ┆ 2027.0     ┆ 11831.0    ┆ 18332     │
    │            ┆            ┆         ┆ 00:00:00   ┆   ┆         ┆            ┆            ┆           │
    │            ┆            ┆         ┆ UTC        ┆   ┆         ┆            ┆            ┆           │
    │ 12         ┆ 1          ┆ 51      ┆ 2027-12-01 ┆ … ┆ 10132.0 ┆ 2027.0     ┆ 11831.0    ┆ 18333     │
    │            ┆            ┆         ┆ 00:00:00   ┆   ┆         ┆            ┆            ┆           │
    │            ┆            ┆         ┆ UTC        ┆   ┆         ┆            ┆            ┆           │
    │ 12         ┆ 1          ┆ 51      ┆ 2027-12-01 ┆ … ┆ 3955.0  ┆ 2027.0     ┆ 11831.0    ┆ 18334     │
    │            ┆            ┆         ┆ 00:00:00   ┆   ┆         ┆            ┆            ┆           │
    │            ┆            ┆         ┆ UTC        ┆   ┆         ┆            ┆            ┆           │
    │ 12         ┆ 1          ┆ 51      ┆ 2027-12-01 ┆ … ┆ 7294.0  ┆ 2027.0     ┆ 11831.0    ┆ 18335     │
    │            ┆            ┆         ┆ 00:00:00   ┆   ┆         ┆            ┆            ┆           │
    │            ┆            ┆         ┆ UTC        ┆   ┆         ┆            ┆            ┆           │
    │ 12         ┆ 1          ┆ 51      ┆ 2027-12-01 ┆ … ┆ 9903.0  ┆ 2027.0     ┆ 11831.0    ┆ 18336     │
    │            ┆            ┆         ┆ 00:00:00   ┆   ┆         ┆            ┆            ┆           │
    │            ┆            ┆         ┆ UTC        ┆   ┆         ┆            ┆            ┆           │
    └────────────┴────────────┴─────────┴────────────┴───┴─────────┴────────────┴────────────┴───────────┘
