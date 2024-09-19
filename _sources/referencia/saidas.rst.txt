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

    import pandas as pd
    meta_df = pd.read_parquet("sintese/METADADOS_OPERACAO.parquet")
    meta_df

                chave nome_curto_variavel              nome_longo_variavel nome_curto_agregacao      nome_longo_agregacao  unidade  calculado  limitado
    0         CMO_SBM                 CMO       Custo Marginal de Operação                  SBM                Submercado  'R$/MWh'      False     False
    1       VAGUA_REE               VAGUA                    Valor da Água                  REE  Reservatório Equivalente  'R$/MWh'      False     False
    2       VAGUA_UHE               VAGUA                    Valor da Água                  UHE       Usina Hidroelétrica  'R$/hm3'      False     False
    3      VAGUAI_UHE   VAGUA Incremental        Valor da Água Incremental                  UHE       Usina Hidroelétrica  'R$/hm3'      False     False
    4        CTER_SBM         Custo de GT         Custo de Geração Térmica                  SBM                Submercado '10^6 R$'      False     False
    ..            ...                 ...                              ...                  ...                       ...      ...        ...       ...
    164  VNEGEVAP_UHE  Violação Neg. EVAP  Violação Negativa de Evaporação                  UHE       Usina Hidroelétrica                False     False
    165     VEVAP_UHE       Violação EVAP           Violação de Evaporação                  UHE       Usina Hidroelétrica     'hm3'       True     False
    166     VEVAP_REE       Violação EVAP           Violação de Evaporação                  REE  Reservatório Equivalente     'hm3'       True     False
    167     VEVAP_SBM       Violação EVAP           Violação de Evaporação                  SBM                Submercado     'hm3'       True     False
    168     VEVAP_SIN       Violação EVAP           Violação de Evaporação                  SIN       Sistema Interligado     'hm3'       True     False
    
    [169 rows x 8 columns]


Formato das Estatísticas
--------------------------

As sínteses da operação e dos cenários gerados também produzem estatísticas dos dados envolvidos. Em cada uma das sínteses, as estatísticas são armazenadas segundo diferentes premissas, dependendo geralmente
da agregação espacial dos dados. No caso da síntese dos cenários, além da agregação espacial, também é considerada a etapa da execução do modelo para a qual os cenários foram gerados (forward, backward ou simulação final).

As estatísticas são armazenadas em arquivos com o prefixo `ESTATISTICAS_` e o nome da síntese. Por exemplo, para a síntese da operação, as estatísticas são armazenadas em arquivos com prefixo `ESTATISTICAS_OPERACAO_`, sendo um arquivo por agregação espacial.

Por exemplo, em uma síntese da operação, as estatísticas podem ser acessadas como:


.. code-block:: python

    import pandas as pd
    hydro_df = pd.read_parquet("sintese/ESTATISTICAS_OPERACAO_UHE.parquet")
    hydro_df

            variavel  estagio data_inicio   data_fim cenario  patamar  ...       valor  codigo_usina  codigo_ree  codigo_submercado  limite_inferior  limite_superior
    0          VAGUA        1  2023-10-01 2023-11-01     max        0  ...   13.249930             1          10                  1             -inf              inf
    1         VAGUAI        1  2023-10-01 2023-11-01     max        0  ...    2.568698             1          10                  1             -inf              inf
    2           VTUR        1  2023-10-01 2023-11-01     max        0  ...  522.970000             1          10                  1              0.0           562.82
    3           VVER        1  2023-10-01 2023-11-01     max        0  ...    0.850000             1          10                  1              0.0              inf
    4           QTUR        1  2023-10-01 2023-11-01     max        0  ...  198.850000             1          10                  1              0.0           214.00
    ...          ...      ...         ...        ...     ...      ...  ...         ...           ...         ...                ...              ...              ...
    2451565     GHID       51  2027-12-01 2028-01-01     std        3  ...   21.759415           314           8                  4             -inf              inf
    2451566   VGHMIN       51  2027-12-01 2028-01-01     std        3  ...    0.000000           314           8                  4             -inf              inf
    2451567    VFPHA       51  2027-12-01 2028-01-01     std        3  ...         NaN           314           8                  4             -inf              inf
    2451568     HJUS       51  2027-12-01 2028-01-01     std        3  ...    0.136938           314           8                  4             -inf              inf
    2451569     HLIQ       51  2027-12-01 2028-01-01     std        3  ...    0.136938           314           8                  4             -inf              inf
    
    [2451570 rows x 13 columns]


No arquivo de estatísticas, ao invés dos dados associados aos `N` cenários da etapa de simulação final, são armazenadas as estatísticas dos dados associados a cada entidade, em cada estágio / patamar, calculadas nos cenários.
Nestes arquivos, a coluna `cenario` possui tipo `str`, assumindo valores `mean`, `std` e percentis de 5 em 5 (`min`, `p5`, ..., `p45`, `median`, `p55`, ..., `p95`, `max`).


Formato dos Dados Brutos
--------------------------

Os dados brutos também são armazenados em arquivos de mesma extensão dos demais produzidos pela síntese. Por exemplo, para a síntese da operação, os dados são armazenados em arquivos que possuem os nomes da chave identificadora da variável e da agregação espacial,
como `CMO_SBM` e `EARMF_REE`. Para uma mesma entidade, os arquivos de todas as variáveis possuem as mesmas colunas:


.. code-block:: python

    import pandas as pd
    eer_df = pd.read_parquet("sintese/EARMF_REE.parquet")
    eer_df

           codigo_ree  codigo_submercado  estagio data_inicio   data_fim  cenario  patamar  duracao_patamar    valor  limite_inferior  limite_superior
    0               1                  1        1  2023-10-01 2023-11-01        1        0            730.0  30647.0          10194.0          50969.0
    1               1                  1        1  2023-10-01 2023-11-01        2        0            730.0  30494.0          10194.0          50969.0
    2               1                  1        1  2023-10-01 2023-11-01        3        0            730.0  31585.0          10194.0          50969.0
    3               1                  1        1  2023-10-01 2023-11-01        4        0            730.0  30273.0          10194.0          50969.0
    4               1                  1        1  2023-10-01 2023-11-01        5        0            730.0  31046.0          10194.0          50969.0
    ...           ...                ...      ...         ...        ...      ...      ...              ...      ...              ...              ...
    18332          12                  1       51  2027-12-01 2028-01-01        3        0            730.0  10132.0           2027.0          11831.0
    18333          12                  1       51  2027-12-01 2028-01-01        4        0            730.0  10132.0           2027.0          11831.0
    18334          12                  1       51  2027-12-01 2028-01-01        5        0            730.0   3955.0           2027.0          11831.0
    18335          12                  1       51  2027-12-01 2028-01-01        6        0            730.0   7294.0           2027.0          11831.0
    18336          12                  1       51  2027-12-01 2028-01-01        7        0            730.0   9903.0           2027.0          11831.0
    
    [4284 rows x 11 columns]
