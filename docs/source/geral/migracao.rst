Guia de Migração v1.x para v2.x
==================================

Este guia descreve as mudanças necessárias para migrar código e fluxos de trabalho
da série de versões **v1.x** para a série **v2.x** do ``sintetizador-newave``.

.. warning::

   A versão v2.0.0 introduziu mudanças incompatíveis com versões anteriores em
   diversas áreas. Código que consome os arquivos de saída da série v1.x precisará
   ser atualizado. Leia este guia na íntegra antes de iniciar a migração.

.. warning::

   **Não existem scripts de migração automática.** Todas as adaptações devem ser
   realizadas manualmente no código downstream que consome as saídas do sintetizador.

Resumo das Mudanças
--------------------

A tabela abaixo lista as categorias de mudanças incompatíveis introduzidas na v2.0.0.

.. list-table:: Mudanças Incompatíveis na v2.0.0
   :widths: 25 50 25
   :header-rows: 1

   * - Categoria
     - Descrição
     - Impacto
   * - Versão do Python
     - Suporte ao Python 3.8 descontinuado. Mínimo obrigatório: Python >= 3.10.
     - Alto
   * - Nomenclatura das colunas
     - Todas as colunas dos DataFrames foram renomeadas para ``snake_case``.
     - Alto
   * - Indexação de entidades
     - Entidades indexadas por código numérico ao invés de nome textual
       (ex.: ``usina`` -> ``codigo_usina``).
     - Alto
   * - Formato dos arquivos Parquet
     - Compressão alterada de ``gzip`` para ``snappy`` (padrão Arrow).
       Extensão dos arquivos passa de ``.parquet.gz`` para ``.parquet``.
     - Alto
   * - Reestruturação dos Patamares
     - Colunas ``patamar`` e ``duracao_patamar`` adicionadas em todas as sínteses.
       ``patamar = 0`` representa o valor médio do estágio.
     - Alto
   * - Arquivos de estatísticas
     - Novas saídas ``ESTATISTICAS_*.parquet`` com estatísticas pré-calculadas
       por variável e entidade.
     - Médio
   * - Arquivos de metadados
     - Novas saídas ``METADADOS_*.parquet`` com informações sobre as sínteses
       realizadas.
     - Médio
   * - Sínteses de violação removidas
     - Sínteses específicas de violações foram removidas e substituídas pelas
       colunas ``limite_inferior`` e ``limite_superior`` nos dados brutos.
     - Alto


Versão do Python
-----------------

O suporte ao Python 3.8 foi descontinuado na v2.0.0. A versão mínima suportada
passou a ser **Python >= 3.10**, que é a versão mínima garantida nos ambientes de
CI e com reprodutibilidade assegurada.

**v1.x (antes)**:

.. code-block:: none

    Python >= 3.8 suportado

**v2.x (depois)**:

.. code-block:: none

    Python >= 3.10 obrigatório

Verifique sua versão instalada com::

    $ python --version

Caso necessário, atualize o ambiente antes de instalar a v2.x:

.. code-block:: none

    $ pip install sintetizador-newave>=2.0.0


Nomenclatura das Colunas
--------------------------

Todas as colunas dos DataFrames de saída foram padronizadas para ``snake_case``
na v2.0.0. Nomes de colunas com letras maiúsculas, acentos ou espaços foram
substituídos por equivalentes em minúsculas com underscores.

**v1.x (antes)**:

.. code-block:: none

    >>> import pandas as pd
    >>> df = pd.read_parquet("sintese/CMO_SBM.parquet")
    >>> df.columns.tolist()
    ['Submercado', 'Estagio', 'Data', 'Cenario', 'Valor']

**v2.x (depois)**:

.. code-block:: none

    >>> import polars as pl
    >>> df = pl.read_parquet("sintese/CMO_SBM.parquet")
    >>> df.columns
    ['codigo_submercado', 'estagio', 'data_inicio', 'data_fim', 'cenario', 'patamar', 'duracao_patamar', 'valor', 'limite_inferior', 'limite_superior']

Todo código downstream que referencia colunas pelo nome deve ser atualizado.
Exemplos comuns de mapeamento:

.. list-table:: Exemplos de Renomeação de Colunas
   :widths: 40 40 20
   :header-rows: 1

   * - Nome v1.x
     - Nome v2.x
     - Tipo
   * - ``Estagio``
     - ``estagio``
     - ``i64``
   * - ``Data``
     - ``data_inicio``
     - ``datetime[ns, UTC]``
   * - ``Cenario``
     - ``cenario``
     - ``i64`` ou ``str``
   * - ``Valor``
     - ``valor``
     - ``f64``
   * - ``Submercado``
     - ``codigo_submercado``
     - ``i64``
   * - ``Usina``
     - ``codigo_usina``
     - ``i64``
   * - ``REE``
     - ``codigo_ree``
     - ``i64``


Indexação de Entidades
-----------------------

Na v1.x, as entidades do sistema (usinas, submercados, REEs, etc.) eram
identificadas pelo **nome textual** nos DataFrames de síntese. Na v2.0.0, passaram
a ser indexadas pelo **código numérico** da entidade. O mapeamento entre código e
nome está disponível nos arquivos de síntese do sistema.

**v1.x (antes)** — entidades identificadas pelo nome:

.. code-block:: none

    >>> df = pd.read_parquet("sintese/EARMF_REE.parquet")
    >>> df[["REE", "Estagio", "Valor"]].head(3)
       REE  Estagio      Valor
    0  SUDESTE        1  30647.0
    1  SUL            1  12345.0
    2  NORDESTE       1   8901.0

**v2.x (depois)** — entidades identificadas pelo código numérico:

.. code-block:: none

    >>> df = pl.read_parquet("sintese/EARMF_REE.parquet")
    >>> df[["codigo_ree", "codigo_submercado", "estagio", "valor"]].head(3)
    shape: (3, 4)
    ┌────────────┬───────────────────┬─────────┬─────────┐
    │ codigo_ree ┆ codigo_submercado ┆ estagio ┆ valor   │
    │ ---        ┆ ---               ┆ ---     ┆ ---     │
    │ i64        ┆ i64               ┆ i64     ┆ f64     │
    ╞════════════╪═══════════════════╪═════════╪═════════╡
    │ 1          ┆ 1                 ┆ 1       ┆ 30647.0 │
    │ 2          ┆ 1                 ┆ 1       ┆ 12345.0 │
    │ 3          ┆ 3                 ┆ 1       ┆  8901.0 │
    └────────────┴───────────────────┴─────────┴─────────┘

Para obter o mapeamento entre código e nome, consulte o arquivo ``REE.parquet``
da síntese do sistema:

.. code-block:: none

    >>> ree_df = pl.read_parquet("sintese/REE.parquet")
    >>> ree_df[["codigo_ree", "nome_ree"]]

De forma análoga, sínteses por UHE passaram a incluir colunas adicionais
com os códigos dos conjuntos que englobam a entidade, como ``codigo_ree`` e
``codigo_submercado``, permitindo agrupamentos arbitrários.


Formato dos Arquivos Parquet
------------------------------

.. warning::

   Arquivos gerados pela v1.x **não são compatíveis** com a leitura padrão
   esperada pela v2.x sem decompressão explícita. Regenere as sínteses após
   a atualização.

Na v1.x, os arquivos Parquet eram comprimidos com **gzip** e tinham a extensão
``.parquet.gz``. Na v2.0.0, a compressão padrão passou a ser **snappy** (padrão
do Apache Arrow), e a extensão dos arquivos é simplesmente ``.parquet``.

**v1.x (antes)**:

.. code-block:: none

    $ ls sintese/
    CMO_SBM.parquet.gz
    EARMF_REE.parquet.gz
    GHID_SIN.parquet.gz
    ...

.. code-block:: none

    >>> df = pd.read_parquet("sintese/CMO_SBM.parquet.gz")

**v2.x (depois)**:

.. code-block:: none

    $ ls sintese/
    CMO_SBM.parquet
    EARMF_REE.parquet
    GHID_SIN.parquet
    ...

.. code-block:: none

    >>> df = pl.read_parquet("sintese/CMO_SBM.parquet")

Além disso, colunas do tipo ``datetime`` passaram a garantir que o fuso horário
seja ``UTC``, aumentando a compatibilidade com outras implementações do Arrow.

Atualizar caminhos hardcoded de ``.parquet.gz`` para ``.parquet`` em qualquer
script de leitura downstream.


Reestruturação dos Patamares
------------------------------

Na v1.x, a síntese da operação era dividida em duas variantes por agregação
temporal: uma com sufixo ``EST`` (valor médio do estágio) e outra com sufixo
``PAT`` (por patamar). Na v2.0.0, essa distinção foi eliminada: todas as sínteses
incluem **sempre** as colunas ``patamar`` e ``duracao_patamar``.

A convenção adotada é:

- ``patamar = 0`` representa o valor médio do estágio (equivalente ao sufixo ``EST`` da v1.x)
- ``patamar >= 1`` representa os patamares individuais (equivalente ao sufixo ``PAT`` da v1.x)
- ``duracao_patamar`` contém a duração em horas do patamar no estágio

**v1.x (antes)** — sínteses separadas por agregação temporal:

.. code-block:: none

    $ ls sintese/
    GHID_REE_EST.parquet.gz   # valor médio do estágio
    GHID_REE_PAT.parquet.gz   # por patamar

    >>> df_est = pd.read_parquet("sintese/GHID_REE_EST.parquet.gz")
    >>> df_est.columns.tolist()
    ['REE', 'Estagio', 'Data', 'Cenario', 'Valor']

**v2.x (depois)** — arquivo único com coluna ``patamar``:

.. code-block:: none

    $ ls sintese/
    GHID_REE.parquet   # contém patamar = 0 (médio) e patamar >= 1

    >>> df = pl.read_parquet("sintese/GHID_REE.parquet")
    >>> df.columns
    ['codigo_ree', 'codigo_submercado', 'estagio', 'data_inicio', 'data_fim',
     'cenario', 'patamar', 'duracao_patamar', 'valor', 'limite_inferior', 'limite_superior']

Para obter apenas o valor médio do estágio (equivalente ao ``_EST`` da v1.x):

.. code-block:: none

    >>> df_medio = df.filter(pl.col("patamar") == 0)

Para obter apenas os valores por patamar (equivalente ao ``_PAT`` da v1.x):

.. code-block:: none

    >>> df_pat = df.filter(pl.col("patamar") > 0)


Arquivos de Estatísticas
--------------------------

Na v2.0.0, as sínteses da operação e de cenários passaram a gerar arquivos
adicionais com estatísticas pré-calculadas. Esses arquivos têm o prefixo
``ESTATISTICAS_`` e são gerados automaticamente junto com os dados brutos.

**v1.x (antes)** — sem arquivos de estatísticas separados:

.. code-block:: none

    $ ls sintese/
    EARMF_REE.parquet.gz
    GHID_SBM.parquet.gz
    ...
    # Estatísticas precisavam ser calculadas pelo usuário

**v2.x (depois)** — arquivos de estatísticas gerados automaticamente:

.. code-block:: none

    $ ls sintese/
    EARMF_REE.parquet
    ESTATISTICAS_OPERACAO_REE.parquet
    ESTATISTICAS_OPERACAO_SBM.parquet
    ESTATISTICAS_OPERACAO_SIN.parquet
    ESTATISTICAS_OPERACAO_UHE.parquet
    ESTATISTICAS_CENARIOS_REE_BKW.parquet
    ESTATISTICAS_CENARIOS_REE_SF.parquet
    ...

Nos arquivos de estatísticas, a coluna ``cenario`` possui tipo ``str`` e assume
os valores ``mean``, ``std`` e percentis de 5 em 5 (``min``, ``p5``, ...,
``p45``, ``median``, ``p55``, ..., ``p95``, ``max``).

Consulte :ref:`comandos` para detalhes sobre o formato completo dos arquivos
de estatísticas.


Arquivos de Metadados
-----------------------

Na v2.0.0, cada categoria de síntese passou a gerar um arquivo de metadados
com o prefixo ``METADADOS_``. Esses arquivos descrevem as sínteses realizadas,
incluindo informações como nome da variável, unidade, se é calculada e se é
limitada.

**v1.x (antes)** — sem arquivos de metadados:

.. code-block:: none

    $ ls sintese/
    CMO_SBM.parquet.gz
    EARMF_REE.parquet.gz
    ...
    # Não havia arquivos de metadados

**v2.x (depois)** — arquivo de metadados por categoria de síntese:

.. code-block:: none

    $ ls sintese/
    CMO_SBM.parquet
    EARMF_REE.parquet
    ...
    METADADOS_OPERACAO.parquet
    METADADOS_CENARIOS.parquet
    METADADOS_SISTEMA.parquet
    METADADOS_EXECUCAO.parquet
    METADADOS_POLITICA.parquet

Para acessar os metadados de uma síntese:

.. code-block:: none

    >>> meta_df = pl.read_parquet("sintese/METADADOS_OPERACAO.parquet")
    >>> meta_df[["chave", "nome_curto_variavel", "unidade", "calculado", "limitado"]]

Consulte :ref:`comandos` para exemplos completos do conteúdo dos arquivos de
metadados.


Sínteses de Violação Removidas
---------------------------------

Na v1.x, variáveis que possuíam limites operativos geravam sínteses adicionais
específicas para as violações (ex.: ``VVMINOP_UHE``, ``VGHMIN_UHE``). Na v2.0.0,
essas sínteses específicas foram **removidas** e substituídas pelas colunas
``limite_inferior`` e ``limite_superior`` nos próprios dados brutos de cada variável.

**v1.x (antes)** — sínteses de violação como arquivos separados:

.. code-block:: none

    $ ls sintese/
    VTUR_UHE.parquet.gz       # dados brutos
    VVTUR_UHE.parquet.gz      # violação do limite de turbinamento
    VARMI_REE.parquet.gz      # dados brutos
    VVMINOP_REE.parquet.gz    # violação do volume mínimo operativo
    ...

**v2.x (depois)** — limites integrados como colunas nos dados brutos:

.. code-block:: none

    $ ls sintese/
    VTUR_UHE.parquet    # contém colunas limite_inferior e limite_superior
    VARMI_REE.parquet   # contém colunas limite_inferior e limite_superior
    ...
    # Não há mais arquivos VVTUR_UHE, VVMINOP_REE, etc.

Para calcular violações com base nos dados da v2.x:

.. code-block:: none

    >>> df = pl.read_parquet("sintese/VTUR_UHE.parquet")
    >>> violacoes = df.filter(
    ...     (pl.col("valor") < pl.col("limite_inferior")) |
    ...     (pl.col("valor") > pl.col("limite_superior"))
    ... )

Scripts de Migração Automática
---------------------------------

.. warning::

   **Não existem scripts de migração automática disponíveis.** Toda adaptação
   do código downstream que consome as saídas do sintetizador deve ser realizada
   manualmente.

As mudanças na v2.0.0 afetam nomes de colunas, nomes de arquivos, tipos de dados
e a estrutura de entidades. Recomenda-se a seguinte abordagem para a migração:

1. **Regenerar todas as sínteses** com a versão v2.x instalada, pois os arquivos
   produzidos pela v1.x não são compatíveis com os novos nomes e extensões.

2. **Atualizar referências a colunas** em todo o código downstream, substituindo
   os nomes v1.x pelos equivalentes em ``snake_case`` da v2.x.

3. **Substituir joins por nome de entidade** por joins pelo código numérico,
   usando os arquivos de sistema (``UHE.parquet``, ``REE.parquet``, ``SBM.parquet``)
   para obter o mapeamento código-nome quando necessário.

4. **Atualizar extensões de arquivo** de ``.parquet.gz`` para ``.parquet`` em
   todos os scripts de leitura.

5. **Revisar filtros por patamar**: substituir leitura de arquivos ``_EST`` e
   ``_PAT`` pelo filtro ``patamar == 0`` e ``patamar > 0`` respectivamente.

6. **Remover leitura de sínteses de violação** (arquivos ``VV*.parquet.gz``),
   substituindo pelo cálculo a partir das colunas ``limite_inferior`` e
   ``limite_superior`` dos dados brutos.
