FAQ e Troubleshooting
=====================

Esta página reúne as dúvidas e problemas mais frequentes relatados pelos
usuários do *sintetizador-newave*, organizados por tema. Para uma introdução
ao uso básico da ferramenta, consulte o :doc:`tutorial`. Para instruções de
instalação, consulte :doc:`instalacao`.

.. contents:: Índice desta página
   :local:
   :depth: 2


Instalação
----------

Qual é a versão mínima de Python necessária?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

O *sintetizador-newave* requer **Python >= 3.10**. Versões anteriores não são
suportadas e não recebem correções. O suporte a Python 3.8 e 3.9 foi
descontinuado na versão ``v2.0.0`` da aplicação.

Para verificar a versão do Python disponível no seu ambiente:

.. code-block:: bash

   python --version

Qual versão do ``inewave`` é necessária?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A dependência ``inewave`` deve estar na versão **>= 1.9.2**. Versões
anteriores do ``inewave`` são incompatíveis com a série ``v2.x`` do
*sintetizador-newave* e causarão erros de importação ou de leitura de
arquivos.

Ao instalar com ``pip``, a versão correta é resolvida automaticamente:

.. code-block:: bash

   pip install git+https://github.com/rjmalves/sintetizador-newave

Se você mantém o ``inewave`` instalado separadamente, atualize-o antes de
atualizar o *sintetizador-newave*:

.. code-block:: bash

   pip install --upgrade inewave

Posso instalar com ``uv`` em vez de ``pip``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sim. O projeto usa ``uv`` como gerenciador de ambiente recomendado para
desenvolvimento. Para instalar dependências e executar comandos em um ambiente
gerenciado pelo ``uv``:

.. code-block:: bash

   uv run sintetizador-newave --help

Consulte :doc:`instalacao` para o procedimento completo de instalação via
``pip`` e via repositório Git.


Uso Básico
----------

Quais são os comandos disponíveis?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

O *sintetizador-newave* é uma ferramenta de linha de comando com os seguintes
subcomandos:

.. code-block:: none

   Usage: sintetizador-newave [OPTIONS] COMMAND [ARGS]...

     Aplicação para realizar a síntese de informações em um modelo unificado de
     dados para o NEWAVE.

   Options:
     --help  Show this message and exit.

   Commands:
     completa  Realiza a síntese completa do NEWAVE.
     cenarios  Realiza a síntese dos dados de cenários do NEWAVE.
     execucao  Realiza a síntese dos dados da execução do NEWAVE.
     politica  Realiza a síntese dos dados da política do NEWAVE (NWLISTCF).
     operacao  Realiza a síntese dos dados da operação do NEWAVE (NWLISTOP).
     sistema   Realiza a síntese dos dados do sistema do NEWAVE.
     limpeza   Realiza a limpeza dos dados resultantes de uma síntese.

Para ver as opções de um subcomando específico, use ``--help`` após o nome do
comando. Consulte o :doc:`tutorial` para exemplos de uso detalhados.

Como sintetizar apenas algumas variáveis em vez de todas?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Todos os subcomandos de síntese aceitam argumentos posicionais com os nomes
das variáveis desejadas. Por exemplo, para sintetizar apenas o Custo Marginal
de Operação por submercado e a Energia Armazenada no SIN:

.. code-block:: bash

   sintetizador-newave operacao CMO_SBM EARMF_SIN

Se nenhum argumento for passado, todas as variáveis disponíveis para aquele
subcomando são sintetizadas.

Posso usar curingas (wildcards) para selecionar variáveis?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sim. Desde a versão ``v2.0.0`` é possível usar o caractere ``*`` para
selecionar múltiplas variáveis com base em um prefixo ou sufixo comum.
Por exemplo, para sintetizar todas as variáveis de energia armazenada:

.. code-block:: bash

   sintetizador-newave operacao "EARM*"

.. note::

   Use aspas ao redor do argumento com ``*`` para evitar que o shell expanda
   o curinga antes de passá-lo ao programa.

Como executar o sintetizador a partir de um diretório diferente do caso?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Por padrão, os subcomandos de síntese (``operacao``, ``cenarios``,
``sistema``, ``execucao``, ``politica``, ``completa``) lêem os arquivos do
diretório de trabalho atual (``os.curdir``). Para executar a síntese apontando
para um diretório diferente, defina a variável de ambiente ``APP_BASEDIR``
antes de chamar o comando ``limpeza``.

.. warning::

   A variável de ambiente ``APP_BASEDIR`` é obrigatória apenas para o comando
   ``limpeza``. Os demais comandos sempre utilizam o diretório de trabalho
   corrente como diretório base. Para executar a síntese sobre um caso em
   outro diretório, mude para esse diretório antes de executar o comando.

.. code-block:: bash

   cd /caminho/para/o/caso
   sintetizador-newave operacao --processadores 4


Formato de Saída
----------------

Qual é o formato padrão dos arquivos de saída?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

O formato padrão é **PARQUET**, um formato colunar eficiente para armazenamento
de dados tabulares, amplamente utilizado em aplicações de *big data*. Os
arquivos são gerados com compressão **Snappy** (padrão do Apache Arrow) e
extensão ``.parquet``.

.. note::

   A partir da versão ``v2.0.0``, o formato PARQUET não utiliza mais compressão
   ``gzip`` automática. A extensão dos arquivos mudou de ``.parquet.gz`` para
   ``.parquet``.

Os arquivos são gravados no subdiretório ``sintese/`` dentro do diretório base
do caso.

Como alterar o formato de saída para CSV?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use o argumento ``--formato`` seguido do nome do formato desejado. Os valores
aceitos são ``PARQUET`` (padrão) e ``CSV``:

.. code-block:: bash

   sintetizador-newave execucao --formato CSV
   sintetizador-newave operacao --formato CSV --processadores 4

Os arquivos CSV são gerados sem índice e com codificação UTF-8.

Por que as colunas têm nomes diferentes da versão 1.x?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A partir da versão ``v2.0.0``, todos os nomes de colunas dos DataFrames de
síntese foram padronizados para ``snake_case``. Exemplos de mudanças:

- ``Usina`` → ``usina`` / ``codigo_usina``
- ``Submercado`` → ``submercado`` / ``codigo_submercado``
- ``Estagio`` → ``estagio``

Além disso, as entidades passaram a ser indexadas pelos seus **códigos
numéricos** ao invés de nomes nos DataFrames das sínteses de operação e
cenários. Por exemplo, a coluna ``usina`` foi substituída por
``codigo_usina``. O mapeamento entre códigos e nomes está disponível na
síntese do sistema (``sistema``).

Como ler os arquivos Parquet gerados?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Os arquivos ``.parquet`` gerados são compatíveis com o Apache Arrow e podem
ser lidos com Polars, pandas ou PySpark:

.. code-block:: python

   # Usando Polars
   import polars as pl
   df = pl.read_parquet("sintese/CMO_SBM.parquet")

   # Usando pandas
   import pandas as pd
   df = pd.read_parquet("sintese/CMO_SBM.parquet")

As colunas de data e hora são armazenadas em fuso horário **UTC** para
garantir compatibilidade na leitura em diferentes implementações do Arrow.


Paralelismo
-----------

Quais comandos suportam o argumento ``--processadores``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Os subcomandos que suportam paralelização são ``operacao``, ``cenarios`` e
``completa``. Os subcomandos ``sistema``, ``execucao`` e ``politica`` não
aceitam ``--processadores`` e sempre executam de forma sequencial.

.. code-block:: bash

   # Suportado
   sintetizador-newave operacao --processadores 8
   sintetizador-newave cenarios --processadores 8
   sintetizador-newave completa --processadores 24

   # NÃO suportado (o argumento será ignorado ou causará erro)
   sintetizador-newave sistema --processadores 4

Quantos processadores devo usar?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

O valor ideal depende do hardware disponível e do número de variáveis a
sintetizar. Como referência:

- Para máquinas com 4 a 8 núcleos, valores de ``--processadores`` entre 4 e 8
  costumam oferecer bom desempenho.
- Para servidores com muitos núcleos, valores maiores (por exemplo, 16 ou 24)
  podem ser usados com ``completa``.
- Valores maiores que o número de núcleos físicos disponíveis tendem a não
  trazer ganho adicional e podem aumentar a contenção de I/O.

O padrão é ``--processadores 1`` (execução sequencial) quando o argumento é
omitido.

O paralelismo aplica-se a todas as variáveis?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sim. Desde a versão ``v1.1.0``, o paralelismo está habilitado para todas as
variáveis das sínteses de operação e cenários, e não apenas para variáveis
relacionadas a UHEs. A síntese ``completa`` aplica o paralelismo a todas as
categorias de síntese que suportam essa funcionalidade.


Erros Comuns
------------

Erro: arquivos de entrada não são encontrados
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

O erro mais comum ocorre quando o *sintetizador-newave* é executado a partir de
um diretório que não contém os arquivos de saída do modelo NEWAVE. A aplicação
lê os arquivos do diretório de trabalho corrente.

.. code-block:: none

   ERROR: Arquivo 'nwlistop.rel' não encontrado.

Solução: mude para o diretório do caso antes de executar o sintetizador:

.. code-block:: bash

   cd /caminho/para/o/caso/newave
   sintetizador-newave operacao

Erro de incompatibilidade com versões do NEWAVE anteriores a 29.4
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A partir da versão ``v2.1.0``, o *sintetizador-newave* adicionou suporte
completo ao NEWAVE >= 29.4, que renomeou diversos arquivos de saída. Ao
utilizar uma versão do NEWAVE anterior a 29.4 com uma versão recente do
sintetizador, pode ocorrer falha na leitura de arquivos renomeados.

.. warning::

   Para garantir compatibilidade total, recomenda-se utilizar o NEWAVE na
   versão **>= 29.4** com o *sintetizador-newave* na versão **>= 2.1.0**.

Se necessário utilizar uma versão mais antiga do NEWAVE, verifique se o
*sintetizador-newave* na versão ``v1.2.0`` (série de compatibilidade ``v1-compat``)
atende às suas necessidades.

Erro de incompatibilidade de formato após atualização para v2.0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A versão ``v2.0.0`` introduziu mudanças incompatíveis no formato dos arquivos
de saída: nomes de colunas em ``snake_case``, indexação por códigos e novos
arquivos de metadados e estatísticas. Scripts e pipelines construídos para a
série ``v1.x`` precisam ser atualizados.

Principais mudanças que podem causar erros em código legado:

- Colunas renomeadas para ``snake_case`` (ex.: ``codigo_usina``, ``estagio``)
- Novos arquivos ``METADADOS_*.parquet`` e ``ESTATISTICAS_*.parquet`` são
  gerados automaticamente
- A coluna ``patamar`` substitui a separação anterior por sufixo ``_EST`` /
  ``_PAT``; ``patamar = 0`` representa o valor médio do estágio

Consulte o ``CHANGELOG.md`` do repositório para a lista completa de
mudanças dessa versão.

A síntese termina sem erros mas os arquivos de saída não aparecem
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Os arquivos de saída são gravados no subdiretório ``sintese/`` dentro do
diretório de trabalho corrente (ou no caminho indicado pela variável de
ambiente ``DIRETORIO_SINTESE``). Verifique se esse subdiretório existe e se
há permissão de escrita:

.. code-block:: bash

   ls -la sintese/

Se o diretório ``sintese/`` não existir, ele é criado automaticamente pela
aplicação na primeira execução. Se houver erro de permissão, ajuste as
permissões do diretório pai:

.. code-block:: bash

   chmod u+w /caminho/para/o/caso
