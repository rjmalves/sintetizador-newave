.. _arquitetura:

Arquitetura Interna
===================

Esta página descreve a organização interna do *sintetizador-newave*, cobrindo
as camadas que compõem a aplicação, o fluxo de dados desde a leitura dos
arquivos do NEWAVE até a exportação das sínteses, e o papel de cada pacote
principal.


Visao Geral
-----------

O *sintetizador-newave* é estruturado em seis pacotes principais dentro do
diretório ``app/``. A organização segue uma arquitetura em camadas: a interface
de linha de comando recebe os comandos do usuário, os despacha para a camada de
serviços, que coordena a leitura dos arquivos de entrada (deck do NEWAVE) e a
produção dos DataFrames de saída, que por sua vez são exportados pelo repositório
de persistência.

Cada camada tem uma responsabilidade claramente delimitada e depende apenas das
camadas abaixo dela, o que facilita o teste unitário e a substituição de
implementações concretas por alternativas (por exemplo, diferentes formatos de
exportação ou diferentes fontes de arquivos).

O diagrama a seguir ilustra a dependência entre os pacotes:

.. code-block:: none

    ┌──────────────────────────────────────────────────────────┐
    │                   app/app.py  (CLI - click)              │
    └────────────────────────┬─────────────────────────────────┘
                             │ despacha Commands
                             ▼
    ┌──────────────────────────────────────────────────────────┐
    │          app/services/handlers.py  (Handlers)            │
    └──────────┬───────────────────────────────────┬───────────┘
               │ instancia Synthetizers             │ usa UnitOfWork
               ▼                                   ▼
    ┌─────────────────────┐           ┌─────────────────────────┐
    │  app/services/      │           │  app/services/          │
    │  synthesis/         │◄──────────│  unitofwork.py          │
    │  (Synthetizers)     │  usa Deck │                         │
    └──────────┬──────────┘           └──────────┬──────────────┘
               │ lê dados do Deck                 │ provê repositórios
               ▼                                  ▼
    ┌─────────────────────┐           ┌─────────────────────────┐
    │  app/services/      │           │  app/adapters/          │
    │  deck/              │           │  repository/            │
    │  (Deck + módulos)   │           │  (Files + Export)       │
    └──────────┬──────────┘           └──────────┬──────────────┘
               │ via inewave                      │ lê/escreve
               ▼                                  ▼
    ┌─────────────────────┐           ┌─────────────────────────┐
    │  Arquivos do NEWAVE │           │  Arquivos de saída      │
    │  (caso.dat, dger,   │           │  sintese/*.parquet      │
    │   nwlistop, etc.)   │           │  sintese/*.csv          │
    └─────────────────────┘           └─────────────────────────┘

    Dependências transversais (usadas em todas as camadas):
      app/model/      – enumerações e dataclasses do modelo de dados
      app/internal/   – constantes e nomes de colunas compartilhados
      app/utils/      – logging, temporização, encoding, singleton, etc.


Fluxo de Dados
--------------

O fluxo completo de uma execução de síntese percorre as seguintes etapas:

.. code-block:: none

    Usuário
      │
      │  $ sintetizador-newave operacao CMO_SBM EARMF_SIN
      │
      ▼
    CLI (app/app.py)
      │  Cria: commands.SynthetizeOperation(["CMO_SBM", "EARMF_SIN"])
      │  Cria: FSUnitOfWork (diretório de trabalho atual)
      │
      ▼
    Handler (app/services/handlers.py)
      │  Chama: OperationSynthetizer.synthetize(variables, uow)
      │
      ▼
    Synthetizer (app/services/synthesis/operation.py)
      │  Resolve mnemônicos → OperationSynthesis (variável + agregação espacial)
      │  Para cada síntese:
      │
      ├──► Deck (app/services/deck/deck.py)
      │      │  Lê e armazena em cache os arquivos do deck do NEWAVE
      │      │  (dger.dat, confhd.dat, patamar.dat, ree.dat, ...)
      │      │  via AbstractFilesRepository (app/adapters/repository/files.py)
      │      │  que usa a biblioteca `inewave` para parsing
      │      └─► Retorna DataFrames Polars com dados do sistema
      │
      ├──► Processa os dados brutos:
      │      – Filtra, agrega e calcula limites operativos
      │      – Aplica unidades e resolução espacial
      │      – Monta o DataFrame final normalizado
      │
      └──► AbstractExportRepository (app/adapters/repository/export.py)
             │  Recebe o DataFrame Polars ou pandas
             │  Escreve em disco no formato configurado:
             │    PARQUET → sintese/CMO_SBM.parquet
             │    CSV     → sintese/CMO_SBM.csv
             └─► Arquivo de saída gravado


Modulos Principais
------------------

Esta seção descreve o papel de cada pacote do diretório ``app/``.


app/domain
~~~~~~~~~~

Contém os **comandos** da aplicação, implementados como dataclasses simples.
Os comandos carregam apenas os dados de entrada fornecidos pelo usuário (lista
de mnemônicos de variáveis) e não possuem lógica de negócio. Seguem o padrão
Command do Domain-Driven Design.

.. list-table:: Arquivos do pacote domain
   :widths: 35 65
   :header-rows: 1

   * - Arquivo
     - Papel
   * - ``commands.py``
     - Define ``SynthetizeSystem``, ``SynthetizeExecution``, ``SynthetizeScenarios``,
       ``SynthetizeOperation`` e ``SynthetizePolicy`` como dataclasses com o
       campo ``variables: List[str]``.


app/model
~~~~~~~~~

Define o **modelo unificado de dados**: enumerações de variáveis, agregações
espaciais, etapas temporais e dataclasses de síntese para cada categoria.
Este pacote não realiza I/O nem processamento; é a linguagem compartilhada
entre as camadas de serviço e de domínio.

.. list-table:: Subpacotes do pacote model
   :widths: 35 65
   :header-rows: 1

   * - Subpacote
     - Papel
   * - ``model/operation/``
     - Enumerações de variáveis da operação (``Variable``), agregação espacial
       (``SpatialResolution``) e unidade (``Unit``); dataclass ``OperationSynthesis``.
   * - ``model/scenario/``
     - Enumerações de variáveis de cenários, agregação espacial e etapa
       (``Step``); dataclass ``ScenarioSynthesis``.
   * - ``model/execution/``
     - Enumeração de variáveis de execução; dataclass ``ExecutionSynthesis``.
   * - ``model/policy/``
     - Enumeração de variáveis da política; dataclass ``PolicySynthesis``.
   * - ``model/system/``
     - Enumeração de variáveis do sistema; dataclass ``SystemSynthesis``.
   * - ``model/settings.py``
     - Classe ``Settings`` (singleton) que lê variáveis de ambiente como
       ``FORMATO_SINTESE``, ``PROCESSADORES`` e ``DIRETORIO_SINTESE``.


app/services
~~~~~~~~~~~~

Contém toda a **lógica de orquestração e processamento**: os handlers que
recebem comandos e os despacham, os sintetizadores que implementam o pipeline
de síntese para cada categoria, o Deck que fornece acesso cacheado aos dados
de entrada, e o Unit of Work que gerencia o ciclo de vida dos repositórios.

.. list-table:: Arquivos e subpacotes do pacote services
   :widths: 35 65
   :header-rows: 1

   * - Arquivo / Subpacote
     - Papel
   * - ``handlers.py``
     - Funções de despacho de alto nível. Cada função recebe um Command e um
       ``AbstractUnitOfWork``, instancia o sintetizador correspondente e delega
       a execução.
   * - ``unitofwork.py``
     - Define ``AbstractUnitOfWork`` e a implementação concreta ``FSUnitOfWork``.
       Gerencia o ciclo de vida dos repositórios de arquivos e de exportação,
       criando o diretório de saída (``sintese/``) quando necessário.
   * - ``synthesis/system.py``
     - ``SystemSynthetizer``: sintetiza dados estáticos do sistema (submercados,
       usinas, REEs, patamares, etc.) lendo os arquivos de deck via ``Deck``.
   * - ``synthesis/execution.py``
     - ``ExecutionSynthetizer``: sintetiza metadados da execução (versão do
       modelo, tempo de execução, convergência).
   * - ``synthesis/scenario.py``
     - ``ScenarioSynthetizer``: sintetiza cenários estocásticos de energia
       natural afluente e vazão incremental a partir de arquivos como
       ``energiaf*.dat`` e ``vazaof*.dat``.
   * - ``synthesis/operation.py``
     - ``OperationSynthetizer``: sintetiza resultados da operação (CMO, EAR,
       geração, vazões, etc.) a partir dos arquivos ``nwlistop``.
   * - ``synthesis/policy.py``
     - ``PolicySynthetizer``: sintetiza dados da política operativa (cortes de
       Benders, estados visitados) a partir dos arquivos ``nwlistcf``.
   * - ``deck/deck.py``
     - Classe ``Deck`` com métodos de classe cacheados para acesso aos dados de
       entrada do NEWAVE. Cada método acessa um arquivo de deck específico
       (``dger``, ``confhd``, ``patamar``, etc.) através do repositório de
       arquivos e retorna DataFrames Polars prontos para uso.
   * - ``deck/accessors.py``
     - Funções auxiliares de acesso e cache dos objetos de arquivo do NEWAVE.
   * - ``deck/entities.py``
     - Constrói DataFrames de entidades do sistema (usinas, REEs, submercados).
   * - ``deck/hydro.py``
     - Funções de processamento de dados hidráulicos (volumes, cotas, produção).
   * - ``deck/thermal.py``
     - Funções de processamento de dados de usinas termoelétricas.
   * - ``deck/energy.py``
     - Funções de processamento de energia natural afluente e armazenada.
   * - ``deck/exchange.py``
     - Funções de processamento de dados de intercâmbio entre submercados.
   * - ``deck/storage.py``
     - Funções de processamento de dados de armazenamento em reservatórios.
   * - ``deck/temporal.py``
     - Funções de construção de índices temporais (estágios, datas de início e fim).
   * - ``deck/misc.py``
     - Funções auxiliares diversas para o processamento do deck.
   * - ``deck/policy.py``
     - Funções de acesso a dados da política operativa (cortes, estados).
   * - ``deck/bounds.py``
     - Funções de cálculo de limites operativos (mínimos e máximos) das variáveis.
   * - ``deck/context.py``
     - Funções de construção do contexto de execução (número de séries,
       aberturas, estágios individualizados).
   * - ``deck/readers.py``
     - Funções de leitura dos arquivos nwlistop via mapeamento
       (variável, agregação espacial) → função leitora.


app/adapters
~~~~~~~~~~~~

Implementa o padrão **Repository**: abstrai o acesso aos arquivos de entrada
do NEWAVE e a gravação dos arquivos de saída sintetizados, permitindo que a
camada de serviços não dependa de detalhes de I/O ou de formatos específicos.

.. list-table:: Arquivos do pacote adapters
   :widths: 35 65
   :header-rows: 1

   * - Arquivo
     - Papel
   * - ``repository/files.py``
     - Define ``AbstractFilesRepository`` e a implementação concreta
       ``RawFilesRepository``, que utiliza a biblioteca ``inewave`` para fazer
       o parsing dos arquivos de entrada do NEWAVE (``caso.dat``, ``dger.dat``,
       ``confhd.dat``, arquivos nwlistop, nwlistcf, etc.). Os objetos de arquivo
       são mantidos em cache para evitar releituras.
   * - ``repository/export.py``
     - Define ``AbstractExportRepository`` e as implementações concretas
       ``ParquetExportRepository`` (escreve via PyArrow com compatibilidade
       Spark), ``CSVExportRepository`` (escreve via pandas) e
       ``TestExportRepository`` (no-op para testes). A implementação concreta
       é selecionada pela variável de ambiente ``FORMATO_SINTESE``.
   * - ``repository/mappings/``
     - Contém o mapeamento ``(Variable, SpatialResolution) → função leitora``
       utilizado por ``RawFilesRepository.get_nwlistop()`` para despachar a
       leitura do arquivo correto de acordo com a variável e a agregação
       solicitadas.


app/utils
~~~~~~~~~

Fornece **utilitários transversais** utilizados em todas as camadas da
aplicação.

.. list-table:: Arquivos do pacote utils
   :widths: 35 65
   :header-rows: 1

   * - Arquivo
     - Papel
   * - ``log.py``
     - Classe ``Log`` (singleton) que configura o logging assíncrono baseado em
       ``multiprocessing.Queue``, permitindo que processos filhos (usados no
       paralelismo de síntese) enviem registros de log ao processo principal.
   * - ``timing.py``
     - Decorador e função auxiliar ``time_and_log`` para medir e registrar o
       tempo de execução de cada etapa da síntese.
   * - ``singleton.py``
     - Metaclasse ``Singleton`` utilizada por ``Log`` e ``Settings`` para
       garantir instância única durante toda a execução.
   * - ``encoding.py``
     - Função assíncrona ``converte_codificacao`` que invoca o script shell
       ``converte_utf8.sh`` para normalizar a codificação de arquivos de
       entrada antes da leitura pelo ``inewave``.
   * - ``graph.py``
     - Funções de construção e travessia do grafo de usinas hidroelétricas
       (relação jusante/montante) para cálculos de agregação em bacias.
   * - ``regex.py``
     - Função ``match_variables_with_wildcards`` para resolver mnemônicos
       fornecidos pelo usuário (que podem conter curingas) em listas de
       variáveis suportadas.
   * - ``terminal.py``
     - Utilitários para formatação de saída no terminal.
   * - ``fs.py``
     - Funções auxiliares de sistema de arquivos.
   * - ``operations.py``
     - Funções de operações numéricas auxiliares.
   * - ``tz.py``
     - Função ``enforce_utc`` que garante que colunas de datetime nos
       DataFrames exportados estejam na timezone UTC, assegurando
       compatibilidade na leitura posterior.


app/internal
~~~~~~~~~~~~

Centraliza as **constantes** compartilhadas por todas as camadas da aplicação,
evitando strings mágicas espalhadas pelo código.

.. list-table:: Arquivos do pacote internal
   :widths: 35 65
   :header-rows: 1

   * - Arquivo
     - Papel
   * - ``constants.py``
     - Define nomes de colunas dos DataFrames (e.g., ``STAGE_COL``,
       ``SCENARIO_COL``, ``VALUE_COL``), nomes de arquivos de metadados de
       saída (e.g., ``EXECUTION_SYNTHESIS_METADATA_OUTPUT``), fatores de
       conversão de unidades (e.g., ``HM3_M3S_MONTHLY_FACTOR``), e listas de
       colunas comuns a cada categoria de síntese.


Modelo de Dados
---------------

O modelo de dados unificado é descrito em detalhe na página
:ref:`modelo`. Em resumo, as sínteses são organizadas em cinco categorias:

.. list-table:: Categorias de Sintese
   :widths: 20 80
   :header-rows: 1

   * - Categoria
     - Descricao
   * - **Sistema**
     - Dados estáticos da representação do sistema: submercados, usinas
       hidroelétricas e termoelétricas, reservatórios equivalentes de energia
       (REEs), patamares de carga e estágios do estudo.
   * - **Execucao**
     - Metadados sobre a execução do modelo: versão, título, tempo de CPU,
       convergência iterativa e composição de custos.
   * - **Cenarios**
     - Séries temporais dos cenários estocásticos gerados ou processados pelo
       modelo: energia natural afluente e vazão incremental, nas etapas
       forward, backward e simulação final.
   * - **Operacao**
     - Resultados da operação: custo marginal, energia armazenada, geração
       hidráulica e térmica, vazões, intercâmbios e violações operativas,
       por estágio, cenário e agregação espacial.
   * - **Politica**
     - Dados da política operativa: cortes de Benders calculados e recebidos
       e os estados visitados na construção dos cortes.

Cada variável sintetizada é identificada por um mnemônico no formato
``VARIAVEL_AGREGACAO`` (e.g., ``CMO_SBM``, ``EARMF_SIN``, ``GHID_UHE``).
O mapeamento completo entre mnemônicos e grandezas físicas está em
:ref:`modelo`.
