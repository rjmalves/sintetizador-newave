Guia de Performance
===================

Este guia descreve como configurar o *sintetizador-newave* para obter o melhor
desempenho possível, especialmente em casos grandes do NEWAVE com muitos
cenários ou muitas usinas hidroelétricas.

.. contents:: Índice desta página
   :local:
   :depth: 2


Paralelismo
-----------

O *sintetizador-newave* suporta a paralelização da leitura dos arquivos de
saída do NEWAVE através do argumento ``--processadores``. Internamente, a
aplicação utiliza ``ProcessPoolExecutor`` (da biblioteca padrão
``concurrent.futures``) para distribuir o trabalho de leitura entre múltiplos
processos do sistema operacional.

Quais comandos aceitam ``--processadores``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

O argumento ``--processadores`` está disponível nos seguintes subcomandos:

- ``operacao`` — síntese dos dados de operação (NWLISTOP)
- ``cenarios`` — síntese dos dados de cenários
- ``completa`` — realiza todos os tipos de síntese; aplica o paralelismo a
  todas as categorias que o suportam

Os subcomandos ``sistema``, ``execucao`` e ``politica`` não aceitam
``--processadores`` e sempre executam de forma sequencial.

Como funciona o paralelismo?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Para as sínteses de operação e cenários, os processos workers são criados
em grupos por resolução espacial (por submercado, por REE, por UHE, etc.).
Um pool de processos é aberto no início de cada grupo e encerrado ao final
dele. O número de workers em cada pool é controlado pelo valor de
``--processadores``.

A síntese do sistema interligado (resolução ``SIN``) não utiliza pool de
processos e é sempre executada sequencialmente.

Valores recomendados
~~~~~~~~~~~~~~~~~~~~~

Como ponto de partida, utilize um valor igual ao número de núcleos físicos
disponíveis na máquina. Ajuste para baixo caso a memória RAM seja limitada,
pois cada processo worker carrega dados de síntese em memória
independentemente.

- Em máquinas com 4 a 8 núcleos, valores de ``--processadores`` entre 4 e 8
  costumam oferecer bom desempenho.
- Em servidores com muitos núcleos, valores maiores (por exemplo, 16 ou 24)
  podem ser usados com o comando ``completa``.
- Valores superiores ao número de núcleos físicos disponíveis tendem a não
  trazer ganho adicional e podem aumentar a contenção de I/O em disco.

O padrão é ``--processadores 1`` (execução sequencial) quando o argumento é
omitido.

.. note::

   Ao utilizar ``--processadores`` em conjunto com ``--formato CSV``, o ganho
   de paralelismo no processamento pode ser parcialmente limitado pela menor
   velocidade de escrita do formato CSV em comparação ao PARQUET.

Exemplos de uso com paralelismo:

.. code-block:: bash

   # Síntese de operação com 8 processos paralelos
   sintetizador-newave operacao --processadores 8

   # Síntese completa com 16 processos e saída em PARQUET
   sintetizador-newave completa --processadores 16 --formato PARQUET


Formato de Saída
----------------

O *sintetizador-newave* suporta dois formatos para escrita dos arquivos de
síntese: **PARQUET** (padrão) e **CSV**. A escolha do formato afeta
significativamente o tamanho dos arquivos gerados e a velocidade de leitura
posterior.

PARQUET (padrão)
~~~~~~~~~~~~~~~~

O formato PARQUET é um formato colunar binário desenvolvido pelo Apache e
amplamente utilizado em aplicações de *big data*. Os arquivos gerados pelo
*sintetizador-newave* utilizam compressão **Snappy** (padrão do Apache Arrow),
resultando em arquivos substancialmente menores do que o equivalente em CSV.

Características do formato PARQUET:

- Armazenamento colunar eficiente, especialmente vantajoso para DataFrames com
  muitas linhas e poucas colunas consultadas
- Compressão automática com Snappy, sem necessidade de configuração adicional
- Preservação de tipos de dados (inteiros, floats, datas com fuso horário UTC)
- Compatível com Apache Arrow, Polars, pandas e PySpark

.. note::

   A partir da versão ``v2.0.0``, o PARQUET utiliza compressão **Snappy**
   (padrão do Arrow) em vez de ``gzip``. A extensão dos arquivos passou de
   ``.parquet.gz`` para ``.parquet``.

CSV
~~~

O formato CSV é um formato de texto simples, compatível com qualquer
ferramenta que processe dados tabulares. No entanto, apresenta desvantagens
em relação ao PARQUET para casos grandes:

- Arquivos significativamente maiores, pois não há compressão
- Escrita e leitura mais lentas para grandes volumes de dados
- Sem preservação de tipos: colunas de data/hora são armazenadas como texto e
  precisam ser convertidas na leitura
- Sem metadados de esquema, o que pode exigir inferência de tipos pelo leitor

Use CSV apenas quando a ferramenta de destino não suportar o formato PARQUET.

Comparação de uso:

.. code-block:: bash

   # Formato padrão PARQUET (recomendado)
   sintetizador-newave operacao --processadores 8

   # Formato CSV (compatibilidade com ferramentas legadas)
   sintetizador-newave operacao --formato CSV

   # Síntese completa com PARQUET explícito e paralelismo
   sintetizador-newave completa --processadores 16 --formato PARQUET

.. note::

   O argumento ``--formato`` está disponível em todos os subcomandos de
   síntese (``operacao``, ``cenarios``, ``sistema``, ``execucao``,
   ``politica``, ``completa``). O valor padrão é ``PARQUET`` e não precisa
   ser especificado explicitamente.


Migração para Polars
--------------------

A partir da série ``v2.x``, o *sintetizador-newave* migrou o processamento
interno de DataFrames de **pandas** para **Polars**. O Polars é uma biblioteca
de DataFrames em Rust com bindings Python, projetada para eficiência de memória
e velocidade de processamento.

Impacto da migração:

- **Menor uso de memória**: o Polars utiliza internamente o formato Apache
  Arrow, que representa os dados de forma mais compacta do que o pandas.
- **Processamento mais rápido**: operações sobre DataFrames — como filtros,
  junções e agregações — são executadas com maior eficiência, especialmente
  em DataFrames com muitas linhas.
- **Integração nativa com PARQUET**: a escrita de arquivos PARQUET a partir
  de Polars DataFrames evita conversões intermediárias desnecessárias,
  reduzindo o tempo de exportação.

O pipeline de síntese utiliza ``pl.DataFrame`` (Polars) ao longo de todo o
processamento interno, desde a leitura dos dados brutos até a exportação
final.

.. note::

   Os arquivos ``.parquet`` gerados são totalmente compatíveis com pandas e
   PySpark para leitura posterior, independentemente do formato interno
   utilizado na escrita.


Recomendações para Casos Grandes
---------------------------------

Casos grandes do NEWAVE — com muitos cenários, muitas usinas hidroelétricas
(UHEs) ou muitos estágios — exigem atenção à configuração de paralelismo,
formato de saída e recursos de hardware disponíveis.

Paralelismo
~~~~~~~~~~~

- Utilize ``--processadores`` com um valor próximo ao número de núcleos
  físicos disponíveis na máquina.
- Para a síntese completa de um caso grande, o comando ``completa`` com
  ``--processadores`` aplica o paralelismo a todas as categorias que o
  suportam, sendo preferível a executar cada categoria separadamente.
- Monitore o uso de memória RAM ao aumentar o número de processadores: cada
  processo worker carrega seus dados de forma independente.

Formato de saída
~~~~~~~~~~~~~~~~

- Use sempre ``PARQUET`` para casos grandes. A diferença no tamanho dos
  arquivos e na velocidade de leitura posterior é significativa em relação ao
  CSV.
- Certifique-se de que há espaço em disco suficiente no diretório de
  síntese. Para casos com muitas variáveis e muitas entidades, o volume total
  de arquivos PARQUET pode ser considerável.

Diretório de saída
~~~~~~~~~~~~~~~~~~

- Por padrão, os arquivos de síntese são gravados no subdiretório ``sintese/``
  dentro do diretório de trabalho corrente. Para alterar o destino, defina a
  variável de ambiente ``DIRETORIO_SINTESE`` antes de executar o sintetizador.
- Prefira executar a síntese em um sistema de arquivos local rápido. Diretórios
  em rede ou com alta latência de I/O podem limitar o ganho obtido com o
  paralelismo.

Exemplo de invocação recomendada para um caso grande:

.. code-block:: bash

   # Síntese completa com paralelismo máximo e formato eficiente
   sintetizador-newave completa --processadores 16 --formato PARQUET

.. note::

   O número ideal de processadores depende do hardware disponível e das
   características do caso. Recomenda-se testar com valores crescentes
   (por exemplo, 4, 8, 16) e observar o tempo total de execução reportado
   no log: ``Tempo para sintese da operacao: X.XX s``.
