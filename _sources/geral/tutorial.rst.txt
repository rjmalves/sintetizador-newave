Tutorial
============


Categorias de Síntese
-----------------------

O `sintetizador-newave` está disponível como uma ferramenta CLI. Para visualizar quais comandos este pode realizar,
que estão associados aos tipos de sínteses, basta fazer::

    $ sintetizador-newave --help

A saída observada deve ser::

    >>> Usage: sintetizador-newave [OPTIONS] COMMAND [ARGS]...
    >>> 
    >>>   Aplicação para realizar a síntese de informações em um modelo unificado de
    >>>   dados para o NEWAVE.
    >>> 
    >>> Options:
    >>>   --help  Show this message and exit.
    >>> 
    >>> Commands:
    >>>   completa  Realiza a síntese completa do NEWAVE.
    >>>   cenarios  Realiza a síntese dos dados de cenários do NEWAVE.
    >>>   execucao  Realiza a síntese dos dados da execução do NEWAVE.
    >>>   politica  Realiza a síntese dos dados da política do NEWAVE (NWLISTCF).
    >>>   operacao  Realiza a síntese dos dados da operação do NEWAVE (NWLISTOP).
    >>>   sistema   Realiza a síntese dos dados do sistema do NEWAVE.
    >>>   limpeza   Realiza a limpeza dos dados resultantes de uma síntese.

Além disso, cada um dos comandos possui um menu específico, que pode ser visto com, por exemplo::

    $ sintetizador-newave operacao --help

Que deve ter como saída::

    >>> Usage: sintetizador-newave operacao [OPTIONS] [VARIAVEIS]...
    >>> 
    >>>   Realiza a síntese dos dados da operação do NEWAVE (NWLISTOP).
    >>> 
    >>> Options:
    >>>   --formato TEXT           formato para escrita da síntese
    >>>   --processadores INTEGER  numero de processadores para paralelizar
    >>>   --help                   Show this message and exit.


Argumentos Existentes
-----------------------

Para realizar a síntese completa do caso, está disponível o comando `completa`, que realizará toda a síntese possível::

    $ sintetizador-newave completa 

Se for desejado não realizar a síntese completa, mas apenas de alguns dos elementos, é possível chamar cada elemento a ser sintetizado::

    $ sintetizador-newave operacao CMO_SBM EARMF_SIN GTER_SBM

O formato de escrita padrão das sínteses é `PARQUET <https://www.databricks.com/glossary/what-is-parquet>`, que é um formato eficiente
de armazenamento de dados tabulares para aplicações de *big data*.

Caso seja desejado, é possível forçar a saída das sínteses através do argumento opcional `--formato`, para qualquer categoria de síntese::

    $ sintetizador-newave execucao --formato CSV

No caso das sínteses da operação e de cenários, é possível paralelizar a leitura dos arquivos através do argumento opcional `--processadores`::

    $ sintetizador-newave operacao --processadores 8
    $ sintetizador-newave cenarios --processadores 8

A síntese completa também aceita o paralelismo, aplicando-o a todas as categorias de síntese que são suportadas::

    $ sintetizador-newave completa --processadores 24



Exemplo de Uso
------------------


Um exemplo de chamada ao programa para realizar a síntese da operação de um caso do NEWAVE é o seguinte::

    $ sintetizador-newave operacao --processadores 4

O log observado no terminal deve ser semelhante a::

    >>> 2024-04-22 09:53:56,845 INFO: # Realizando síntese da OPERACAO #
    >>> 2024-04-22 09:53:56,868 INFO: Sinteses: [CMO_SBM, VAGUA_REE, VAGUA_UHE, ...]
    >>> 2024-04-22 09:53:56,870 INFO: Realizando sintese de CMO_SBM
    >>> 2024-04-22 09:53:58,734 INFO: Tempo para obter dados de SBM: 1.85 s
    >>> 2024-04-22 09:53:58,743 INFO: Tempo para compactacao dos dados: 0.01 s
    >>> 2024-04-22 09:53:58,744 INFO: Tempo para calculo dos limites: 0.00 s
    >>> 2024-04-22 09:53:58,747 INFO: Tempo para preparacao para exportacao: 0.00 s
    >>> 2024-04-22 09:53:58,753 INFO: Tempo para exportacao dos dados: 0.01 s
    >>> 2024-04-22 09:53:58,754 INFO: Tempo para sintese de CMO_SBM: 1.88 s
    >>> ...
    >>> 2024-04-22 09:51:17,293 INFO: Realizando sintese de VEVAP_SBM
    >>> 2024-04-22 09:51:17,295 INFO: Tempo para compactacao dos dados: 0.00 s
    >>> 2024-04-22 09:51:17,295 INFO: Tempo para calculo dos limites: 0.00 s
    >>> 2024-04-22 09:51:17,315 INFO: Tempo para preparacao para exportacao: 0.02 s
    >>> 2024-04-22 09:51:17,320 INFO: Tempo para exportacao dos dados: 0.00 s
    >>> 2024-04-22 09:51:17,320 INFO: Tempo para sintese de VEVAP_SBM: 0.03 s
    >>> 2024-04-22 09:51:17,320 INFO: Realizando sintese de VEVAP_SIN
    >>> 2024-04-22 09:51:17,322 INFO: Tempo para compactacao dos dados: 0.00 s
    >>> 2024-04-22 09:51:17,322 INFO: Tempo para calculo dos limites: 0.00 s
    >>> 2024-04-22 09:51:17,342 INFO: Tempo para preparacao para exportacao: 0.02 s
    >>> 2024-04-22 09:51:17,346 INFO: Tempo para exportacao dos dados: 0.00 s
    >>> 2024-04-22 09:51:17,346 INFO: Tempo para sintese de VEVAP_SIN: 0.03 s
    >>> 2024-04-22 09:51:19,529 INFO: Tempo para sintese da operacao: 33.33 s
    >>> 2024-04-22 09:51:19,529 INFO: # Fim da síntese #