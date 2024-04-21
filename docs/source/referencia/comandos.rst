.. _comandos:

Comandos
=========

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

    $ sintetizador-newave operacao CMO_SBM_EST EARMF_SIN_EST GTER_SBM_PAT

O formato de escrita padrão das sínteses é `PARQUET <https://www.databricks.com/glossary/what-is-parquet>`, que é um formato eficiente
de armazenamento de dados tabulares para aplicações de *big data*.

Caso seja desejado, é possível forçar a saída das sínteses através do argumento opcional `--formato`, para qualquer categoria de síntese::

    $ sintetizador-newave execucao --formato CSV

No caso das sínteses da operação e de cenários, é possível paralelizar a leitura dos arquivos através do argumento opcional `--processadores`::

    $ sintetizador-newave operacao --processadores 8
    $ sintetizador-newave cenarios --processadores 8

A síntese completa também aceita o paralelismo, aplicando-o a todas as categorias de síntese que são suportadas::

    $ sintetizador-newave completa --processadores 24


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

