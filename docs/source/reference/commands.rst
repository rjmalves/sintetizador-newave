.. _comandos:

Comandos
=========

O `sintetizador-newave` está disponível como uma ferramenta CLI. Para visualizar quais comandos este pode realizar,
que estão associados aos tipos de sínteses, basta fazer:

    $ sintetizador-newave --help

A saída observada deve ser:

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
    >>>   execucao  Realiza a síntese dos dados da execução do NEWAVE.
    >>>   limpeza   Realiza a limpeza dos dados resultantes de uma síntese.
    >>>   operacao  Realiza a síntese dos dados da operação do NEWAVE (NWLISTOP).
    >>>   sistema   Realiza a síntese dos dados do sistema do NEWAVE.

Além disso, cada um dos comandos possui um menu específico, que pode ser visto com, por exemplo:

    $ sintetizador-newave operacao --help

Que deve ter como saída:

    >>> Usage: sintetizador-newave operacao [OPTIONS] [VARIAVEIS]...
    >>> 
    >>>   Realiza a síntese dos dados da operação do NEWAVE (NWLISTOP).
    >>> 
    >>> Options:
    >>>   --formato TEXT  formato para escrita da síntese
    >>>   --help          Show this message and exit.

