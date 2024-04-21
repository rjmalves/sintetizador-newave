Como contribuir?
=================

O módulo `inewave` e dependências de desenvolvimento
------------------------------------------------------------

O módulo *inewave* é desenvolvido considerando o framework proposto no módulo `cfinterface <https://github.com/rjmalves/cfi>`_. Este fornece para o desenvolvedor
uma modelagem de cada arquivo de entrada e saída do modelo NEWAVE em uma classe específica.

A leitura de arquivos de entrada utilizando o módulo *inewave* é sempre feita através do método *read*
de cada classe, que atua como construtor.

........

No *sintetizador-newave* a dependência do módulo *inewave* é concentrada na classe Deck, que fornece
objetos nativos, DataFrames ou arrays para as demais partes da aplicação.

Para instalar as dependências de desenvolvimento, incluindo as necessárias para a geração automática do site::
    
    $ git clone https://github.com/rjmalves/sintetizador-newave.git
    $ cd sintetizador-newave
    $ pip install -r dev-requirements.txt

.. warning::

    O conteúdo da documentação não deve ser movido para o repositório. Isto é feito
    automaticamente pelos scripts de CI no caso de qualquer modificação no branch `main`.


Diretrizes de modelagem para o módulo `inewave`
------------------------------------------------

A principal diretriz para o desenvolvimento do *inewave* é a relação entre arquivos do modelo NEWAVE e as classes
disponíveis para uso. Cada arquivo de entrada do modelo é mapeado para uma classe do módulo, que segue
o nome geralmente utilizado para o arquivo nos casos de exemplo que são fornecidos junto com o modelo pelo desenvolvedor
ou pelo ONS para publicação dos decks de PMO. É utilizado sempre `PascalCase` para determinação dos nomes
das classes, sendo que abreviações que possivelmente se encontram nos nomes dos arquivos são ignoradas na mudança de caso. Por exemplo:

- `arquivos.dat` é modelado na classe :ref:`Arquivos <arquivos>`
- `confhd.dat` é modelado na classe :ref:`Confhd <confhd>`
- `earmfpXXX.out` é modelado na classe :ref:`Earmfp <earmfp>`

É convencionado, sempre que possível, que as propriedades das classes que contém os dados processados dos arquivos
lidem com objetos do tipo :obj:`~pandas.DataFrame` para a representação de dados tabulares. Além disso, se possível,
é recomendado processar a informação contida nos arquivos para que esteja na seguindo as formas normais
para dados tabulares, mesmo quando há divergência na representação textual nos arquivos de entrada do NEWAVE. Um exemplo
pode ser visto na modelagem dos arquivos de saída do NWLISTOP quando executado na opção 2.

O arquivo `gttotsin.out`, modelado na classe :ref:`Gttotsin <gttotsin>`, contém informações escritas em
tabelas que são dividas por ano de estudo. Dentro de cada tabela, cada linha representa os 12 meses do ano
para uma das séries utilizadas na simulação final realizada no modelo:

.. code-block:: text

      PMO ABRIL - 2022 - Niveis para 26/03 NW Versao 28
         GERACAO TERMICA TOTAL PARA O SIN (MWmes)

         ANO: 2022
    SERIE   PAT      1        2        3        4        5        6        7        8        9       10       11       12     MEDIA
        1    1      0.0      0.0      0.0    915.3   1341.4   1687.3   1920.0   2132.3   1714.0   1604.4   1297.0   1144.2   1528.4
             2      0.0      0.0      0.0    761.0    741.8    943.9   1104.8   1135.7   1322.1   1315.5   1621.5   1377.7   1147.1
             3      0.0      0.0      0.0   1792.1   1697.5   2189.7   2643.9   2479.4   2839.8   3048.9   2918.5   2313.9   2436.0
         TOTAL      0.0      0.0      0.0   3468.4   3780.6   4820.9   5668.8   5747.3   5875.9   5968.8   5836.9   4835.8   5111.5
        2    1      0.0      0.0      0.0    915.3   1341.4   1695.9   1920.0   2132.3   1788.2   1604.4   1297.0   1144.2   1537.6
             2      0.0      0.0      0.0    761.0    741.8    948.8   1104.8   1135.7   1379.3   1315.5   1621.5   1377.7   1154.0
             3      0.0      0.0      0.0   1792.1   1697.5   2200.8   2643.9   2479.4   2962.8   3048.9   2918.5   2313.9   2450.9
                                                       .
                                                       .
                                                       .
    MEDIA           0.0      0.0      0.0   3566.9   3846.3   4845.3   5682.0   5811.2   5937.0   6037.4   5893.9   4892.9   5168.1
    DPADRAO         0.0      0.0      0.0    138.5    129.5     33.8     64.1    131.9    114.0    143.1    206.0    242.5
    MIN             0.0      0.0      0.0   3463.3   3780.6   4820.9   5644.2   5722.7   5851.3   5944.2   5812.3   4811.2
    P5              0.0      0.0      0.0   3468.4   3780.6   4820.9   5644.2   5747.3   5875.9   5968.8   5812.3   4811.2
    P95             0.0      0.0      0.0   3738.4   4180.8   4957.9   5781.1   6001.8   6130.4   6335.6   6203.7   5292.0
    MAX             0.0      0.0      0.0   3868.6   4822.5   5242.2   6699.3   6902.6   6922.1   7138.0  10175.4   9677.2

Quando processado, as informações contidas nste arquivo são convertidas para um formato normal, com uma coluna para representar `data`, `patamar`, `serie` e `valor`.
Quando as informações contidas nos arquivos não forem representadas em formato semelhante, é recomendado que seja feita uma transformação
semelhante, visto que a maioria das informações armazenadas em bancos de dados estão em formato semelhante.


.. code-block:: python

    from inewave.nwlistop import Gttotsin
    arquivo_gttot = Gttotsin.read("./gttotsin.out")
    arquivo_gttot.valores

                data patamar serie   valor
    0     2022-01-01       1     1     0.0
    1     2022-02-01       1     1     0.0
    2     2022-03-01       1     1     0.0
    3     2022-04-01       1     1   915.3
    4     2022-05-01       1     1  1341.4
    ...          ...     ...   ...     ...
    95995 2022-08-01   TOTAL  2000  5747.3
    95996 2022-09-01   TOTAL  2000  5875.9
    95997 2022-10-01   TOTAL  2000  5968.8
    95998 2022-11-01   TOTAL  2000  5812.3
    95999 2022-12-01   TOTAL  2000  4811.2


As propriedades das classes e também as colunas dos :obj:`~pandas.DataFrame` que são produzidos são convencionados de
serem nomeados em `snake_case`. Além disso, deve-se evitar ao máximo ambiguidades na escolha dos nomes das propriedades e
das colunas. Alguns pontos recorrentes onde são encontradas ambiguidades e deve-se adotar um termo único são:

- Propriedade ou :obj:`~pandas.DataFrame` que contenha informações de usinas (hidrelétricas, termelétricas, etc.) e venham e conter atributos
  como código (`int`) e nome (`str`) convenciona-se chamar de *nome_usina* e *codigo_usina*, para garantir o único sentido possível.
- Propriedade ou :obj:`~pandas.DataFrame` que contenha informações relativas aos submercados de energia, que ora são
  mencionados como subsistemas de energia, adota-se o termo único *submercado*. De modo semelhante, locais onde apareçam
  informações desta entendidade são denominados *codigo_submercado* e *nome_submercado*. O mesmo raciocínio se aplica
  ao se referir a REE.


Convenções de código
---------------------

O *inewave* considera critérios de qualidade de código em seus scripts de Integração Contínua (CI), além de uma bateria de testes unitários.
Desta forma, não é possível realizar uma *release* de uma versão que não passe em todos os testes estabelecidos ou não
atenda aos critérios de qualidade de código impostos.

A primeira convenção é que sejam seguidas as diretrizes de sintaxe `PEP8 <https://peps.python.org/pep-0008/>`_, provenientes do guia de estilo
do autor da linguagem. Além disso, não é recomendado que existam funções muito complexas, com uma quantidade
excessiva de *branches* e *loops*, o que piora e legibilidade do código. Isto pode ser garantido através de módulos
específicos para análise de qualidade de código, como será mencionado a seguir. A única exceção é a regra `E203 <https://www.flake8rules.com/rules/E203.html>`_.

Para garantir a formatação é recomendado utilizar o módulo `black <https://github.com/psf/black>`_, que realiza formatação automática e possui
integração nativa com alguns editores de texto no formato de *plugins* ou extensões. 

A segunda convenção é que seja utilizada tipagem estática. Isto é, não deve ser uitilizada uma variável em código a qual possua
tipo de dados que possa mudar durante a execução do mesmo. Além disso, não deve ser declarada uma variável cujo tipo não é possível de
ser inferido em qualquer situação, permanencendo incerto para o leitor o tipo de dados da variável a menos que seja feita uma
execução de teste do programa.


Procedimentos de teste
-----------------------

O *inewave* realiza testes utilizando o pacote de testes de Python `pytest <https://pytest.org>`_
e controle da qualidade de código com `pylama <https://pylama.readthedocs.io/en/latest//>`_.
A tipagem estática é garantida através do uso de `mypy <http://mypy-lang.org/>`_
, que é sempre executado nos scripts de Integração Contínua (CI).

Antes de realizar um ``git push`` é recomendado que se realize estes três procedimentos
descritos, que serão novamente executados pelo ambiente de CI::

    $ pytest ./tests
    $ mypy ./inewave
    $ pylama ./inewave --ignore E203
