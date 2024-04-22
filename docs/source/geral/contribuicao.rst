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


Convenções de código
---------------------

O *sintetizador-newave* considera critérios de qualidade de código em seus scripts de Integração Contínua (CI), além de uma bateria de testes unitários.
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

O *sintetizador-newave* realiza testes utilizando o pacote de testes de Python `pytest <https://pytest.org>`_
e controle da qualidade de código com `pylama <https://pylama.readthedocs.io/en/latest//>`_.
A tipagem estática é garantida através do uso de `mypy <http://mypy-lang.org/>`_
, que é sempre executado nos scripts de Integração Contínua (CI).

Antes de realizar um ``git push`` é recomendado que se realize estes três procedimentos
descritos, que serão novamente executados pelo ambiente de CI::

    $ pytest ./tests
    $ mypy ./app
    $ pylama ./app --ignore E203
