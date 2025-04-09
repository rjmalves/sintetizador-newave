Aplicação CLI para síntese das saídas do modelo NEWAVE
=======================================================

O *sintetizador-newave* é uma aplicação CLI para formatação e consolidação dos arquivos de saída do modelo `NEWAVE <https://www.cepel.br/linhas-de-pesquisa/newave/>`_. O NEWAVE é desenvolvido pelo `CEPEL <http://www.cepel.br/>`_ e utilizado para a planejamento da operação do Sistema Interligado Nacional (SIN).

O *sintetizador-newave* fornece uma maneira de consolidar resultados provenientes de execuções do modelo NEWAVE, que são impressos majoritariamente em
arquivos textuais ou binários com formatos personalizados, em tabelas normalizadas e estruturadas em DataFrames do `pandas <https://pandas.pydata.org/pandas-docs/stable/index.html>`_.

A aplicação atualmente utiliza o módulo `inewave <https://github.com/rjmalves/inewave>`_ para manipulação dos arquivos de saída do modelo, abstraindo a maioria das regras de negócio existentes neste processamento.

O modelo de dados adotado para as saídas sintetizadas é compartilhado com outras aplicações CLI, responsáveis por realizar o mesmo processo com os arquivos de saída de outros modelos utilizados no planejamento energético.
