Instalação
============

O *sintetizador-newave* é compatível com versões de Python >= 3.8.

Em posse de uma instalação local de Python, é recomendado que se use um ambiente virtual para instalação de módulos de terceiros, sendo que o *idessem* não é uma exceção.
Para mais detalhes sobre o uso de ambientes virtuais, recomenda-se a leitura do recurso oficial de Python para ambientes virtuais: `venv <https://docs.python.org/3/library/venv.html>`_.

Antes de prosseguir, é necessário verificar se está instalada a última versão do ``pip``, o gerenciador de pacotes de Python. Isso pode ser feito com, por exemplo::

    $ pip install ---upgrade pip

Para maiores informações, é recomendado visitar a documentação oficial do `pip <https://pip.pypa.io/en/stable/installing/>`_.



Instalando a partir do repositório
-----------------------------------

É possível realizar a instalação desta versão fazendo o uso do `Git <https://git-scm.com/>`_. Para instalar a versão de desenvolvimento, basta fazer::

    $ git clone https://github.com/rjmalves/sintetizador-newave
    $ cd sintetizador-newave
    $ python setup.py install

