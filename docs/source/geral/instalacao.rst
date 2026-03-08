Instalação
============

.. note::

    O *sintetizador-newave* é compatível com versões de Python >= 3.11.

Instalando via PyPI
--------------------

A forma mais simples de instalar o *sintetizador-newave* é diretamente do `PyPI <https://pypi.org/project/sintetizador-newave/>`_::

    $ pip install sintetizador-newave

Instalando com uv
------------------

O `uv <https://docs.astral.sh/uv/>`_ é um gerenciador de pacotes moderno e mais rápido que o pip. Para instalar o *sintetizador-newave* com uv::

    $ uv pip install sintetizador-newave

Instalando a partir do repositório oficial
-------------------------------------------

Para instalar a versão de desenvolvimento mais recente diretamente do repositório, é necessário
primeiramente desinstalar a versão instalada (se houver), com::

    $ pip uninstall sintetizador-newave

Em seguida, basta fazer::

    $ pip install git+https://github.com/rjmalves/sintetizador-newave

Também é possível selecionar um branch ou release específicos::

    $ pip install git+https://github.com/rjmalves/sintetizador-newave@v1.1.0
