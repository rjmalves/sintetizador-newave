# sintetizador-newave


[![tests](https://github.com/rjmalves/sintetizador-newave/actions/workflows/main.yml/badge.svg)](https://github.com/rjmalves/sintetizador-newave/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/rjmalves/sintetizador-newave/branch/main/graph/badge.svg?token=9AJRL5L21W)](https://codecov.io/gh/rjmalves/sintetizador-newave)


Programa auxiliar para realizar a síntese de dados do programa NEWAVE em arquivos ou banco de dados.


## Instalação

A instalação pode ser feita diretamente a partir do repositório:
```
$ git clone https://github.com/rjmalves/sintetizador-newave
$ cd sintetizador-newave
$ python setup.py install
```

## Modelo Unificado de Dados

O `sintetizador-newave` busca organizar as informações de entrada e saída do modelo NEWAVE em um modelo padronizado para lidar com os modelos do planejamento energético do SIN.

## Comandos

O `sintetizador-newave` é uma aplicação CLI, que pode ser utilizada diretamente no terminal após a instalação:

```
$ sintetizador-newave completa

> 2023-02-10 02:02:05,214 INFO: # Realizando síntese da OPERACAO #
> 2023-02-10 02:02:05,225 INFO: Lendo arquivo dger.dat
> 2023-02-10 02:02:05,227 INFO: Lendo arquivo ree.dat
...
> 2023-02-10 02:02:06,636 INFO: # Fim da síntese #
```

## Documentação

Guias, tutoriais e as referências podem ser encontrados no site oficial do pacote: https://rjmalves.github.io/sintetizador-newave