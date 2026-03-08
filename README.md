# sintetizador-newave

[![tests](https://github.com/rjmalves/sintetizador-newave/actions/workflows/main.yml/badge.svg)](https://github.com/rjmalves/sintetizador-newave/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/rjmalves/sintetizador-newave/branch/main/graph/badge.svg?token=9AJRL5L21W)](https://codecov.io/gh/rjmalves/sintetizador-newave)
[![PyPI](https://img.shields.io/pypi/v/sintetizador-newave)](https://pypi.org/project/sintetizador-newave/)
[![Python](https://img.shields.io/pypi/pyversions/sintetizador-newave)](https://pypi.org/project/sintetizador-newave/)
[![docs](https://github.com/rjmalves/sintetizador-newave/actions/workflows/docs.yml/badge.svg)](https://rjmalves.github.io/sintetizador-newave/)
[![license](https://img.shields.io/github/license/rjmalves/sintetizador-newave)](https://github.com/rjmalves/sintetizador-newave/blob/main/LICENSE)

## Sobre

O `sintetizador-newave` é uma aplicação CLI para formatação e consolidação dos arquivos de saída do modelo [NEWAVE](https://www.cepel.br/linhas-de-pesquisa/newave/), desenvolvido pelo [CEPEL](http://www.cepel.br/) e utilizado no planejamento da operação do Sistema Interligado Nacional (SIN). A ferramenta lê os arquivos textuais e binários gerados pelo NEWAVE e os converte em tabelas normalizadas e estruturadas em DataFrames [Polars](https://docs.pola.rs/), prontas para análise e integração com outros sistemas.

O modelo de dados adotado para as saídas sintetizadas é compatível com o ecossistema de ferramentas de síntese do planejamento energético brasileiro, facilitando a interoperabilidade entre casos de diferentes modelos. Internamente, a aplicação utiliza o módulo [inewave](https://github.com/rjmalves/inewave) para abstrair as regras de leitura dos arquivos proprietários do NEWAVE.

A ferramenta é destinada a engenheiros, pesquisadores e desenvolvedores que trabalham com análise de resultados de planejamento energético e necessitam de acesso programático eficiente às saídas do NEWAVE em formatos modernos e interoperáveis.

## Funcionalidades

- Síntese de dados de **operação** (NWLISTOP): variáveis como CMO, EARM, GTER, VAGUA e dezenas de outras por submercado, REE e UHE
- Síntese de dados de **cenários**: afluências e outras variáveis estocásticas geradas pelo modelo
- Síntese de dados de **política** (NWLISTCF): função de custo futuro e cortes de Benders
- Síntese de dados de **sistema**: configuração de submercados, REEs, usinas e intercâmbios
- Síntese de dados de **execução**: tempo de processamento, convergência e informações do caso
- Saída em formato **Parquet** por padrão, com suporte opcional a CSV
- Paralelização da leitura de arquivos via argumento `--processadores`
- Seleção granular de variáveis individuais ou síntese completa com um único comando
- Suporte a Python 3.11, 3.12, 3.13 e 3.14

## Início Rápido

### Instalação

```bash
pip install sintetizador-newave
```

Ou com `uv`:

```bash
uv pip install sintetizador-newave
```

### Uso

Execute a síntese completa de um caso NEWAVE a partir do diretório onde estão os arquivos de saída:

```bash
$ sintetizador-newave completa
```

Para sintetizar apenas categorias específicas ou variáveis individuais:

```bash
$ sintetizador-newave operacao CMO_SBM EARMF_SIN GTER_SBM
```

Para paralelizar a leitura dos arquivos:

```bash
$ sintetizador-newave operacao --processadores 4
```

Saída esperada no terminal:

```
2024-04-22 09:53:56,845 INFO: # Realizando síntese da OPERACAO #
2024-04-22 09:53:56,868 INFO: Sinteses: [CMO_SBM, VAGUA_REE, VAGUA_UHE, ...]
2024-04-22 09:53:56,870 INFO: Realizando sintese de CMO_SBM
2024-04-22 09:53:58,734 INFO: Tempo para obter dados de SBM: 1.85 s
2024-04-22 09:53:58,753 INFO: Tempo para exportacao dos dados: 0.01 s
2024-04-22 09:53:58,754 INFO: Tempo para sintese de CMO_SBM: 1.88 s
...
2024-04-22 09:51:19,529 INFO: Tempo para sintese da operacao: 33.33 s
2024-04-22 09:51:19,529 INFO: # Fim da síntese #
```

## Documentação

Guias, tutoriais e referências completas estão disponíveis no site oficial do pacote: https://rjmalves.github.io/sintetizador-newave

## Licença

Este projeto está licenciado sob a licença [MIT](https://github.com/rjmalves/sintetizador-newave/blob/main/LICENSE).
