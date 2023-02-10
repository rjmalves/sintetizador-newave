"""
========================================
Síntese da Operação
========================================
"""

#%%
# Para realizar a síntese da operação de um caso do NEWAVE é necessário estar em um diretório
# no qual estão os principais arquivos de saída do modelo. Em geral, as variáveis da operação
# são extraídas das saídas do programa auxiliar NWLISTOP, no modo TABELAS (opção 2).
# Para a síntese do CMO por submercado, por estágio, são necessários os arquivos cmargXXX-med.out.
# Para a síntese do EARM para o SIN, é necessário o earmfsin.out.
# Além dos arquivos dos quais são extraídas as variáveis em si, são lidos também alguns arquivos de entrada
# do modelo, como o `dger.dat`, `ree.dat` e `sistema.dat`. Neste contexto, basta fazer::
#
#    $ sintetizador-newave operacao CMO_SBM_EST EARMF_SIN_EST
#

#%%
# O sintetizador irá exibir o log da sua execução::
#
#    >>> 2023-02-10 02:02:05,214 INFO: # Realizando síntese da OPERACAO #
#    >>> 2023-02-10 02:02:05,225 INFO: Lendo arquivo dger.dat
#    >>> 2023-02-10 02:02:05,227 INFO: Lendo arquivo ree.dat
#    >>> 2023-02-10 02:02:05,232 INFO: Caso com geração de cenários de eólica: False
#    >>> 2023-02-10 02:02:05,232 INFO: Caso com modelagem híbrida: True
#    >>> 2023-02-10 02:02:05,232 INFO: Variáveis: [CMO_SBM_EST, EARMF_SIN_EST]
#    >>> 2023-02-10 02:02:05,232 INFO: Realizando síntese de CMO_SBM_EST
#    >>> 2023-02-10 02:02:05,232 INFO: Lendo arquivo sistema.dat
#    >>> 2023-02-10 02:02:05,248 INFO: Processando arquivo do submercado: 1 - SUDESTE
#    >>> 2023-02-10 02:02:05,427 INFO: Processando arquivo do submercado: 2 - SUL
#    >>> 2023-02-10 02:02:05,605 INFO: Processando arquivo do submercado: 3 - NORDESTE
#    >>> 2023-02-10 02:02:05,782 INFO: Processando arquivo do submercado: 4 - NORTE
#    >>> 2023-02-10 02:02:06,300 INFO: Realizando síntese de EARMF_SIN_EST
#    >>> 2023-02-10 02:02:06,300 INFO: Processando arquivo do SIN
#    >>> 2023-02-10 02:02:06,636 INFO: # Fim da síntese #


#%%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

cmo = pd.read_parquet("sintese/CMO_SBM_EST.parquet.gzip")
earm = pd.read_parquet("sintese/EARMF_SIN_EST.parquet.gzip")

#%%
# O formato dos dados de CMO:
cmo.head(10)

#%%
# O formato dos dados de EARM:
earm.head(10)

#%%
# De modo geral, os arquivos das sínteses de operação sempre possuem as colunas
# `estagio`, `dataInicio`, `dataFim`, `cenario` e `valor`. A depender se o arquivo é
# relativo a uma agregação espacial diferente de todo o SIN ou agregação temporal
# diferente do valor médio por estágio, existirão outras colunas adicionais para determinar
# de qual subconjunto da agregação o dado pertence. Por exemplo, no arquivo da síntese de
# CMO_SBM_EST, existe uma coluna adicional de nome `submercado`.

#%%
# A coluna de cenários contém não somente inteiros de 1 a N, onde N é o número de séries da
# simulação final do modelo, mas também algumas outras palavras especiais, associadas a estatísticas
# processadas sobre os cenários: `min`, `max`, `mean`, `p5`, `p10`, ..., `p95`.

cenarios = earm["cenario"].unique().tolist()
cenarios_estatisticas = [c for c in cenarios if c not in list(range(1, 2001))]
print(cenarios_estatisticas)

#%%
# Através das estatísticas é possível fazer um gráfico de quantis, para ilustrar a dispersão
# da variável da operação com os cenários:
fig = go.Figure()
for p in range(10, 91, step=10):
    earm_p = earm.loc[earm["cenario"] == f"p{p}"]
    fig.add_trace(
        go.Scatter(
            x=earm_p["dataFim"],
            y=earm_p["valor"],
            line={
                "color": "rgba(66, 135, 245, 0.3)",
                "width": 2,
            },
            name=f"p{p}",
            showlegend=False,
        )
    )
fig

#%%
# Também é possível fazer uma análise por meio de gráficos de linhas com áreas sombreadas,
# para ilustrar a cobertura dos cenários no domínio da variável:
fig = go.Figure()
earm_mean = earm.loc[earm["cenario"] == "mean"]
earm_max = earm.loc[earm["cenario"] == "max"]
earm_min = earm.loc[earm["cenario"] == "min"]
fig.add_trace(
    go.Scatter(
        x=earm_mean["dataFim"],
        y=earm_mean["valor"],
        line={
            "color": "rgba(66, 135, 245, 0.9)",
            "width": 4,
        },
        name="mean",
    )
)
fig.add_trace(
    go.Scatter(
        x=earm_min["dataFim"],
        y=earm_min["valor"],
        line={
            "color": "rgba(66, 135, 245, 0.9)",
            "width": 4,
        },
        line_color="rgba(66, 135, 245, 0.3)",
        fillcolor="rgba(66, 135, 245, 0.3)",
        name="min",
    )
)
fig.add_trace(
    go.Scatter(
        x=earm_max["dataFim"],
        y=earm_max["valor"],
        line={
            "color": "rgba(66, 135, 245, 0.9)",
            "width": 4,
        },
        line_color="rgba(66, 135, 245, 0.3)",
        fill="tonexty",
        name="max",
    )
)
fig
