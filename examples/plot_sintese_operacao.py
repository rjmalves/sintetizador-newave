"""
========================================
Síntese da Operação
========================================
"""

# %%
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

# %%
# O sintetizador irá exibir o log da sua execução::
#
#    >>> 2024-04-22 09:53:56,845 INFO: # Realizando síntese da OPERACAO #
#    >>> 2024-04-22 09:53:56,868 INFO: Sinteses: [CMO_SBM, EARMF_SIN]
#    >>> 2024-04-22 09:53:56,870 INFO: Realizando sintese de CMO_SBM
#    >>> 2024-04-22 09:53:58,734 INFO: Tempo para obter dados de SBM: 1.85 s
#    >>> 2024-04-22 09:53:58,743 INFO: Tempo para compactacao dos dados: 0.01 s
#    >>> 2024-04-22 09:53:58,744 INFO: Tempo para calculo dos limites: 0.00 s
#    >>> 2024-04-22 09:53:58,747 INFO: Tempo para preparacao para exportacao: 0.00 s
#    >>> 2024-04-22 09:53:58,753 INFO: Tempo para exportacao dos dados: 0.01 s
#    >>> 2024-04-22 09:53:58,754 INFO: Tempo para sintese de CMO_SBM: 1.88 s
#    >>> 2024-04-22 09:53:56,870 INFO: Realizando sintese de EARMF_SIN
#    >>> 2024-04-22 09:53:58,734 INFO: Tempo para obter dados do SIN: 0.29 s
#    >>> 2024-04-22 09:53:58,743 INFO: Tempo para compactacao dos dados: 0.01 s
#    >>> 2024-04-22 09:53:58,744 INFO: Tempo para calculo dos limites: 0.8 s
#    >>> 2024-04-22 09:53:58,747 INFO: Tempo para preparacao para exportacao: 0.00 s
#    >>> 2024-04-22 09:53:58,753 INFO: Tempo para exportacao dos dados: 0.01 s
#    >>> 2024-04-22 09:53:58,754 INFO: Tempo para sintese de CMO_SBM: 1.11 s
#    >>> 2024-04-22 09:51:19,529 INFO: Tempo para sintese da operacao: 3.50 s
#    >>> 2024-04-22 09:51:19,529 INFO: # Fim da síntese #

# %%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

cmo = pd.read_parquet("sintese/CMO_SBM.parquet")
earm = pd.read_parquet("sintese/EARMF_SIN.parquet")


# %%
# O formato dos dados de CMO:
cmo.head(10)

# %%
# Os tipos de dados da síntese de CMO:
cmo.dtypes

# %%
# O formato dos dados de EARM:
earm.head(10)

# %%
# Os tipos de dados da síntese de EARM:
earm.dtypes

# %%
# De modo geral, os arquivos das sínteses de operação sempre possuem as colunas
# `estagio`, `data_inicio`, `data_fim`, `cenario`, `patamar` e `valor`. A depender se o arquivo é
# relativo a uma agregação espacial diferente de todo o SIN, existirão outras colunas adicionais para determinar
# de qual subconjunto da agregação o dado pertence. Por exemplo, no arquivo da síntese de
# CMO_SBM, existe uma coluna adicional de nome `submercado`.

# %%
# A coluna de cenários contém somente inteiros de 1 a N, onde N é o número de séries da
# simulação final do modelo.

cenarios = earm["cenario"].unique().tolist()
print(cenarios)

# %%
# Através das estatísticas é possível fazer um gráfico de caixas, para ilustrar a dispersão
# da variável da operação com os cenários:
fig = px.box(earm, x="data_inicio", y="valor")
fig


# %%
# Para variáveis da operação que possuam diferentes subconjuntos, como os submercados, podem ser utilizados
# gráficos de violino para avaliação da dispersão:
cmos_2omes = cmo.loc[cmo["estagio"] == 2]
fig = px.violin(
    cmos_2omes,
    y="valor",
    color="submercado",
    box=True,
)
fig
