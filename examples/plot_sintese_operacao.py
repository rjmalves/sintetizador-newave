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
#    $ sintetizador-newave operacao --processadores 4
#

# %%
# O sintetizador irá exibir o log da sua execução::
#
#    >>> 2024-04-22 09:53:56,845 INFO: # Realizando síntese da OPERACAO #
#    >>> .
#    >>> .
#    >>> .
#    >>> 2024-07-16 17:33:36,613 INFO: Realizando sintese de VEVAP_UHE
#    >>> 2024-07-16 17:33:36,615 INFO: Tempo para compactacao dos dados: 0.00 s
#    >>> 2024-07-16 17:33:36,616 INFO: Tempo para calculo dos limites: 0.00 s
#    >>> 2024-07-16 17:33:36,640 INFO: Tempo para armazenamento na cache: 0.00 s
#    >>> 2024-07-16 17:33:36,641 INFO: Tempo para preparacao para exportacao: 0.02 s
#    >>> 2024-07-16 17:33:36,646 INFO: Tempo para exportacao dos dados: 0.01 s
#    >>> 2024-07-16 17:33:36,647 INFO: Tempo para sintese de VEVAP_UHE: 0.03 s
#    >>> 2024-07-16 17:33:36,647 INFO: Realizando sintese de VEVAP_REE
#    >>> 2024-07-16 17:33:36,649 INFO: Tempo para compactacao dos dados: 0.00 s
#    >>> 2024-07-16 17:33:36,650 INFO: Tempo para calculo dos limites: 0.00 s
#    >>> 2024-07-16 17:33:36,676 INFO: Tempo para preparacao para exportacao: 0.03 s
#    >>> 2024-07-16 17:33:36,681 INFO: Tempo para exportacao dos dados: 0.01 s
#    >>> 2024-07-16 17:33:36,682 INFO: Tempo para sintese de VEVAP_REE: 0.03 s
#    >>> 2024-07-16 17:33:36,682 INFO: Realizando sintese de VEVAP_SBM
#    >>> 2024-07-16 17:33:36,684 INFO: Tempo para compactacao dos dados: 0.00 s
#    >>> 2024-07-16 17:33:36,684 INFO: Tempo para calculo dos limites: 0.00 s
#    >>> 2024-07-16 17:33:36,706 INFO: Tempo para preparacao para exportacao: 0.02 s
#    >>> 2024-07-16 17:33:36,711 INFO: Tempo para exportacao dos dados: 0.01 s
#    >>> 2024-07-16 17:33:36,712 INFO: Tempo para sintese de VEVAP_SBM: 0.03 s
#    >>> 2024-07-16 17:33:36,712 INFO: Realizando sintese de VEVAP_SIN
#    >>> 2024-07-16 17:33:36,714 INFO: Tempo para compactacao dos dados: 0.00 s
#    >>> 2024-07-16 17:33:36,714 INFO: Tempo para calculo dos limites: 0.00 s
#    >>> 2024-07-16 17:33:36,738 INFO: Tempo para preparacao para exportacao: 0.02 s
#    >>> 2024-07-16 17:33:36,743 INFO: Tempo para exportacao dos dados: 0.00 s
#    >>> 2024-07-16 17:33:36,743 INFO: Tempo para sintese de VEVAP_SIN: 0.03 s
#    >>> 2024-07-16 17:33:39,162 INFO: Tempo para sintese da operacao: 36.47 s
#    >>> 2024-07-16 17:33:39,187 INFO: # Fim da síntese #

# %%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


# %%
# Para a síntese da operação é produzido um arquivo com as informações das sínteses
# que foram realizadas:
metadados = pd.read_parquet("sintese/METADADOS_OPERACAO.parquet")
print(metadados.head(10))


# %%
# Os arquivos com os nomes das sínteses de operação armazenam os dados
# de todos os cenários simulados.
cmo = pd.read_parquet("sintese/CMO_SBM.parquet")
earmf = pd.read_parquet("sintese/EARMF_SIN.parquet")
varmf = pd.read_parquet("sintese/VARMF_UHE.parquet")


# %%
# O formato dos dados de CMO:
print(cmo.head(10))

# %%
# Os tipos de dados da síntese de CMO:
cmo.dtypes

# %%
# O formato dos dados de EARMF:
print(earmf.head(10))

# %%
# Os tipos de dados da síntese de EARMF:
earmf.dtypes

# %%
# O formato dos dados de VARMF:
print(varmf.head(10))

# %%
# Os tipos de dados da síntese de VARMF:
varmf.dtypes

# %%
# De modo geral, os arquivos das sínteses de operação sempre possuem as colunas
# `estagio`, `data_inicio`, `data_fim`, `cenario`, `patamar` e `valor`. A depender se o arquivo é
# relativo a uma agregação espacial diferente de todo o SIN, existirão outras colunas adicionais para determinar
# de qual subconjunto da agregação o dado pertence. Por exemplo, no arquivo da síntese de
# CMO_SBM, existe uma coluna adicional de nome `submercado`.

# %%
# A coluna de cenários contém somente inteiros de 1 a N, onde N é o número de séries da
# simulação final do modelo.

cenarios = earmf["cenario"].unique().tolist()
print(cenarios)

# %%
# Através das estatísticas é possível fazer um gráfico de caixas, para ilustrar a dispersão
# da variável da operação com os cenários:
fig = px.box(earmf, x="data_inicio", y="valor")
fig


# %%
# Para variáveis da operação que possuam diferentes subconjuntos, como os submercados, podem ser utilizados
# gráficos de violino para avaliação da dispersão:
cmos_2omes = cmo.loc[cmo["estagio"] == 2]
fig = px.violin(
    cmos_2omes,
    y="valor",
    color="codigo_submercado",
    box=True,
)
fig


# %%
# Para dados por UHE, como o número de subconjuntos é muito grande, é possível
# fazer um subconjunto dos elementos de interesse para a visualização:
varmf_1oano = varmf.loc[
    (varmf["estagio"] <= 12) & varmf["codigo_usina"].isin([6, 169, 251, 275])
]
fig = px.box(
    varmf_1oano,
    x="data_inicio",
    y="valor",
    facet_col_wrap=2,
    facet_col="codigo_usina",
)
fig


# %%
# Além dos arquivos com as sínteses dos cenários, estão disponíveis também os arquivos
# que agregam estatísticas das previsões:

estatisticas = pd.read_parquet("sintese/ESTATISTICAS_OPERACAO_UHE.parquet")
print(estatisticas.head(10))

# %%
# As informações dos arquivos de estatísticas são:
print(estatisticas.columns)

# %%
# Um arquivo único é gerado para as estatísticas de todas as variáveis, agregadas
# por cada elemento do sistema.:
print(estatisticas["variavel"].unique())


# %%
# As estatísticas disponíveis são os valores mínimos, máximos, médios e quantis a cada
# 5 percentis para cada variável de cada elemento de sistema. No caso das UHEs:
print(estatisticas["cenario"].unique())
