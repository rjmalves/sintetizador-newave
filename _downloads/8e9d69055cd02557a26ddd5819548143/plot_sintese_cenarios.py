"""
========================================
Síntese dos Cenários
========================================
"""

# %%
# Para realizar a síntese dos cenários de um caso do NEWAVE é necessário estar em um diretório
# no qual estão os principais arquivos de saída do modelo. Em geral, as variáveis dos cenários gerados
# são extraídos dos arquivos binários energiafXXX.dat, vazaofXXX.dat, etc.
# Além dos arquivos dos quais são extraídas as variáveis em si, são lidos também alguns arquivos de entrada
# do modelo, como o `dger.dat`, `ree.dat` e `sistema.dat`. Neste contexto, basta fazer::
#
#    $ sintetizador-newave cenarios --processadores 4
#

# %%
# O sintetizador irá exibir o log da sua execução::
#
#    >>> 2024-04-22 09:53:56,845 INFO: # Realizando síntese dos CENÁRIOS #
#    >>> 2024-07-16 17:34:18,473 INFO: Realizando síntese de ENAA_REE_FOR
#    >>> 2024-07-16 17:34:19,201 INFO: Obtendo energias forward da it. 1
#    >>> 2024-07-16 17:34:19,202 INFO: Obtendo energias forward da it. 4
#    >>> 2024-07-16 17:34:19,201 INFO: Obtendo energias forward da it. 3
#    >>> 2024-07-16 17:34:19,202 INFO: Obtendo energias forward da it. 2
#    >>> 2024-07-16 17:34:19,229 INFO: Obtendo energias forward da it. 5
#    >>> 2024-07-16 17:34:19,229 INFO: Obtendo energias forward da it. 6
#    >>> 2024-07-16 17:34:19,229 INFO: Obtendo energias forward da it. 7
#    >>> 2024-07-16 17:34:19,279 INFO: Obtendo energias forward da it. 9
#    >>> 2024-07-16 17:34:19,279 INFO: Obtendo energias forward da it. 10
#    >>> 2024-07-16 17:34:19,279 INFO: Obtendo energias forward da it. 8
#    >>> 2024-07-16 17:34:21,179 INFO: Tempo para obter energias forward: 1.99 s
#    >>> 2024-07-16 17:34:21,182 INFO: Tempo para compactacao dos dados: 0.00 s
#    >>> 2024-07-16 17:34:27,813 INFO: Tempo para calculo da MLT por REE: 6.62 s
#    >>> 2024-07-16 17:34:27,834 INFO: Tempo para exportacao dos dados: 0.01 s
#    >>> 2024-07-16 17:34:27,834 INFO: Tempo para sintese de ENAA_REE_FOR: 9.36 s
#    >>> .
#    >>> .
#    >>> .
#    >>> 2024-07-16 17:34:35,766 INFO: Realizando síntese de QINC_SIN_SF
#    >>> 2024-07-16 17:34:35,786 INFO: Tempo para exportacao dos dados: 0.00 s
#    >>> 2024-07-16 17:34:35,786 INFO: Tempo para sintese de QINC_SIN_SF: 0.02 s
#    >>> 2024-07-16 17:34:35,952 INFO: Tempo para síntese dos cenários: 17.50 s
#    >>> 2024-07-16 17:33:39,187 INFO: # Fim da síntese #

# %%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


# %%
# Para a síntese dos cenários é produzido um arquivo com as informações das sínteses
# que foram realizadas:
metadados = pd.read_parquet("sintese/METADADOS_CENARIOS.parquet")
print(metadados.head(10))


# %%
# Os arquivos com os nomes das sínteses de cenários armazenam os dados
# de todos os cenários gerados.
cenarios_uhe_for = pd.read_parquet("sintese/QINC_UHE_FOR.parquet")
cenarios_ree_sf = pd.read_parquet("sintese/ENAA_REE_SF.parquet")
cenarios_sin_bkw = pd.read_parquet("sintese/ENAA_SIN_BKW.parquet")


# %%
# O formato dos dados por UHE:
print(cenarios_uhe_for.head(10))

# %%
# Os tipos de dados da síntese de cenários por UHE:
cenarios_uhe_for.dtypes

# %%
# O formato dos dados por REE:
print(cenarios_ree_sf.head(10))

# %%
# Os tipos de dados da síntese de cenários por REE:
cenarios_ree_sf.dtypes

# %%
# O formato dos dados para o SIN:
print(cenarios_sin_bkw.head(10))

# %%
# Os tipos de dados da síntese de cenários para o SIN:
cenarios_sin_bkw.dtypes

# %%
# De modo geral, os arquivos das sínteses de cenários sempre possuem as colunas
# `estagio`, `data_inicio`, `data_fim`, `cenario`, `valor`, `mlt` e `valor_mlt`. A depender se o arquivo é
# referente à etapa forward ou backward,existirá uma coluna adicional `iteracao`. Apenas
# na etapa backward existirá a coluna `abertura`.

# %%
# Da mesma maneira que para as demais sínteses, é possível produzir visualizações
# e estatísticas a partir dos arquivos gerados.
fig = px.box(
    cenarios_sin_bkw,
    x="estagio",
    y="valor_mlt",
    color="iteracao",
)
fig

# %%
# Repare que para os arquivos da síntese da operação, a referência da numeração
# dos estágios é diferente da numeração dos estágios da síntese dos cenários.
# Na síntese dos cenários, como os arquivos processados são os binários do modelo,
# o estágio "1" não se refere necessariamente ao primeiro mês do período do estudo.


# %%
# Além dos arquivos com as sínteses dos cenários, estão disponíveis também os arquivos
# que agregam estatísticas das previsões:

estatisticas = pd.read_parquet("sintese/ESTATISTICAS_CENARIOS_UHE_FOR.parquet")
print(estatisticas.head(10))

# %%
# As informações dos arquivos de estatísticas são:
print(estatisticas.columns)


# %%
# As estatísticas disponíveis são os valores mínimos, máximos, médios e quantis a cada
# 5 percentis para cada variável de cada elemento de sistema. No caso das UHEs:
print(estatisticas["cenario"].unique())
