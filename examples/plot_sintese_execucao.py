"""
========================================
Síntese da Execução
========================================
"""

#%%
# Para realizar a síntese da execução de um caso do NEWAVE é necessário estar em um diretório
# no qual estão os principais arquivos de saída do modelo. Por exemplo, para se realizar a
# síntese de tempo de execução, é necessario o `newave.tim`. Para a síntese da convergência,
# o `pmo.dat`. Neste contexto, basta fazer::
#
#    $ sintetizador-newave execucao CONVERGENCIA CUSTOS TEMPO
#

#%%
# O sintetizador irá exibir o log da sua execução::
#
#    >>> 2023-02-10 01:27:19,894 INFO: # Realizando síntese da EXECUÇÃO #
#    >>> 2023-02-10 01:27:19,894 INFO: Realizando síntese de CONVERGENCIA
#    >>> 2023-02-10 01:27:19,897 INFO: Lendo arquivo pmo.dat
#    >>> 2023-02-10 01:27:26,532 INFO: Realizando síntese de CUSTOS
#    >>> 2023-02-10 01:27:26,585 INFO: Realizando síntese de TEMPO
#    >>> 2023-02-10 01:27:26,585 INFO: Lendo arquivo newave.tim
#    >>> 2023-02-10 01:27:26,592 INFO: # Fim da síntese #


#%%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
import pandas as pd

convergencia = pd.read_parquet("sintese/CONVERGENCIA.parquet.gzip")
custos = pd.read_parquet("sintese/CUSTOS.parquet.gzip")
tempo = pd.read_parquet("sintese/TEMPO.parquet.gzip")
print(convergencia)
print(custos)
print(tempo)

#%%
# Cada arquivo pode ser visualizado de diferentes maneiras, a depender da aplicação.
# Por exemplo, é comum avaliar a convergência do modelo através da variação do Zinf.

fig = px.line(
    convergencia,
    x="iter",
    y="dZinf",
)
fig

#%%
# Quando se analisam os custos de cada fonte, geralmente são feitos gráficos de barras
# empilhadas ou setores, bem como quando é avaliado o tempo computacional por etapa da execução:

fig = px.pie(custos.loc[custos["mean"] > 0], values="mean", names="parcela")
fig

fig = px.bar(tempo, y="tempo", color="etapa")
fig
