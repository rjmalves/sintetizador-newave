"""
========================================
Síntese da Execução
========================================
"""

# %%
# Para realizar a síntese da execução de um caso do NEWAVE é necessário estar em um diretório
# no qual estão os principais arquivos de saída do modelo. Por exemplo, para se realizar a
# síntese de tempo de execução, é necessario o `newave.tim`. Para a síntese da convergência,
# o `pmo.dat`. Neste contexto, basta fazer::
#
#    $ sintetizador-newave execucao
#

# %%
# O sintetizador irá exibir o log da sua execução::
#
#    >>> 2024-04-22 10:33:42,304 INFO: # Realizando síntese da EXECUÇÃO #
#    >>> 2024-04-22 10:33:42,306 INFO: Realizando síntese de PROGRAMA
#    >>> 2024-04-22 10:33:42,315 INFO: Tempo para sintese de PROGRAMA: 0.01 s
#    >>> 2024-04-22 10:33:42,315 INFO: Realizando síntese de CONVERGENCIA
#    >>> 2024-04-22 10:33:42,941 INFO: Tempo para sintese de CONVERGENCIA: 0.63 s
#    >>> 2024-04-22 10:33:42,942 INFO: Realizando síntese de TEMPO
#    >>> 2024-04-22 10:33:42,946 INFO: Tempo para sintese de TEMPO: 0.00 s
#    >>> 2024-04-22 10:33:42,946 INFO: Realizando síntese de CUSTOS
#    >>> 2024-04-22 10:33:42,948 INFO: Tempo para sintese de CUSTOS: 0.00 s
#    >>> 2024-04-22 10:33:42,951 INFO: Tempo para sintese da execucao: 0.65 s
#    >>> 2024-04-22 10:33:42,951 INFO: # Fim da síntese #

# %%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
import pandas as pd

convergencia = pd.read_parquet("sintese/CONVERGENCIA.parquet")
custos = pd.read_parquet("sintese/CUSTOS.parquet")
tempo = pd.read_parquet("sintese/TEMPO.parquet")

# %%
# O formato dos dados de CONVERGÊNCIA:
print(convergencia.head(10))

# %%
# O formato dos dados de CUSTOS:
print(custos.head(10))

# %%
# O formato dos dados de TEMPO:
print(tempo.head(5))

# %%
# Cada arquivo pode ser visualizado de diferentes maneiras, a depender da aplicação.
# Por exemplo, é comum avaliar a convergência do modelo através da variação do Zinf.

fig = px.line(
    convergencia,
    x="iteracao",
    y="delta_zinf",
)
fig

# %%
# Quando se analisam os custos de cada fonte, geralmente são feitos gráficos de barras
# empilhadas ou setores:

fig = px.pie(
    custos.loc[custos["valor_esperado"] > 0],
    values="valor_esperado",
    names="parcela",
)
fig

# %%
# Uma abordagem semelhante é utilizada na análise do tempo de execução:
from datetime import timedelta

tempo["tempo"] = pd.to_timedelta(tempo["tempo"], unit="s") / timedelta(hours=1)
tempo["label"] = [str(timedelta(hours=d)) for d in tempo["tempo"].tolist()]
fig = px.bar(
    tempo,
    x="etapa",
    y="tempo",
    text="label",
    barmode="group",
)
fig
