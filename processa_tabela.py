from inewave.nwlistop.earmfpsin import EarmfpSIN
from inewave.config import MESES_DF
from datetime import datetime
import pandas as pd

earm = EarmfpSIN.le_arquivo(".")
df = earm.valores
anos = df["Ano"].unique().tolist()
labels = pd.date_range(
    datetime(year=anos[0], month=1, day=1),
    datetime(year=anos[-1], month=12, day=1),
    freq="MS",
)
df_series = pd.DataFrame()
for a in anos:
    df_ano = df.loc[df["Ano"] == a, MESES_DF].T
    df_ano.columns = list(range(1, df_ano.shape[1] + 1))
    df_series = pd.concat([df_series, df_ano], ignore_index=True)
df_series.index = labels

print(df_series)
