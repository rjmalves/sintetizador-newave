import pandas as pd
import numpy as np
from time import time

STAGE_DURATION_HOURS = 730
N_PAT = 3
N_CENARIOS = 2000

from functools import wraps
import time


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Function {func.__name__} Took {total_time:.4f} seconds")
        return result

    return timeit_wrapper


@timeit
def stub_calc_pat_0_weighted_mean(df: pd.DataFrame) -> pd.DataFrame:

    @timeit
    def calcula_valor(df: pd.DataFrame) -> pd.DataFrame:
        df_pat0 = df.copy()
        df_pat0["patamar"] = 0
        df_pat0["valor"] = (
            df_pat0["valor"] * df_pat0["duracaoPatamar"]
        ) / STAGE_DURATION_HOURS
        return df_pat0

    df_pat0 = calcula_valor(df)
    cols_group = [
        c
        for c in df.columns
        if c
        not in [
            "patamar",
            "duracaoPatamar",
            "valor",
        ]
    ]

    @timeit
    def agrupa(df_pat0: pd.DataFrame) -> pd.DataFrame:

        df_pat0 = (
            df_pat0.groupby(cols_group, sort=False)[
                ["duracaoPatamar", "valor"]
            ]
            .sum(engine="numba")
            .reset_index()
        )
        return df_pat0

    @timeit
    def concatena(df_pat0: pd.DataFrame) -> pd.DataFrame:
        df_pat0 = agrupa(df_pat0)
        df_pat0["patamar"] = 0
        df_pat0 = pd.concat([df, df_pat0], ignore_index=True)
        return df_pat0

    @timeit
    def ordena(df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(cols_group + ["patamar"])
        return df

    df = concatena(df_pat0)
    df = ordena(df)
    return df


@timeit
def stub_calc_pat_0_weighted_mean_2(df: pd.DataFrame) -> pd.DataFrame:

    cols_dup = [
        c
        for c in df.columns
        if c
        not in [
            "dataInicio",
            "dataFim",
            "patamar",
            "duracaoPatamar",
            "valor",
        ]
    ]
    df_pat0 = df.copy()
    df_pat0["valor"] = (
        df_pat0["valor"] * df_pat0["duracaoPatamar"]
    ) / STAGE_DURATION_HOURS
    df_base = df.iloc[::N_PAT].reset_index().copy()
    # df_base = df.drop_duplicates(subset=cols_dup, ignore_index=True).copy()
    df_base["patamar"] = 0
    df_base["duracaoPatamar"] = STAGE_DURATION_HOURS
    arr = df_pat0["valor"].to_numpy()
    n_linhas = arr.shape[0]
    n_elementos_distintos = n_linhas // N_PAT
    df_base["valor"] = arr.reshape((n_elementos_distintos, -1)).sum(axis=1)
    df = pd.concat([df, df_base], ignore_index=True, copy=True)
    df = df.sort_values(cols_dup + ["patamar"])
    return df


df = pd.read_parquet("./sintese/HLIQ_UHE.parquet.gzip")

df = df.loc[
    df["cenario"].isin([str(c) for c in list(range(1, 2001))])
    & (df["patamar"] > 0)
    & (df["submercado"].isin(["SUL", "NORTE"]))
].copy()
df = df.astype({"cenario": int})

_ = stub_calc_pat_0_weighted_mean_2(df)


@timeit
def remove_dups(df: pd.DataFrame) -> pd.DataFrame:
    cols_dup = [
        c
        for c in df.columns
        if c
        not in [
            "dataInicio",
            "dataFim",
            "patamar",
            "duracaoPatamar",
            "valor",
        ]
    ]
    df_base = df.drop_duplicates(subset=cols_dup, ignore_index=True).copy()
    return df_base


df_base = remove_dups(df)

df.iloc[::3]

cols_dup = [
    "usina",
    "ree",
    "submercado",
    "estagio",
    "dataInicio",
    "dataFim",
    "patamar",
    "duracaoPatamar",
    "cenario",
]

df.iloc[::2000].copy()
