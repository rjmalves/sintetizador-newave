from typing import Any, Dict

import pandas as pd
import polars as pl
from inewave.newave import (
    Curva,
    Dger,
    Modif,
    Newavetim,
    Pmo,
)

from app.internal.constants import (
    EER_CODE_COL,
    HYDRO_CODE_COL,
    SCENARIO_COL,
)
from app.services.deck import readers
from app.services.unitofwork import AbstractUnitOfWork


def dger(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> Dger:
    val = cache.get("dger")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_dger(deck_cls, uow),
            Dger,
            "processamento do dger.dat",
        )
        cache["dger"] = val
    return val


def pmo(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> Pmo:
    val = cache.get("pmo")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_pmo(deck_cls, uow),
            Pmo,
            "processamento do pmo.dat",
        )
        cache["pmo"] = val
    return val


def curva(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> Curva:
    val = cache.get("curva")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_curva(deck_cls, uow),
            Curva,
            "processamento do curva.dat",
        )
        cache["curva"] = val
    return val


def modif(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> Modif:
    val = cache.get("modif")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_modif(deck_cls, uow),
            Modif,
            "processamento do modif.dat",
        )
        cache["modif"] = val
    return val


def confhd(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    val = cache.get("confhd")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_confhd(deck_cls, uow).usinas,
            pd.DataFrame,
            "processamento do confhd.dat",
        )
        val = pl.from_pandas(val)
        cache["confhd"] = val
    return val


def clast(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    val = cache.get("clast")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_clast(deck_cls, uow).usinas,
            pd.DataFrame,
            "processamento do clast.dat",
        )
        val = pl.from_pandas(val)
        cache["clast"] = val
    return val


def term(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    val = cache.get("term")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_term(deck_cls, uow).usinas,
            pd.DataFrame,
            "processamento do term.dat",
        )
        val = pl.from_pandas(val)
        cache["term"] = val
    return val


def manutt(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    val = cache.get("manutt")
    if val is None:
        df_manutt = readers.get_manutt(deck_cls, uow).manutencoes
        if df_manutt is None:
            val = pl.DataFrame(
                schema={
                    "codigo_empresa": pl.Int64,
                    "nome_empresa": pl.Utf8,
                    "codigo_usina": pl.Int64,
                    "nome_usina": pl.Utf8,
                    "codigo_unidade": pl.Int64,
                    "data_inicio": pl.Datetime,
                    "duracao": pl.Int64,
                    "potencia": pl.Float64,
                }
            )
        else:
            val = readers.validate_data(
                deck_cls,
                df_manutt,
                pd.DataFrame,
                "processamento do manutt.dat",
            )
            val = pl.from_pandas(val)
        cache["manutt"] = val
    return val


def expt(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    val = cache.get("expt")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_expt(deck_cls, uow).expansoes,
            pd.DataFrame,
            "processamento do expt.dat",
        )
        val = pl.from_pandas(val)
        cache["expt"] = val
    return val


def hidr(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    val = cache.get("hidr")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_hidr(deck_cls, uow).cadastro,
            pd.DataFrame,
            "processamento do hidr.dat",
        )
        val = pl.from_pandas(val, include_index=True)
        cache["hidr"] = val
    return val


def newavetim(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> Newavetim:
    val = cache.get("newavetim")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_newavetim(deck_cls, uow),
            Newavetim,
            "processamento do newave.tim",
        )
        cache["newavetim"] = val
    return val


def engnat(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    val = cache.get("engnat")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_engnat(deck_cls, uow).series,
            pd.DataFrame,
            "processamento do engnat.dat",
        )
        val = pl.from_pandas(val)
        cache["engnat"] = val
    return val


def energiaf(
    deck_cls: Any,
    uow: AbstractUnitOfWork,
    iteracao: int,
) -> pl.DataFrame:
    arq = readers.get_energiaf(deck_cls, iteracao, uow)
    if arq is None or arq.series is None:
        return pl.DataFrame()
    return pl.from_pandas(arq.series).rename(
        {"ree": EER_CODE_COL, "serie": SCENARIO_COL}
    )


def enavazf(
    deck_cls: Any,
    uow: AbstractUnitOfWork,
    iteracao: int,
) -> pl.DataFrame:
    arq = readers.get_enavazf(deck_cls, iteracao, uow)
    if arq is None or arq.series is None:
        return pl.DataFrame()
    return pl.from_pandas(arq.series).rename(
        {"ree": EER_CODE_COL, "serie": SCENARIO_COL}
    )


def vazaof(
    deck_cls: Any,
    uow: AbstractUnitOfWork,
    iteracao: int,
) -> pl.DataFrame:
    arq = readers.get_vazaof(deck_cls, iteracao, uow)
    if arq is None or arq.series is None:
        return pl.DataFrame()
    return pl.from_pandas(arq.series).rename(
        {"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL}
    )


def energiab(
    deck_cls: Any,
    uow: AbstractUnitOfWork,
    iteracao: int,
) -> pl.DataFrame:
    arq = readers.get_energiab(deck_cls, iteracao, uow)
    if arq is None or arq.series is None:
        return pl.DataFrame()
    return pl.from_pandas(arq.series).rename(
        {"ree": EER_CODE_COL, "serie": SCENARIO_COL}
    )


def enavazb(
    deck_cls: Any,
    uow: AbstractUnitOfWork,
    iteracao: int,
) -> pl.DataFrame:
    arq = readers.get_enavazb(deck_cls, iteracao, uow)
    if arq is None or arq.series is None:
        return pl.DataFrame()
    return pl.from_pandas(arq.series).rename(
        {"ree": EER_CODE_COL, "serie": SCENARIO_COL}
    )


def vazaob(
    deck_cls: Any,
    uow: AbstractUnitOfWork,
    iteracao: int,
) -> pl.DataFrame:
    arq = readers.get_vazaob(deck_cls, iteracao, uow)
    if arq is not None:
        df = arq.series
        if df is None:
            return pl.DataFrame()
        else:
            return pl.from_pandas(df).rename(
                {"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL}
            )
    else:
        return pl.DataFrame()


def energias(
    deck_cls: Any,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    arq = readers.get_energias(deck_cls, uow)
    if arq is None or arq.series is None:
        return pl.DataFrame()
    return pl.from_pandas(arq.series).rename(
        {"ree": EER_CODE_COL, "serie": SCENARIO_COL}
    )


def enavazs(
    deck_cls: Any,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    arq = readers.get_enavazs(deck_cls, uow)
    if arq is None or arq.series is None:
        return pl.DataFrame()
    return pl.from_pandas(arq.series).rename(
        {"ree": EER_CODE_COL, "serie": SCENARIO_COL}
    )


def vazaos(
    deck_cls: Any,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    arq = readers.get_vazaos(deck_cls, uow)
    if arq is None or arq.series is None:
        return pl.DataFrame()
    return pl.from_pandas(arq.series).rename(
        {"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL}
    )


def vazoes(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    val = cache.get("vazoes")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_vazoes(deck_cls, uow).vazoes,
            pd.DataFrame,
            "processamento do vazoes.dat",
        )
        val = pl.from_pandas(val)
        cache["vazoes"] = val
    return val


def study_title(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> str:
    cached: str | None = cache.get("study_title")
    if cached is not None:
        return cached
    dger_obj = dger(deck_cls, cache, uow)
    val = readers.validate_data(
        deck_cls,
        dger_obj.nome_caso,
        str,
        "nome do caso (dger.dat)",
    )
    cache["study_title"] = val
    return val  # type: ignore[no-any-return]


def version(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> str:
    cached: str | None = cache.get("version")
    if cached is not None:
        return cached
    pmo_obj = pmo(deck_cls, cache, uow)
    val = readers.validate_data(
        deck_cls,
        pmo_obj.versao_modelo,
        str,
        "versao do modelo (pmo.dat)",
    )
    cache["version"] = val
    return val  # type: ignore[no-any-return]
