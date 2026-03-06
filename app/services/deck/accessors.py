from typing import Any, Dict

import pandas as pd
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
    deck_cls,
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
    deck_cls,
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
    deck_cls,
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
    deck_cls,
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
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    val = cache.get("confhd")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_confhd(deck_cls, uow).usinas,
            pd.DataFrame,
            "processamento do confhd.dat",
        )
        cache["confhd"] = val
    return val.copy()


def clast(
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    val = cache.get("clast")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_clast(deck_cls, uow).usinas,
            pd.DataFrame,
            "processamento do clast.dat",
        )
        cache["clast"] = val
    return val.copy()


def term(
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    val = cache.get("term")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_term(deck_cls, uow).usinas,
            pd.DataFrame,
            "processamento do term.dat",
        )
        cache["term"] = val
    return val.copy()


def manutt(
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    val = cache.get("manutt")
    if val is None:
        df_manutt = readers.get_manutt(deck_cls, uow).manutencoes
        if df_manutt is None:
            df_manutt = pd.DataFrame(
                columns=[
                    "codigo_empresa",
                    "nome_empresa",
                    "codigo_usina",
                    "nome_usina",
                    "codigo_unidade",
                    "data_inicio",
                    "duracao",
                    "potencia",
                ]
            )
        val = readers.validate_data(
            deck_cls,
            df_manutt,
            pd.DataFrame,
            "processamento do manutt.dat",
        )
        cache["manutt"] = val
    return val.copy()


def expt(
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    val = cache.get("expt")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_expt(deck_cls, uow).expansoes,
            pd.DataFrame,
            "processamento do expt.dat",
        )
        cache["expt"] = val
    return val.copy()


def hidr(
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    val = cache.get("hidr")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_hidr(deck_cls, uow).cadastro,
            pd.DataFrame,
            "processamento do hidr.dat",
        )
        cache["hidr"] = val
    return val.copy()


def newavetim(
    deck_cls,
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
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    val = cache.get("engnat")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_engnat(deck_cls, uow).series,
            pd.DataFrame,
            "processamento do engnat.dat",
        )
        cache["engnat"] = val
    return val


def energiaf(
    deck_cls,
    uow: AbstractUnitOfWork,
    iteracao: int,
) -> pd.DataFrame:
    arq = readers.get_energiaf(deck_cls, iteracao, uow)
    if arq is not None:
        df = arq.series
        if df is None:
            return pd.DataFrame()
        else:
            return df.rename(
                columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
            )
    else:
        return pd.DataFrame()


def enavazf(
    deck_cls,
    uow: AbstractUnitOfWork,
    iteracao: int,
) -> pd.DataFrame:
    arq = readers.get_enavazf(deck_cls, iteracao, uow)
    if arq is not None:
        df = arq.series
        if df is None:
            return pd.DataFrame()
        else:
            return df.rename(
                columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
            )
    else:
        return pd.DataFrame()


def vazaof(
    deck_cls,
    uow: AbstractUnitOfWork,
    iteracao: int,
) -> pd.DataFrame:
    arq = readers.get_vazaof(deck_cls, iteracao, uow)
    if arq is not None:
        df = arq.series
        if df is None:
            return pd.DataFrame()
        else:
            return df.rename(
                columns={"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL}
            )
    else:
        return pd.DataFrame()


def energiab(
    deck_cls,
    uow: AbstractUnitOfWork,
    iteracao: int,
) -> pd.DataFrame:
    arq = readers.get_energiab(deck_cls, iteracao, uow)
    if arq is not None:
        df = arq.series
        if df is None:
            return pd.DataFrame()
        else:
            return df.rename(
                columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
            )
    else:
        return pd.DataFrame()


def enavazb(
    deck_cls,
    uow: AbstractUnitOfWork,
    iteracao: int,
) -> pd.DataFrame:
    arq = readers.get_enavazb(deck_cls, iteracao, uow)
    if arq is not None:
        df = arq.series
        if df is None:
            return pd.DataFrame()
        else:
            return df.rename(
                columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
            )
    else:
        return pd.DataFrame()


def vazaob(
    deck_cls,
    uow: AbstractUnitOfWork,
    iteracao: int,
) -> pd.DataFrame:
    arq = readers.get_vazaob(deck_cls, iteracao, uow)
    if arq is not None:
        df = arq.series
        if df is None:
            return pd.DataFrame()
        else:
            return df.rename(
                columns={"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL}
            )
    else:
        return pd.DataFrame()


def energias(
    deck_cls,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    arq = readers.get_energias(deck_cls, uow)
    if arq is not None:
        df = arq.series
        if df is None:
            return pd.DataFrame()
        else:
            return df.rename(
                columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
            )
    else:
        return pd.DataFrame()


def enavazs(
    deck_cls,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    arq = readers.get_enavazs(deck_cls, uow)
    if arq is not None:
        df = arq.series
        if df is None:
            return pd.DataFrame()
        else:
            return df.rename(
                columns={"ree": EER_CODE_COL, "serie": SCENARIO_COL}
            )
    else:
        return pd.DataFrame()


def vazaos(
    deck_cls,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    arq = readers.get_vazaos(deck_cls, uow)
    if arq is not None:
        df = arq.series
        if df is None:
            return pd.DataFrame()
        else:
            return df.rename(
                columns={"uhe": HYDRO_CODE_COL, "serie": SCENARIO_COL}
            )
    else:
        return pd.DataFrame()


def vazoes(
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    val = cache.get("vazoes")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_vazoes(deck_cls, uow).vazoes,
            pd.DataFrame,
            "processamento do vazoes.dat",
        )
        cache["vazoes"] = val
    return val.copy()


def study_title(
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> str:
    val = cache.get("study_title")
    if val is None:
        dger_obj = dger(deck_cls, cache, uow)
        val = readers.validate_data(
            deck_cls,
            dger_obj.nome_caso,
            str,
            "nome do caso (dger.dat)",
        )
        cache["study_title"] = val
    return val


def version(
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> str:
    val = cache.get("version")
    if val is None:
        pmo_obj = pmo(deck_cls, cache, uow)
        val = readers.validate_data(
            deck_cls,
            pmo_obj.versao_modelo,
            str,
            "versao do modelo (pmo.dat)",
        )
        cache["version"] = val
    return val
