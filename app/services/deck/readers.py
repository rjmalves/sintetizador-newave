from logging import ERROR
from typing import Any, Dict, Optional, Tuple, Type, TypeVar

import pandas as pd
from inewave.newave import (
    Clast,
    Confhd,
    Conft,
    Curva,
    Dger,
    Dsvagua,
    Enavazb,
    Enavazf,
    Energiab,
    Energiaf,
    Energias,
    Engnat,
    Expt,
    Hidr,
    Manutt,
    Modif,
    Newavetim,
    Patamar,
    Pmo,
    Ree,
    Shist,
    Sistema,
    Term,
    Vazaob,
    Vazaof,
    Vazaos,
    Vazoes,
)
from inewave.nwlistcf import Estados, Nwlistcfrel  # type: ignore[attr-defined]

from app.services.unitofwork import AbstractUnitOfWork

_T = TypeVar("_T")
T_any = None  # placeholder; real TypeVar defined in callers


def _log(deck_cls: Any, msg: str, level: int = 20) -> None:
    if deck_cls.logger is not None:
        deck_cls.logger.log(level, msg)


def get_dger(deck_cls: Any, uow: AbstractUnitOfWork) -> Dger:
    with uow:
        dger = uow.files.get_dger()
        if dger is None:
            msg = "Erro no processamento do dger.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return dger


def get_shist(deck_cls: Any, uow: AbstractUnitOfWork) -> Shist:
    with uow:
        shist = uow.files.get_shist()
        if shist is None:
            msg = "Erro no processamento do shist.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return shist


def get_curva(deck_cls: Any, uow: AbstractUnitOfWork) -> Curva:
    with uow:
        curva = uow.files.get_curva()
        if curva is None:
            msg = "Erro no processamento do curva.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return curva


def get_ree(deck_cls: Any, uow: AbstractUnitOfWork) -> Ree:
    with uow:
        ree = uow.files.get_ree()
        if ree is None:
            msg = "Erro no processamento do ree.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return ree


def get_confhd(deck_cls: Any, uow: AbstractUnitOfWork) -> Confhd:
    with uow:
        confhd = uow.files.get_confhd()
        if confhd is None:
            msg = "Erro no processamento do confhd.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return confhd


def get_dsvagua(deck_cls: Any, uow: AbstractUnitOfWork) -> Dsvagua:
    with uow:
        dsvagua = uow.files.get_dsvagua()
        if dsvagua is None:
            msg = "Erro no processamento do dsvagua.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return dsvagua


def get_modif(deck_cls: Any, uow: AbstractUnitOfWork) -> Modif:
    with uow:
        modif = uow.files.get_modif()
        if modif is None:
            msg = "Erro no processamento do modif.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return modif


def get_hidr(deck_cls: Any, uow: AbstractUnitOfWork) -> Hidr:
    with uow:
        hidr = uow.files.get_hidr()
        if hidr is None:
            msg = "Erro no processamento do hidr.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return hidr


def get_vazoes(deck_cls: Any, uow: AbstractUnitOfWork) -> Vazoes:
    with uow:
        vazoes = uow.files.get_vazoes()
        if vazoes is None:
            msg = "Erro no processamento do vazoes.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return vazoes


def get_conft(deck_cls: Any, uow: AbstractUnitOfWork) -> Conft:
    with uow:
        conft = uow.files.get_conft()
        if conft is None:
            msg = "Erro no processamento do conft.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return conft


def get_sistema(deck_cls: Any, uow: AbstractUnitOfWork) -> Sistema:
    with uow:
        sist = uow.files.get_sistema()
        if sist is None:
            msg = "Erro no processamento do sistema.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return sist


def get_clast(deck_cls: Any, uow: AbstractUnitOfWork) -> Clast:
    with uow:
        clast = uow.files.get_clast()
        if clast is None:
            msg = "Erro no processamento do clast.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return clast


def get_term(deck_cls: Any, uow: AbstractUnitOfWork) -> Term:
    with uow:
        term = uow.files.get_term()
        if term is None:
            msg = "Erro no processamento do term.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return term


def get_manutt(deck_cls: Any, uow: AbstractUnitOfWork) -> Manutt:
    with uow:
        manutt = uow.files.get_manutt()
        if manutt is None:
            msg = "Erro no processamento do manutt.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return manutt


def get_expt(deck_cls: Any, uow: AbstractUnitOfWork) -> Expt:
    with uow:
        expt = uow.files.get_expt()
        if expt is None:
            msg = "Erro no processamento do expt.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return expt


def get_patamar(deck_cls: Any, uow: AbstractUnitOfWork) -> Patamar:
    with uow:
        pat = uow.files.get_patamar()
        if pat is None:
            msg = "Erro no processamento do patamar.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return pat


def get_pmo(deck_cls: Any, uow: AbstractUnitOfWork) -> Pmo:
    with uow:
        pmo = uow.files.get_pmo()
        if pmo is None:
            msg = "Erro no processamento do pmo.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return pmo


def get_newavetim(deck_cls: Any, uow: AbstractUnitOfWork) -> Newavetim:
    with uow:
        newavetim = uow.files.get_newavetim()
        if newavetim is None:
            msg = "Erro no processamento do newave.tim para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return newavetim


def get_engnat(deck_cls: Any, uow: AbstractUnitOfWork) -> Engnat:
    with uow:
        engnat = uow.files.get_engnat()
        if engnat is None:
            msg = "Erro no processamento do engnat.dat para sintese"
            _log(deck_cls, msg, ERROR)
            raise RuntimeError(msg)
        return engnat


def get_energiaf(
    deck_cls: Any, iteracao: int, uow: AbstractUnitOfWork
) -> Optional[Energiaf]:
    with uow:
        energiaf = uow.files.get_energiaf(iteracao)
        return energiaf


def get_enavazf(
    deck_cls: Any, iteracao: int, uow: AbstractUnitOfWork
) -> Optional[Enavazf]:
    with uow:
        enavazf = uow.files.get_enavazf(iteracao)
        return enavazf


def get_vazaof(
    deck_cls: Any, iteracao: int, uow: AbstractUnitOfWork
) -> Optional[Vazaof]:
    with uow:
        vazaof = uow.files.get_vazaof(iteracao)
        return vazaof


def get_energiab(
    deck_cls: Any, iteracao: int, uow: AbstractUnitOfWork
) -> Optional[Energiab]:
    with uow:
        energiab = uow.files.get_energiab(iteracao)
        return energiab


def get_enavazb(
    deck_cls: Any, iteracao: int, uow: AbstractUnitOfWork
) -> Optional[Enavazb]:
    with uow:
        enavazb = uow.files.get_enavazb(iteracao)
        return enavazb


def get_vazaob(
    deck_cls: Any, iteracao: int, uow: AbstractUnitOfWork
) -> Optional[Vazaob]:
    with uow:
        vazaob = uow.files.get_vazaob(iteracao)
        return vazaob


def get_energias(deck_cls: Any, uow: AbstractUnitOfWork) -> Optional[Energias]:
    with uow:
        energias = uow.files.get_energias()
        return energias


def get_enavazs(deck_cls: Any, uow: AbstractUnitOfWork) -> Optional[Energias]:
    with uow:
        enavazs = uow.files.get_enavazs()
        return enavazs


def get_vazaos(deck_cls: Any, uow: AbstractUnitOfWork) -> Optional[Vazaos]:
    with uow:
        vazaos = uow.files.get_vazaos()
        return vazaos


def get_cortes(deck_cls: Any, uow: AbstractUnitOfWork) -> Optional[Nwlistcfrel]:
    with uow:
        cortes = uow.files.get_nwlistcf_cortes()
        return cortes


def get_estados(deck_cls: Any, uow: AbstractUnitOfWork) -> Optional[Estados]:
    with uow:
        estados = uow.files.get_nwlistcf_estados()
        return estados


def validate_data(
    deck_cls: Any, data: _T, type: Type[Any], msg: str = "dados"
) -> _T:
    if not isinstance(data, type):
        msg = f"Erro de validação: {msg}"
        _log(deck_cls, msg, ERROR)
        raise AssertionError(msg, ERROR)
    return data


# ---------------------------------------------------------------------------
# modif.dat helpers (used by hydro module to avoid circular imports)
# ---------------------------------------------------------------------------


def get_value_and_unit_from_modif_entry(
    r: object,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Extrai um dado de um registro do modif.dat com a sua unidade.
    """
    from inewave.newave.modelos.modif import (
        CFUGA,
        CMONT,
        TURBMAXT,
        TURBMINT,
        VAZMAXT,
        VAZMIN,
        VAZMINT,
        VMAXT,
        VMINT,
        VOLMAX,
        VOLMIN,
    )

    from app.model.operation.unit import Unit

    if isinstance(r, (VOLMIN, VMINT, VOLMAX, VMAXT)):
        return r.volume, r.unidade
    elif isinstance(r, (VAZMIN, VAZMINT, VAZMAXT)):
        return r.vazao, Unit.m3s.value
    elif isinstance(r, (TURBMINT, TURBMAXT)):
        return r.turbinamento, Unit.m3s.value
    elif isinstance(r, (CMONT, CFUGA)):
        return r.nivel, ""
    return None, None


def apply_modif_changes_to_hydros(
    deck_cls: Any,
    cache: Dict[str, Any],
    df: "pd.DataFrame",
    hydro_data_col: str,
    hydro_data_unit_col: str,
    register_type: type,
    uow: "AbstractUnitOfWork",
) -> "pd.DataFrame":
    """
    Realiza a extração de modificações cadastrais de volumes de usinas
    hidrelétricas a partir do arquivo modif.dat, atualizando os cadastros
    conforme as declarações de modificações são encontradas.
    """
    from app.services.deck import accessors

    modif = accessors.modif(deck_cls, cache, uow)
    for idx in df.index:
        hydro_changes = modif.modificacoes_usina(idx)
        if hydro_changes is not None:
            regs_usina = [
                r for r in hydro_changes if isinstance(r, register_type)
            ]
            if len(regs_usina) > 0:
                r = regs_usina[-1]
                value, unit = get_value_and_unit_from_modif_entry(r)
                if value is not None:
                    df.at[idx, hydro_data_col] = value
                if unit is not None:
                    df.at[idx, hydro_data_unit_col] = unit.lower()
    return df


def apply_modif_changes_to_hydros_in_stages(
    deck_cls: Any,
    cache: Dict[str, Any],
    df: "pd.DataFrame",
    hydro_data_col: str,
    hydro_data_unit_col: str,
    register_type: type,
    uow: "AbstractUnitOfWork",
) -> "pd.DataFrame":
    """
    Realiza a extração de modificações cadastrais de volumes de usinas
    hidrelétricas a partir do arquivo modif.dat, considerando também
    modificações cadastrais com relação temporal.
    """
    from app.internal.constants import HYDRO_CODE_COL
    from app.services.deck import accessors, entities, temporal

    modif = accessors.modif(deck_cls, cache, uow)
    num_stages = temporal.num_hydro_simulation_stages_final_simulation(
        deck_cls,
        cache,
        uow,
        entities.eers(
            deck_cls, cache, uow
        ).to_pandas(),  # SHIM: remove after polars migration of temporal.py
    )
    dates = temporal.stages_starting_dates_final_simulation(
        deck_cls, cache, uow
    )[:num_stages]
    hydro_codes = df[HYDRO_CODE_COL].unique().tolist()
    for i, u in enumerate(hydro_codes):
        hydro_changes = modif.modificacoes_usina(u)
        i_i = i * num_stages
        i_f = i_i + num_stages - 1
        if hydro_changes is not None:
            hydro_registers = [
                r for r in hydro_changes if isinstance(r, register_type)
            ]
            for reg in hydro_registers:
                if reg.data_inicio not in dates:  # type: ignore
                    continue
                idx_data = dates.index(reg.data_inicio)  # type: ignore
                value, unit = get_value_and_unit_from_modif_entry(reg)
                df.loc[i_i + idx_data : i_f, hydro_data_col] = value
                df.loc[i_i + idx_data : i_f, hydro_data_unit_col] = unit
    return df
