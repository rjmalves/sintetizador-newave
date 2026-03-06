from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, Tuple

from app.adapters.repository.mappings import (
    cost,
    energy,
    exchange,
    flow,
    generation,
    hydraulic,
    wind,
)
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable

if TYPE_CHECKING:
    from app.adapters.repository.files import RawFilesRepository


def build_regras(
    repo: "RawFilesRepository",
) -> Dict[Tuple[Variable, SpatialResolution], Callable]:
    """Merge all category rule dicts into a single mapping."""
    rules: Dict[Tuple[Variable, SpatialResolution], Callable] = {}
    rules.update(cost.get_rules(repo))
    rules.update(energy.get_rules(repo))
    rules.update(exchange.get_rules(repo))
    rules.update(flow.get_rules(repo))
    rules.update(generation.get_rules(repo))
    rules.update(hydraulic.get_rules(repo))
    rules.update(wind.get_rules(repo))
    return rules
