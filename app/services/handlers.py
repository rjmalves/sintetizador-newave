import pathlib
import shutil

import app.domain.commands as commands
from app.model.settings import Settings
from app.services.synthesis.execution import ExecutionSynthetizer
from app.services.synthesis.operation import OperationSynthetizer
from app.services.synthesis.policy import PolicySynthetizer
from app.services.synthesis.scenario import ScenarioSynthetizer
from app.services.synthesis.system import SystemSynthetizer
from app.services.unitofwork import AbstractUnitOfWork


def synthetize_system(
    command: commands.SynthetizeSystem, uow: AbstractUnitOfWork
) -> None:
    SystemSynthetizer.synthetize(command.variables, uow)


def synthetize_execution(
    command: commands.SynthetizeExecution, uow: AbstractUnitOfWork
) -> None:
    ExecutionSynthetizer.synthetize(command.variables, uow)


def synthetize_scenarios(
    command: commands.SynthetizeScenarios, uow: AbstractUnitOfWork
) -> None:
    ScenarioSynthetizer.synthetize(command.variables, uow)


def synthetize_operation(
    command: commands.SynthetizeOperation, uow: AbstractUnitOfWork
) -> None:
    OperationSynthetizer.synthetize(command.variables, uow)


def synthetize_policy(
    command: commands.SynthetizePolicy, uow: AbstractUnitOfWork
) -> None:
    PolicySynthetizer.synthetize(command.variables, uow)


def clean() -> None:
    settings = Settings()
    basedir = settings.basedir
    if basedir is None:
        raise RuntimeError("APP_BASEDIR not set")
    path = pathlib.Path(basedir).joinpath(settings.synthesis_dir)
    shutil.rmtree(path)
