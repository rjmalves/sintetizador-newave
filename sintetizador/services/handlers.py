import pathlib
import shutil
from sintetizador.model.settings import Settings
import sintetizador.domain.commands as commands
from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.services.synthesis.system import SystemSynthetizer
from sintetizador.services.synthesis.execution import ExecutionSynthetizer
from sintetizador.services.synthesis.operation import OperationSynthetizer


def synthetize_system(
    command: commands.SynthetizeSystem, uow: AbstractUnitOfWork
):
    SystemSynthetizer.synthetize(command.variables, uow)


def synthetize_execution(
    command: commands.SynthetizeExecution, uow: AbstractUnitOfWork
):
    ExecutionSynthetizer.synthetize(command.variables, uow)


def synthetize_operation(
    command: commands.SynthetizeOperation, uow: AbstractUnitOfWork
):
    OperationSynthetizer.synthetize(command.variables, uow)


def clean():
    path = pathlib.Path(Settings().basedir).joinpath(Settings().synthesis_dir)
    shutil.rmtree(path)
