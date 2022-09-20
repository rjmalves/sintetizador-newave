import pathlib
import shutil
from sintetizador.model.settings import Settings
from sintetizador.services.synthesis.operation import OperationSynthetizer
import sintetizador.domain.commands as commands
from sintetizador.services.unitofwork import AbstractUnitOfWork


def synthetize_operation(
    command: commands.SynthetizeOperation, uow: AbstractUnitOfWork
):
    OperationSynthetizer.synthetize(command.variables, uow)


def clean():
    path = pathlib.Path(Settings().basedir).joinpath(Settings().synthesis_dir)
    shutil.rmtree(path)
