import asyncio
from typing import List, Tuple, Optional


RETRY_DEFAULT = 3
TIMEOUT_DEFAULT = 10


async def run_terminal_retry(
    cmds: List[str],
    num_retry: int = RETRY_DEFAULT,
    timeout: float = TIMEOUT_DEFAULT,
) -> Tuple[int, List[str]]:
    """
    Runs a command on the terminal (with retries) and returns.

    :param cmds: Commands and args to be executed
    :param num_retry: Max number of retries
    :param timeout: Timeout for giving up on the command
    :return: Return code and outputs
    :rtype: Tuple[int, List[str]]
    """
    for _ in range(num_retry):
        cod, outputs = await run_terminal(cmds, timeout)
        if cod == 0:
            return cod, outputs
    return -1, []


async def run_terminal(
    cmds: List[str], timeout: float = TIMEOUT_DEFAULT
) -> Tuple[Optional[int], List[str]]:
    """
    Runs a command on the terminal and returns.

    :param cmds: Commands and args to be executed
    :param timeout: Timeout for giving up on the command
    :return: Return code and outputs
    :rtype: Tuple[int, List[str]]
    """
    cmd = " ".join(cmds)
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await asyncio.wait_for(
        proc.communicate(), timeout=timeout
    )
    if stdout:
        return proc.returncode, stdout.decode("utf-8")
    if stderr:
        return proc.returncode, stderr.decode("utf-8")
