from typing import List, Union, Any

from dbt.common.exceptions import CompilationError, CommandError, scrub_secrets, env_secrets


class SymbolicLinkError(CompilationError):
    def __init__(self) -> None:
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = (
            "dbt encountered an error when attempting to create a symbolic link. "
            "If this error persists, please create an issue at: \n\n"
            "https://github.com/dbt-labs/dbt-core"
        )

        return msg


class ExecutableError(CommandError):
    def __init__(self, cwd: str, cmd: List[str], msg: str) -> None:
        super().__init__(cwd, cmd, msg)


class WorkingDirectoryError(CommandError):
    def __init__(self, cwd: str, cmd: List[str], msg: str) -> None:
        super().__init__(cwd, cmd, msg)

    def __str__(self):
        return f'{self.msg}: "{self.cwd}"'


class CommandResultError(CommandError):
    def __init__(
        self,
        cwd: str,
        cmd: List[str],
        returncode: Union[int, Any],
        stdout: bytes,
        stderr: bytes,
        msg: str = "Got a non-zero returncode",
    ) -> None:
        super().__init__(cwd, cmd, msg)
        self.returncode = returncode
        self.stdout = scrub_secrets(stdout.decode("utf-8"), env_secrets())
        self.stderr = scrub_secrets(stderr.decode("utf-8"), env_secrets())
        self.args = (cwd, self.cmd, returncode, self.stdout, self.stderr, msg)

    def __str__(self):
        return f"{self.msg} running: {self.cmd}"
