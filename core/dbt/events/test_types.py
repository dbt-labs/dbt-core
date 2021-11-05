from dataclasses import dataclass
from .types import (
    InfoLevel,
    DebugLevel,
    WarnLevel,
    ErrorLevel,
    ShowException,
    CliEventABC
)


# Keeping log messages for testing separate since they are used for debugging.
# Reuse the existing messages when adding logs to tests.

@dataclass
class IntegrationTestInfo(InfoLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return f"Integration Test: {self.msg}"


@dataclass
class IntegrationTestDebug(DebugLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return f"Integration Test: {self.msg}"


@dataclass
class IntegrationTestWarn(WarnLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return f"Integration Test: {self.msg}"


@dataclass
class IntegrationTestError(ErrorLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return f"Integration Test: {self.msg}"


@dataclass
class IntegrationTestException(ShowException, ErrorLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return f"Integration Test: {self.msg}"


# since mypy doesn't run on every file we need to suggest to mypy that every
# class gets instantiated. But we don't actually want to run this code.
# making the conditional `if False` causes mypy to skip it as dead code so
# we need to skirt around that by computing something it doesn't check statically.
#
# TODO remove these lines once we run mypy everywhere.
if 1 == 0:
    IntegrationTestInfo(msg='')
    IntegrationTestDebug(msg='')
    IntegrationTestWarn(msg='')
    IntegrationTestError(msg='')
    IntegrationTestException(msg='')
