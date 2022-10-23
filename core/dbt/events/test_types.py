from dataclasses import dataclass
from dbt.events.types import InfoLevel, DebugLevel, WarnLevel, ErrorLevel
from dbt.events.base_types import NoFile
from dbt.events import proto_types as pl
from dbt.events.proto_types import EventInfo  # noqa


# Keeping log messages for testing separate since they are used for debugging.
# Reuse the existing messages when adding logs to tests.


@dataclass
class IntegrationTestInfo(InfoLevel, NoFile, pl.IntegrationTestInfo):
    def code(self):
        return "T001"

    def message(self) -> str:
        return f"Integration Test: {self.msg}"


@dataclass
class IntegrationTestDebug(DebugLevel, NoFile, pl.IntegrationTestDebug):
    def code(self):
        return "T002"

    def message(self) -> str:
        return f"Integration Test: {self.msg}"


@dataclass
class IntegrationTestWarn(WarnLevel, NoFile, pl.IntegrationTestWarn):
    def code(self):
        return "T003"

    def message(self) -> str:
        return f"Integration Test: {self.msg}"


@dataclass
class IntegrationTestError(ErrorLevel, NoFile, pl.IntegrationTestError):
    def code(self):
        return "T004"

    def message(self) -> str:
        return f"Integration Test: {self.msg}"


@dataclass
class IntegrationTestException(ErrorLevel, NoFile, pl.IntegrationTestException):
    def code(self):
        return "T005"

    def message(self) -> str:
        return f"Integration Test: {self.msg}"


@dataclass
class UnitTestInfo(InfoLevel, NoFile, pl.UnitTestInfo):
    def code(self):
        return "T006"

    def message(self) -> str:
        return f"Unit Test: {self.msg}"


# since mypy doesn't run on every file we need to suggest to mypy that every
# class gets instantiated. But we don't actually want to run this code.
# making the conditional `if False` causes mypy to skip it as dead code so
# we need to skirt around that by computing something it doesn't check statically.
#
# TODO remove these lines once we run mypy everywhere.
if 1 == 0:
    IntegrationTestInfo(msg="")
    IntegrationTestDebug(msg="")
    IntegrationTestWarn(msg="")
    IntegrationTestError(msg="")
    IntegrationTestException(msg="")
    UnitTestInfo(msg="")
