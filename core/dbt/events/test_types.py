from dbt.events.types import InfoLevel, DebugLevel, WarnLevel, ErrorLevel
from dbt.events.base_types import NoFile


# Keeping log messages for testing separate since they are used for debugging.
# Reuse the existing messages when adding logs to tests.


class IntegrationTestInfo(InfoLevel, NoFile):
    def code(self):
        return "T001"

    def message(self) -> str:
        return f"Integration Test: {self.msg}"


class IntegrationTestDebug(DebugLevel, NoFile):
    def code(self):
        return "T002"

    def message(self) -> str:
        return f"Integration Test: {self.msg}"


class IntegrationTestWarn(WarnLevel, NoFile):
    def code(self):
        return "T003"

    def message(self) -> str:
        return f"Integration Test: {self.msg}"


class IntegrationTestError(ErrorLevel, NoFile):
    def code(self):
        return "T004"

    def message(self) -> str:
        return f"Integration Test: {self.msg}"


class IntegrationTestException(ErrorLevel, NoFile):
    def code(self):
        return "T005"

    def message(self) -> str:
        return f"Integration Test: {self.msg}"


class UnitTestInfo(InfoLevel, NoFile):
    def code(self):
        return "T006"

    def message(self) -> str:
        return f"Unit Test: {self.msg}"
