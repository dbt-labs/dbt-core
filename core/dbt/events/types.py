from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any


# types to represent log levels

# in preparation for #3977
class TestLevel():
    def level_tag(self) -> str:
        return "test"


class DebugLevel():
    def level_tag(self) -> str:
        return "debug"


class InfoLevel():
    def level_tag(self) -> str:
        return "info"


class WarnLevel():
    def level_tag(self) -> str:
        return "warn"


class ErrorLevel():
    def level_tag(self) -> str:
        return "error"


@dataclass
class ShowException():
    exc_info: Any = None
    stack_info: Any = None
    extra: Any = None


# The following classes represent the data necessary to describe a
# particular event to both human readable logs, and machine reliable
# event streams. classes extend superclasses that indicate what
# destinations they are intended for, which mypy uses to enforce
# that the necessary methods are defined.


# top-level superclass for all events
class Event(metaclass=ABCMeta):
    # do not define this yourself. inherit it from one of the above level types.
    @abstractmethod
    def level_tag(self) -> str:
        raise Exception("level_tag not implemented for event")


class CliEventABC(Event, metaclass=ABCMeta):
    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    @abstractmethod
    def cli_msg(self) -> str:
        raise Exception("cli_msg not implemented for cli event")


class ParsingStart(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Start parsing."


class ParsingCompiling(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Compiling."


class ParsingWritingManifest(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Writing manifest."


class ParsingDone(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Done."


class ManifestDependenciesLoaded(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Dependencies loaded"


class ManifestLoaderCreated(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "ManifestLoader created"


class ManifestLoaded(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Manifest loaded"


class ManifestChecked(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Manifest checked"


class ManifestFlatGraphBuilt(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Flat graph built"


@dataclass
class ReportPerformancePath(InfoLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"Performance info: {self.path}"


@dataclass
class MacroEventInfo(InfoLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return self.msg


@dataclass
class MacroEventDebug(DebugLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return self.msg


# since mypy doesn't run on every file we need to suggest to mypy that every
# class gets instantiated. But we don't actually want to run this code.
# making the conditional `if False` causes mypy to skip it as dead code so
# we need to skirt around that by computing something it doesn't check statically.
#
# TODO remove these lines once we run mypy everywhere.
if 1 == 0:
    ParsingStart()
    ParsingCompiling()
    ParsingWritingManifest()
    ParsingDone()
    ManifestDependenciesLoaded()
    ManifestLoaderCreated()
    ManifestLoaded()
    ManifestChecked()
    ManifestFlatGraphBuilt()
    ReportPerformancePath(path='')
    MacroEventInfo(msg='')
    MacroEventDebug(msg='')
