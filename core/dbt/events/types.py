from abc import ABCMeta, abstractmethod


# The following classes represent the data necessary to describe a
# particular event to both human readable logs, and machine reliable
# event streams. classes extend superclasses that indicate what
# destinations they are intended for, which mypy uses to enforce
# that the necessary methods are defined.


# top-level superclass for all events
class Event():
    pass


class CliEventABC(Event, metaclass=ABCMeta):
    # Solely the human readable message. Timestamps and formatting will be added by the logger
    @abstractmethod
    def cli_msg(self) -> str:
        raise Exception("cli_msg not implemented for cli event")


# base class used for type-level membership checking only
class ParsingProgressBase():
    pass


class ParsingStart(CliEventABC, ParsingProgressBase):
    def cli_msg(self) -> str:
        return "Start parsing."


class ParsingCompiling(CliEventABC, ParsingProgressBase):
    def cli_msg(self) -> str:
        return "Compiling."


class ParsingWritingManifest(CliEventABC, ParsingProgressBase):
    def cli_msg(self) -> str:
        return "Writing manifest."


class ParsingDone(CliEventABC, ParsingProgressBase):
    def cli_msg(self) -> str:
        return "Done."


# base class used for type-level membership checking only
class ManifestProgressBase():
    pass


class ManifestDependenciesLoaded(CliEventABC, ManifestProgressBase):
    def cli_msg(self) -> str:
        return "Dependencies loaded"


class ManifestLoaderCreated(CliEventABC, ManifestProgressBase):
    def cli_msg(self) -> str:
        return "ManifestLoader created"


class ManifestLoaded(CliEventABC, ManifestProgressBase):
    def cli_msg(self) -> str:
        return "Manifest loaded"


class ManifestChecked(CliEventABC, ManifestProgressBase):
    def cli_msg(self) -> str:
        return "Manifest checked"


class ManifestFlatGraphBuilt(CliEventABC, ManifestProgressBase):
    def cli_msg(self) -> str:
        return "Flat graph built"


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
