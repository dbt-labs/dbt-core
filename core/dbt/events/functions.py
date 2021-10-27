
import dbt.logger as logger  # type: ignore # TODO eventually remove dependency on this logger
from dbt.events.history import EVENT_HISTORY
from dbt.events.types import *  # noqa: F403
from dbt.events.types import ParsingProgressBase, ManifestProgressBase
from typing import NoReturn


# common trick for getting mypy to do exhaustiveness checks
# will come up with something like `"assert_never" has incompatible type`
# if something is missing.
def assert_never(x: NoReturn) -> NoReturn:
    raise AssertionError("Unhandled type: {}".format(type(x).__name__))


# TODO is there a type-level way to do this in mypy? `isinstance(e, CliEvent)`
# triggers `Parameterized generics cannot be used with class or instance checks`
def is_cli_event(e: Event) -> bool:
    return isinstance(e, ParsingProgressBase) or isinstance(e, ManifestProgressBase)


# top-level method for accessing the new eventing system
# this is where all the side effects happen branched by event type
# (i.e. - mutating the event history, printing to stdout, logging
# to files, etc.)
def fire_event(e: Event) -> None:
    EVENT_HISTORY.append(e)
    if is_cli_event(e):
        # TODO handle log levels
        logger.GLOBAL_LOGGER.info(cli_msg(e))


# These functions translate any instance of the above event types
# into various message types to later be sent to their final destination.
#
# These could instead be implemented as methods on an ABC for all the
# above classes, but this way we can enforce exhaustiveness with mypy


# returns the string to be printed to the CLI
def cli_msg(e: CliEvent) -> str:
    if isinstance(e, ParsingStart):
        return logger.timestamped_line("Start parsing.")
    elif isinstance(e, ParsingCompiling):
        return logger.timestamped_line("Compiling.")
    elif isinstance(e, ParsingWritingManifest):
        return logger.timestamped_line("Writing manifest.")
    elif isinstance(e, ParsingDone):
        return logger.timestamped_line("Done.")
    elif isinstance(e, ManifestDependenciesLoaded):
        return logger.timestamped_line("Dependencies loaded")
    elif isinstance(e, ManifestLoaderCreated):
        return logger.timestamped_line("ManifestLoader created")
    elif isinstance(e, ManifestLoaded):
        return logger.timestamped_line("Manifest loaded")
    elif isinstance(e, ManifestChecked):
        return logger.timestamped_line("Manifest checked")
    elif isinstance(e, ManifestFlatGraphBuilt):
        return logger.timestamped_line("Flat graph built")
    else:
        assert_never(e)
