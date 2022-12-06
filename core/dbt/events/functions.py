import betterproto
from dbt.constants import METADATA_ENV_PREFIX
from dbt.events.base_types import BaseEvent, Cache, EventLevel, NoFile, NoStdOut
from dbt.events.eventmgr import EventManager, LoggerConfig, LineFormat, NoFilter
from dbt.events.helpers import env_secrets, scrub_secrets
from dbt.events.proto_types import EventInfo
from dbt.events.types import EmptyLine
import dbt.flags as flags
from dbt.logger import GLOBAL_LOGGER, make_log_dir_if_missing
from functools import partial
import json
import os
import sys
from typing import Callable, Dict, Optional, TextIO
import uuid


LOG_VERSION = 3
metadata_vars: Optional[Dict[str, str]] = None

# The default event manager will not log anything, but some tests run code that
# generates events, without configuring the event manager.
EVENT_MANAGER: EventManager = EventManager()


def setup_event_logger(log_path: str, level_override: Optional[EventLevel] = None):
    cleanup_event_logger()
    make_log_dir_if_missing(log_path)
    if flags.ENABLE_LEGACY_LOGGER:
        EVENT_MANAGER.add_logger(_get_logbook_log_config(level_override))
    else:
        EVENT_MANAGER.add_logger(_get_stdout_config(level_override))

        if _CAPTURE_STREAM:
            # Create second stdout logger to support test which want to know what's
            # being sent to stdout.
            capture_config = _get_stdout_config(level_override)
            capture_config.output_stream = _CAPTURE_STREAM
            EVENT_MANAGER.add_logger(capture_config)

        # create and add the file logger to the event manager
        EVENT_MANAGER.add_logger(_get_logfile_config(os.path.join(log_path, "dbt.log")))


def _get_stdout_config(level: Optional[EventLevel]) -> LoggerConfig:
    fmt = LineFormat.PlainText
    if flags.LOG_FORMAT == "json":
        fmt = LineFormat.Json
    elif flags.DEBUG:
        fmt = LineFormat.DebugText

    return LoggerConfig(
        name="stdout_log",
        level=level or (EventLevel.DEBUG if flags.DEBUG else EventLevel.INFO),
        use_colors=bool(flags.USE_COLORS),
        line_format=fmt,
        scrubber=env_scrubber,
        filter=partial(
            _stdout_filter, bool(flags.LOG_CACHE_EVENTS), bool(flags.DEBUG), bool(flags.QUIET)
        ),
        output_stream=sys.stdout,
    )


def _stdout_filter(
    log_cache_events: bool, debug_mode: bool, quiet_mode: bool, evt: BaseEvent
) -> bool:
    return (
        not isinstance(evt, NoStdOut)
        and (not isinstance(evt, Cache) or log_cache_events)
        and (evt.log_level() != EventLevel.DEBUG or debug_mode)
        and (evt.log_level() == EventLevel.ERROR or not quiet_mode)
        and not (flags.LOG_FORMAT == "json" and type(evt) == EmptyLine)
    )


def _get_logfile_config(log_path: str) -> LoggerConfig:
    return LoggerConfig(
        name="file_log",
        line_format=LineFormat.Json if flags.LOG_FORMAT == "json" else LineFormat.DebugText,
        use_colors=bool(flags.USE_COLORS),
        level=EventLevel.DEBUG,  # File log is *always* debug level
        scrubber=env_scrubber,
        filter=partial(_logfile_filter, bool(flags.LOG_CACHE_EVENTS)),
        output_file_name=log_path,
    )


def _logfile_filter(log_cache_events: bool, evt: BaseEvent) -> bool:
    return (
        not isinstance(evt, NoFile)
        and not (isinstance(evt, Cache) and not log_cache_events)
        and not (flags.LOG_FORMAT == "json" and type(evt) == EmptyLine)
    )


def _get_logbook_log_config(level: Optional[EventLevel]) -> LoggerConfig:
    config = _get_stdout_config(level)
    config.name = "logbook_log"
    config.filter = NoFilter if flags.LOG_CACHE_EVENTS else lambda e: not isinstance(e, Cache)
    config.logger = GLOBAL_LOGGER
    return config


def env_scrubber(msg: str) -> str:
    return scrub_secrets(msg, env_secrets())


def cleanup_event_logger():
    # Reset to a no-op manager to release streams associated with logs. This is
    # especially important for tests, since pytest replaces the stdout stream
    # during test runs, and closes the stream after the test is over.
    EVENT_MANAGER.loggers.clear()
    EVENT_MANAGER.callbacks.clear()


# This global, and the following two functions for capturing stdout logs are
# an unpleasant hack we intend to remove as part of API-ification. The GitHub
# issue #6350 was opened for that work.
_CAPTURE_STREAM: Optional[TextIO] = None


# used for integration tests
def capture_stdout_logs(stream: TextIO):
    global _CAPTURE_STREAM
    _CAPTURE_STREAM = stream


def stop_capture_stdout_logs():
    global _CAPTURE_STREAM
    _CAPTURE_STREAM = None


# returns a dictionary representation of the event fields.
# the message may contain secrets which must be scrubbed at the usage site.
def event_to_json(event: BaseEvent) -> str:
    event_dict = event_to_dict(event)
    raw_log_line = json.dumps(event_dict, sort_keys=True)
    return raw_log_line


def event_to_dict(event: BaseEvent) -> dict:
    event_dict = dict()
    try:
        event_dict = event.to_dict(casing=betterproto.Casing.SNAKE, include_default_values=True)  # type: ignore
    except AttributeError as exc:
        event_type = type(event).__name__
        raise Exception(f"type {event_type} is not serializable. {str(exc)}")
    # We don't want an empty NodeInfo in output
    if "node_info" in event_dict and event_dict["node_info"]["node_name"] == "":
        del event_dict["node_info"]
    return event_dict


def warn_or_error(event, node=None):
    if flags.WARN_ERROR:
        from dbt.exceptions import raise_compiler_error

        raise_compiler_error(scrub_secrets(event.info.msg, env_secrets()), node)
    else:
        fire_event(event)


# an alternative to fire_event which only creates and logs the event value
# if the condition is met. Does nothing otherwise.
def fire_event_if(conditional: bool, lazy_e: Callable[[], BaseEvent]) -> None:
    if conditional:
        fire_event(lazy_e())


# top-level method for accessing the new eventing system
# this is where all the side effects happen branched by event type
# (i.e. - mutating the event history, printing to stdout, logging
# to files, etc.)
def fire_event(e: BaseEvent) -> None:
    EVENT_MANAGER.fire_event(e)


def get_metadata_vars() -> Dict[str, str]:
    global metadata_vars
    if metadata_vars is None:
        metadata_vars = {
            k[len(METADATA_ENV_PREFIX) :]: v
            for k, v in os.environ.items()
            if k.startswith(METADATA_ENV_PREFIX)
        }
    return metadata_vars


def reset_metadata_vars() -> None:
    global metadata_vars
    metadata_vars = None


def get_invocation_id() -> str:
    return EVENT_MANAGER.invocation_id


def set_invocation_id() -> None:
    # This is primarily for setting the invocation_id for separate
    # commands in the dbt servers. It shouldn't be necessary for the CLI.
    EVENT_MANAGER.invocation_id = str(uuid.uuid4())


# Currently used to set the level in EventInfo, so logging events can
# provide more than one "level". Might be used in the future to set
# more fields in EventInfo, once some of that information is no longer global
def info(level="info"):
    info = EventInfo(level=level)
    return info
