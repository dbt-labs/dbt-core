import dbt.flags
import dbt.ui.colors

import json
import logging
import os
import sys
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, ContextManager, Callable, Dict, Any

import colorama
import logbook
from hologram import JsonSchemaMixin

# Colorama needs some help on windows because we're using logger.info
# intead of print(). If the Windows env doesn't have a TERM var set,
# then we should override the logging stream to use the colorama
# converter. If the TERM var is set (as with Git Bash), then it's safe
# to send escape characters and no log handler injection is needed.
colorama_stdout = sys.stdout
colorama_wrap = True

colorama.init(wrap=colorama_wrap)


if sys.platform == 'win32' and not os.environ.get('TERM'):
    colorama_wrap = False
    colorama_stdout = colorama.AnsiToWin32(sys.stdout).stream

elif sys.platform == 'win32':
    colorama_wrap = False

colorama.init(wrap=colorama_wrap)


STDOUT_LOG_FORMAT = '{record.message}'
# TODO: can we change the time to just "{record.time:%Y-%m-%d %H:%M:%S.%f%z}"?
DEBUG_LOG_FORMAT = (
    '{record.time:%Y-%m-%d %H:%M:%S%z},{record.time.microsecond:03} '
    '({record.thread_name}): '
    '{record.message}'
)


ExceptionInformation = str
Extras = Dict[str, Any]


@dataclass
class LogMessage(JsonSchemaMixin):
    timestamp: datetime
    message: str
    channel: str
    level: int
    levelname: str
    thread_name: str
    process: int
    extra: Optional[Extras] = None
    exc_info: Optional[ExceptionInformation] = None

    @classmethod
    def from_record_formatted(cls, record: logbook.LogRecord, message: str):
        extra = dict(record.extra)
        log_message = LogMessage(
            timestamp=record.time,
            message=message,
            channel=record.channel,
            level=record.level,
            levelname=logbook.get_level_name(record.level),
            extra=extra,
            thread_name=record.thread_name,
            process=record.process,
            exc_info=record.formatted_exception,
        )
        return log_message


class LogMessageFormatter(logbook.StringFormatter):
    def __call__(self, record, handler):
        data = self.format_record(record, handler)
        exc = self.format_exception(record)
        if exc:
            data.exc_info = exc
        return data

    def format_record(self, record, handler):
        message = super().format_record(record, handler)
        return LogMessage.from_record_formatted(record, message)


class JsonFormatter(LogMessageFormatter):
    def __call__(self, record, handler):
        """Return a the record converted to LogMessage's JSON form"""
        log_message = super().__call__(record, handler)
        return json.dumps(log_message.to_dict())


class FormatterMixin:
    def __init__(self, format_string):
        self._text_format_string = format_string
        self.formatter_class = logbook.StringFormatter
        # triggers a formatter update via logbook.StreamHandler
        self.format_string = self._text_format_string

    def format_json(self):
        # set our formatter to the json formatter
        self.formatter_class = JsonFormatter
        self.format_string = STDOUT_LOG_FORMAT

    def format_text(self):
        # set our formatter to the regular stdout/stderr handler
        self.formatter_class = logbook.StringFormatter
        self.format_string = self._text_format_string


class OutputHandler(logbook.StreamHandler, FormatterMixin):
    """Output handler.

    The `format_string` parameter only changes the default text output, not
      debug mode or json.
    """
    def __init__(
        self,
        stream,
        level=logbook.INFO,
        format_string=STDOUT_LOG_FORMAT,
        bubble=True,
    ) -> None:
        self._default_format = format_string
        logbook.StreamHandler.__init__(
            self,
            stream=stream,
            level=level,
            format_string=format_string,
            bubble=bubble,
        )
        FormatterMixin.__init__(self, format_string)

    def set_text_format(self, format_string: str):
        """Set the text format to format_string. In JSON output mode, this is
        a noop.
        """
        if self.formatter_class is logbook.StringFormatter:
            # reset text format
            self._text_format_string = format_string
            self.format_text()

    def reset(self):
        self.level = logbook.INFO
        self._text_format_string = self._default_format
        self.format_text()


def _redirect_std_logging():
    logbook.compat.redirect_logging()


logger = logbook.Logger('dbt')
# provide this for the cache, disabled by default
CACHE_LOGGER = logbook.Logger('dbt.cache')
CACHE_LOGGER.disable()

warnings.filterwarnings("ignore", category=ResourceWarning,
                        message="unclosed.*<socket.socket.*>")

initialized = False


def make_log_dir_if_missing(log_dir):
    import dbt.clients.system
    dbt.clients.system.make_directory(log_dir)


class DebugWarnings(logbook.compat.redirected_warnings):
    """Log warnings, except send them to 'debug' instead of 'warning' level.
    """
    def make_record(self, message, exception, filename, lineno):
        rv = super().make_record(message, exception, filename, lineno)
        rv.level = logbook.DEBUG
        rv.extra['from_warnings'] = True
        return rv


# push Python warnings to debug level logs. This will suppress all import-time
# warnings.
DebugWarnings().__enter__()
# redirect stdlib logging to logbook
_redirect_std_logging()


class DelayedFileHandler(logbook.TimedRotatingFileHandler, FormatterMixin):
    def __init__(
        self,
        log_dir: Optional[str] = None,
        level=logbook.DEBUG,
        filter=None,
        bubble=True,
    ) -> None:
        self.disabled = False
        self._msg_buffer: Optional[List[logbook.LogRecord]] = []
        # if we get 1k messages without a logfile being set, something is wrong
        self._bufmax = 1000
        self._log_path = None
        # we need the base handler class' __init__ to run so handling works
        logbook.Handler.__init__(self, level, filter, bubble)
        if log_dir is not None:
            self.set_path(log_dir)

    def reset(self):
        if self.initialized:
            self.close()
        self._log_path = None
        self._msg_buffer = []
        self.disabled = False

    @property
    def initialized(self):
        return self._log_path is not None

    def set_path(self, log_dir):
        """log_dir can be the path to a log directory, or `None` to avoid
        writing to a file (for `dbt debug`)
        """
        assert not (self.initialized or self.disabled), 'set_path called twice'

        if log_dir is None:
            self.disabled = True
            return

        make_log_dir_if_missing(log_dir)
        log_path = os.path.join(log_dir, 'dbt.log')
        self._super_init(log_path)
        self._replay_buffered()
        self._log_path = log_path

    def _super_init(self, log_path):
        logbook.TimedRotatingFileHandler.__init__(
            self,
            filename=log_path,
            level=self.level,
            filter=self.filter,
            bubble=self.bubble,
            format_string=DEBUG_LOG_FORMAT,
            date_format='%Y-%m-%d',
            backup_count=7,
            timed_filename_for_current=False,
        )
        FormatterMixin.__init__(self, DEBUG_LOG_FORMAT)

    def _replay_buffered(self):
        for record in self._msg_buffer:
            super().emit(record)
        self._msg_buffer = None

    def format(self, record: logbook.LogRecord) -> str:
        msg = super().format(record)
        subbed = str(msg)
        for escape_sequence in dbt.ui.colors.COLORS.values():
            subbed = subbed.replace(escape_sequence, '')
        return subbed

    def emit(self, record: logbook.LogRecord):
        """emit is not thread-safe with set_path, but it is thread-safe with
        itself
        """
        if self.disabled:
            return
        elif self.initialized:
            super().emit(record)
        else:
            assert self._msg_buffer is not None, \
                '_msg_buffer should never be None if _log_path is set'
            self._msg_buffer.append(record)
            assert len(self._msg_buffer) < self._bufmax, \
                'too many messages received before initilization!'


class LogManager(logbook.NestedSetup):
    def __init__(self, stdout=colorama_stdout, stderr=sys.stderr):
        self.stdout = stdout
        self.stderr = stderr
        self._null_handler = logbook.NullHandler()
        self._output_handler = OutputHandler(self.stdout)
        self._file_handler = DelayedFileHandler()
        super().__init__([
            self._null_handler,
            self._output_handler,
            self._file_handler,
        ])

    def disable(self):
        self.add_handler(logbook.NullHandler())

    def add_handler(self, handler):
        """add an handler to the log manager that runs before the file handler.
        """
        self.objects.append(handler)

    # this is used by `dbt ls` to allow piping stdout to jq, etc
    def stderr_console(self):
        """Output to stderr at WARNING level instead of stdout"""
        self._output_handler.stream = self.stderr
        self._output_handler.level = logbook.WARNING

    def stdout_console(self):
        """enable stdout and disable stderr"""
        self._output_handler.stream = self.stdout
        self._output_handler.level = logbook.INFO

    def set_debug(self):
        self._output_handler.set_text_format(DEBUG_LOG_FORMAT)
        self._output_handler.level = logbook.DEBUG

    def set_path(self, path):
        self._file_handler.set_path(path)

    def initialized(self):
        return self._file_handler.initialized

    def format_json(self):
        for handler in self.objects:
            if isinstance(handler, FormatterMixin):
                handler.format_json()

    def format_text(self):
        for handler in self.objects:
            if isinstance(handler, FormatterMixin):
                handler.format_text()

    def reset_handlers(self):
        """Reset the handlers to their defaults. This is nice in testing!"""
        self.stdout_console()
        for handler in self.objects:
            if isinstance(handler, FormatterMixin):
                handler.reset()

    def set_output_stream(self, stream, error=None):
        if error is None:
            error = stream

        if self._output_handler.stream is self.stdout_stream:
            self._output_handler.stream = stream
        elif self._output_handler.stream is self.stderr_stream:
            self._output_handler.stream = error

        self.stdout_stream = stream
        self.stderr_stream = error


log_manager = LogManager()


def log_cache_events(flag):
    """Set the cache logger to propagate its messages based on the given flag.
    """
    CACHE_LOGGER.disabled = True


GLOBAL_LOGGER = logger


class LogMessageHandler(logbook.Handler):
    formatter_class = LogMessageFormatter

    def format_logmessage(self, record):
        """Format a LogRecord into a LogMessage"""
        message = self.format(record)
        return LogMessage.from_record_formatted(record, message)


class ListLogHandler(LogMessageHandler):
    def __init__(
        self,
        level: int = logbook.NOTSET,
        filter: Callable = None,
        bubble: bool = False,
        lst: Optional[List[LogMessage]] = None
    ) -> None:
        super().__init__(level, filter, bubble)
        if lst is None:
            lst = []
        self.records: List[LogMessage] = lst

    def emit(self, record: logbook.LogRecord):
        as_dict = self.format_logmessage(record)
        self.records.append(as_dict)


class SuppressBelow(logbook.Handler):
    def __init__(
        self, channels, level=logbook.INFO, filter=None, bubble=False
    ) -> None:
        self.channels = set(channels)
        super().__init__(level, filter, bubble)

    def should_handle(self, record):
        channel = record.channel.split('.')[0]
        if channel not in self.channels:
            return False
        # if we were set to 'info' and record.level is warn/error, we don't
        # want to 'handle' it (so a real logger will)
        return self.level >= record.level

    def handle(self, record):
        return True


# we still need to use logging to suppress these or pytest captures them
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('google').setLevel(logging.INFO)
logging.getLogger('snowflake.connector').setLevel(logging.INFO)
logging.getLogger('parsedatetime').setLevel(logging.INFO)
# we never want to see werkzeug logs
logging.getLogger('werkzeug').setLevel(logging.CRITICAL)


def list_handler(
    lst: Optional[List[LogMessage]],
    level=logbook.NOTSET,
) -> ContextManager:
    """Return a context manager that temporarly attaches a list to the logger.
    """
    return ListLogHandler(lst=lst, level=level, bubble=True)
