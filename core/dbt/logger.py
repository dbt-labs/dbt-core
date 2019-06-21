import dbt.flags
import logging
import logging.handlers
import os
import sys
import warnings

import colorama

# Colorama needs some help on windows because we're using logger.info
# intead of print(). If the Windows env doesn't have a TERM var set,
# then we should override the logging stream to use the colorama
# converter. If the TERM var is set (as with Git Bash), then it's safe
# to send escape characters and no log handler injection is needed.
colorama_stdout = sys.stdout
colorama_wrap = True

colorama.init(wrap=colorama_wrap)

DEBUG = logging.DEBUG
NOTICE = 15
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

logging.addLevelName(NOTICE, 'NOTICE')


class Logger(logging.Logger):
    def notice(self, msg, *args, **kwargs):
        if self.isEnabledFor(NOTICE):
            self._log(NOTICE, msg, args, **kwargs)


logging.setLoggerClass(Logger)


if sys.platform == 'win32' and not os.environ.get('TERM'):
    colorama_wrap = False
    colorama_stdout = colorama.AnsiToWin32(sys.stdout).stream

elif sys.platform == 'win32':
    colorama_wrap = False

colorama.init(wrap=colorama_wrap)

# create a global console logger for dbt
stdout_handler = logging.StreamHandler(colorama_stdout)
stdout_handler.setFormatter(logging.Formatter('%(message)s'))
stdout_handler.setLevel(NOTICE)

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setFormatter(logging.Formatter('%(message)s'))
stderr_handler.setLevel(WARNING)


logger = logging.getLogger('dbt')
logger.addHandler(stdout_handler)
logger.setLevel(DEBUG)
logging.getLogger().setLevel(CRITICAL)

# Quiet these down in the logs
logging.getLogger('botocore').setLevel(INFO)
logging.getLogger('requests').setLevel(INFO)
logging.getLogger('urllib3').setLevel(INFO)
logging.getLogger('google').setLevel(INFO)
logging.getLogger('snowflake.connector').setLevel(INFO)
logging.getLogger('parsedatetime').setLevel(INFO)
# we never want to seek werkzeug logs
logging.getLogger('werkzeug').setLevel(CRITICAL)

# provide this for the cache.
CACHE_LOGGER = logging.getLogger('dbt.cache')
# add a dummy handler to avoid `No handlers could be found for logger`
nothing_handler = logging.StreamHandler()
nothing_handler.setLevel(CRITICAL)
CACHE_LOGGER.addHandler(nothing_handler)
# provide this for RPC connection logging
RPC_LOGGER = logging.getLogger('dbt.rpc')


# Redirect warnings through our logging setup
# They will be logged to a file below
logging.captureWarnings(True)
warnings.filterwarnings("ignore", category=ResourceWarning,
                        message="unclosed.*<socket.socket.*>")

initialized = False


def _swap_handler(logger, old, new):
    if old in logger.handlers:
        logger.handlers.remove(old)
    if new not in logger.handlers:
        logger.addHandler(new)


def log_to_stderr(logger):
    _swap_handler(logger, stdout_handler, stderr_handler)


def log_to_stdout(logger):
    _swap_handler(logger, stderr_handler, stdout_handler)


def make_log_dir_if_missing(log_dir):
    import dbt.clients.system
    dbt.clients.system.make_directory(log_dir)


class ColorFilter(logging.Filter):
    def filter(self, record):
        subbed = str(record.msg)
        for escape_sequence in dbt.ui.colors.COLORS.values():
            subbed = subbed.replace(escape_sequence, '')
        record.msg = subbed

        return True


def default_formatter():
    return logging.Formatter('%(asctime)-18s (%(threadName)s): %(message)s')


def initialize_logger(debug_mode=False, path=None):
    global initialized, logger, stdout_handler, stderr_handler

    if initialized:
        return

    if debug_mode:
        # we'll only use one of these, but just set both up
        stdout_handler.setFormatter(default_formatter())
        stdout_handler.setLevel(DEBUG)
        stderr_handler.setFormatter(default_formatter())
        stderr_handler.setLevel(DEBUG)

    if path is not None:
        make_log_dir_if_missing(path)
        log_path = os.path.join(path, 'dbt.log')

        # log to directory as well
        logdir_handler = logging.handlers.TimedRotatingFileHandler(
            filename=log_path,
            when='d',
            interval=1,
            backupCount=7,
        )

        color_filter = ColorFilter()
        logdir_handler.addFilter(color_filter)

        logdir_handler.setFormatter(default_formatter())
        logdir_handler.setLevel(DEBUG)

        logger.addHandler(logdir_handler)

        # Log Python warnings to file
        warning_logger = logging.getLogger('py.warnings')
        warning_logger.addHandler(logdir_handler)
        warning_logger.setLevel(DEBUG)

    initialized = True


def logger_initialized():
    return initialized


def log_cache_events(flag):
    """Set the cache logger to propagate its messages based on the given flag.
    """
    CACHE_LOGGER.propagate = flag


GLOBAL_LOGGER = logger


class QueueFormatter(logging.Formatter):
    def format(self, record):
        record.message = record.getMessage()
        record.asctime = self.formatTime(record, self.datefmt)
        formatted = self.formatMessage(record)

        output = {
            'message': formatted,
            'timestamp': record.asctime,
            'levelname': record.levelname,
            'level': record.levelno,
        }
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
            output['exc_info'] = record.exc_text
        return output


class QueueLogHandler(logging.Handler):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def emit(self, record):
        msg = self.format(record)
        self.queue.put_nowait(['log', msg])


def add_queue_handler(queue):
    """Add a queue log handler to the global logger."""
    handler = QueueLogHandler(queue)
    handler.setFormatter(QueueFormatter())
    handler.setLevel(DEBUG)
    GLOBAL_LOGGER.addHandler(handler)
