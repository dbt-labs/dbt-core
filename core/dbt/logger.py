import dbt.compat
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

if sys.platform == 'win32' and not os.environ.get('TERM'):
    colorama_wrap = False
    colorama_stdout = colorama.AnsiToWin32(sys.stdout).stream

elif sys.platform == 'win32':
    colorama_wrap = False

colorama.init(wrap=colorama_wrap)

# create a global console logger for dbt
stdout_handler = logging.StreamHandler(colorama_stdout)
stdout_handler.setFormatter(logging.Formatter('%(message)s'))
stdout_handler.setLevel(logging.INFO)

logger = logging.getLogger('dbt')
logger.addHandler(stdout_handler)
logger.setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.CRITICAL)

# Quiet these down in the logs
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('google').setLevel(logging.INFO)
logging.getLogger('snowflake.connector').setLevel(logging.INFO)
logging.getLogger('parsedatetime').setLevel(logging.INFO)
# we never want to seek werkzeug logs
logging.getLogger('werkzeug').setLevel(logging.CRITICAL)

# provide this for the cache.
CACHE_LOGGER = logging.getLogger('dbt.cache')
# provide this for RPC connection logging
RPC_LOGGER = logging.getLogger('dbt.rpc')


# Redirect warnings through our logging setup
# They will be logged to a file below
logging.captureWarnings(True)
dbt.compat.suppress_warnings()

initialized = False


def make_log_dir_if_missing(log_dir):
    import dbt.clients.system
    dbt.clients.system.make_directory(log_dir)


class ColorFilter(logging.Filter):
    def filter(self, record):
        subbed = dbt.compat.to_string(record.msg)
        for escape_sequence in dbt.ui.colors.COLORS.values():
            subbed = subbed.replace(escape_sequence, '')
        record.msg = subbed

        return True


def default_formatter():
    return logging.Formatter('%(asctime)-18s (%(threadName)s): %(message)s')


def initialize_logger(debug_mode=False, path=None):
    global initialized, logger, stdout_handler

    if initialized:
        return

    if debug_mode:
        stdout_handler.setFormatter(default_formatter())
        stdout_handler.setLevel(logging.DEBUG)

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
        logdir_handler.setLevel(logging.DEBUG)

        logger.addHandler(logdir_handler)

        # Log Python warnings to file
        warning_logger = logging.getLogger('py.warnings')
        warning_logger.addHandler(logdir_handler)
        warning_logger.setLevel(logging.DEBUG)

    initialized = True


def logger_initialized():
    return initialized


def log_cache_events(flag):
    """Set the cache logger to propagate its messages based on the given flag.
    """
    CACHE_LOGGER.propagate = flag


GLOBAL_LOGGER = logger


class QueueLogHandler(logging.Handler):
    def __init__(self, queue):
        super(QueueLogHandler, self).__init__()
        self.queue = queue

    def emit(self, record):
        msg = self.format(record)
        self.queue.put_nowait(['log', msg])


def add_queue_handler(queue):
    """Add a queue log handler to the global logger."""
    handler = QueueLogHandler(queue)
    handler.setFormatter(default_formatter())
    handler.setLevel(logging.DEBUG)
    GLOBAL_LOGGER.addHandler(handler)
