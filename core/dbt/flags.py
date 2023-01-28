# Do not import the os package because we expose this package in jinja
from os import name as os_name, path as os_path, getcwd as os_getcwd, getenv as os_getenv
import multiprocessing
from argparse import Namespace

if os_name != "nt":
    # https://bugs.python.org/issue41567
    import multiprocessing.popen_spawn_posix  # type: ignore
from pathlib import Path
from typing import Optional

# PROFILES_DIR must be set before the other flags
# It also gets set in main.py and in set_from_args because the rpc server
# doesn't go through exactly the same main arg processing.
GLOBAL_PROFILES_DIR = os_path.join(os_path.expanduser("~"), ".dbt")
LOCAL_PROFILES_DIR = os_getcwd()
# Use the current working directory if there is a profiles.yml file present there
if os_path.exists(Path(LOCAL_PROFILES_DIR) / Path("profiles.yml")):
    DEFAULT_PROFILES_DIR = LOCAL_PROFILES_DIR
else:
    DEFAULT_PROFILES_DIR = GLOBAL_PROFILES_DIR
PROFILES_DIR = os_path.expanduser(os_getenv("DBT_PROFILES_DIR", DEFAULT_PROFILES_DIR))

STRICT_MODE = False  # Only here for backwards compatibility
FULL_REFRESH = False  # subcommand
STORE_FAILURES = False  # subcommand

# Global CLI commands
USE_EXPERIMENTAL_PARSER = None
STATIC_PARSER = None
WARN_ERROR = None
WARN_ERROR_OPTIONS = None
WRITE_JSON = None
PARTIAL_PARSE = None
USE_COLORS = None
DEBUG = None
LOG_FORMAT = None
VERSION_CHECK = None
FAIL_FAST = None
SEND_ANONYMOUS_USAGE_STATS = None
PRINTER_WIDTH = 80
WHICH = None
INDIRECT_SELECTION = None
LOG_CACHE_EVENTS = None
QUIET = None
NO_PRINT = None
CACHE_SELECTED_ONLY = None
TARGET_PATH = None
LOG_PATH = None

_NON_BOOLEAN_FLAGS = [
    "LOG_FORMAT",
    "PRINTER_WIDTH",
    "PROFILES_DIR",
    "INDIRECT_SELECTION",
    "TARGET_PATH",
    "LOG_PATH",
    "WARN_ERROR_OPTIONS",
]

_NON_DBT_ENV_FLAGS = ["DO_NOT_TRACK"]


# Global CLI defaults. These flags are set from three places:
# CLI args, environment variables, and user_config (profiles.yml).
# Environment variables use the pattern 'DBT_{flag name}', like DBT_PROFILES_DIR
flag_defaults = {
    "USE_EXPERIMENTAL_PARSER": False,
    "STATIC_PARSER": True,
    "WARN_ERROR": False,
    "WARN_ERROR_OPTIONS": "{}",
    "WRITE_JSON": True,
    "PARTIAL_PARSE": True,
    "USE_COLORS": True,
    "PROFILES_DIR": DEFAULT_PROFILES_DIR,
    "DEBUG": False,
    "LOG_FORMAT": None,
    "VERSION_CHECK": True,
    "FAIL_FAST": False,
    "SEND_ANONYMOUS_USAGE_STATS": True,
    "PRINTER_WIDTH": 80,
    "INDIRECT_SELECTION": "eager",
    "LOG_CACHE_EVENTS": False,
    "QUIET": False,
    "NO_PRINT": False,
    "CACHE_SELECTED_ONLY": False,
    "TARGET_PATH": None,
    "LOG_PATH": None,
}


def env_set_truthy(key: str) -> Optional[str]:
    """Return the value if it was set to a "truthy" string value or None
    otherwise.
    """
    value = os_getenv(key)
    if not value or value.lower() in ("0", "false", "f"):
        return None
    return value


def env_set_bool(env_value):
    if env_value in ("1", "t", "true", "y", "yes"):
        return True
    return False


def env_set_path(key: str) -> Optional[Path]:
    value = os_getenv(key)
    if value is None:
        return value
    else:
        return Path(value)


MACRO_DEBUGGING = env_set_truthy("DBT_MACRO_DEBUGGING")
DEFER_MODE = env_set_truthy("DBT_DEFER_TO_STATE")
FAVOR_STATE_MODE = env_set_truthy("DBT_FAVOR_STATE")
ARTIFACT_STATE_PATH = env_set_path("DBT_ARTIFACT_STATE_PATH")
ENABLE_LEGACY_LOGGER = env_set_truthy("DBT_ENABLE_LEGACY_LOGGER")


def _get_context():
    # TODO: change this back to use fork() on linux when we have made that safe
    return multiprocessing.get_context("spawn")


# This is not a flag, it's a place to store the lock
MP_CONTEXT = _get_context()


def set_from_args(args, user_config):
    # N.B. Multiple `globals` are purely for line length.
    # Because `global` is a parser directive (as opposed to a language construct)
    # black insists in putting them all on one line
    global STRICT_MODE, FULL_REFRESH, WARN_ERROR, WARN_ERROR_OPTIONS, USE_EXPERIMENTAL_PARSER, STATIC_PARSER
    global WRITE_JSON, PARTIAL_PARSE, USE_COLORS, STORE_FAILURES, PROFILES_DIR, DEBUG, LOG_FORMAT
    global INDIRECT_SELECTION, VERSION_CHECK, FAIL_FAST, SEND_ANONYMOUS_USAGE_STATS
    global PRINTER_WIDTH, WHICH, LOG_CACHE_EVENTS, QUIET, NO_PRINT, CACHE_SELECTED_ONLY
    global TARGET_PATH, LOG_PATH

    STRICT_MODE = False  # backwards compatibility
    # cli args without user_config or env var option
    FULL_REFRESH = getattr(args, "full_refresh", FULL_REFRESH)
    STORE_FAILURES = getattr(args, "store_failures", STORE_FAILURES)
    WHICH = getattr(args, "which", WHICH)

    # global cli flags with env var and user_config alternatives
    USE_EXPERIMENTAL_PARSER = get_flag_value("USE_EXPERIMENTAL_PARSER", args, user_config)
    STATIC_PARSER = get_flag_value("STATIC_PARSER", args, user_config)
    WARN_ERROR = get_flag_value("WARN_ERROR", args, user_config)
    WARN_ERROR_OPTIONS = get_flag_value("WARN_ERROR_OPTIONS", args, user_config)
    _check_mutually_exclusive(["WARN_ERROR", "WARN_ERROR_OPTIONS"], args, user_config)
    WRITE_JSON = get_flag_value("WRITE_JSON", args, user_config)
    PARTIAL_PARSE = get_flag_value("PARTIAL_PARSE", args, user_config)
    USE_COLORS = get_flag_value("USE_COLORS", args, user_config)
    PROFILES_DIR = get_flag_value("PROFILES_DIR", args, user_config)
    DEBUG = get_flag_value("DEBUG", args, user_config)
    LOG_FORMAT = get_flag_value("LOG_FORMAT", args, user_config)
    VERSION_CHECK = get_flag_value("VERSION_CHECK", args, user_config)
    FAIL_FAST = get_flag_value("FAIL_FAST", args, user_config)
    SEND_ANONYMOUS_USAGE_STATS = get_flag_value("SEND_ANONYMOUS_USAGE_STATS", args, user_config)
    PRINTER_WIDTH = get_flag_value("PRINTER_WIDTH", args, user_config)
    INDIRECT_SELECTION = get_flag_value("INDIRECT_SELECTION", args, user_config)
    LOG_CACHE_EVENTS = get_flag_value("LOG_CACHE_EVENTS", args, user_config)
    QUIET = get_flag_value("QUIET", args, user_config)
    NO_PRINT = get_flag_value("NO_PRINT", args, user_config)
    CACHE_SELECTED_ONLY = get_flag_value("CACHE_SELECTED_ONLY", args, user_config)
    TARGET_PATH = get_flag_value("TARGET_PATH", args, user_config)
    LOG_PATH = get_flag_value("LOG_PATH", args, user_config)

    _set_overrides_from_env()


def _set_overrides_from_env():
    global SEND_ANONYMOUS_USAGE_STATS

    flag_value = _get_flag_value_from_env("DO_NOT_TRACK")
    if flag_value is None:
        return

    SEND_ANONYMOUS_USAGE_STATS = not flag_value


def get_flag_value(flag, args, user_config):
    flag_value, _ = _load_flag_value(flag, args, user_config)

    if flag == "PRINTER_WIDTH":  # must be ints
        flag_value = int(flag_value)
    if flag == "PROFILES_DIR":
        flag_value = os_path.abspath(flag_value)

    return flag_value


def _check_mutually_exclusive(group, args, user_config):
    set_flag = None
    for flag in group:
        flag_set_by_user = not _flag_value_from_default(flag, args, user_config)
        if flag_set_by_user and set_flag:
            raise ValueError(f"{flag.lower()}: not allowed with argument {set_flag.lower()}")
        elif flag_set_by_user:
            set_flag = flag


def _flag_value_from_default(flag, args, user_config):
    _, from_default = _load_flag_value(flag, args, user_config)

    return from_default


def _load_flag_value(flag, args, user_config):
    lc_flag = flag.lower()
    flag_value = getattr(args, lc_flag, None)
    if flag_value is not None:
        return flag_value, False

    flag_value = _get_flag_value_from_env(flag)
    if flag_value is not None:
        return flag_value, False

    if user_config is not None and getattr(user_config, lc_flag, None) is not None:
        return getattr(user_config, lc_flag), False

    return flag_defaults[flag], True


def _get_flag_value_from_env(flag):
    # Environment variables use pattern 'DBT_{flag name}'
    env_flag = _get_env_flag(flag)
    env_value = os_getenv(env_flag)
    if env_value is None or env_value == "":
        return None

    if flag in _NON_BOOLEAN_FLAGS:
        flag_value = env_value
    else:
        flag_value = env_set_bool(env_value.lower())

    return flag_value


def _get_env_flag(flag):
    return flag if flag in _NON_DBT_ENV_FLAGS else f"DBT_{flag}"


def get_flag_dict():
    return {
        "use_experimental_parser": USE_EXPERIMENTAL_PARSER,
        "static_parser": STATIC_PARSER,
        "warn_error": WARN_ERROR,
        "warn_error_options": WARN_ERROR_OPTIONS,
        "write_json": WRITE_JSON,
        "partial_parse": PARTIAL_PARSE,
        "use_colors": USE_COLORS,
        "profiles_dir": PROFILES_DIR,
        "debug": DEBUG,
        "log_format": LOG_FORMAT,
        "version_check": VERSION_CHECK,
        "fail_fast": FAIL_FAST,
        "send_anonymous_usage_stats": SEND_ANONYMOUS_USAGE_STATS,
        "printer_width": PRINTER_WIDTH,
        "indirect_selection": INDIRECT_SELECTION,
        "log_cache_events": LOG_CACHE_EVENTS,
        "quiet": QUIET,
        "no_print": NO_PRINT,
        "cache_selected_only": CACHE_SELECTED_ONLY,
        "target_path": TARGET_PATH,
        "log_path": LOG_PATH,
    }


# This is used by core/dbt/context/base.py to return a flag object
# in Jinja.
def get_flag_obj():
    new_flags = Namespace()
    for k, v in get_flag_dict().items():
        setattr(new_flags, k.upper(), v)
    # The following 3 are CLI arguments only so they're not full-fledged flags,
    # but we put in flags for users.
    setattr(new_flags, "FULL_REFRESH", FULL_REFRESH)
    setattr(new_flags, "STORE_FAILURES", STORE_FAILURES)
    setattr(new_flags, "WHICH", WHICH)
    return new_flags
