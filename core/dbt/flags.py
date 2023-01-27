# Do not import the os package because we expose this package in jinja
from os import name as os_name, path as os_path, getcwd as os_getcwd, getenv as os_getenv
from argparse import Namespace
from multiprocessing import get_context
from typing import Optional
from pathlib import Path

def env_set_truthy(key: str) -> Optional[str]:
    """Return the value if it was set to a "truthy" string value or None
    otherwise.
    """
    value = os_getenv(key)
    if not value or value.lower() in ("0", "false", "f"):
        return None
    return value

# for setting up logger for legacy logger
ENABLE_LEGACY_LOGGER = env_set_truthy("DBT_ENABLE_LEGACY_LOGGER")
LOG_FORMAT = None
DEBUG = None
USE_COLORS = None
LOG_CACHE_EVENTS = None
QUIET = None

# This is not a flag, it's a place to store the lock
MP_CONTEXT = get_context()


# this roughly follows the patten of EVENT_MANAGER in dbt/events/functions.py
# During de-globlization, we'll need to handle both similarly
GLOBAL_FLAGS = Namespace()

def set_flags(flags):
    global GLOBAL_FLAGS
    GLOBAL_FLAGS = flags

def get_flag(key:str, default=None):
    return getattr(GLOBAL_FLAGS, key, flag_defaults.get(key.upper(), default))

def set_from_args(args, user_config):
    pass
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


def get_flag_dict():
    flag_attr = {
        "use_experimental_parser",
        "static_parser",
        "warn_error",
        "warn_error_options",
        "write_json",
        "partial_parse",
        "use_colors",
        "profiles_dir",
        "debug",
        "log_format",
        "version_check",
        "fail_fast",
        "send_anonymous_usage_stats",
        "anonymous_usage_stats",
        "printer_width",
        "indirect_selection",
        "log_cache_events",
        "quiet",
        "no_print",
        "cache_selected_only",
        "target_path",
        "log_path",
    }
    return {
        key: getattr(GLOBAL_FLAGS, key.upper(), getattr(flag_defaults, key.upper(), None))
        for key in flag_attr 
    }


# This is used by core/dbt/context/base.py to return a flag object
# in Jinja.
def get_flag_obj():
    new_flags = Namespace()
    for key, val in get_flag_dict().items():
        setattr(new_flags, key.upper(), val)
    # The following 3 are CLI arguments only so they're not full-fledged flags,
    # but we put in flags for users.
    setattr(new_flags, "FULL_REFRESH", getattr(GLOBAL_FLAGS, "FULL_REFRESH", None))
    setattr(new_flags, "STORE_FAILURES", getattr(GLOBAL_FLAGS, "STORE_FAILURES", None))
    setattr(new_flags, "WHICH", getattr(GLOBAL_FLAGS, "WHICH", None))
    return new_flags
