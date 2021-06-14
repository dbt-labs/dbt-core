import os
import multiprocessing
if os.name != 'nt':
    # https://bugs.python.org/issue41567
    import multiprocessing.popen_spawn_posix  # type: ignore
from pathlib import Path
from typing import Optional

# initially all flags are set to None, the on-load call of reset() will set
# them for their first time.
STRICT_MODE = None
FULL_REFRESH = None
USE_CACHE = None
WARN_ERROR = None
TEST_NEW_PARSER = None
USE_EXPERIMENTAL_PARSER = None
WRITE_JSON = None
PARTIAL_PARSE = None
USE_COLORS = None
STORE_FAILURES = None


def env_set_truthy(key: str) -> Optional[str]:
    """Return the value if it was set to a "truthy" string value, or None
    otherwise.
    """
    value = os.getenv(key)
    if not value or value.lower() in ('0', 'false', 'f'):
        return None
    return value


def env_set_path(key: str) -> Optional[Path]:
    value = os.getenv(key)
    if value is None:
        return value
    else:
        return Path(value)


SINGLE_THREADED_WEBSERVER = env_set_truthy('DBT_SINGLE_THREADED_WEBSERVER')
SINGLE_THREADED_HANDLER = env_set_truthy('DBT_SINGLE_THREADED_HANDLER')
MACRO_DEBUGGING = env_set_truthy('DBT_MACRO_DEBUGGING')
DEFER_MODE = env_set_truthy('DBT_DEFER_TO_STATE')
ARTIFACT_STATE_PATH = env_set_path('DBT_ARTIFACT_STATE_PATH')


def _get_context():
    # TODO: change this back to use fork() on linux when we have made that safe
    return multiprocessing.get_context('spawn')


MP_CONTEXT = _get_context()


def reset():
    global STRICT_MODE, FULL_REFRESH, USE_CACHE, WARN_ERROR, TEST_NEW_PARSER, \
        USE_EXPERIMENTAL_PARSER, WRITE_JSON, PARTIAL_PARSE, MP_CONTEXT, USE_COLORS, \
        STORE_FAILURES

    STRICT_MODE = False
    FULL_REFRESH = False
    USE_CACHE = True
    WARN_ERROR = False
    TEST_NEW_PARSER = False
    USE_EXPERIMENTAL_PARSER = False
    WRITE_JSON = True
    PARTIAL_PARSE = False
    MP_CONTEXT = _get_context()
    USE_COLORS = True
    STORE_FAILURES = False


def set_from_args(args):
    global STRICT_MODE, FULL_REFRESH, USE_CACHE, WARN_ERROR, TEST_NEW_PARSER, \
        USE_EXPERIMENTAL_PARSER, WRITE_JSON, PARTIAL_PARSE, MP_CONTEXT, USE_COLORS, \
        STORE_FAILURES

    USE_CACHE = getattr(args, 'use_cache', USE_CACHE)

    FULL_REFRESH = getattr(args, 'full_refresh', FULL_REFRESH)
    STRICT_MODE = getattr(args, 'strict', STRICT_MODE)
    WARN_ERROR = (
        STRICT_MODE or
        getattr(args, 'warn_error', STRICT_MODE or WARN_ERROR)
    )

    TEST_NEW_PARSER = getattr(args, 'test_new_parser', TEST_NEW_PARSER)
    USE_EXPERIMENTAL_PARSER = getattr(args, 'use_experimental_parser', USE_EXPERIMENTAL_PARSER)
    WRITE_JSON = getattr(args, 'write_json', WRITE_JSON)
    PARTIAL_PARSE = getattr(args, 'partial_parse', None)
    MP_CONTEXT = _get_context()

    # The use_colors attribute will always have a value because it is assigned
    # None by default from the add_mutually_exclusive_group function
    use_colors_override = getattr(args, 'use_colors')

    if use_colors_override is not None:
        USE_COLORS = use_colors_override

    STORE_FAILURES = getattr(args, 'store_failures', STORE_FAILURES)


# initialize everything to the defaults on module load
reset()
