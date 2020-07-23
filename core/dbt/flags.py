import os
import multiprocessing
from typing import Optional
# initially all flags are set to None, the on-load call of reset() will set
# them for their first time.
STRICT_MODE = None
FULL_REFRESH = None
USE_CACHE = None
WARN_ERROR = None
TEST_NEW_PARSER = None
WRITE_JSON = None
PARTIAL_PARSE = None


def env_set_truthy(key: str) -> Optional[str]:
    """Return the value if it was set to a "truthy" string value, or None
    otherwise.
    """
    value = os.getenv(key)
    if not value or value.lower() in ('0', 'false', 'f'):
        return None
    return value


SINGLE_THREADED_WEBSERVER = env_set_truthy('DBT_SINGLE_THREADED_WEBSERVER')
SINGLE_THREADED_HANDLER = env_set_truthy('DBT_SINGLE_THREADED_HANDLER')
MACRO_DEBUGGING = env_set_truthy('DBT_MACRO_DEBUGGING')
DEFER_MODE = env_set_truthy('DBT_DEFER_TO_STATE')


def _get_context():
    # TODO: change this back to use fork() on linux when we have made that safe
    return multiprocessing.get_context('spawn')


MP_CONTEXT = _get_context()


def reset():
    global STRICT_MODE, FULL_REFRESH, USE_CACHE, WARN_ERROR, TEST_NEW_PARSER, \
        WRITE_JSON, PARTIAL_PARSE, MP_CONTEXT

    STRICT_MODE = False
    FULL_REFRESH = False
    USE_CACHE = True
    WARN_ERROR = False
    TEST_NEW_PARSER = False
    WRITE_JSON = True
    PARTIAL_PARSE = False
    MP_CONTEXT = _get_context()


def set_from_args(args):
    global STRICT_MODE, FULL_REFRESH, USE_CACHE, WARN_ERROR, TEST_NEW_PARSER, \
        WRITE_JSON, PARTIAL_PARSE, MP_CONTEXT

    USE_CACHE = getattr(args, 'use_cache', USE_CACHE)

    FULL_REFRESH = getattr(args, 'full_refresh', FULL_REFRESH)
    STRICT_MODE = getattr(args, 'strict', STRICT_MODE)
    WARN_ERROR = (
        STRICT_MODE or
        getattr(args, 'warn_error', STRICT_MODE or WARN_ERROR)
    )

    TEST_NEW_PARSER = getattr(args, 'test_new_parser', TEST_NEW_PARSER)
    WRITE_JSON = getattr(args, 'write_json', WRITE_JSON)
    PARTIAL_PARSE = getattr(args, 'partial_parse', None)
    MP_CONTEXT = _get_context()


# initialize everything to the defaults on module load
reset()
