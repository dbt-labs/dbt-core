STRICT_MODE = False
FULL_REFRESH = False
USE_CACHE = True
WARN_ERROR = False
TEST_NEW_PARSER = False


def reset():
    global STRICT_MODE, FULL_REFRESH, USE_CACHE, WARN_ERROR, TEST_NEW_PARSER

    STRICT_MODE = False
    FULL_REFRESH = False
    USE_CACHE = True
    WARN_ERROR = False
    TEST_NEW_PARSER = False


def set_from_args(args):
    global STRICT_MODE, FULL_REFRESH, USE_CACHE, WARN_ERROR, TEST_NEW_PARSER
    USE_CACHE = getattr(args, 'use_cache', True)

    FULL_REFRESH = getattr(args, 'full_refresh', False)
    STRICT_MODE = getattr(args, 'strict', False)
    WARN_ERROR = (
        STRICT_MODE or
        getattr(args, 'warn_error', False)
    )

    TEST_NEW_PARSER = getattr(args, 'test_new_parser', False)
