# initially all flags are set to None, the on-load call of reset() will set
# them for their first time.
STRICT_MODE = None
FULL_REFRESH = None
USE_CACHE = None
WARN_ERROR = None
TEST_NEW_PARSER = None
WRITE_JSON = None
PARTIAL_PARSE = None


def reset():
    global STRICT_MODE, FULL_REFRESH, USE_CACHE, WARN_ERROR, TEST_NEW_PARSER, \
        WRITE_JSON, PARTIAL_PARSE

    STRICT_MODE = False
    FULL_REFRESH = False
    USE_CACHE = True
    WARN_ERROR = False
    TEST_NEW_PARSER = False
    WRITE_JSON = True
    PARTIAL_PARSE = False


def set_from_args(args):
    global STRICT_MODE, FULL_REFRESH, USE_CACHE, WARN_ERROR, TEST_NEW_PARSER, \
        WRITE_JSON, PARTIAL_PARSE
    USE_CACHE = getattr(args, 'use_cache', USE_CACHE)

    FULL_REFRESH = getattr(args, 'full_refresh', FULL_REFRESH)
    STRICT_MODE = getattr(args, 'strict', STRICT_MODE)
    WARN_ERROR = (
        STRICT_MODE or
        getattr(args, 'warn_error', STRICT_MODE or WARN_ERROR)
    )

    TEST_NEW_PARSER = getattr(args, 'test_new_parser', TEST_NEW_PARSER)
    WRITE_JSON = getattr(args, 'write_json', WRITE_JSON)
    PARTIAL_PARSE = getattr(args, 'partial_parse', PARTIAL_PARSE)


# initialize everything to the defaults on module load
reset()
