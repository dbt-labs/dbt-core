
from typing import NamedTuple, NoReturn, Union


# common trick for getting mypy to do exhaustiveness checks
# will come up with something like `"assert_never" has incompatible type`
# if something is missing.
def assert_never(x: NoReturn) -> NoReturn:
    raise AssertionError("Unhandled type: {}".format(type(x).__name__))

# The following classes represent the data necessary to describe a
# particular event to both human readable logs, and machine reliable
# event streams. The transformation to these forms will live in outside
# functions.
#
# Until we drop support for Python 3.6 we must use NamedTuples over
# frozen dataclasses.

# TODO dummy class
class OK(NamedTuple):
    result: int


# TODO dummy class
class Failure(NamedTuple):
    msg: str


# using a union instead of inheritance means that this set cannot
# be extended outside this file, and thus mypy can do exhaustiveness
# checks for us.
Event = Union[OK, Failure]


# function that translates any instance of the above event types
# into its human-readable message.
#
# This could instead be implemented as a method on an ABC for all
# above classes, but this at least puts all that logic in one place.
def humanMsg(r: Event) -> str:
    if isinstance(r, OK):
        return str(r.result)
    elif isinstance(r, Failure):
        return "Failure: " + r.msg
    else:
        assert_never(r)
