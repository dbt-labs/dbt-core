import functools
from typing import Optional

from dbt.events.functions import warn_or_error
from dbt.events.types import ClassDeprecated, FunctionDeprecated


def deprecated_func(suggested_action: str, version: str, reason: Optional[str]):
    def inner(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            function_name = func.__name__

            warn_or_error(
                FunctionDeprecated(
                    function_name=function_name,
                    suggested_action=suggested_action,
                    version=version,
                    reason=reason,
                )
            )  # TODO: pass in event?
            return func(*args, **kwargs)

        return wrapped

    return inner


def deprecated_class(suggested_action: str, version: str, reason: Optional[str]):
    def inner(cls):
        class Wrapped(cls):
            pass

        class_name = cls.__name__

        warn_or_error(
            ClassDeprecated(
                class_name=class_name,
                suggested_action=suggested_action,
                version=version,
                reason=reason,
            )
        )  # TODO: pass in event?
        return Wrapped

    return inner
