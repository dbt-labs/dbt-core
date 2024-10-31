import functools
from typing import Optional

from dbt.events.types import InternalDeprecation
from dbt_common.events.functions import warn_or_error


def deprecated(suggested_action: str, version: str, reason: Optional[str]):
    def inner(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            name = func.__name__

            warn_or_error(
                InternalDeprecation(
                    name=name,
                    suggested_action=suggested_action,
                    version=version,
                    reason=reason,
                )
            )  # TODO: pass in event?
            return func(*args, **kwargs)

        return wrapped

    return inner
