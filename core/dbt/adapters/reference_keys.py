# this module exists to resolve circular imports with the events module

from collections import namedtuple
from typing import Any, Optional


_ReferenceKey = namedtuple("_ReferenceKey", "database schema identifier")


def lowercase(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    else:
        return value.lower()


# For backwards compatibility. New code should use _make_ref_key
def _make_key(relation: Any) -> _ReferenceKey:
    return _make_ref_key(relation)


def _make_ref_key(relation: Any) -> _ReferenceKey:
    """Make _ReferenceKeys with lowercase values for the cache so we don't have
    to keep track of quoting
    """
    # databases and schemas can both be None
    return _ReferenceKey(
        lowercase(relation.database), lowercase(relation.schema), lowercase(relation.identifier)
    )


def _make_ref_key_dict(relation: Any):
    return {
        "database": relation.database,
        "schema": relation.schema,
        "identifier": relation.identifier,
    }
