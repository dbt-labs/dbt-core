# this module exists to resolve circular imports with the events module

from collections import namedtuple
from typing import Optional


_ReferenceKey = namedtuple('_ReferenceKey', 'database schema identifier')


def lowercase(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    else:
        return value.lower()


def _make_key(relation) -> _ReferenceKey:
    """Make _ReferenceKeys with lowercase values for the cache so we don't have
    to keep track of quoting
    """
    # databases and schemas can both be None
    return _ReferenceKey(lowercase(relation.database),
                         lowercase(relation.schema),
                         lowercase(relation.identifier))
