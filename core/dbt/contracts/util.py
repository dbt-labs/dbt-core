from typing import Any, List, Tuple

# Leave imports of `Mergeable` to preserve import paths
from dbt_common.contracts.util import Mergeable  # noqa:F401
from dbt_common.dataclass_schema import ValidatedStringMixin, ValidationError

SourceKey = Tuple[str, str]


def list_str() -> List[str]:
    """Mypy gets upset about stuff like:

    from dataclasses import dataclass, field
    from typing import Optional, List

    @dataclass
    class Foo:
        x: Optional[List[str]] = field(default_factory=list)


    Because `list` could be any kind of list, I guess
    """
    return []


class Identifier(ValidatedStringMixin):
    """Our definition of a valid Identifier is the same as what's valid for an unquoted database table name.

    That is:
    1. It can contain a-z, A-Z, 0-9, and _
    1. It cannot start with a number
    """

    ValidationRegex = r"^[^\d\W]\w*$"

    @classmethod
    def is_valid(cls, value: Any) -> bool:
        if not isinstance(value, str):
            return False

        try:
            cls.validate(value)
        except ValidationError:
            return False

        return True
