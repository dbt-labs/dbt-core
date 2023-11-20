from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List

from dbt.common.dataclass_schema import dbtClassMixin


class ConstraintType(str, Enum):
    check = "check"
    not_null = "not_null"
    unique = "unique"
    primary_key = "primary_key"
    foreign_key = "foreign_key"
    custom = "custom"

    @classmethod
    def is_valid(cls, item) -> bool:
        try:
            cls(item)
        except ValueError:
            return False
        return True


@dataclass
class ColumnLevelConstraint(dbtClassMixin):
    type: ConstraintType
    name: Optional[str] = None
    # expression is a user-provided field that will depend on the constraint type.
    # It could be a predicate (check type), or a sequence sql keywords (e.g. unique type),
    # so the vague naming of 'expression' is intended to capture this range.
    expression: Optional[str] = None
    warn_unenforced: bool = (
        True  # Warn if constraint cannot be enforced by platform but will be in DDL
    )
    warn_unsupported: bool = (
        True  # Warn if constraint is not supported by the platform and won't be in DDL
    )


@dataclass
class ModelLevelConstraint(ColumnLevelConstraint):
    columns: List[str] = field(default_factory=list)
