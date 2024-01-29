from dataclasses import dataclass
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.contracts.util import Replaceable
from typing import Optional


@dataclass
class Docs(dbtClassMixin, Replaceable):
    show: bool = True
    node_color: Optional[str] = None
