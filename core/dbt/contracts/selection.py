from dataclasses import dataclass
from typing import Any, Dict, List, Union

from dbt_common.dataclass_schema import dbtClassMixin


@dataclass
class SelectorDefinition(dbtClassMixin):
    name: str
    definition: Union[str, Dict[str, Any]]
    description: str = ""
    default: bool = False


@dataclass
class SelectorFile(dbtClassMixin):
    selectors: List[SelectorDefinition]
    version: int = 2


# @dataclass
# class SelectorCollection:
#     packages: Dict[str, List[SelectorFile]] = field(default_factory=dict)
