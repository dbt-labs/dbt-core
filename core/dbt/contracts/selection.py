from dataclasses import dataclass
from dbt.dataclass_schema import dbtClassMixin

from typing import List, Dict, Any, Union


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
