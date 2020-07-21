from dataclasses import dataclass
from hologram import JsonSchemaMixin

from typing import List, Dict, Any, Union


@dataclass
class SelectorDefinition(JsonSchemaMixin):
    name: str
    definition: Union[str, Dict[str, Any]]


@dataclass
class SelectorFile(JsonSchemaMixin):
    selectors: List[SelectorDefinition]
    version: int = 2


# @dataclass
# class SelectorCollection:
#     packages: Dict[str, List[SelectorFile]] = field(default_factory=dict)
