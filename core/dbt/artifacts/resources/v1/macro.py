from dataclasses import dataclass, field
import time
from typing import Literal, List, Dict, Optional, Any

from dbt_common.dataclass_schema import dbtClassMixin
from dbt.artifacts.resources.base import BaseResource
from dbt.artifacts.resources.types import NodeType, ModelLanguage
from dbt.artifacts.resources.v1.docs import Docs


@dataclass
class MacroArgument(dbtClassMixin):
    name: str
    type: Optional[str] = None
    description: str = ""


@dataclass
class MacroDependsOn(dbtClassMixin):
    macros: List[str] = field(default_factory=list)

    # 'in' on lists is O(n) so this is O(n^2) for # of macros
    def add_macro(self, value: str):
        if value not in self.macros:
            self.macros.append(value)


@dataclass
class Macro(BaseResource):
    macro_sql: str
    resource_type: Literal[NodeType.Macro]
    depends_on: MacroDependsOn = field(default_factory=MacroDependsOn)
    description: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    docs: Docs = field(default_factory=Docs)
    patch_path: Optional[str] = None
    arguments: List[MacroArgument] = field(default_factory=list)
    created_at: float = field(default_factory=lambda: time.time())
    supported_languages: Optional[List[ModelLanguage]] = None
