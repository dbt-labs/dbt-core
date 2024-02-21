from dataclasses import dataclass, field
import time
from typing import Literal, List, Dict, Optional, Any

from dbt_common.dataclass_schema import dbtClassMixin
from dbt.artifacts.resources.base import BaseResource, Docs
from dbt.artifacts.resources.types import NodeType, ModelLanguage
from dbt.artifacts.resources.v1.components import MacroDependsOn


@dataclass
class MacroArgument(dbtClassMixin):
    name: str
    type: Optional[str] = None
    description: str = ""


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
