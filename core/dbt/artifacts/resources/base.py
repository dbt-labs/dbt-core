from dataclasses import dataclass
from dbt_common.dataclass_schema import dbtClassMixin
from typing import List

from dbt.artifacts.resources.types import NodeType


@dataclass
class BaseResource(dbtClassMixin):
    name: str
    resource_type: NodeType
    package_name: str
    path: str
    original_file_path: str
    unique_id: str


@dataclass
class GraphResource(BaseResource):
    fqn: List[str]
