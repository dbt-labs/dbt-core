from dataclasses import dataclass
from dbt.common.dataclass_schema import dbtClassMixin
from dbt.common.contracts.util import Replaceable

from dbt.artifacts.nodes.types import NodeType


@dataclass
class BaseArtifactNode(dbtClassMixin, Replaceable):
    name: str
    resource_type: NodeType
    package_name: str
    path: str
    original_file_path: str
    unique_id: str
