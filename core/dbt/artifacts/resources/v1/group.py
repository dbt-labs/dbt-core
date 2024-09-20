from dataclasses import dataclass
from typing import Literal

from dbt.artifacts.resources.base import BaseResource
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.owner import Owner


@dataclass
class Group(BaseResource):
    name: str
    owner: Owner
    resource_type: Literal[NodeType.Group]
