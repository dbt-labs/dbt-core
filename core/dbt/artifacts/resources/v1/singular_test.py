from dataclasses import dataclass, field
from typing import Literal
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.components import CompiledResource
from dbt.artifacts.resources.v1.config import TestConfig


@dataclass
class SingularTest(CompiledResource):
    resource_type: Literal[NodeType.Test]
    # Was not able to make mypy happy and keep the code working. We need to
    # refactor the various configs.
    config: TestConfig = field(default_factory=TestConfig)  # type: ignore
