from dataclasses import dataclass, field
from typing import Any, Dict, Literal

from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.components import CompiledResource
from dbt.artifacts.resources.v1.config import TestConfig


@dataclass
class SingularTest(CompiledResource):
    resource_type: Literal[NodeType.Test]
    # Was not able to make mypy happy and keep the code working. We need to
    # refactor the various configs.
    config: TestConfig = field(default_factory=TestConfig)  # type: ignore

    @classmethod
    def __pre_deserialize__(cls, d: Dict[str, Any]) -> Dict[str, Any]:
        # SingularTest and GenericTest share resource_type=NodeType.Test, so
        # mashumaro's Union resolution can't tell them apart by type alone.
        # Reject dicts that carry GenericTest-only fields so resolution falls
        # through to GenericTest. Without this, every test deserializes as
        # SingularTest and loses test_metadata silently.
        if "test_metadata" in d or "column_name" in d or "attached_node" in d:
            raise ValueError("Not a SingularTest payload (carries generic-test fields).")
        return d
