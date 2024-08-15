from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.components import CompiledResource
from dbt.artifacts.resources.v1.config import TestConfig
from dbt_common.dataclass_schema import dbtClassMixin


@dataclass
class TestMetadata(dbtClassMixin):
    __test__ = False

    name: str = "test"  # dummy default to allow default in GenericTestNode. Should always be set.
    # kwargs are the args that are left in the test builder after
    # removing configs. They are set from the test builder when
    # the test node is created.
    kwargs: Dict[str, Any] = field(default_factory=dict)
    namespace: Optional[str] = None


@dataclass
class GenericTest(CompiledResource):
    resource_type: Literal[NodeType.Test]
    column_name: Optional[str] = None
    file_key_name: Optional[str] = None
    # Was not able to make mypy happy and keep the code working. We need to
    # refactor the various configs.
    config: TestConfig = field(default_factory=TestConfig)  # type: ignore
    attached_node: Optional[str] = None
    test_metadata: TestMetadata = field(default_factory=TestMetadata)
