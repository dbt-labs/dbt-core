from dbt.dataclass_schema import dbtClassMixin
from dataclasses import dataclass, field
from typing import List, Any, Dict, Sequence, Optional
from dbt.node_types import NodeType


@dataclass
class InputFixture(dbtClassMixin):
    input: str
    rows: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class UnitTestOverrides(dbtClassMixin):
    macros: Dict[str, Any] = field(default_factory=dict)
    vars: Dict[str, Any] = field(default_factory=dict)
    env_vars: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UnitTestCase(dbtClassMixin):
    name: str
    given: Sequence[InputFixture]
    expect: List[Dict[str, Any]]
    description: str = ""
    overrides: Optional[UnitTestOverrides] = None


@dataclass
class UnparsedUnitTestSuite(dbtClassMixin):
    model: str  # name of the model being unit tested
    tests: Sequence[UnitTestCase]


@dataclass
class UnitTestSuite(dbtClassMixin):
    model: str
    tests: Sequence[UnitTestCase]
    name: str
    resource_type: NodeType
    package_name: str
    path: str
    original_file_path: str
    unique_id: str
