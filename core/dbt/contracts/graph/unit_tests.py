from dbt.dataclass_schema import dbtClassMixin
from dataclasses import dataclass, field
from typing import List, Any, Dict, Sequence, Optional


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
class UnitTestSuite(dbtClassMixin):
    model: str  # name of the model being unit tested
    tests: Sequence[UnitTestCase]
