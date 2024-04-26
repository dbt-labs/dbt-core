from dataclasses import dataclass, field
import time
from typing import Optional, Sequence, Dict, Any, List, Union

from dbt_common.contracts.config.base import (
    BaseConfig,
    CompareBehavior,
    MergeBehavior,
)
from dbt_common.contracts.config.metadata import ShowBehavior
from dbt_common.dataclass_schema import dbtClassMixin, StrEnum

from dbt.artifacts.resources.v1.config import metas, list_str
from dbt.artifacts.resources.base import GraphResource
from dbt.artifacts.resources import NodeVersion, DependsOn


@dataclass
class UnitTestConfig(BaseConfig):
    tags: Union[str, List[str]] = field(
        default_factory=list_str,
        metadata=metas(ShowBehavior.Hide, MergeBehavior.Append, CompareBehavior.Exclude),
    )
    meta: Dict[str, Any] = field(
        default_factory=dict,
        metadata=MergeBehavior.Update.meta(),
    )


class UnitTestFormat(StrEnum):
    CSV = "csv"
    Dict = "dict"
    SQL = "sql"


@dataclass
class UnitTestInputFixture(dbtClassMixin):
    input: str
    rows: Optional[Union[str, List[Dict[str, Any]]]] = None
    format: UnitTestFormat = UnitTestFormat.Dict
    fixture: Optional[str] = None


@dataclass
class UnitTestOverrides(dbtClassMixin):
    macros: Dict[str, Any] = field(default_factory=dict)
    vars: Dict[str, Any] = field(default_factory=dict)
    env_vars: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UnitTestNodeVersions(dbtClassMixin):
    include: Optional[List[NodeVersion]] = None
    exclude: Optional[List[NodeVersion]] = None


@dataclass
class UnitTestOutputFixture(dbtClassMixin):
    rows: Optional[Union[str, List[Dict[str, Any]]]] = None
    format: UnitTestFormat = UnitTestFormat.Dict
    fixture: Optional[str] = None


@dataclass
class UnitTestDefinitionMandatory:
    model: str
    given: Sequence[UnitTestInputFixture]
    expect: UnitTestOutputFixture


@dataclass
class UnitTestDefinition(GraphResource, UnitTestDefinitionMandatory):
    description: str = ""
    overrides: Optional[UnitTestOverrides] = None
    depends_on: DependsOn = field(default_factory=DependsOn)
    config: UnitTestConfig = field(default_factory=UnitTestConfig)
    checksum: Optional[str] = None
    schema: Optional[str] = None
    created_at: float = field(default_factory=lambda: time.time())
    versions: Optional[UnitTestNodeVersions] = None
    version: Optional[NodeVersion] = None
