from dataclasses import dataclass, field
from numbers import Real
from typing import Optional, Union, List, Any, Dict

from hologram import JsonSchemaMixin

from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.results import (
    TimingInfo,
    CatalogResults,
    ExecutionResult,
)
from dbt.logger import LogMessage

# Inputs


@dataclass
class RPCParameters(JsonSchemaMixin):
    timeout: Optional[Real]
    task_tags: Optional[Dict[str, Any]]


@dataclass
class RPCExecParameters(RPCParameters):
    name: str
    sql: str
    macros: Optional[str]


@dataclass
class RPCCompileParameters(RPCParameters):
    models: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None


@dataclass
class RPCTestParameters(RPCCompileParameters):
    data: bool = False
    schema: bool = False


@dataclass
class RPCSeedParameters(RPCParameters):
    show: bool = False


@dataclass
class RPCDocsGenerateParameters(RPCParameters):
    compile: bool = True


@dataclass
class RPCCliParameters(RPCParameters):
    cli: str


# Outputs


@dataclass
class RemoteCatalogResults(CatalogResults):
    logs: List[LogMessage] = field(default_factory=list)


@dataclass
class RemoteCompileResult(JsonSchemaMixin):
    raw_sql: str
    compiled_sql: str
    node: CompileResultNode
    timing: List[TimingInfo]
    logs: List[LogMessage]

    @property
    def error(self):
        return None


@dataclass
class RemoteExecutionResult(ExecutionResult):
    logs: List[LogMessage]


@dataclass
class ResultTable(JsonSchemaMixin):
    column_names: List[str]
    rows: List[Any]


@dataclass
class RemoteRunResult(RemoteCompileResult):
    table: ResultTable
