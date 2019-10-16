from dataclasses import dataclass
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


@dataclass
class RPCNoParameters(RPCParameters):
    pass


# Outputs

@dataclass
class RemoteResult(JsonSchemaMixin):
    logs: List[LogMessage]


@dataclass
class RemoteEmptyResult(RemoteResult):
    pass


@dataclass
class RemoteCatalogResults(CatalogResults, RemoteResult):
    pass


@dataclass
class RemoteCompileResult(RemoteResult):
    raw_sql: str
    compiled_sql: str
    node: CompileResultNode
    timing: List[TimingInfo]

    @property
    def error(self):
        return None


@dataclass
class RemoteExecutionResult(ExecutionResult, RemoteResult):
    pass


@dataclass
class ResultTable(JsonSchemaMixin):
    column_names: List[str]
    rows: List[Any]


@dataclass
class RemoteRunResult(RemoteCompileResult):
    table: ResultTable


RPCResult = Union[
    RemoteCompileResult,
    RemoteExecutionResult,
    RemoteCatalogResults,
    RemoteEmptyResult,
]
