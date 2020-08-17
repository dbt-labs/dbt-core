from dataclasses import dataclass
from typing import (
    Type, Hashable, Optional, ContextManager, List, Generic, TypeVar, ClassVar,
    Tuple, Union, Dict, Any
)
from typing_extensions import Protocol

import agate

from dbt.contracts.connection import Connection, AdapterRequiredConfig
from dbt.contracts.graph.compiled import (
    CompiledNode, NonSourceNode, NonSourceCompiledNode
)
from dbt.contracts.graph.parsed import ParsedNode, ParsedSourceDefinition
from dbt.contracts.graph.model_config import BaseConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.relation import Policy, HasQuoting

from dbt.graph import Graph


@dataclass
class AdapterConfig(BaseConfig):
    pass


class ConnectionManagerProtocol(Protocol):
    TYPE: str


class ColumnProtocol(Protocol):
    pass


Self = TypeVar('Self', bound='RelationProtocol')


class RelationProtocol(Protocol):
    @classmethod
    def get_default_quote_policy(cls) -> Policy:
        ...

    @classmethod
    def create_from(
        cls: Type[Self],
        config: HasQuoting,
        node: Union[CompiledNode, ParsedNode, ParsedSourceDefinition],
    ) -> Self:
        ...


class CompilerProtocol(Protocol):
    def compile(self, manifest: Manifest, write=True) -> Graph:
        ...

    def compile_node(
        self,
        node: NonSourceNode,
        manifest: Manifest,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> NonSourceCompiledNode:
        ...


AdapterConfig_T = TypeVar(
    'AdapterConfig_T', bound=AdapterConfig
)
ConnectionManager_T = TypeVar(
    'ConnectionManager_T', bound=ConnectionManagerProtocol
)
Relation_T = TypeVar(
    'Relation_T', bound=RelationProtocol
)
Column_T = TypeVar(
    'Column_T', bound=ColumnProtocol
)
Compiler_T = TypeVar('Compiler_T', bound=CompilerProtocol)


class AdapterProtocol(
    Protocol,
    Generic[
        AdapterConfig_T,
        ConnectionManager_T,
        Relation_T,
        Column_T,
        Compiler_T,
    ]
):
    AdapterSpecificConfigs: ClassVar[Type[AdapterConfig_T]]
    Column: ClassVar[Type[Column_T]]
    Relation: ClassVar[Type[Relation_T]]
    ConnectionManager: ClassVar[Type[ConnectionManager_T]]
    connections: ConnectionManager_T

    def __init__(self, config: AdapterRequiredConfig):
        ...

    @classmethod
    def type(cls) -> str:
        pass

    def set_query_header(self, manifest: Manifest) -> None:
        ...

    @staticmethod
    def get_thread_identifier() -> Hashable:
        ...

    def get_thread_connection(self) -> Connection:
        ...

    def set_thread_connection(self, conn: Connection) -> None:
        ...

    def get_if_exists(self) -> Optional[Connection]:
        ...

    def clear_thread_connection(self) -> None:
        ...

    def clear_transaction(self) -> None:
        ...

    def exception_handler(self, sql: str) -> ContextManager:
        ...

    def set_connection_name(self, name: Optional[str] = None) -> Connection:
        ...

    def cancel_open(self) -> Optional[List[str]]:
        ...

    def open(cls, connection: Connection) -> Connection:
        ...

    def release(self) -> None:
        ...

    def cleanup_all(self) -> None:
        ...

    def begin(self) -> None:
        ...

    def commit(self) -> None:
        ...

    def close(cls, connection: Connection) -> Connection:
        ...

    def commit_if_has_connection(self) -> None:
        ...

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[str, agate.Table]:
        ...

    def get_compiler(self) -> Compiler_T:
        ...
