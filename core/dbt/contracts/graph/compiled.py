from dbt.contracts.graph.parsed import (
    HasTestMetadata,
    ParsedNode,
    ParsedAnalysisNode,
    ParsedDataTestNode,
    ParsedHookNode,
    ParsedModelNode,
    ParsedResource,
    ParsedRPCNode,
    ParsedSchemaTestNode,
    ParsedSeedNode,
    ParsedSnapshotNode,
    ParsedSourceDefinition,
    SeedConfig,
    TestConfig,
)
from dbt.node_types import NodeType
from dbt.contracts.util import Replaceable
from dbt.exceptions import RuntimeException

from hologram import JsonSchemaMixin
from dataclasses import dataclass, field
import sqlparse  # type: ignore
from typing import Optional, List, Union, Dict, Type


@dataclass
class InjectedCTE(JsonSchemaMixin, Replaceable):
    id: str
    sql: str


@dataclass
class CompiledNodeMixin(JsonSchemaMixin):
    # this is a special mixin class to provide a required argument. If a node
    # is missing a `compiled` flag entirely, it must not be a CompiledNode.
    compiled: bool


@dataclass
class CompiledNode(ParsedNode, CompiledNodeMixin):
    compiled_sql: Optional[str] = None
    extra_ctes_injected: bool = False
    extra_ctes: List[InjectedCTE] = field(default_factory=list)
    injected_sql: Optional[str] = None

    def prepend_ctes(self, prepended_ctes: List[InjectedCTE]):
        self.extra_ctes_injected = True
        self.extra_ctes = prepended_ctes
        if self.compiled_sql is None:
            raise RuntimeException(
                'Cannot prepend ctes to an unparsed node', self
            )
        self.injected_sql = _inject_ctes_into_sql(
            self.compiled_sql,
            prepended_ctes,
        )
        self.validate(self.to_dict())

    def set_cte(self, cte_id: str, sql: str):
        """This is the equivalent of what self.extra_ctes[cte_id] = sql would
        do if extra_ctes were an OrderedDict
        """
        for cte in self.extra_ctes:
            if cte.id == cte_id:
                cte.sql = sql
                break
        else:
            self.extra_ctes.append(InjectedCTE(id=cte_id, sql=sql))


@dataclass
class CompiledAnalysisNode(CompiledNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Analysis]})


@dataclass
class CompiledHookNode(CompiledNode):
    resource_type: NodeType = field(
        metadata={'restrict': [NodeType.Operation]}
    )
    index: Optional[int] = None


@dataclass
class CompiledModelNode(CompiledNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Model]})


@dataclass
class CompiledRPCNode(CompiledNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.RPCCall]})


@dataclass
class CompiledSeedNode(CompiledNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Seed]})
    config: SeedConfig = field(default_factory=SeedConfig)

    @property
    def empty(self):
        """ Seeds are never empty"""
        return False


@dataclass
class CompiledSnapshotNode(CompiledNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Snapshot]})


@dataclass
class CompiledDataTestNode(CompiledNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Test]})
    config: TestConfig = field(default_factory=TestConfig)


@dataclass
class CompiledSchemaTestNode(CompiledNode, HasTestMetadata):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Test]})
    column_name: Optional[str] = None
    config: TestConfig = field(default_factory=TestConfig)


CompiledTestNode = Union[CompiledDataTestNode, CompiledSchemaTestNode]


def _inject_ctes_into_sql(sql: str, ctes: List[InjectedCTE]) -> str:
    """
    `ctes` is a list of InjectedCTEs like:

        [
            InjectedCTE(
                id="cte_id_1",
                sql="__dbt__CTE__ephemeral as (select * from table)",
            ),
            InjectedCTE(
                id="cte_id_2",
                sql="__dbt__CTE__events as (select id, type from events)",
            ),
        ]

    Given `sql` like:

      "with internal_cte as (select * from sessions)
       select * from internal_cte"

    This will spit out:

      "with __dbt__CTE__ephemeral as (select * from table),
            __dbt__CTE__events as (select id, type from events),
            with internal_cte as (select * from sessions)
       select * from internal_cte"

    (Whitespace enhanced for readability.)
    """
    if len(ctes) == 0:
        return sql

    parsed_stmts = sqlparse.parse(sql)
    parsed = parsed_stmts[0]

    with_stmt = None
    for token in parsed.tokens:
        if token.is_keyword and token.normalized == 'WITH':
            with_stmt = token
            break

    if with_stmt is None:
        # no with stmt, add one, and inject CTEs right at the beginning
        first_token = parsed.token_first()
        with_stmt = sqlparse.sql.Token(sqlparse.tokens.Keyword, 'with')
        parsed.insert_before(first_token, with_stmt)
    else:
        # stmt exists, add a comma (which will come after injected CTEs)
        trailing_comma = sqlparse.sql.Token(sqlparse.tokens.Punctuation, ',')
        parsed.insert_after(with_stmt, trailing_comma)

    token = sqlparse.sql.Token(
        sqlparse.tokens.Keyword,
        ", ".join(c.sql for c in ctes)
    )
    parsed.insert_after(with_stmt, token)

    return str(parsed)


PARSED_TYPES: Dict[Type[CompiledNode], Type[ParsedResource]] = {
    CompiledAnalysisNode: ParsedAnalysisNode,
    CompiledModelNode: ParsedModelNode,
    CompiledHookNode: ParsedHookNode,
    CompiledRPCNode: ParsedRPCNode,
    CompiledSeedNode: ParsedSeedNode,
    CompiledSnapshotNode: ParsedSnapshotNode,
    CompiledDataTestNode: ParsedDataTestNode,
    CompiledSchemaTestNode: ParsedSchemaTestNode,
}


COMPILED_TYPES: Dict[Type[ParsedResource], Type[CompiledNode]] = {
    ParsedAnalysisNode: CompiledAnalysisNode,
    ParsedModelNode: CompiledModelNode,
    ParsedHookNode: CompiledHookNode,
    ParsedRPCNode: CompiledRPCNode,
    ParsedSeedNode: CompiledSeedNode,
    ParsedSnapshotNode: CompiledSnapshotNode,
    ParsedDataTestNode: CompiledDataTestNode,
    ParsedSchemaTestNode: CompiledSchemaTestNode,
}


# for some types, the compiled type is the parsed type, so make this easy
CompiledType = Union[Type[CompiledNode], Type[ParsedResource]]
CompiledResource = Union[ParsedResource, CompiledNode]


def compiled_type_for(parsed: ParsedNode) -> CompiledType:
    if type(parsed) in COMPILED_TYPES:
        return COMPILED_TYPES[type(parsed)]
    else:
        return type(parsed)


def parsed_instance_for(compiled: CompiledNode) -> ParsedResource:
    cls = PARSED_TYPES.get(type(compiled))
    if cls is None:
        # how???
        raise ValueError('invalid resource_type: {}'
                         .format(compiled.resource_type))

    # validate=False to allow extra keys from compiling
    return cls.from_dict(compiled.to_dict(), validate=False)


NonSourceCompiledNode = Union[
    CompiledAnalysisNode,
    CompiledDataTestNode,
    CompiledModelNode,
    CompiledHookNode,
    CompiledRPCNode,
    CompiledSchemaTestNode,
    CompiledSeedNode,
    CompiledSnapshotNode,
]

NonSourceParsedNode = Union[
    ParsedAnalysisNode,
    ParsedDataTestNode,
    ParsedHookNode,
    ParsedModelNode,
    ParsedRPCNode,
    ParsedSchemaTestNode,
    ParsedSeedNode,
    ParsedSnapshotNode,
]


# This is anything that can be in manifest.nodes.
NonSourceNode = Union[
    NonSourceCompiledNode,
    NonSourceParsedNode,
]

# We allow either parsed or compiled nodes, or parsed sources, as some
# 'compile()' calls in the runner actually just return the original parsed
# node they were given.
CompileResultNode = Union[
    NonSourceNode,
    ParsedSourceDefinition,
]
