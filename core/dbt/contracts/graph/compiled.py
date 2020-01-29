from dbt.contracts.graph.parsed import (
    ParsedNode,
    ParsedAnalysisNode,
    ParsedModelNode,
    ParsedHookNode,
    ParsedResource,
    ParsedRPCNode,
    ParsedSeedNode,
    ParsedSnapshotNode,
    ParsedSourceDefinition,
    ParsedTestNode,
    SeedConfig,
    TestConfig,
    TestMetadata,
    PARSED_TYPES,
)
from dbt.node_types import NodeType
from dbt.contracts.util import Replaceable
from dbt.exceptions import InternalException, RuntimeException

from hologram import JsonSchemaMixin
from dataclasses import dataclass, field
import sqlparse  # type: ignore
from typing import Optional, List, Union, Dict, Type


@dataclass
class InjectedCTE(JsonSchemaMixin, Replaceable):
    id: str
    sql: str

# for some frustrating reason, we can't subclass from ParsedNode directly,
# or typing.Union will flatten CompiledNode+ParsedNode into just ParsedNode.
# TODO: understand that issue and come up with some way for these two to share
# logic


@dataclass
class CompiledNode(ParsedNode):
    compiled: bool = False
    compiled_sql: Optional[str] = None
    extra_ctes_injected: bool = False
    extra_ctes: List[InjectedCTE] = field(default_factory=list)
    injected_sql: Optional[str] = None
    wrapped_sql: Optional[str] = None

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
    seed_file_path: str = ''

    def __post_init__(self):
        if self.seed_file_path == '':
            raise InternalException(
                'Seeds should always have a seed_file_path'
            )

    @property
    def empty(self):
        """ Seeds are never empty"""
        return False


@dataclass
class CompiledSnapshotNode(CompiledNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Snapshot]})


@dataclass
class CompiledTestNode(CompiledNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Test]})
    column_name: Optional[str] = None
    config: TestConfig = field(default_factory=TestConfig)
    test_metadata: Optional[TestMetadata] = None


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


COMPILED_TYPES: Dict[NodeType, Type[CompiledNode]] = {
    NodeType.Analysis: CompiledAnalysisNode,
    NodeType.Model: CompiledModelNode,
    NodeType.Operation: CompiledHookNode,
    NodeType.RPCCall: CompiledRPCNode,
    NodeType.Seed: CompiledSeedNode,
    NodeType.Snapshot: CompiledSnapshotNode,
    NodeType.Test: CompiledTestNode,
}


def compiled_type_for(parsed: ParsedNode):
    if parsed.resource_type in COMPILED_TYPES:
        return COMPILED_TYPES[parsed.resource_type]
    else:
        return type(parsed)


def parsed_instance_for(compiled: CompiledNode) -> ParsedResource:
    cls = PARSED_TYPES.get(compiled.resource_type)
    if cls is None:
        # how???
        raise ValueError('invalid resource_type: {}'
                         .format(compiled.resource_type))

    # validate=False to allow extra keys from compiling
    return cls.from_dict(compiled.to_dict(), validate=False)


# This is anything that can be in manifest.nodes and isn't a Source.
NonSourceNode = Union[
    CompiledAnalysisNode,
    CompiledModelNode,
    CompiledHookNode,
    CompiledRPCNode,
    CompiledSeedNode,
    CompiledSnapshotNode,
    CompiledTestNode,
    ParsedAnalysisNode,
    ParsedModelNode,
    ParsedHookNode,
    ParsedRPCNode,
    ParsedSeedNode,
    ParsedSnapshotNode,
    ParsedTestNode,
]

# We allow either parsed or compiled nodes, or parsed sources, as some
# 'compile()' calls in the runner actually just return the original parsed
# node they were given.
CompileResultNode = Union[
    NonSourceNode,
    ParsedSourceDefinition,
]
