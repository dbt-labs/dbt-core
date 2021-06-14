from dbt.contracts.graph.parsed import (
    HasTestMetadata,
    ParsedNode,
    ParsedAnalysisNode,
    ParsedDataTestNode,
    ParsedHookNode,
    ParsedModelNode,
    ParsedExposure,
    ParsedResource,
    ParsedRPCNode,
    ParsedSchemaTestNode,
    ParsedSeedNode,
    ParsedSnapshotNode,
    ParsedSourceDefinition,
    SeedConfig,
    TestConfig,
    same_seeds,
)
from dbt.node_types import NodeType
from dbt.contracts.util import Replaceable

from dbt.dataclass_schema import dbtClassMixin
from dataclasses import dataclass, field
from typing import Optional, List, Union, Dict, Type


@dataclass
class InjectedCTE(dbtClassMixin, Replaceable):
    id: str
    sql: str


@dataclass
class CompiledNodeMixin(dbtClassMixin):
    # this is a special mixin class to provide a required argument. If a node
    # is missing a `compiled` flag entirely, it must not be a CompiledNode.
    compiled: bool


@dataclass
class CompiledNode(ParsedNode, CompiledNodeMixin):
    compiled_sql: Optional[str] = None
    extra_ctes_injected: bool = False
    extra_ctes: List[InjectedCTE] = field(default_factory=list)
    relation_name: Optional[str] = None
    _pre_injected_sql: Optional[str] = None

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

    def __post_serialize__(self, dct):
        dct = super().__post_serialize__(dct)
        if '_pre_injected_sql' in dct:
            del dct['_pre_injected_sql']
        return dct


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
    # keep this in sync with ParsedSeedNode!
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Seed]})
    config: SeedConfig = field(default_factory=SeedConfig)

    @property
    def empty(self):
        """ Seeds are never empty"""
        return False

    def same_body(self, other) -> bool:
        return same_seeds(self, other)


@dataclass
class CompiledSnapshotNode(CompiledNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Snapshot]})


@dataclass
class CompiledDataTestNode(CompiledNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Test]})
    config: TestConfig = field(default_factory=TestConfig)


@dataclass
class CompiledSchemaTestNode(CompiledNode, HasTestMetadata):
    # keep this in sync with ParsedSchemaTestNode!
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Test]})
    column_name: Optional[str] = None
    config: TestConfig = field(default_factory=TestConfig)

    def same_contents(self, other) -> bool:
        if other is None:
            return False

        return (
            self.same_config(other) and
            self.same_fqn(other) and
            True
        )


CompiledTestNode = Union[CompiledDataTestNode, CompiledSchemaTestNode]


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

    return cls.from_dict(compiled.to_dict(omit_none=True))


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
ManifestNode = Union[
    NonSourceCompiledNode,
    NonSourceParsedNode,
]

# We allow either parsed or compiled nodes, or parsed sources, as some
# 'compile()' calls in the runner actually just return the original parsed
# node they were given.
CompileResultNode = Union[
    ManifestNode,
    ParsedSourceDefinition,
]

# anything that participates in the graph: sources, exposures, manifest nodes
GraphMemberNode = Union[
    CompileResultNode,
    ParsedExposure,
]
