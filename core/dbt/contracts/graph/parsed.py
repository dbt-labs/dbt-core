import os
from dataclasses import dataclass, field
from typing import (
    Optional,
    Union,
    List,
    Dict,
    Any,
    Type,
    Tuple,
)

from hologram import JsonSchemaMixin

from dbt.clients.system import write_file
import dbt.flags
from dbt.contracts.graph.unparsed import (
    UnparsedNode, UnparsedDocumentation, Quoting, Docs,
    UnparsedBaseNode, FreshnessThreshold, ExternalTable,
    HasYamlMetadata, MacroArgument
)
from dbt.contracts.util import Replaceable
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.node_types import NodeType


from .model_config import (
    NodeConfig,
    SeedConfig,
    TestConfig,
    CheckSnapshotConfig,
    TimestampSnapshotConfig,
    GenericSnapshotConfig,

    # utility types
    SnapshotStrategy,
)

# FIXME: exports
from .model_config import Hook  # noqa


@dataclass
class ColumnInfo(JsonSchemaMixin, Replaceable):
    name: str
    description: str = ''
    meta: Dict[str, Any] = field(default_factory=dict)
    data_type: Optional[str] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class HasFqn(JsonSchemaMixin, Replaceable):
    fqn: List[str]


@dataclass
class HasUniqueID(JsonSchemaMixin, Replaceable):
    unique_id: str


@dataclass
class MacroDependsOn(JsonSchemaMixin, Replaceable):
    macros: List[str] = field(default_factory=list)

    # 'in' on lists is O(n) so this is O(n^2) for # of macros
    def add_macro(self, value: str):
        if value not in self.macros:
            self.macros.append(value)


@dataclass
class DependsOn(MacroDependsOn):
    nodes: List[str] = field(default_factory=list)

    def add_node(self, value: str):
        if value not in self.nodes:
            self.nodes.append(value)


@dataclass
class HasRelationMetadata(JsonSchemaMixin, Replaceable):
    database: str
    schema: str


class ParsedNodeMixins(JsonSchemaMixin):
    resource_type: NodeType
    depends_on: DependsOn
    config: NodeConfig

    @property
    def is_refable(self):
        return self.resource_type in NodeType.refable()

    @property
    def is_ephemeral(self):
        return self.config.materialized == 'ephemeral'

    @property
    def is_ephemeral_model(self):
        return self.is_refable and self.is_ephemeral

    @property
    def depends_on_nodes(self):
        return self.depends_on.nodes

    def patch(self, patch: 'ParsedNodePatch'):
        """Given a ParsedNodePatch, add the new information to the node."""
        # explicitly pick out the parts to update so we don't inadvertently
        # step on the model name or anything
        self.patch_path: Optional[str] = patch.original_file_path
        self.description = patch.description
        self.columns = patch.columns
        self.meta = patch.meta
        self.docs = patch.docs
        if dbt.flags.STRICT_MODE:
            assert isinstance(self, JsonSchemaMixin)
            self.to_dict(validate=True)

    def get_materialization(self):
        return self.config.materialized

    def local_vars(self):
        return self.config.vars


@dataclass
class ParsedNodeMandatory(
    UnparsedNode,
    HasUniqueID,
    HasFqn,
    HasRelationMetadata,
    Replaceable
):
    alias: str

    @property
    def identifier(self):
        return self.alias


@dataclass
class ParsedNodeDefaults(ParsedNodeMandatory):
    config: NodeConfig = field(default_factory=NodeConfig)
    tags: List[str] = field(default_factory=list)
    refs: List[List[str]] = field(default_factory=list)
    sources: List[List[Any]] = field(default_factory=list)
    depends_on: DependsOn = field(default_factory=DependsOn)
    description: str = field(default='')
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)
    docs: Docs = field(default_factory=Docs)
    patch_path: Optional[str] = None
    build_path: Optional[str] = None

    def write_node(self, target_path: str, subdirectory: str, payload: str):
        full_path = os.path.join(
            target_path, subdirectory, self.package_name, self.path
        )

        write_file(full_path, payload)
        return full_path


@dataclass
class ParsedNode(ParsedNodeDefaults, ParsedNodeMixins):
    pass


@dataclass
class ParsedAnalysisNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Analysis]})


@dataclass
class ParsedHookNode(ParsedNode):
    resource_type: NodeType = field(
        metadata={'restrict': [NodeType.Operation]}
    )
    index: Optional[int] = None


@dataclass
class ParsedModelNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Model]})


@dataclass
class ParsedRPCNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.RPCCall]})


# class SeedConfig(NodeConfig):
#     quote_columns: Optional[bool] = None


@dataclass
class ParsedSeedNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Seed]})
    config: SeedConfig = field(default_factory=SeedConfig)

    @property
    def empty(self):
        """ Seeds are never empty"""
        return False


# @dataclass
# class TestConfig(NodeConfig):
#     severity: Severity = Severity('error')


@dataclass
class TestMetadata(JsonSchemaMixin):
    namespace: Optional[str]
    name: str
    kwargs: Dict[str, Any]


@dataclass
class HasTestMetadata(JsonSchemaMixin):
    test_metadata: TestMetadata


@dataclass
class ParsedDataTestNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Test]})
    config: TestConfig = field(default_factory=TestConfig)


@dataclass
class ParsedSchemaTestNode(ParsedNode, HasTestMetadata):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Test]})
    column_name: Optional[str] = None
    config: TestConfig = field(default_factory=TestConfig)


@dataclass
class IntermediateSnapshotNode(ParsedNode):
    # at an intermediate stage in parsing, where we've built something better
    # than an unparsed node for rendering in parse mode, it's pretty possible
    # that we won't have critical snapshot-related information that is only
    # defined in config blocks. To fix that, we have an intermediate type that
    # uses a regular node config, which the snapshot parser will then convert
    # into a full ParsedSnapshotNode after rendering.
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Snapshot]})


def _create_if_else_chain(
    key: str,
    criteria: List[Tuple[str, Type[JsonSchemaMixin]]],
    default: Type[JsonSchemaMixin]
) -> Dict[str, Any]:
    """Mutate a given schema key that contains a 'oneOf' to instead be an
    'if-then-else' chain. This results is much better/more consistent errors
    from jsonschema.
    """
    schema: Dict[str, Any] = {}
    result: Dict[str, Any] = {}
    criteria = criteria[:]
    while criteria:
        if_clause, then_clause = criteria.pop()
        schema['if'] = {'properties': {
            key: {'enum': [if_clause]}
        }}
        schema['then'] = then_clause.json_schema()
        schema['else'] = {}
        schema = schema['else']
    schema.update(default.json_schema())
    return result


@dataclass
class ParsedSnapshotNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Snapshot]})
    config: Union[
        CheckSnapshotConfig,
        TimestampSnapshotConfig,
        GenericSnapshotConfig,
    ]

    @classmethod
    def json_schema(cls, embeddable: bool = False) -> Dict[str, Any]:
        schema = super().json_schema(embeddable)

        # mess with config
        configs: List[Tuple[str, Type[JsonSchemaMixin]]] = [
            (str(SnapshotStrategy.Check), CheckSnapshotConfig),
            (str(SnapshotStrategy.Timestamp), TimestampSnapshotConfig),
        ]

        if embeddable:
            dest = schema[cls.__name__]['properties']
        else:
            dest = schema['properties']
        dest['config'] = _create_if_else_chain(
            'strategy', configs, GenericSnapshotConfig
        )
        return schema


@dataclass
class ParsedPatch(HasYamlMetadata, Replaceable):
    name: str
    description: str
    meta: Dict[str, Any]
    docs: Docs


# The parsed node update is only the 'patch', not the test. The test became a
# regular parsed node. Note that description and columns must be present, but
# may be empty.
@dataclass
class ParsedNodePatch(ParsedPatch):
    columns: Dict[str, ColumnInfo]


@dataclass
class ParsedMacroPatch(ParsedPatch):
    arguments: List[MacroArgument] = field(default_factory=list)


@dataclass
class ParsedMacro(UnparsedBaseNode, HasUniqueID):
    name: str
    macro_sql: str
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Macro]})
    # TODO: can macros even have tags?
    tags: List[str] = field(default_factory=list)
    # TODO: is this ever populated?
    depends_on: MacroDependsOn = field(default_factory=MacroDependsOn)
    description: str = ''
    meta: Dict[str, Any] = field(default_factory=dict)
    patch_path: Optional[str] = None
    arguments: List[MacroArgument] = field(default_factory=list)

    def local_vars(self):
        return {}

    def patch(self, patch: ParsedMacroPatch):
        self.patch_path: Optional[str] = patch.original_file_path
        self.description = patch.description
        self.meta = patch.meta
        self.arguments = patch.arguments
        if dbt.flags.STRICT_MODE:
            assert isinstance(self, JsonSchemaMixin)
            self.to_dict(validate=True)


@dataclass
class ParsedDocumentation(UnparsedDocumentation, HasUniqueID):
    name: str
    block_contents: str

    @property
    def search_name(self):
        return self.name


@dataclass
class ParsedSourceDefinition(
    UnparsedBaseNode,
    HasUniqueID,
    HasRelationMetadata,
    HasFqn
):
    name: str
    source_name: str
    source_description: str
    loader: str
    identifier: str
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Source]})
    quoting: Quoting = field(default_factory=Quoting)
    loaded_at_field: Optional[str] = None
    freshness: Optional[FreshnessThreshold] = None
    external: Optional[ExternalTable] = None
    description: str = ''
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)
    source_meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)

    @property
    def is_refable(self):
        return False

    @property
    def is_ephemeral(self):
        return False

    @property
    def is_ephemeral_model(self):
        return False

    @property
    def depends_on_nodes(self):
        return []

    @property
    def refs(self):
        return []

    @property
    def sources(self):
        return []

    @property
    def has_freshness(self):
        return bool(self.freshness) and self.loaded_at_field is not None

    @property
    def search_name(self):
        return f'{self.source_name}.{self.name}'


ParsedResource = Union[
    ParsedMacro, ParsedNode, ParsedDocumentation, ParsedSourceDefinition
]
