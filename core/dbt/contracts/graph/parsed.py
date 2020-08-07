import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Optional,
    Union,
    List,
    Dict,
    Any,
    Sequence,
    Tuple,
    Iterator,
    TypeVar,
)

from hologram import JsonSchemaMixin
from hologram.helpers import ExtensibleJsonSchemaMixin

from dbt.clients.system import write_file
from dbt.contracts.files import FileHash, MAXIMUM_SEED_SIZE_NAME
from dbt.contracts.graph.unparsed import (
    UnparsedNode, UnparsedDocumentation, Quoting, Docs,
    UnparsedBaseNode, FreshnessThreshold, ExternalTable,
    HasYamlMetadata, MacroArgument, UnparsedSourceDefinition,
    UnparsedSourceTableDefinition, UnparsedColumn, TestDef
)
from dbt.contracts.util import Replaceable, AdditionalPropertiesMixin
from dbt.exceptions import warn_or_error
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt import flags
from dbt.node_types import NodeType


from .model_config import (
    NodeConfig,
    SeedConfig,
    TestConfig,
    SourceConfig,
    EmptySnapshotConfig,
    SnapshotVariants,
)
# import these 3 so the SnapshotVariants forward ref works.
from .model_config import (  # noqa
    TimestampSnapshotConfig,
    CheckSnapshotConfig,
    GenericSnapshotConfig,
)


@dataclass
class ColumnInfo(
    AdditionalPropertiesMixin,
    ExtensibleJsonSchemaMixin,
    Replaceable
):
    name: str
    description: str = ''
    meta: Dict[str, Any] = field(default_factory=dict)
    data_type: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    _extra: Dict[str, Any] = field(default_factory=dict)


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
    database: Optional[str]
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
        if flags.STRICT_MODE:
            assert isinstance(self, JsonSchemaMixin)
            self.to_dict(validate=True, omit_none=False)

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
    checksum: FileHash
    config: NodeConfig = field(default_factory=NodeConfig)

    @property
    def identifier(self):
        return self.alias


@dataclass
class ParsedNodeDefaults(ParsedNodeMandatory):
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
    deferred: bool = False

    def write_node(self, target_path: str, subdirectory: str, payload: str):
        if (os.path.basename(self.path) ==
                os.path.basename(self.original_file_path)):
            # One-to-one relationship of nodes to files.
            path = self.original_file_path
        else:
            #  Many-to-one relationship of nodes to files.
            path = os.path.join(self.original_file_path, self.path)
        full_path = os.path.join(
            target_path, subdirectory, self.package_name, path
        )

        write_file(full_path, payload)
        return full_path


T = TypeVar('T', bound='ParsedNode')


@dataclass
class ParsedNode(ParsedNodeDefaults, ParsedNodeMixins):

    def _persist_column_docs(self) -> bool:
        return bool(self.config.persist_docs.get('columns'))

    def _persist_relation_docs(self) -> bool:
        return bool(self.config.persist_docs.get('relation'))

    def _same_body(self: T, other: T) -> bool:
        return self.raw_sql == other.raw_sql

    def _same_description_persisted(self: T, other: T) -> bool:
        # the check on configs will handle the case where we have different
        # persist settings, so we only have to care about the cases where they
        # are the same..
        if self._persist_relation_docs():
            if self.description != other.description:
                return False

        if self._persist_column_docs():
            # assert other._persist_column_docs()
            column_descriptions = {
                k: v.description for k, v in self.columns.items()
            }
            other_column_descriptions = {
                k: v.description for k, v in other.columns.items()
            }
            if column_descriptions != other_column_descriptions:
                return False

        return True

    def _same_name(self: T, old: T) -> bool:
        return (
            self.database == old.database and
            self.schema == old.schema and
            self.identifier == old.identifier and
            True
        )

    def same_contents(self: T, old: Optional[T]) -> bool:
        if old is None:
            return False

        return (
            self.resource_type == old.resource_type and
            self._same_body(old) and
            self.config.same_contents(old.config) and
            self._same_description_persisted(old) and
            self._same_name(old) and
            self.fqn == old.fqn and
            True
        )


@dataclass
class ParsedAnalysisNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Analysis]})


@dataclass
class HookMixin(JsonSchemaMixin):
    resource_type: NodeType = field(
        metadata={'restrict': [NodeType.Operation]}
    )
    index: Optional[int] = None


@dataclass
class SeedMixin(JsonSchemaMixin):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Seed]})
    config: SeedConfig = field(default_factory=SeedConfig)

    @property
    def empty(self):
        """ Seeds are never empty"""
        return False

    def _same_body(self: 'ParsedSeedNode', other: 'ParsedSeedNode') -> bool:
        # for seeds, we check the hashes. If the hashes are different types,
        # no match. If the hashes are both the same 'path', log a warning and
        # assume they are the same
        # if the current checksum is a path, we want to log a warning.
        result = self.checksum == other.checksum

        if self.checksum.name == 'path':
            msg: str
            if other.checksum.name != 'path':
                msg = (
                    f'Found a seed >{MAXIMUM_SEED_SIZE_NAME} in size. The '
                    f'previous file was <={MAXIMUM_SEED_SIZE_NAME}, so it '
                    f'has changed'
                )
            elif result:
                msg = (
                    f'Found a seed >{MAXIMUM_SEED_SIZE_NAME} in size at '
                    f'the same path, dbt cannot tell if it has changed: '
                    f'assuming they are the same'
                )
            elif not result:
                msg = (
                    f'Found a seed >{MAXIMUM_SEED_SIZE_NAME} in size. The '
                    f'previous file was in a different location, assuming it '
                    f'has changed'
                )
            else:
                msg = (
                    f'Found a seed >{MAXIMUM_SEED_SIZE_NAME} in size. The '
                    f'previous file had a checksum type of '
                    f'{other.checksum.name}, so it has changed'
                )
            warn_or_error(msg, node=self)

        return result


@dataclass
class ParsedHookNode(HookMixin, ParsedNode):
    pass


@dataclass
class ParsedModelNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Model]})


@dataclass
class ParsedRPCNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.RPCCall]})


@dataclass
class ParsedSeedNode(SeedMixin, ParsedNode):
    pass


@dataclass
class TestMetadata(JsonSchemaMixin, Replaceable):
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
class SchemaTestMixin(JsonSchemaMixin):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Test]})
    column_name: Optional[str] = None
    config: TestConfig = field(default_factory=TestConfig)

    # make sure to keep this in sync with CompiledSchemaTestNode...
    def _same_body(
        self: 'ParsedSchemaTestNode', other: 'ParsedSchemaTestNode'
    ) -> bool:
        return self.test_metadata == other.test_metadata


@dataclass
class ParsedSchemaTestNode(SchemaTestMixin, ParsedNode, HasTestMetadata):
    pass


@dataclass
class IntermediateSnapshotNode(ParsedNode):
    # at an intermediate stage in parsing, where we've built something better
    # than an unparsed node for rendering in parse mode, it's pretty possible
    # that we won't have critical snapshot-related information that is only
    # defined in config blocks. To fix that, we have an intermediate type that
    # uses a regular node config, which the snapshot parser will then convert
    # into a full ParsedSnapshotNode after rendering.
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Snapshot]})
    config: EmptySnapshotConfig = field(default_factory=EmptySnapshotConfig)


@dataclass
class ParsedSnapshotNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Snapshot]})
    config: SnapshotVariants


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
    docs: Docs = field(default_factory=Docs)
    patch_path: Optional[str] = None
    arguments: List[MacroArgument] = field(default_factory=list)

    def local_vars(self):
        return {}

    def patch(self, patch: ParsedMacroPatch):
        self.patch_path: Optional[str] = patch.original_file_path
        self.description = patch.description
        self.meta = patch.meta
        self.docs = patch.docs
        self.arguments = patch.arguments
        if flags.STRICT_MODE:
            assert isinstance(self, JsonSchemaMixin)
            self.to_dict(validate=True, omit_none=False)

    def same_contents(self, other: Optional['ParsedMacro']) -> bool:
        if other is None:
            return False
        # the only thing that makes one macro different from another with the
        # same name/package is its content
        return self.macro_sql == other.macro_sql


@dataclass
class ParsedDocumentation(UnparsedDocumentation, HasUniqueID):
    name: str
    block_contents: str

    @property
    def search_name(self):
        return self.name

    def same_contents(self, other: Optional['ParsedDocumentation']) -> bool:
        if other is None:
            return False
        # the only thing that makes one doc different from another with the
        # same name/package is its content
        return self.block_contents == other.block_contents


def normalize_test(testdef: TestDef) -> Dict[str, Any]:
    if isinstance(testdef, str):
        return {testdef: {}}
    else:
        return testdef


@dataclass
class UnpatchedSourceDefinition(UnparsedBaseNode, HasUniqueID, HasFqn):
    source: UnparsedSourceDefinition
    table: UnparsedSourceTableDefinition
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Source]})
    patch_path: Optional[Path] = None

    def get_full_source_name(self):
        return f'{self.source.name}_{self.table.name}'

    def get_source_representation(self):
        return f'source("{self.source.name}", "{self.table.name}")'

    @property
    def name(self) -> str:
        return self.get_full_source_name()

    @property
    def quote_columns(self) -> Optional[bool]:
        result = None
        if self.source.quoting.column is not None:
            result = self.source.quoting.column
        if self.table.quoting.column is not None:
            result = self.table.quoting.column
        return result

    @property
    def columns(self) -> Sequence[UnparsedColumn]:
        if self.table.columns is None:
            return []
        else:
            return self.table.columns

    def get_tests(
        self
    ) -> Iterator[Tuple[Dict[str, Any], Optional[UnparsedColumn]]]:
        for test in self.tests:
            yield normalize_test(test), None

        for column in self.columns:
            if column.tests is not None:
                for test in column.tests:
                    yield normalize_test(test), column

    @property
    def tests(self) -> List[TestDef]:
        if self.table.tests is None:
            return []
        else:
            return self.table.tests


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
    config: SourceConfig = field(default_factory=SourceConfig)
    patch_path: Optional[Path] = None

    def same_contents(self, old: Optional['ParsedSourceDefinition']) -> bool:
        # existing when it didn't before is a change!
        if old is None:
            return True

        # config changes are changes (because the only config is "enabled", and
        # enabling a source is a change!)
        # changing the database/schema/identifier is a change
        # messing around with external stuff is a change (uh, right?)
        # quoting changes are changes
        # freshness changes are changes, I guess
        # metadata/tags changes are not "changes"
        # patching/description changes are not "changes"
        return (
            old.config == self.config and
            old.freshness == self.freshness and
            old.database == self.database and
            old.schema == self.schema and
            old.identifier == self.identifier
        )

    def get_full_source_name(self):
        return f'{self.source_name}_{self.name}'

    def get_source_representation(self):
        return f'source("{self.source.name}", "{self.table.name}")'

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
