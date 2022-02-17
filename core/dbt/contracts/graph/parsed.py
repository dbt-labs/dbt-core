import os
import time
from dataclasses import dataclass, field
from mashumaro.types import SerializableType
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

from dbt.dataclass_schema import dbtClassMixin, ExtensibleDbtClassMixin

from dbt.clients.system import write_file
from dbt.contracts.files import FileHash, MAXIMUM_SEED_SIZE_NAME
from dbt.contracts.graph.unparsed import (
    UnparsedNode,
    UnparsedDocumentation,
    Quoting,
    Docs,
    UnparsedBaseNode,
    FreshnessThreshold,
    ExternalTable,
    HasYamlMetadata,
    MacroArgument,
    UnparsedSourceDefinition,
    UnparsedSourceTableDefinition,
    UnparsedColumn,
    TestDef,
    ExposureOwner,
    ExposureType,
    MaturityType,
    MetricFilter,
)
from dbt.contracts.util import Replaceable, AdditionalPropertiesMixin
from dbt.exceptions import warn_or_error
from dbt import flags
from dbt.node_types import NodeType


from .model_config import (
    NodeConfig,
    SeedConfig,
    TestConfig,
    SourceConfig,
    EmptySnapshotConfig,
    SnapshotConfig,
)


@dataclass
class ColumnInfo(AdditionalPropertiesMixin, ExtensibleDbtClassMixin, Replaceable):
    name: str
    description: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    data_type: Optional[str] = None
    quote: Optional[bool] = None
    tags: List[str] = field(default_factory=list)
    _extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HasFqn(dbtClassMixin, Replaceable):
    fqn: List[str]

    def same_fqn(self, other: "HasFqn") -> bool:
        return self.fqn == other.fqn


@dataclass
class HasUniqueID(dbtClassMixin, Replaceable):
    unique_id: str


@dataclass
class MacroDependsOn(dbtClassMixin, Replaceable):
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
class HasRelationMetadata(dbtClassMixin, Replaceable):
    database: Optional[str]
    schema: str

    # Can't set database to None like it ought to be
    # because it messes up the subclasses and default parameters
    # so hack it here
    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data


class ParsedNodeMixins(dbtClassMixin):
    resource_type: NodeType
    depends_on: DependsOn
    config: NodeConfig

    @property
    def is_refable(self):
        return self.resource_type in NodeType.refable()

    @property
    def should_store_failures(self):
        return self.resource_type == NodeType.Test and (
            self.config.store_failures
            if self.config.store_failures is not None
            else flags.STORE_FAILURES
        )

    # will this node map to an object in the database?
    @property
    def is_relational(self):
        return self.resource_type in NodeType.refable() or self.should_store_failures

    @property
    def is_ephemeral(self):
        return self.config.materialized == "ephemeral"

    @property
    def is_ephemeral_model(self):
        return self.is_refable and self.is_ephemeral

    @property
    def depends_on_nodes(self):
        return self.depends_on.nodes

    def patch(self, patch: "ParsedNodePatch"):
        """Given a ParsedNodePatch, add the new information to the node."""
        # explicitly pick out the parts to update so we don't inadvertently
        # step on the model name or anything
        # Note: config should already be updated
        self.patch_path: Optional[str] = patch.file_id
        # update created_at so process_docs will run in partial parsing
        self.created_at = time.time()
        self.description = patch.description
        self.columns = patch.columns
        self.docs = patch.docs

    def get_materialization(self):
        return self.config.materialized


@dataclass
class ParsedNodeMandatory(UnparsedNode, HasUniqueID, HasFqn, HasRelationMetadata, Replaceable):
    alias: str
    checksum: FileHash
    config: NodeConfig = field(default_factory=NodeConfig)

    @property
    def identifier(self):
        return self.alias


@dataclass
class NodeInfoMixin:
    _event_status: Dict[str, Any] = field(default_factory=dict)

    @property
    def node_info(self):
        node_info = {
            "node_path": getattr(self, "path", None),
            "node_name": getattr(self, "name", None),
            "unique_id": getattr(self, "unique_id", None),
            "resource_type": str(getattr(self, "resource_type", "")),
            "materialized": self.config.get("materialized"),
            "node_status": str(self._event_status.get("node_status")),
            "node_started_at": self._event_status.get("started_at"),
            "node_finished_at": self._event_status.get("finished_at"),
        }
        return node_info


@dataclass
class ParsedNodeDefaults(NodeInfoMixin, ParsedNodeMandatory):
    tags: List[str] = field(default_factory=list)
    refs: List[List[str]] = field(default_factory=list)
    sources: List[List[str]] = field(default_factory=list)
    depends_on: DependsOn = field(default_factory=DependsOn)
    description: str = field(default="")
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)
    docs: Docs = field(default_factory=Docs)
    patch_path: Optional[str] = None
    compiled_path: Optional[str] = None
    build_path: Optional[str] = None
    deferred: bool = False
    unrendered_config: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=lambda: time.time())
    config_call_dict: Dict[str, Any] = field(default_factory=dict)

    def write_node(self, target_path: str, subdirectory: str, payload: str):
        if os.path.basename(self.path) == os.path.basename(self.original_file_path):
            # One-to-one relationship of nodes to files.
            path = self.original_file_path
        else:
            #  Many-to-one relationship of nodes to files.
            path = os.path.join(self.original_file_path, self.path)
        full_path = os.path.join(target_path, subdirectory, self.package_name, path)

        write_file(full_path, payload)
        return full_path


T = TypeVar("T", bound="ParsedNode")


@dataclass
class ParsedNode(ParsedNodeDefaults, ParsedNodeMixins, SerializableType):
    def _serialize(self):
        return self.to_dict()

    def __post_serialize__(self, dct):
        if "config_call_dict" in dct:
            del dct["config_call_dict"]
        if "_event_status" in dct:
            del dct["_event_status"]
        return dct

    @classmethod
    def _deserialize(cls, dct: Dict[str, int]):
        # The serialized ParsedNodes do not differ from each other
        # in fields that would allow 'from_dict' to distinguis
        # between them.
        resource_type = dct["resource_type"]
        if resource_type == "model":
            return ParsedModelNode.from_dict(dct)
        elif resource_type == "analysis":
            return ParsedAnalysisNode.from_dict(dct)
        elif resource_type == "seed":
            return ParsedSeedNode.from_dict(dct)
        elif resource_type == "rpc":
            return ParsedRPCNode.from_dict(dct)
        elif resource_type == "sql":
            return ParsedSqlNode.from_dict(dct)
        elif resource_type == "test":
            if "test_metadata" in dct:
                return ParsedGenericTestNode.from_dict(dct)
            else:
                return ParsedSingularTestNode.from_dict(dct)
        elif resource_type == "operation":
            return ParsedHookNode.from_dict(dct)
        elif resource_type == "seed":
            return ParsedSeedNode.from_dict(dct)
        elif resource_type == "snapshot":
            return ParsedSnapshotNode.from_dict(dct)
        else:
            return cls.from_dict(dct)

    def _persist_column_docs(self) -> bool:
        if hasattr(self.config, "persist_docs"):
            assert isinstance(self.config, NodeConfig)
            return bool(self.config.persist_docs.get("columns"))
        return False

    def _persist_relation_docs(self) -> bool:
        if hasattr(self.config, "persist_docs"):
            assert isinstance(self.config, NodeConfig)
            return bool(self.config.persist_docs.get("relation"))
        return False

    def same_body(self: T, other: T) -> bool:
        return self.raw_sql == other.raw_sql

    def same_persisted_description(self: T, other: T) -> bool:
        # the check on configs will handle the case where we have different
        # persist settings, so we only have to care about the cases where they
        # are the same..
        if self._persist_relation_docs():
            if self.description != other.description:
                return False

        if self._persist_column_docs():
            # assert other._persist_column_docs()
            column_descriptions = {k: v.description for k, v in self.columns.items()}
            other_column_descriptions = {k: v.description for k, v in other.columns.items()}
            if column_descriptions != other_column_descriptions:
                return False

        return True

    def same_database_representation(self, other: T) -> bool:
        # compare the config representation, not the node's config value. This
        # compares the configured value, rather than the ultimate value (so
        # generate_*_name and unset values derived from the target are
        # ignored)
        keys = ("database", "schema", "alias")
        for key in keys:
            mine = self.unrendered_config.get(key)
            others = other.unrendered_config.get(key)
            if mine != others:
                return False
        return True

    def same_config(self, old: T) -> bool:
        return self.config.same_contents(
            self.unrendered_config,
            old.unrendered_config,
        )

    def same_contents(self: T, old: Optional[T]) -> bool:
        if old is None:
            return False

        return (
            self.same_body(old)
            and self.same_config(old)
            and self.same_persisted_description(old)
            and self.same_fqn(old)
            and self.same_database_representation(old)
            and True
        )


@dataclass
class ParsedAnalysisNode(ParsedNode):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Analysis]})


@dataclass
class ParsedHookNode(ParsedNode):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Operation]})
    index: Optional[int] = None


@dataclass
class ParsedModelNode(ParsedNode):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Model]})


# TODO: rm?
@dataclass
class ParsedRPCNode(ParsedNode):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.RPCCall]})


@dataclass
class ParsedSqlNode(ParsedNode):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.SqlOperation]})


def same_seeds(first: ParsedNode, second: ParsedNode) -> bool:
    # for seeds, we check the hashes. If the hashes are different types,
    # no match. If the hashes are both the same 'path', log a warning and
    # assume they are the same
    # if the current checksum is a path, we want to log a warning.
    result = first.checksum == second.checksum

    if first.checksum.name == "path":
        msg: str
        if second.checksum.name != "path":
            msg = (
                f"Found a seed ({first.package_name}.{first.name}) "
                f">{MAXIMUM_SEED_SIZE_NAME} in size. The previous file was "
                f"<={MAXIMUM_SEED_SIZE_NAME}, so it has changed"
            )
        elif result:
            msg = (
                f"Found a seed ({first.package_name}.{first.name}) "
                f">{MAXIMUM_SEED_SIZE_NAME} in size at the same path, dbt "
                f"cannot tell if it has changed: assuming they are the same"
            )
        elif not result:
            msg = (
                f"Found a seed ({first.package_name}.{first.name}) "
                f">{MAXIMUM_SEED_SIZE_NAME} in size. The previous file was in "
                f"a different location, assuming it has changed"
            )
        else:
            msg = (
                f"Found a seed ({first.package_name}.{first.name}) "
                f">{MAXIMUM_SEED_SIZE_NAME} in size. The previous file had a "
                f"checksum type of {second.checksum.name}, so it has changed"
            )
        warn_or_error(msg, node=first)

    return result


@dataclass
class ParsedSeedNode(ParsedNode):
    # keep this in sync with CompiledSeedNode!
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Seed]})
    config: SeedConfig = field(default_factory=SeedConfig)

    @property
    def empty(self):
        """Seeds are never empty"""
        return False

    def same_body(self: T, other: T) -> bool:
        return same_seeds(self, other)


@dataclass
class TestMetadata(dbtClassMixin, Replaceable):
    name: str
    # kwargs are the args that are left in the test builder after
    # removing configs. They are set from the test builder when
    # the test node is created.
    kwargs: Dict[str, Any] = field(default_factory=dict)
    namespace: Optional[str] = None


@dataclass
class HasTestMetadata(dbtClassMixin):
    test_metadata: TestMetadata


@dataclass
class ParsedSingularTestNode(ParsedNode):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Test]})
    # Was not able to make mypy happy and keep the code working. We need to
    # refactor the various configs.
    config: TestConfig = field(default_factory=TestConfig)  # type: ignore

    @property
    def test_node_type(self):
        return "singular"


@dataclass
class ParsedGenericTestNode(ParsedNode, HasTestMetadata):
    # keep this in sync with CompiledGenericTestNode!
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Test]})
    column_name: Optional[str] = None
    file_key_name: Optional[str] = None
    # Was not able to make mypy happy and keep the code working. We need to
    # refactor the various configs.
    config: TestConfig = field(default_factory=TestConfig)  # type: ignore

    def same_contents(self, other) -> bool:
        if other is None:
            return False

        return self.same_config(other) and self.same_fqn(other) and True

    @property
    def test_node_type(self):
        return "generic"


@dataclass
class IntermediateSnapshotNode(ParsedNode):
    # at an intermediate stage in parsing, where we've built something better
    # than an unparsed node for rendering in parse mode, it's pretty possible
    # that we won't have critical snapshot-related information that is only
    # defined in config blocks. To fix that, we have an intermediate type that
    # uses a regular node config, which the snapshot parser will then convert
    # into a full ParsedSnapshotNode after rendering.
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Snapshot]})
    config: EmptySnapshotConfig = field(default_factory=EmptySnapshotConfig)


@dataclass
class ParsedSnapshotNode(ParsedNode):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Snapshot]})
    config: SnapshotConfig


@dataclass
class ParsedPatch(HasYamlMetadata, Replaceable):
    name: str
    description: str
    meta: Dict[str, Any]
    docs: Docs
    config: Dict[str, Any]


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
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Macro]})
    # TODO: can macros even have tags?
    tags: List[str] = field(default_factory=list)
    # TODO: is this ever populated?
    depends_on: MacroDependsOn = field(default_factory=MacroDependsOn)
    description: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    docs: Docs = field(default_factory=Docs)
    patch_path: Optional[str] = None
    arguments: List[MacroArgument] = field(default_factory=list)
    created_at: float = field(default_factory=lambda: time.time())

    def patch(self, patch: ParsedMacroPatch):
        self.patch_path: Optional[str] = patch.file_id
        self.description = patch.description
        self.created_at = time.time()
        self.meta = patch.meta
        self.docs = patch.docs
        self.arguments = patch.arguments

    def same_contents(self, other: Optional["ParsedMacro"]) -> bool:
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

    def same_contents(self, other: Optional["ParsedDocumentation"]) -> bool:
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
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Source]})
    patch_path: Optional[Path] = None

    def get_full_source_name(self):
        return f"{self.source.name}_{self.table.name}"

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

    def get_tests(self) -> Iterator[Tuple[Dict[str, Any], Optional[UnparsedColumn]]]:
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
class ParsedSourceMandatory(
    UnparsedBaseNode,
    HasUniqueID,
    HasRelationMetadata,
    HasFqn,
):
    name: str
    source_name: str
    source_description: str
    loader: str
    identifier: str
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Source]})


@dataclass
class ParsedSourceDefinition(NodeInfoMixin, ParsedSourceMandatory):
    quoting: Quoting = field(default_factory=Quoting)
    loaded_at_field: Optional[str] = None
    freshness: Optional[FreshnessThreshold] = None
    external: Optional[ExternalTable] = None
    description: str = ""
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)
    source_meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    config: SourceConfig = field(default_factory=SourceConfig)
    patch_path: Optional[Path] = None
    unrendered_config: Dict[str, Any] = field(default_factory=dict)
    relation_name: Optional[str] = None
    created_at: float = field(default_factory=lambda: time.time())

    def __post_serialize__(self, dct):
        if "_event_status" in dct:
            del dct["_event_status"]
        return dct

    def same_database_representation(self, other: "ParsedSourceDefinition") -> bool:
        return (
            self.database == other.database
            and self.schema == other.schema
            and self.identifier == other.identifier
            and True
        )

    def same_quoting(self, other: "ParsedSourceDefinition") -> bool:
        return self.quoting == other.quoting

    def same_freshness(self, other: "ParsedSourceDefinition") -> bool:
        return (
            self.freshness == other.freshness
            and self.loaded_at_field == other.loaded_at_field
            and True
        )

    def same_external(self, other: "ParsedSourceDefinition") -> bool:
        return self.external == other.external

    def same_config(self, old: "ParsedSourceDefinition") -> bool:
        return self.config.same_contents(
            self.unrendered_config,
            old.unrendered_config,
        )

    def same_contents(self, old: Optional["ParsedSourceDefinition"]) -> bool:
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
            self.same_database_representation(old)
            and self.same_fqn(old)
            and self.same_config(old)
            and self.same_quoting(old)
            and self.same_freshness(old)
            and self.same_external(old)
            and True
        )

    def get_full_source_name(self):
        return f"{self.source_name}_{self.name}"

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
    def depends_on(self):
        return DependsOn(macros=[], nodes=[])

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
        return f"{self.source_name}.{self.name}"


@dataclass
class ParsedExposure(UnparsedBaseNode, HasUniqueID, HasFqn):
    name: str
    type: ExposureType
    owner: ExposureOwner
    resource_type: NodeType = NodeType.Exposure
    description: str = ""
    maturity: Optional[MaturityType] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    url: Optional[str] = None
    depends_on: DependsOn = field(default_factory=DependsOn)
    refs: List[List[str]] = field(default_factory=list)
    sources: List[List[str]] = field(default_factory=list)
    created_at: float = field(default_factory=lambda: time.time())

    @property
    def depends_on_nodes(self):
        return self.depends_on.nodes

    @property
    def search_name(self):
        return self.name

    def same_depends_on(self, old: "ParsedExposure") -> bool:
        return set(self.depends_on.nodes) == set(old.depends_on.nodes)

    def same_description(self, old: "ParsedExposure") -> bool:
        return self.description == old.description

    def same_maturity(self, old: "ParsedExposure") -> bool:
        return self.maturity == old.maturity

    def same_owner(self, old: "ParsedExposure") -> bool:
        return self.owner == old.owner

    def same_exposure_type(self, old: "ParsedExposure") -> bool:
        return self.type == old.type

    def same_url(self, old: "ParsedExposure") -> bool:
        return self.url == old.url

    def same_contents(self, old: Optional["ParsedExposure"]) -> bool:
        # existing when it didn't before is a change!
        # metadata/tags changes are not "changes"
        if old is None:
            return True

        return (
            self.same_fqn(old)
            and self.same_exposure_type(old)
            and self.same_owner(old)
            and self.same_maturity(old)
            and self.same_url(old)
            and self.same_description(old)
            and self.same_depends_on(old)
            and True
        )


@dataclass
class ParsedMetric(UnparsedBaseNode, HasUniqueID, HasFqn):
    model: str
    name: str
    description: str
    label: str
    type: str
    sql: Optional[str]
    timestamp: Optional[str]
    filters: List[MetricFilter]
    time_grains: List[str]
    dimensions: List[str]
    resource_type: NodeType = NodeType.Metric
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    sources: List[List[str]] = field(default_factory=list)
    depends_on: DependsOn = field(default_factory=DependsOn)
    refs: List[List[str]] = field(default_factory=list)
    created_at: float = field(default_factory=lambda: time.time())

    @property
    def depends_on_nodes(self):
        return self.depends_on.nodes

    @property
    def search_name(self):
        return self.name

    def same_model(self, old: "ParsedMetric") -> bool:
        return self.model == old.model

    def same_dimensions(self, old: "ParsedMetric") -> bool:
        return self.dimensions == old.dimensions

    def same_filters(self, old: "ParsedMetric") -> bool:
        return self.filters == old.filters

    def same_description(self, old: "ParsedMetric") -> bool:
        return self.description == old.description

    def same_label(self, old: "ParsedMetric") -> bool:
        return self.label == old.label

    def same_type(self, old: "ParsedMetric") -> bool:
        return self.type == old.type

    def same_sql(self, old: "ParsedMetric") -> bool:
        return self.sql == old.sql

    def same_timestamp(self, old: "ParsedMetric") -> bool:
        return self.timestamp == old.timestamp

    def same_time_grains(self, old: "ParsedMetric") -> bool:
        return self.time_grains == old.time_grains

    def same_contents(self, old: Optional["ParsedMetric"]) -> bool:
        # existing when it didn't before is a change!
        # metadata/tags changes are not "changes"
        if old is None:
            return True

        return (
            self.same_model(old)
            and self.same_dimensions(old)
            and self.same_filters(old)
            and self.same_description(old)
            and self.same_label(old)
            and self.same_type(old)
            and self.same_sql(old)
            and self.same_timestamp(old)
            and self.same_time_grains(old)
            and True
        )


ManifestNodes = Union[
    ParsedAnalysisNode,
    ParsedSingularTestNode,
    ParsedHookNode,
    ParsedModelNode,
    ParsedRPCNode,
    ParsedSqlNode,
    ParsedGenericTestNode,
    ParsedSeedNode,
    ParsedSnapshotNode,
]


ParsedResource = Union[
    ParsedDocumentation,
    ParsedMacro,
    ParsedNode,
    ParsedExposure,
    ParsedMetric,
    ParsedSourceDefinition,
]
