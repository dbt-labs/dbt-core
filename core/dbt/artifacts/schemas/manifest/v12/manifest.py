from dataclasses import dataclass, field
from typing import Mapping, Iterable, Tuple, Optional, Dict, List, Any, Union
from uuid import UUID

from dbt.artifacts.schemas.base import (
    BaseArtifactMetadata,
    ArtifactMixin,
    schema_version,
    get_artifact_schema_version,
)
from dbt.artifacts.schemas.upgrades import upgrade_manifest_json
from dbt.artifacts.resources import (
    Documentation,
    Exposure,
    Group,
    Macro,
    Metric,
    SavedQuery,
    SemanticModel,
    SourceDefinition,
    UnitTestDefinition,
    Seed,
    Analysis,
    SingularTest,
    HookNode,
    Model,
    SqlOperation,
    GenericTest,
    Snapshot,
)


NodeEdgeMap = Dict[str, List[str]]
UniqueID = str
ManifestResource = Union[
    Seed,
    Analysis,
    SingularTest,
    HookNode,
    Model,
    SqlOperation,
    GenericTest,
    Snapshot,
]
DisabledManifestResource = Union[
    ManifestResource,
    SourceDefinition,
    Exposure,
    Metric,
    SavedQuery,
    SemanticModel,
    UnitTestDefinition,
]


@dataclass
class ManifestMetadata(BaseArtifactMetadata):
    """Metadata for the manifest."""

    dbt_schema_version: str = field(
        default_factory=lambda: str(WritableManifest.dbt_schema_version)
    )
    project_name: Optional[str] = field(
        default=None,
        metadata={
            "description": "Name of the root project",
        },
    )
    project_id: Optional[str] = field(
        default=None,
        metadata={
            "description": "A unique identifier for the project, hashed from the project name",
        },
    )
    user_id: Optional[UUID] = field(
        default=None,
        metadata={
            "description": "A unique identifier for the user",
        },
    )
    send_anonymous_usage_stats: Optional[bool] = field(
        default=None,
        metadata=dict(
            description=("Whether dbt is configured to send anonymous usage statistics")
        ),
    )
    adapter_type: Optional[str] = field(
        default=None,
        metadata=dict(description="The type name of the adapter"),
    )

    @classmethod
    def default(cls):
        return cls(
            dbt_schema_version=str(WritableManifest.dbt_schema_version),
        )


@dataclass
@schema_version("manifest", 12)
class WritableManifest(ArtifactMixin):
    nodes: Mapping[UniqueID, ManifestResource] = field(
        metadata=dict(description=("The nodes defined in the dbt project and its dependencies"))
    )
    sources: Mapping[UniqueID, SourceDefinition] = field(
        metadata=dict(description=("The sources defined in the dbt project and its dependencies"))
    )
    macros: Mapping[UniqueID, Macro] = field(
        metadata=dict(description=("The macros defined in the dbt project and its dependencies"))
    )
    docs: Mapping[UniqueID, Documentation] = field(
        metadata=dict(description=("The docs defined in the dbt project and its dependencies"))
    )
    exposures: Mapping[UniqueID, Exposure] = field(
        metadata=dict(
            description=("The exposures defined in the dbt project and its dependencies")
        )
    )
    metrics: Mapping[UniqueID, Metric] = field(
        metadata=dict(description=("The metrics defined in the dbt project and its dependencies"))
    )
    groups: Mapping[UniqueID, Group] = field(
        metadata=dict(description=("The groups defined in the dbt project"))
    )
    selectors: Mapping[UniqueID, Any] = field(
        metadata=dict(description=("The selectors defined in selectors.yml"))
    )
    disabled: Optional[Mapping[UniqueID, List[DisabledManifestResource]]] = field(
        metadata=dict(description="A mapping of the disabled nodes in the target")
    )
    parent_map: Optional[NodeEdgeMap] = field(
        metadata=dict(
            description="A mapping from child nodes to their dependencies",
        )
    )
    child_map: Optional[NodeEdgeMap] = field(
        metadata=dict(
            description="A mapping from parent nodes to their dependents",
        )
    )
    group_map: Optional[NodeEdgeMap] = field(
        metadata=dict(
            description="A mapping from group names to their nodes",
        )
    )
    saved_queries: Mapping[UniqueID, SavedQuery] = field(
        metadata=dict(description=("The saved queries defined in the dbt project"))
    )
    semantic_models: Mapping[UniqueID, SemanticModel] = field(
        metadata=dict(description=("The semantic models defined in the dbt project"))
    )
    metadata: ManifestMetadata = field(
        metadata=dict(
            description="Metadata about the manifest",
        )
    )
    unit_tests: Mapping[UniqueID, UnitTestDefinition] = field(
        metadata=dict(
            description="The unit tests defined in the project",
        )
    )

    @classmethod
    def compatible_previous_versions(cls) -> Iterable[Tuple[str, int]]:
        return [
            ("manifest", 4),
            ("manifest", 5),
            ("manifest", 6),
            ("manifest", 7),
            ("manifest", 8),
            ("manifest", 9),
            ("manifest", 10),
            ("manifest", 11),
        ]

    @classmethod
    def upgrade_schema_version(cls, data):
        """This overrides the "upgrade_schema_version" call in VersionedSchema (via
        ArtifactMixin) to modify the dictionary passed in from earlier versions of the manifest."""
        manifest_schema_version = get_artifact_schema_version(data)
        if manifest_schema_version < cls.dbt_schema_version.version:
            data = upgrade_manifest_json(data, manifest_schema_version)
        return cls.from_dict(data)
