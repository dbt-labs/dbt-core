from typing import Any, Dict, List, Optional
from datetime import datetime


from dataclasses import dataclass, field

from dbt.contracts.util import (
    AdditionalPropertiesMixin,
    ArtifactMixin,
    BaseArtifactMetadata,
    schema_version,
)
from dbt.contracts.graph.unparsed import NodeVersion
from dbt.dataclass_schema import dbtClassMixin, ExtensibleDbtClassMixin


@dataclass
class ProjectDependency(AdditionalPropertiesMixin, ExtensibleDbtClassMixin):
    name: str
    _extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProjectDependencies(dbtClassMixin):
    projects: List[ProjectDependency] = field(default_factory=list)


@dataclass
class PublicationMetadata(BaseArtifactMetadata):
    dbt_schema_version: str = field(
        default_factory=lambda: str(PublicationArtifact.dbt_schema_version)
    )
    adapter_type: Optional[str] = None
    quoting: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PublicModel(dbtClassMixin):
    """Used to represent cross-project models"""

    name: str
    package_name: str
    unique_id: str
    relation_name: str
    identifier: str
    schema: str
    database: Optional[str] = None
    version: Optional[NodeVersion] = None
    latest_version: Optional[NodeVersion] = None
    # list of model unique_ids
    public_node_dependencies: List[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.utcnow)
    deprecation_date: Optional[datetime] = None


@dataclass
class PublicationMandatory:
    project_name: str


@dataclass
@schema_version("publication", 1)
class PublicationArtifact(ArtifactMixin, PublicationMandatory):
    public_models: Dict[str, PublicModel] = field(default_factory=dict)
    metadata: PublicationMetadata = field(default_factory=PublicationMetadata)
    # list of project name strings
    dependencies: List[str] = field(default_factory=list)


@dataclass
class PublicationConfig(ArtifactMixin, PublicationMandatory):
    """This is for the part of the publication artifact which is stored in
    the internal manifest. The public_nodes are stored separately in the manifest,
    and just the unique_ids of the public models are stored here."""

    metadata: PublicationMetadata = field(default_factory=PublicationMetadata)
    # list of project name strings
    dependencies: List[str] = field(default_factory=list)
    public_node_ids: List[str] = field(default_factory=list)

    @classmethod
    def from_publication(cls, publication: PublicationArtifact):
        return cls(
            project_name=publication.project_name,
            metadata=publication.metadata,
            dependencies=publication.dependencies,
            public_node_ids=list(publication.public_models.keys()),
        )
