from dataclasses import dataclass
from pathlib import Path

# just exports, they need "noqa" so flake8 will not complain.
from dbt.contracts.util import ArtifactMixin, schema_version  # noqa
from dbt.contracts.util import BaseArtifactMetadata, AdditionalPropertiesMixin  # noqa
from dbt.dataclass_schema import dbtClassMixin  # noqa


@dataclass
class ExternalArtifact:
    path: Path
    artifact: ArtifactMixin
