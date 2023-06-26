from dataclasses import dataclass, field
from pathlib import Path

from dbt.contracts.util import ArtifactMixin as ExternalArtifact
from dbt.contracts.util import BaseArtifactMetadata, schema_version, AdditionalPropertiesMixin
from dbt.dataclass_schema import dbtClassMixin
