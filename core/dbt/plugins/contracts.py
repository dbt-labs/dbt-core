from typing import Dict

# just exports, they need "noqa" so flake8 will not complain.
from dbt.artifacts.schemas.base import ArtifactMixin as PluginArtifact, schema_version  # noqa
from dbt.artifacts.schemas.base import BaseArtifactMetadata  # noqa
from dbt_common.dataclass_schema import dbtClassMixin, ExtensibleDbtClassMixin  # noqa


PluginArtifacts = Dict[str, PluginArtifact]
