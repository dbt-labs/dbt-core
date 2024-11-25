
from dataclasses import dataclass
import pytest


from dbt.artifacts.schemas.base import (
    ArtifactMixin,
    BaseArtifactMetadata,
    schema_version,
)

@dataclass
@schema_version("manifest", 12)
class Artifact(ArtifactMixin):
    a: int


@dataclass
@schema_version("manifest", 12)
class ArtifactMinorSchemaChange(ArtifactMixin):
    a: int
    b: int = 2


class TestMinorSchemaChange:
    @pytest.fixture
    def artifact(self):
        return Artifact(a = 1, metadata=BaseArtifactMetadata(dbt_schema_version="1.9.0"))

    @pytest.fixture
    def artifact_minor_schema_change(self):
        return ArtifactMinorSchemaChange(a=1, metadata=BaseArtifactMetadata(dbt_schema_version="1.9.0"))

    def test_serializing_new_default_field_is_backward_compatabile(
        self, artifact_minor_schema_change
    ):
        # old code (using old class) can create an instance of itself given new data (new class)
        new_data = artifact_minor_schema_change.to_dict()
        # Ensure new data has additional field
        assert new_data["b"] == 2
        artifact = Artifact.from_dict(new_data)
        
        assert artifact.a == 1
        # Additional fields are ignored
        assert not hasattr(artifact, "b")

    def test_serializing_new_default_field_is_forward_compatible(self, artifact):
        # new code (using new class) can create an instance of itself given old data (old class)
        artifact = ArtifactMinorSchemaChange.from_dict(artifact.to_dict())

        assert artifact.a == 1
        assert artifact.b == 2

    def test_serializing_removed_default_field_is_backward_compatabile(self, artifact):
        # old code (using old class with default field) can create an instance of itself given new data (class w/o default field)
        old_artifact = ArtifactMinorSchemaChange.from_dict(artifact.to_dict())
        
        # set to the default value when not provided in data
        assert old_artifact.b == 2

    def test_serializing_removed_default_field_is_forward_compatible(
        self, artifact_minor_schema_change
    ):
        # new code (using class without default field) can create an instance of itself given old data (class with old field)
        Artifact.from_dict(artifact_minor_schema_change.to_dict())