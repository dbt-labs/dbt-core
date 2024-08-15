import pytest

from dbt.contracts.graph.semantic_manifest import SemanticManifest


# Overwrite the default nods to construct the manifest
@pytest.fixture
def nodes(metricflow_time_spine_model):
    return [metricflow_time_spine_model]


@pytest.fixture
def semantic_models(
    semantic_model,
) -> list:
    return [semantic_model]


@pytest.fixture
def metrics(
    metric,
) -> list:
    return [metric]


class TestSemanticManifest:
    def test_validate(self, manifest):
        sm_manifest = SemanticManifest(manifest)
        assert sm_manifest.validate()
