from unittest.mock import patch

import pytest

from core.dbt.contracts.graph.manifest import Manifest
from core.dbt.contracts.graph.nodes import ModelNode
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
        with patch("dbt.contracts.graph.semantic_manifest.get_flags") as patched_get_flags:
            patched_get_flags.return_value.allow_mf_time_spines_without_yaml_configuration = True
            sm_manifest = SemanticManifest(manifest)
            assert sm_manifest.validate()

    def test_allow_mf_time_spines_without_yaml_configuration(
        self, manifest: Manifest, metricflow_time_spine_model: ModelNode
    ):
        with patch("dbt.contracts.graph.semantic_manifest.get_flags") as patched_get_flags, patch(
            "dbt.contracts.graph.semantic_manifest.deprecations"
        ) as patched_deprecations:
            patched_get_flags.return_value.allow_mf_time_spines_without_yaml_configuration = False
            manifest.nodes[metricflow_time_spine_model.unique_id] = metricflow_time_spine_model
            sm_manifest = SemanticManifest(manifest)
            assert sm_manifest.validate()
            assert patched_deprecations.warn.call_count == 1
