from unittest.mock import patch

import pytest

from core.dbt.contracts.graph.manifest import Manifest
from core.dbt.contracts.graph.nodes import Metric, ModelNode
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.metric import MetricTimeWindow, MetricTypeParams
from dbt.contracts.graph.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.metric_type import MetricType


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

    @pytest.mark.parametrize(
        "metric",
        [
            (
                Metric(
                    name="my_metric",
                    type=MetricType.CUMULATIVE,
                    type_params=MetricTypeParams(grain_to_date=TimeGranularity.MONTH),
                    resource_type=NodeType.Metric,
                    package_name="test",
                    path="models/test/my_metric.yml",
                    original_file_path="models/test/my_metric.yml",
                    unique_id="metric.test.my_metric",
                    fqn=["test", "my_metric"],
                    description="My metric",
                    label="My Metric",
                )
            ),
            (
                Metric(
                    name="my_metric",
                    type=MetricType.CUMULATIVE,
                    type_params=MetricTypeParams(
                        window=MetricTimeWindow(count=1, granularity=TimeGranularity.MONTH)
                    ),
                    resource_type=NodeType.Metric,
                    package_name="test",
                    path="models/test/my_metric.yml",
                    original_file_path="models/test/my_metric.yml",
                    unique_id="metric.test.my_metric",
                    fqn=["test", "my_metric"],
                    description="My metric",
                    label="My Metric",
                )
            ),
        ],
    )
    def test_deprecate_cumulative_type_params(self, manifest: Manifest, metric: Metric):
        with patch("dbt.contracts.graph.semantic_manifest.get_flags") as patched_get_flags, patch(
            "dbt.contracts.graph.semantic_manifest.deprecations"
        ) as patched_deprecations:
            patched_get_flags.return_value.allow_legacy_mf_cumulative_type_params = False
            manifest.metrics[metric.unique_id] = metric
            sm_manifest = SemanticManifest(manifest)
            assert sm_manifest.validate()
            assert patched_deprecations.warn.call_count == 1
