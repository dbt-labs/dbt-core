from unittest.mock import patch

import pytest

from core.dbt.contracts.graph.manifest import Manifest
from core.dbt.contracts.graph.nodes import Metric, ModelNode
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.metric import (
    CumulativeTypeParams,
    MetricTimeWindow,
    MetricTypeParams,
)
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
            patched_get_flags.return_value.require_yaml_configuration_for_mf_time_spines = True
            sm_manifest = SemanticManifest(manifest)
            assert sm_manifest.validate()

    def test_require_yaml_configuration_for_mf_time_spines(
        self, manifest: Manifest, metricflow_time_spine_model: ModelNode
    ):
        with patch("dbt.contracts.graph.semantic_manifest.get_flags") as patched_get_flags, patch(
            "dbt.contracts.graph.semantic_manifest.deprecations"
        ) as patched_deprecations:
            patched_get_flags.return_value.require_yaml_configuration_for_mf_time_spines = False
            manifest.nodes[metricflow_time_spine_model.unique_id] = metricflow_time_spine_model
            sm_manifest = SemanticManifest(manifest)
            assert sm_manifest.validate()
            assert patched_deprecations.warn.call_count == 1

    @pytest.mark.parametrize(
        "metric_type_params, num_warns, should_error, flag_value",
        [
            (
                MetricTypeParams(grain_to_date=TimeGranularity.MONTH),
                1,
                False,
                False,
            ),
            (
                MetricTypeParams(
                    window=MetricTimeWindow(count=1, granularity=TimeGranularity.MONTH.value)
                ),
                1,
                False,
                False,
            ),
            (
                MetricTypeParams(
                    cumulative_type_params=CumulativeTypeParams(
                        grain_to_date=TimeGranularity.MONTH.value,
                    )
                ),
                0,
                False,
                False,
            ),
            (
                MetricTypeParams(
                    cumulative_type_params=CumulativeTypeParams(
                        window=MetricTimeWindow(count=1, granularity=TimeGranularity.MONTH.value),
                    )
                ),
                0,
                False,
                False,
            ),
            (
                MetricTypeParams(grain_to_date=TimeGranularity.MONTH),
                0,
                True,
                True,
            ),
            (
                MetricTypeParams(
                    window=MetricTimeWindow(count=1, granularity=TimeGranularity.MONTH.value)
                ),
                0,
                True,
                True,
            ),
            (
                MetricTypeParams(
                    cumulative_type_params=CumulativeTypeParams(
                        grain_to_date=TimeGranularity.MONTH.value,
                    )
                ),
                0,
                False,
                True,
            ),
            (
                MetricTypeParams(
                    cumulative_type_params=CumulativeTypeParams(
                        window=MetricTimeWindow(count=1, granularity=TimeGranularity.MONTH.value),
                    )
                ),
                0,
                False,
                True,
            ),
        ],
    )
    def test_deprecate_cumulative_type_params(
        self,
        manifest: Manifest,
        metric_type_params: MetricTypeParams,
        num_warns: int,
        should_error: bool,
        flag_value: bool,
    ):
        with patch("dbt.contracts.graph.semantic_manifest.get_flags") as patched_get_flags, patch(
            "dbt.contracts.graph.semantic_manifest.deprecations"
        ) as patched_deprecations:
            patched_get_flags.return_value.require_nested_cumulative_type_params = flag_value
            manifest.metrics["metric.test.my_metric"] = Metric(
                name="my_metric",
                type=MetricType.CUMULATIVE,
                type_params=metric_type_params,
                resource_type=NodeType.Metric,
                package_name="test",
                path="models/test/my_metric.yml",
                original_file_path="models/test/my_metric.yml",
                unique_id="metric.test.my_metric",
                fqn=["test", "my_metric"],
                description="My metric",
                label="My Metric",
            )
            sm_manifest = SemanticManifest(manifest)
            assert sm_manifest.validate() != should_error
            assert patched_deprecations.warn.call_count == num_warns
