import pytest

from dbt.tests.util import run_dbt, get_manifest
from dbt.contracts.graph.metrics import ResolvedMetricReference

from tests.functional.metrics.fixtures import models_people_sql, basic_metrics_yml


class TestMetricHelperFunctions:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metrics.yml": basic_metrics_yml,
            "people.sql": models_people_sql,
        }

    def test_expression_metric(
        self,
        project,
    ):

        # initial parse
        run_dbt(["compile"])

        # make sure all the metrics are in the manifest
        manifest = get_manifest(project.project_root)
        parsed_metric = manifest.metrics["metric.test.average_tenure_plus_one"]
        testing_metric = ResolvedMetricReference(parsed_metric, manifest, None)

        full_metric_dependency = set(testing_metric.full_metric_dependency())
        expected_full_metric_dependency = set(
            ["average_tenure_plus_one", "average_tenure", "collective_tenure", "number_of_people"]
        )
        assert full_metric_dependency == expected_full_metric_dependency

        base_metric_dependency = set(testing_metric.base_metric_dependency())
        expected_base_metric_dependency = set(["collective_tenure", "number_of_people"])
        assert base_metric_dependency == expected_base_metric_dependency

        derived_metric_dependency = set(testing_metric.derived_metric_dependency())
        expected_derived_metric_dependency = set(["average_tenure_plus_one", "average_tenure"])
        assert derived_metric_dependency == expected_derived_metric_dependency

        derived_metric_dependency_depth = list(testing_metric.derived_metric_dependency_depth())
        expected_derived_metric_dependency_depth = list(
            [{"average_tenure_plus_one": 1}, {"average_tenure": 2}]
        )
        assert derived_metric_dependency_depth == expected_derived_metric_dependency_depth
