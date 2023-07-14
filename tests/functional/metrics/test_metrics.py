import pytest

from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import ParsingError
from dbt.tests.util import run_dbt, get_manifest


from tests.functional.metrics.fixtures import (
    mock_purchase_data_csv,
    models_people_sql,
    models_people_metrics_yml,
    invalid_models_people_metrics_yml,
    invalid_metrics_missing_model_yml,
    invalid_metrics_missing_expression_yml,
    names_with_spaces_metrics_yml,
    names_with_special_chars_metrics_yml,
    names_with_leading_numeric_metrics_yml,
    long_name_metrics_yml,
    downstream_model_sql,
    invalid_derived_metric_contains_model_yml,
    derived_metric_yml,
    invalid_metric_without_timestamp_with_time_grains_yml,
    invalid_metric_without_timestamp_with_window_yml,
    metricflow_time_spine_sql,
    semantic_model_people_yml,
    semantic_model_purchasing_yml,
    purchasing_model_sql,
)


class TestSimpleMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": models_people_metrics_yml,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_model_people.yml": semantic_model_people_yml,
            "people.sql": models_people_sql,
        }

    def test_simple_metric(
        self,
        project,
    ):
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert result.success
        assert isinstance(result.result, Manifest)
        manifest = get_manifest(project.project_root)
        metric_ids = list(manifest.metrics.keys())
        expected_metric_ids = [
            "metric.test.number_of_people",
            "metric.test.collective_tenure",
            "metric.test.collective_window",
            "metric.test.average_tenure",
            "metric.test.average_tenure_minus_people",
        ]
        assert metric_ids == expected_metric_ids

        assert (
            len(manifest.metrics["metric.test.number_of_people"].type_params.input_measures) == 1
        )
        assert (
            len(manifest.metrics["metric.test.collective_tenure"].type_params.input_measures) == 1
        )
        assert (
            len(manifest.metrics["metric.test.collective_window"].type_params.input_measures) == 1
        )
        assert len(manifest.metrics["metric.test.average_tenure"].type_params.input_measures) == 2
        assert (
            len(
                manifest.metrics[
                    "metric.test.average_tenure_minus_people"
                ].type_params.input_measures
            )
            == 3
        )


class TestInvalidRefMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": invalid_models_people_metrics_yml,
            "people.sql": models_people_sql,
        }

    # tests that we get a ParsingError with an invalid model ref, where
    # the model name does not have quotes
    def test_simple_metric(
        self,
        project,
    ):
        # initial run
        with pytest.raises(ParsingError):
            run_dbt(["run"])


class TestInvalidMetricMissingModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": invalid_metrics_missing_model_yml,
            "people.sql": models_people_sql,
        }

    # tests that we get a ParsingError with an invalid model ref, where
    # the model name does not have quotes
    def test_simple_metric(
        self,
        project,
    ):
        # initial run
        with pytest.raises(ParsingError):
            run_dbt(["run"])


class TestInvalidMetricMissingExpression:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": invalid_metrics_missing_expression_yml,
            "people.sql": models_people_sql,
        }

    # tests that we get a ParsingError with a missing expression
    def test_simple_metric(
        self,
        project,
    ):
        # initial run
        with pytest.raises(ParsingError):
            run_dbt(["run"])


class TestNamesWithSpaces:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": names_with_spaces_metrics_yml,
            "people.sql": models_people_sql,
        }

    def test_names_with_spaces(self, project):
        with pytest.raises(ParsingError) as exc:
            run_dbt(["run"])
        assert "cannot contain spaces" in str(exc.value)


class TestNamesWithSpecialChar:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": names_with_special_chars_metrics_yml,
            "people.sql": models_people_sql,
        }

    def test_names_with_special_char(self, project):
        with pytest.raises(ParsingError) as exc:
            run_dbt(["run"])
        assert "must contain only letters, numbers and underscores" in str(exc.value)


class TestNamesWithLeandingNumber:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": names_with_leading_numeric_metrics_yml,
            "people.sql": models_people_sql,
        }

    def test_names_with_leading_number(self, project):
        with pytest.raises(ParsingError) as exc:
            run_dbt(["run"])
        assert "must begin with a letter" in str(exc.value)


class TestLongName:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": long_name_metrics_yml,
            "people.sql": models_people_sql,
        }

    def test_long_name(self, project):
        with pytest.raises(ParsingError) as exc:
            run_dbt(["run"])
        assert "cannot contain more than 250 characters" in str(exc.value)


class TestInvalidDerivedMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "derived_metric.yml": invalid_derived_metric_contains_model_yml,
            "downstream_model.sql": downstream_model_sql,
        }

    def test_invalid_derived_metrics(self, project):
        with pytest.raises(ParsingError):
            run_dbt(["run"])


class TestMetricDependsOn:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models_people_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_models.yml": semantic_model_people_yml,
            "people_metrics.yml": models_people_metrics_yml,
        }

    def test_metric_depends_on(self, project):
        manifest = run_dbt(["parse"])
        assert isinstance(manifest, Manifest)

        expected_depends_on_for_number_of_people = ["semantic_model.test.semantic_people"]
        expected_depends_on_for_average_tenure = [
            "metric.test.collective_tenure",
            "metric.test.number_of_people",
        ]

        number_of_people_metric = manifest.metrics["metric.test.number_of_people"]
        assert number_of_people_metric.depends_on.nodes == expected_depends_on_for_number_of_people

        average_tenure_metric = manifest.metrics["metric.test.average_tenure"]
        assert average_tenure_metric.depends_on.nodes == expected_depends_on_for_average_tenure


class TestDerivedMetric:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "downstream_model.sql": downstream_model_sql,
            "purchasing.sql": purchasing_model_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_models.yml": semantic_model_purchasing_yml,
            "derived_metric.yml": derived_metric_yml,
        }

    # not strictly necessary to use "real" mock data for this test
    # we just want to make sure that the 'metric' calls match our expectations
    # but this sort of thing is possible, to have actual data flow through and validate results
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "mock_purchase_data.csv": mock_purchase_data_csv,
        }

    def test_derived_metric(
        self,
        project,
    ):
        # initial parse
        results = run_dbt(["parse"])

        # make sure all the metrics are in the manifest
        manifest = get_manifest(project.project_root)
        metric_ids = list(manifest.metrics.keys())
        expected_metric_ids = [
            "metric.test.count_orders",
            "metric.test.sum_order_revenue",
            "metric.test.average_order_value",
        ]
        assert metric_ids == expected_metric_ids

        # make sure the downstream_model depends on these metrics
        metric_names = ["average_order_value", "count_orders", "sum_order_revenue"]
        downstream_model = manifest.nodes["model.test.downstream_model"]
        assert sorted(downstream_model.metrics) == [[metric_name] for metric_name in metric_names]
        assert sorted(downstream_model.depends_on.nodes) == [
            "metric.test.average_order_value",
            "metric.test.count_orders",
            "metric.test.sum_order_revenue",
        ]
        assert sorted(downstream_model.config["metric_names"]) == metric_names

        # make sure the 'expression' metric depends on the two upstream metrics
        derived_metric = manifest.metrics["metric.test.average_order_value"]
        assert sorted(derived_metric.depends_on.nodes) == [
            "metric.test.count_orders",
            "metric.test.sum_order_revenue",
        ]

        # actually compile
        results = run_dbt(["compile", "--select", "downstream_model"])
        compiled_code = results[0].node.compiled_code

        # make sure all these metrics properties show up in compiled SQL
        for metric_name in manifest.metrics:
            parsed_metric_node = manifest.metrics[metric_name]
            for property in [
                "name",
                "label",
                "type",
                "type_params",
                "filter",
            ]:
                expected_value = getattr(parsed_metric_node, property)
                assert f"{property}: {expected_value}" in compiled_code


class TestInvalidTimestampTimeGrainsMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": invalid_metric_without_timestamp_with_time_grains_yml,
            "people.sql": models_people_sql,
        }

    # Tests that we get a ParsingError with an invalid metric definition.
    # This metric definition is missing timestamp but HAS a time_grains property
    def test_simple_metric(
        self,
        project,
    ):
        # initial run
        with pytest.raises(ParsingError):
            run_dbt(["run"])


class TestInvalidTimestampWindowMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": invalid_metric_without_timestamp_with_window_yml,
            "people.sql": models_people_sql,
        }

    # Tests that we get a ParsingError with an invalid metric definition.
    # This metric definition is missing timestamp but HAS a window property
    def test_simple_metric(
        self,
        project,
    ):
        # initial run
        with pytest.raises(ParsingError):
            run_dbt(["run"])
