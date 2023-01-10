import pytest

from dbt.tests.util import run_dbt, get_manifest
from dbt.exceptions import ParsingError


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
    derived_metric_old_attr_names_yml,
    metric_without_timestamp_or_timegrains_yml,
    invalid_metric_without_timestamp_with_time_grains_yml,
    invalid_metric_without_timestamp_with_window_yml
)


class TestSimpleMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": models_people_metrics_yml,
            "people.sql": models_people_sql,
        }

    def test_simple_metric(
        self,
        project,
    ):
        # initial run
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        metric_ids = list(manifest.metrics.keys())
        expected_metric_ids = [
            "metric.test.number_of_people",
            "metric.test.collective_tenure",
            "metric.test.collective_window",
        ]
        assert metric_ids == expected_metric_ids


class TestSimpleMetricsNoTimestamp:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": metric_without_timestamp_or_timegrains_yml,
            "people.sql": models_people_sql,
        }

    def test_simple_metric_no_timestamp(
        self,
        project,
    ):
        # initial run
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        metric_ids = list(manifest.metrics.keys())
        expected_metric_ids = [
            "metric.test.number_of_people",
        ]
        assert metric_ids == expected_metric_ids

        # make sure the 'expression' metric depends on the two upstream metrics
        metric_test = manifest.metrics["metric.test.number_of_people"]
        assert metric_test.timestamp is None


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


class TestDerivedMetric:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "derived_metric.yml": derived_metric_yml,
            "downstream_model.sql": downstream_model_sql,
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
        assert sorted(derived_metric.metrics) == [["count_orders"], ["sum_order_revenue"]]
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
                "calculation_method",
                "expression",
                "timestamp",
                "time_grains",
                "dimensions",
                "filters",
                "window",
            ]:
                expected_value = getattr(parsed_metric_node, property)
                assert f"{property}: {expected_value}" in compiled_code


class TestDerivedMetricOldAttrNames(TestDerivedMetric):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "derived_metric.yml": derived_metric_old_attr_names_yml,
            "downstream_model.sql": downstream_model_sql,
        }


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
