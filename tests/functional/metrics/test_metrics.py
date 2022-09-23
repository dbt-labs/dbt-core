import pytest

from dbt.tests.util import run_dbt, get_manifest
from dbt.exceptions import ParsingException

from tests.functional.metrics.fixture_metrics import mock_purchase_data_csv, models__people_sql


models__people_metrics_yml = """
version: 2

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - name: collective_tenure
    label: "Collective tenure"
    description: Total number of years of team experience
    model: "ref('people')"
    calculation_method: sum
    expression: tenure
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

  - name: collective_window
    label: "Collective window"
    description: Testing window
    model: "ref('people')"
    calculation_method: sum
    expression: tenure
    timestamp: created_at
    time_grains: [day]
    window:
      count: 14
      period: day
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

"""


class TestSimpleMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": models__people_metrics_yml,
            "people.sql": models__people_sql,
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


invalid_models__people_metrics_yml = """
version: 2

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref(people)"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - name: collective_tenure
    label: "Collective tenure"
    description: Total number of years of team experience
    model: "ref(people)"
    calculation_method: sum
    expression: tenure
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

"""


class TestInvalidRefMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": invalid_models__people_metrics_yml,
            "people.sql": models__people_sql,
        }

    # tests that we get a ParsingException with an invalid model ref, where
    # the model name does not have quotes
    def test_simple_metric(
        self,
        project,
    ):
        # initial run
        with pytest.raises(ParsingException):
            run_dbt(["run"])


invalid_metrics__missing_model_yml = """
version: 2

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - name: collective_tenure
    label: "Collective tenure"
    description: Total number of years of team experience
    calculation_method: sum
    expression: tenure
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

"""


class TestInvalidMetricMissingModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": invalid_metrics__missing_model_yml,
            "people.sql": models__people_sql,
        }

    # tests that we get a ParsingException with an invalid model ref, where
    # the model name does not have quotes
    def test_simple_metric(
        self,
        project,
    ):
        # initial run
        with pytest.raises(ParsingException):
            run_dbt(["run"])


names_with_spaces_metrics_yml = """
version: 2

metrics:

  - name: number of people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

"""


class TestNamesWithSpaces:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": names_with_spaces_metrics_yml,
            "people.sql": models__people_sql,
        }

    def test_names_with_spaces(self, project):
        with pytest.raises(ParsingException) as exc:
            run_dbt(["run"])
        assert "cannot contain spaces" in str(exc.value)


names_with_special_chars_metrics_yml = """
version: 2

metrics:

  - name: number_of_people!
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

"""


class TestNamesWithSpecialChar:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": names_with_special_chars_metrics_yml,
            "people.sql": models__people_sql,
        }

    def test_names_with_special_char(self, project):
        with pytest.raises(ParsingException) as exc:
            run_dbt(["run"])
        assert "must contain only letters, numbers and underscores" in str(exc.value)


names_with_leading_numeric_metrics_yml = """
version: 2

metrics:

  - name: 1_number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

"""


class TestNamesWithLeandingNumber:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": names_with_leading_numeric_metrics_yml,
            "people.sql": models__people_sql,
        }

    def test_names_with_leading_number(self, project):
        with pytest.raises(ParsingException) as exc:
            run_dbt(["run"])
        assert "must begin with a letter" in str(exc.value)


long_name_metrics_yml = """
version: 2

metrics:

  - name: this_name_is_going_to_contain_more_than_250_characters_but_be_otherwise_acceptable_and_then_will_throw_an_error_which_I_expect_to_happen_and_repeat_this_name_is_going_to_contain_more_than_250_characters_but_be_otherwise_acceptable_and_then_will_throw_an_error_which_I_expect_to_happen
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

"""


class TestLongName:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": long_name_metrics_yml,
            "people.sql": models__people_sql,
        }

    def test_long_name(self, project):
        with pytest.raises(ParsingException) as exc:
            run_dbt(["run"])
        assert "cannot contain more than 250 characters" in str(exc.value)


downstream_model_sql = """
-- this model will depend on these three metrics
{% set some_metrics = [
    metric('count_orders'),
    metric('sum_order_revenue'),
    metric('average_order_value')
] %}

/*
{% if not execute %}

    -- the only properties available to us at 'parse' time are:
    --      'metric_name'
    --      'package_name' (None if same package)

    {% set metric_names = [] %}
    {% for m in some_metrics %}
        {% do metric_names.append(m.metric_name) %}
    {% endfor %}

    -- this config does nothing, but it lets us check these values below
    {{ config(metric_names = metric_names) }}

{% else %}

    -- these are the properties available to us at 'execution' time

    {% for m in some_metrics %}
        name: {{ m.name }}
        label: {{ m.label }}
        calculation_method: {{ m.calculation_method }}
        expression: {{ m.expression }}
        timestamp: {{ m.timestamp }}
        time_grains: {{ m.time_grains }}
        dimensions: {{ m.dimensions }}
        filters: {{ m.filters }}
        window: {{ m.window }}
    {% endfor %}

{% endif %}

select 1 as id
"""

invalid_derived_metric__contains_model_yml = """
version: 2
metrics:
    - name: count_orders
      label: Count orders
      model: ref('mock_purchase_data')

      calculation_method: count
      expression: "*"
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type

    - name: sum_order_revenue
      label: Total order revenue
      model: ref('mock_purchase_data')

      calculation_method: sum
      expression: "payment_total"
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type

    - name: average_order_value
      label: Average Order Value

      calculation_method: derived
      expression:  "{{metric('sum_order_revenue')}} / {{metric('count_orders')}} "
      model: ref('mock_purchase_data')
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type
"""


class TestInvalidDerivedMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "derived_metric.yml": invalid_derived_metric__contains_model_yml,
            "downstream_model.sql": downstream_model_sql,
        }

    def test_invalid_derived_metrics(self, project):
        with pytest.raises(ParsingException):
            run_dbt(["run"])


derived_metric_yml = """
version: 2
metrics:
    - name: count_orders
      label: Count orders
      model: ref('mock_purchase_data')

      calculation_method: count
      expression: "*"
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type

    - name: sum_order_revenue
      label: Total order revenue
      model: ref('mock_purchase_data')

      calculation_method: sum
      expression: "payment_total"
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type

    - name: average_order_value
      label: Average Order Value

      calculation_method: derived
      expression:  "{{metric('sum_order_revenue')}} / {{metric('count_orders')}} "
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type
"""


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


derived_metric_old_attr_names_yml = """
version: 2
metrics:
    - name: count_orders
      label: Count orders
      model: ref('mock_purchase_data')

      type: count
      sql: "*"
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type

    - name: sum_order_revenue
      label: Total order revenue
      model: ref('mock_purchase_data')

      type: sum
      sql: "payment_total"
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type

    - name: average_order_value
      label: Average Order Value

      type: expression
      sql:  "{{metric('sum_order_revenue')}} / {{metric('count_orders')}} "
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type
"""


class TestDerivedMetricOldAttrNames(TestDerivedMetric):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "derived_metric.yml": derived_metric_old_attr_names_yml,
            "downstream_model.sql": downstream_model_sql,
        }
