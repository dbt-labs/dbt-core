import pytest
from dbt.contracts.graph.model_config import MetricConfig
from dbt.exceptions import CompilationException
from dbt.tests.util import run_dbt, update_config_file, get_manifest


from tests.functional.metrics.fixture_metrics import models__people_sql


class MetricConfigTests:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self):
        pytest.expected_config = MetricConfig(
            enabled=True,
        )


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
    expression: "*"
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

"""


# Test enabled config in dbt_project.yml
class TestMetricEnabledConfigProjectLevel(MetricConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models__people_sql,
            "schema.yml": models__people_metrics_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "metrics": {
                "number_of_people": {
                    "enabled": True,
                },
            }
        }

    def test_enabled_metric_config_dbt_project(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "metric.test.number_of_people" in manifest.metrics

        new_enabled_config = {
            "metrics": {
                "test": {
                    "number_of_people": {
                        "enabled": False,
                    },
                }
            }
        }
        update_config_file(new_enabled_config, project.project_root, "dbt_project.yml")
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "metric.test.number_of_people" not in manifest.metrics
        assert "metric.test.collective_tenure" in manifest.metrics


disabled_metric_level__schema_yml = """
version: 2

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    config:
      enabled: False
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
    expression: "*"
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

"""


# Test enabled config at metrics level in yml file
class TestConfigYamlMetricLevel(MetricConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models__people_sql,
            "schema.yml": disabled_metric_level__schema_yml,
        }

    def test_metric_config_yaml_metric_level(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "metric.test.number_of_people" not in manifest.metrics
        assert "metric.test.collective_tenure" in manifest.metrics


enabled_metric_level__schema_yml = """
version: 2

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    config:
      enabled: True
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
    expression: "*"
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

"""


# Test inheritence - set configs at project and metric level - expect metric level to win
class TestMetricConfigsInheritence(MetricConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models__people_sql,
            "schema.yml": enabled_metric_level__schema_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"metrics": {"enabled": False}}

    def test_metrics_all_configs(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        # This should be overridden
        assert "metric.test.number_of_people" in manifest.metrics
        # This should stay disabled
        assert "metric.test.collective_tenure" not in manifest.metrics

        config_test_table = manifest.metrics.get("metric.test.number_of_people").config

        assert isinstance(config_test_table, MetricConfig)
        assert config_test_table == pytest.expected_config


models__people_metrics_sql = """
-- this model will depend on these two metrics
{% set some_metrics = [
    metric('number_of_people'),
    metric('collective_tenure')
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


# Test CompilationException if a model references a disabled metric
class TestDisabledMetricRef(MetricConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models__people_sql,
            "people_metrics.sql": models__people_metrics_sql,
            "schema.yml": models__people_metrics_yml,
        }

    def test_disabled_metric_ref_model(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "metric.test.number_of_people" in manifest.metrics
        assert "metric.test.collective_tenure" in manifest.metrics
        assert "model.test.people_metrics" in manifest.nodes

        new_enabled_config = {
            "metrics": {
                "test": {
                    "number_of_people": {
                        "enabled": False,
                    },
                }
            }
        }

        update_config_file(new_enabled_config, project.project_root, "dbt_project.yml")
        with pytest.raises(CompilationException):
            run_dbt(["parse"])
