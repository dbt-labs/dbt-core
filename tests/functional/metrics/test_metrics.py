import pytest

from dbt.tests.util import run_dbt, get_manifest
from dbt.exceptions import ParsingException


models__people_metrics_yml = """
version: 2

metrics:

  - model: "ref('people')"
    name: number_of_people
    description: Total count of people
    label: "Number of people"
    type: count
    sql: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - model: "ref('people')"
    name: collective_tenure
    description: Total number of years of team experience
    label: "Collective tenure"
    type: sum
    sql: tenure
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: is
        value: 'true'

"""

models__people_sql = """
select 1 as id, 'Drew' as first_name, 'Banin' as last_name, 'yellow' as favorite_color, true as loves_dbt, 5 as tenure, current_timestamp as created_at
union all
select 1 as id, 'Jeremy' as first_name, 'Cohen' as last_name, 'indigo' as favorite_color, true as loves_dbt, 4 as tenure, current_timestamp as created_at

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
        expected_metric_ids = ["metric.test.number_of_people", "metric.test.collective_tenure"]
        assert metric_ids == expected_metric_ids


invalid_models__people_metrics_yml = """
version: 2

metrics:

  - model: "ref(people)"
    name: number_of_people
    description: Total count of people
    label: "Number of people"
    type: count
    sql: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - model: "ref(people)"
    name: collective_tenure
    description: Total number of years of team experience
    label: "Collective tenure"
    type: sum
    sql: tenure
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: is
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


names_with_spaces_metrics_yml = """
version: 2

metrics:

  - model: "ref('people')"
    name: number of people
    description: Total count of people
    label: "Number of people"
    type: count
    sql: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - model: "ref('people')"
    name: collective tenure
    description: Total number of years of team experience
    label: "Collective tenure"
    type: sum
    sql: tenure
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: is
        value: 'true'

"""


class TestNamesWithSpaces:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_metrics.yml": names_with_spaces_metrics_yml,
            "people.sql": models__people_sql,
        }

    def test_names_with_spaces(self, project):
        with pytest.raises(ParsingException):
            run_dbt(["run"])
