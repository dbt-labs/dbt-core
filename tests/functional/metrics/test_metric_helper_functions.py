import pytest

from dbt.tests.util import run_dbt, get_manifest
from dbt.contracts.graph.metrics import ResolvedMetricReference

metrics__yml = """
version: 2

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    type: count
    sql: "*"
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
    type: sum
    sql: tenure
    timestamp: created_at
    time_grains: [day, week, month]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

  - name: average_tenure
    label: "Average tenure"
    description: "The average tenure per person"
    type: expression
    sql: "{{metric('collective_tenure')}} / {{metric('number_of_people')}} "
    timestamp: created_at
    time_grains: [day, week, month]

  - name: average_tenure_plus_one
    label: "Average tenure"
    description: "The average tenure per person"
    type: expression
    sql: "{{metric('average_tenure')}} + 1 "
    timestamp: created_at
    time_grains: [day, week, month]
"""

models__people_sql = """
select 1 as id, 'Drew' as first_name, 'Banin' as last_name, 'yellow' as favorite_color, true as loves_dbt, 5 as tenure, current_timestamp as created_at
union all
select 1 as id, 'Jeremy' as first_name, 'Cohen' as last_name, 'indigo' as favorite_color, true as loves_dbt, 4 as tenure, current_timestamp as created_at
union all
select 1 as id, 'Callum' as first_name, 'McCann' as last_name, 'emerald' as favorite_color, true as loves_dbt, 0 as tenure, current_timestamp as created_at
"""


class TestMetricHelperFunctions:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metrics.yml": metrics__yml,
            "people.sql": models__people_sql,
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
