import pytest

from dbt.exceptions import CompilationError
from dbt.tests.util import run_dbt


metric_dupes_schema_yml = """
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

  - name: number_of_people
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


class TestDuplicateMetric:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": metric_dupes_schema_yml}

    def test_duplicate_metric(self, project):
        message = "dbt found two metrics with the name"
        with pytest.raises(CompilationError) as exc:
            run_dbt(["compile"])
        assert message in str(exc.value)
