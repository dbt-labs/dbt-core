import pytest

from dbt.tests.util import run_dbt
from dbt.exceptions import ParsingException


from tests.functional.metrics.fixture_metrics import models__people_sql


names_with_spaces_yml = """
version: 2

exposure:

  - name: number of people
    label: "Number of people"
    description: Total count of people
"""


class TestNamesWithSpaces:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_exposure.yml": names_with_spaces_yml,
            "people.sql": models__people_sql,
        }

    def test_names_with_spaces(self, project):
        with pytest.raises(ParsingException):
            run_dbt(["run"])
