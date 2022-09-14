import pytest

from dbt.tests.util import run_dbt, get_manifest
from dbt.exceptions import ParsingException


models_sql = """
select 1 as id
"""

second_model_sql = """
select 1 as id
"""

names_valid_yml = """
version: 2

exposures:
  - name: simple_exposure
    label: simple exposure
    type: dashboard
    depends_on:
      - ref('model')
    owner:
      email: something@example.com
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('model')
      - ref('second_model')
    owner:
      email: something@example.com
      name: Some name
    description: >
      A description of the complex exposure
    maturity: medium
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']
    url: http://example.com/notebook/1
"""


class TestBasicExposures:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "exposure.yml": names_valid_yml,
            "model.sql": models_sql,
            "second_model.sql": second_model_sql,
        }

    def test_names_with_spaces(self, project):
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        exposure_ids = list(manifest.exposures.keys())
        expected_exposure_ids = [
            "exposure.test.simple_exposure",
            "exposure.test.notebook_exposure",
        ]
        assert exposure_ids == expected_exposure_ids


names_with_spaces_yml = """
version: 2

exposures:
  - name: simple exposure
    label: simple exposure
    type: dashboard
    depends_on:
      - ref('model')
    owner:
      email: something@example.com
"""


class TestNamesWithSpaces:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "exposure.yml": names_with_spaces_yml,
            "model.sql": models_sql,
        }

    def test_names_with_spaces(self, project):
        with pytest.raises(ParsingException) as exc:
            run_dbt(["run"])
        assert "cannot contain spaces" in str(exc.value)


leading_number_yml = """
version: 2

exposures:
  - name: 1simple_exposure
    label: simple exposure
    type: dashboard
    depends_on:
      - ref('model')
    owner:
      email: something@example.com
"""


class TestNamesWithLeandingNumber:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "exposure.yml": leading_number_yml,
            "model.sql": models_sql,
        }

    def test_names_with_leading_number(self, project):
        with pytest.raises(ParsingException) as exc:
            run_dbt(["run"])
        assert "must begin with a letter" in str(exc.value)


long_name_yml = """
version: 2

exposures:
  - name: this_name_is_going_to_contain_more_than_126_characters_but_be_otherwise_acceptable_and_then_will_throw_an_error_which_I_expect_to_happen
    label: simple exposure
    type: dashboard
    depends_on:
      - ref('model')
    owner:
      email: something@example.com
"""


class TestLongName:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "exposure.yml": long_name_yml,
            "model.sql": models_sql,
        }

    def test_names_with_leading_number(self, project):
        with pytest.raises(ParsingException) as exc:
            run_dbt(["run"])
        assert "cannot contain more than 126 characters" in str(exc.value)
