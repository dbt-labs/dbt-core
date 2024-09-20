import pytest

from dbt.cli.exceptions import DbtUsageException
from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file

selectors_yml = """
    selectors:
      - name: test_selector
        description: Exclude everything
        default: true
        definition:
           method: package
           value: "foo"
    """

dbt_project_yml = """
name: test
profile: test
flags:
  send_anonymous_usage_stats: false
"""

dbt_project_yml_disabled_models = """
name: test
profile: test
flags:
  send_anonymous_usage_stats: false
models:
  +enabled: false
"""


class TestCompileInlineWithSelector:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "first_model.sql": "select 1 as id",
        }

    @pytest.fixture(scope="class")
    def selectors(self):
        return selectors_yml

    def test_inline_selectors(self, project):
        (results, log_output) = run_dbt_and_capture(
            ["compile", "--inline", "select * from {{ ref('first_model') }}"]
        )
        assert len(results) == 1
        assert "Compiled inline node is:" in log_output

        # Set all models to disabled, check that we still get inline result
        write_file(dbt_project_yml_disabled_models, project.project_root, "dbt_project.yml")
        (results, log_output) = run_dbt_and_capture(["compile", "--inline", "select 1 as id"])
        assert len(results) == 1

        # put back non-disabled dbt_project and check for mutually exclusive error message
        # for --select and --inline
        write_file(dbt_project_yml, project.project_root, "dbt_project.yml")
        with pytest.raises(DbtUsageException):
            run_dbt(["compile", "--select", "first_model", "--inline", "select 1 as id"])

        # check for mutually exclusive --selector and --inline
        with pytest.raises(DbtUsageException):
            run_dbt(["compile", "--selector", "test_selector", "--inline", "select 1 as id"])
