import pytest
import os
from dbt.tests.util import run_dbt
from dbt.exceptions import IncompatibleSchemaException

models__my_model_sql = """
select 1 as id
"""


class TestPreviousVersionState:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": models__my_model_sql}

    def compare_previous_state(
        self,
        project,
        compare_manifest_version,
        expect_pass,
    ):
        state_path = os.path.join(project.test_data_dir, f"previous/{compare_manifest_version}")
        cli_args = [
            "list",
            "--select",
            "state:modified",
            "--state",
            state_path,
        ]
        if expect_pass:
            results = run_dbt(cli_args, expect_pass=expect_pass)
            assert len(results) == 0
        else:
            with pytest.raises(IncompatibleSchemaException):
                run_dbt(cli_args, expect_pass=expect_pass)

    def test_compare_state_v5(self, project):
        self.compare_previous_state(project, "v5", True)

    def test_compare_state_v4(self, project):
        self.compare_previous_state(project, "v4", True)

    def test_compare_state_v3(self, project):
        self.compare_previous_state(project, "v3", False)

    def test_compare_state_v2(self, project):
        self.compare_previous_state(project, "v2", False)

    def test_compare_state_v1(self, project):
        self.compare_previous_state(project, "v1", False)
