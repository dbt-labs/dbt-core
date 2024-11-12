import pytest

from dbt.tests.util import run_dbt
from tests.functional.defer_state.fixtures import model_with_var_in_config_sql
from tests.functional.defer_state.test_modified_state import BaseModifiedState


class TestStateSelectionVarConfigLegacy(BaseModifiedState):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_var_in_config_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "state_modified_compare_more_unrendered_values": False,
            }
        }

    def test_change_var(self, project):
        # Generate ./state without changing variable value
        run_dbt(["run", "--vars", "DBT_TEST_STATE_MODIFIED: view"])
        self.copy_state()

        # Assert no false positive
        results = run_dbt(
            [
                "list",
                "-s",
                "state:modified",
                "--state",
                "./state",
                "--vars",
                "DBT_TEST_STATE_MODIFIED: view",
            ]
        )
        assert len(results) == 0

        # Change var and assert no false negative - legacy behaviour
        results = run_dbt(
            [
                "list",
                "-s",
                "state:modified",
                "--state",
                "./state",
                "--vars",
                "DBT_TEST_STATE_MODIFIED: table",
            ]
        )
        assert len(results) == 1


class TestStateSelectionVarConfig(BaseModifiedState):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_var_in_config_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "state_modified_compare_more_unrendered_values": True,
            }
        }

    def test_change_var(self, project):
        # Generate ./state without changing variable value
        run_dbt(["run", "--vars", "DBT_TEST_STATE_MODIFIED: view"])
        self.copy_state()

        # Assert no false positive
        results = run_dbt(
            [
                "list",
                "-s",
                "state:modified",
                "--state",
                "./state",
                "--vars",
                "DBT_TEST_STATE_MODIFIED: view",
            ]
        )
        assert len(results) == 0

        # Change var and assert no sensitivity to var changes -- new behaviour until state:modified.vars included in state:modified by default
        results = run_dbt(
            [
                "list",
                "-s",
                "state:modified",
                "--state",
                "./state",
                "--vars",
                "DBT_TEST_STATE_MODIFIED: table",
            ]
        )
        assert len(results) == 0
