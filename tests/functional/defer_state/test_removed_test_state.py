import pytest

from dbt.exceptions import CompilationError
from dbt.tests.util import run_dbt
from tests.functional.defer_state.fixtures import (
    removed_test_model_sql,
    removed_test_schema_yml,
    sample_test_sql,
)


class TestRemovedGenericTest:
    """Test that removing a generic test while it's still referenced gives a clear error message."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": removed_test_model_sql,
            "schema.yml": removed_test_schema_yml,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "generic": {
                "sample_test.sql": sample_test_sql,
            }
        }

    def copy_state(self, project):
        import os
        import shutil

        if not os.path.exists(f"{project.project_root}/state"):
            os.makedirs(f"{project.project_root}/state")
        shutil.copyfile(
            f"{project.project_root}/target/manifest.json",
            f"{project.project_root}/state/manifest.json",
        )

    def test_removed_generic_test_with_state_modified(self, project):
        """
        Test that state:modified selector handles missing test macros gracefully.

        Issue #10630: When a generic test is removed but still referenced, using
        --select state:modified would crash with KeyError: None.

        Solution: We check for None macro_uid in the state selector and raise a clear error.
        """
        # Initial run - everything works
        results = run_dbt(["run"])
        assert len(results) == 1

        # Save state
        self.copy_state(project)

        # Remove the generic test file but keep the reference in schema.yml
        import os

        test_file_path = os.path.join(project.project_root, "tests", "generic", "sample_test.sql")
        if os.path.exists(test_file_path):
            os.remove(test_file_path)

        # The key bug fix: dbt run --select state:modified used to crash with KeyError: None
        # After fix: it should give a clear compilation error during the selection phase
        with pytest.raises(CompilationError, match="does not exist|macro or test"):
            run_dbt(["run", "--select", "state:modified", "--state", "state"])


class TestRemovedGenericTestStateModifiedGracefulError:
    """Test that state:modified selector handles missing test macros gracefully."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": removed_test_model_sql,
            "schema.yml": removed_test_schema_yml,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "generic": {
                "sample_test.sql": sample_test_sql,
            }
        }

    def copy_state(self, project):
        import os
        import shutil

        if not os.path.exists(f"{project.project_root}/state"):
            os.makedirs(f"{project.project_root}/state")
        shutil.copyfile(
            f"{project.project_root}/target/manifest.json",
            f"{project.project_root}/state/manifest.json",
        )

    def test_list_with_state_modified_after_test_removal(self, project):
        """
        Test that state:modified selector handles missing test macros gracefully.
        This exercises the selector_methods.py code path that was failing with KeyError: None.
        """
        # Initial run - everything works
        results = run_dbt(["run"])
        assert len(results) == 1

        # Save state
        self.copy_state(project)

        # Remove the generic test file but keep the reference in schema.yml
        import os

        test_file_path = os.path.join(project.project_root, "tests", "generic", "sample_test.sql")
        if os.path.exists(test_file_path):
            os.remove(test_file_path)

        # dbt run with state:modified should not crash with KeyError: None
        # After the fix, it should give a clear CompilationError about the missing test
        # Previously this crashed with KeyError: None in recursively_check_macros_modified
        with pytest.raises(
            CompilationError, match="sample_test|does not exist|macro or generic test"
        ):
            run_dbt(["run", "--select", "state:modified", "--state", "state"])
