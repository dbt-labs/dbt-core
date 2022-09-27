from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.util import run_dbt, check_relations_equal, get_relation_columns


class BaseArrayUtils(BaseUtils):
    def assert_columns_equal(self, project, expected_cols, actual_cols):
        assert (
            expected_cols == actual_cols
        ), f"Type difference detected: {expected_cols} vs. {actual_cols}"

    def test_expected_actual(self, project):
        run_dbt(["build"])

        # check contents equal
        check_relations_equal(project.adapter, ["expected", "actual"])

        # check types equal
        expected_cols = get_relation_columns(project.adapter, "expected")
        actual_cols = get_relation_columns(project.adapter, "actual")
        print(f"Expected: {expected_cols}")
        print(f"Actual: {actual_cols}")
        self.assert_columns_equal(project, expected_cols, actual_cols)
