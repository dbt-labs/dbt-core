from dbt.tests.util import run_dbt, check_relations_equal, get_relation_columns


class BaseDataTypeMacro:
    # make it possible to dynamically update the macro call with a namespace
    # (e.g.) 'dateadd', 'dbt.dateadd', 'dbt_utils.dateadd'
    def macro_namespace(self):
        return ""

    def interpolate_macro_namespace(self, model_sql, macro_name):
        macro_namespace = self.macro_namespace()
        return (
            model_sql.replace(f"{macro_name}(", f"{macro_namespace}.{macro_name}(")
            if macro_namespace
            else model_sql
        )

    def assert_columns_equal(self, project, expected_cols, actual_cols):
        assert (
            expected_cols == actual_cols
        ), f"Type difference detected: {expected_cols} vs. {actual_cols}"

    def test_check_types_assert_match(self, project):
        run_dbt(["build"])

        # check contents equal
        check_relations_equal(project.adapter, ["expected", "actual"])

        # check types equal
        expected_cols = get_relation_columns(project.adapter, "expected")
        actual_cols = get_relation_columns(project.adapter, "actual")
        print(f"Expected: {expected_cols}")
        print(f"Actual: {actual_cols}")
        self.assert_columns_equal(project, expected_cols, actual_cols)
