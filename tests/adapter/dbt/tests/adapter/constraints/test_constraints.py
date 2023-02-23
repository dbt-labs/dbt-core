import pytest
import re

from dbt.tests.util import (
    run_dbt,
    get_manifest,
    run_dbt_and_capture,
    write_file,
    read_file,
    relation_from_name,
)

from dbt.tests.adapter.constraints.fixtures import (
    my_model_sql,
    my_model_wrong_order_sql,
    my_model_wrong_name_sql,
    my_model_with_nulls_sql,
    model_schema_yml,
)


class BaseConstraintsColumnsEqual:
    """
    dbt should catch these mismatches during its "preflight" checks.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    def test__constraints_wrong_column_order(self, project):
        results, log_output = run_dbt_and_capture(
            ["run", "-s", "my_model_wrong_order"], expect_pass=False
        )
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_order"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config is True

        expected_compile_error = "Please ensure the name, order, and number of columns in your `yml` file match the columns in your SQL file."
        expected_schema_file_columns = "Schema File Columns: ['ID', 'COLOR', 'DATE_DAY']"
        expected_sql_file_columns = "SQL File Columns: ['COLOR', 'ID', 'DATE_DAY']"

        assert expected_compile_error in log_output
        assert expected_schema_file_columns in log_output
        assert expected_sql_file_columns in log_output

    def test__constraints_wrong_column_names(self, project):
        results, log_output = run_dbt_and_capture(
            ["run", "-s", "my_model_wrong_name"], expect_pass=False
        )
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_name"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config is True

        expected_compile_error = "Please ensure the name, order, and number of columns in your `yml` file match the columns in your SQL file."
        expected_schema_file_columns = "Schema File Columns: ['ID', 'COLOR', 'DATE_DAY']"
        expected_sql_file_columns = "SQL File Columns: ['ERROR', 'COLOR', 'DATE_DAY']"

        assert expected_compile_error in log_output
        assert expected_schema_file_columns in log_output
        assert expected_sql_file_columns in log_output


# This is SUPER specific to Postgres, and will need replacing on other adapters
# TODO: make more generic
_expected_sql = """
create table {0} (
    id integer not null primary key check (id > 0) ,
    color text ,
    date_day date
) ;
insert into {0} (
    id ,
    color ,
    date_day
) (
    select
        1 as id,
        'blue' as color,
        cast('2019-01-01' as date) as date_day
);
"""


class BaseConstraintsRuntimeEnforcement:
    """
    These constraints pass muster for dbt's preflight checks. Make sure they're
    passed into the DDL statement. If they don't match up with the underlying data,
    the data platform should raise an error at runtime.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        relation = relation_from_name(project.adapter, "my_model")
        tmp_relation = relation.incorporate(path={"identifier": relation.identifier + "__dbt_tmp"})
        return _expected_sql.format(tmp_relation)

    @pytest.fixture(scope="class")
    def expected_color(self):
        return "blue"

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ['null value in column "id"', "violates not-null constraint"]

    def assert_expected_error_messages(self, error_message, expected_error_messages):
        assert all(msg in error_message for msg in expected_error_messages)

    def test__constraints_ddl(self, project, expected_sql):
        results = run_dbt(["run", "-s", "my_model"])
        assert len(results) == 1
        # TODO: consider refactoring this to introspect logs instead
        generated_sql = read_file("target", "run", "test", "models", "my_model.sql")

        generated_sql_check = re.sub(r"\s+", " ", generated_sql).lower().strip()
        expected_sql_check = re.sub(r"\s+", " ", expected_sql).lower().strip()
        assert (
            expected_sql_check == generated_sql_check
        ), f"""
-- GENERATED SQL
{generated_sql}

-- EXPECTED SQL
{expected_sql}
"""

    def test__constraints_enforcement_rollback(
        self, project, expected_color, expected_error_messages
    ):
        results = run_dbt(["run", "-s", "my_model"])
        assert len(results) == 1

        # Make a contract-breaking change to the model
        write_file(my_model_with_nulls_sql, "models", "my_model.sql")

        failing_results = run_dbt(["run", "-s", "my_model"], expect_pass=False)
        assert len(failing_results) == 1

        # Verify the previous table still exists
        relation = relation_from_name(project.adapter, "my_model")
        old_model_exists_sql = f"select * from {relation}"
        old_model_exists = project.run_sql(old_model_exists_sql, fetch="all")
        assert len(old_model_exists) == 1
        assert old_model_exists[0][1] == expected_color

        # Confirm this model was contracted
        # TODO: is this step really necessary?
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract
        assert contract_actual_config is True

        # Its result includes the expected error messages
        self.assert_expected_error_messages(failing_results[0].message, expected_error_messages)


class TestConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    pass


class TestConstraintsRuntimeEnforcement(BaseConstraintsRuntimeEnforcement):
    pass
