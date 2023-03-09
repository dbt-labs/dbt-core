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
    my_model_data_type_sql,
    model_data_type_schema_yml,
    my_model_view_wrong_order_sql,
    my_model_view_wrong_name_sql,
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

    @pytest.fixture
    def string_type(self):
        return "TEXT"

    @pytest.fixture
    def int_type(self):
        return "INT"

    @pytest.fixture
    def schema_int_type(self, int_type):
        return int_type

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["cast('2019-01-01' as date)", "date", "DATE"],
            ["true", "bool", "BOOL"],
            ["'2013-11-03 00:00:00-07'::timestamptz", "timestamptz", "DATETIMETZ"],
            ["'2013-11-03 00:00:00-07'::timestamp", "timestamp", "DATETIME"],
            ["ARRAY['a','b','c']", "text[]", "STRINGARRAY"],
            ["ARRAY[1,2,3]", "int[]", "INTEGERARRAY"],
            ["'1'::numeric", "numeric", "DECIMAL"],
            ["""'{"bar": "baz", "balance": 7.77, "active": false}'::json""", "json", "JSON"],
        ]

    def test__constraints_wrong_column_order(self, project, string_type, int_type):
        results, log_output = run_dbt_and_capture(
            ["run", "-s", "my_model_wrong_order"], expect_pass=False
        )
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_order"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config is True

        expected_compile_error = "Please ensure the name, data_type, order, and number of columns in your `yml` file match the columns in your SQL file."
        expected_schema_file_columns = (
            f"Schema File Columns: id {int_type}, color {string_type}, date_day DATE"
        )
        expected_sql_file_columns = (
            f"SQL File Columns: color {string_type}, id {int_type}, date_day DATE"
        )

        assert expected_compile_error in log_output
        assert expected_schema_file_columns in log_output
        assert expected_sql_file_columns in log_output

    def test__constraints_wrong_column_names(self, project, string_type, int_type):
        results, log_output = run_dbt_and_capture(
            ["run", "-s", "my_model_wrong_name"], expect_pass=False
        )
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_name"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config is True

        expected_compile_error = "Please ensure the name, data_type, order, and number of columns in your `yml` file match the columns in your SQL file."
        expected_schema_file_columns = (
            f"Schema File Columns: id {int_type}, color {string_type}, date_day DATE"
        )
        expected_sql_file_columns = (
            f"SQL File Columns: error {int_type}, color {string_type}, date_day DATE"
        )

        assert expected_compile_error in log_output
        assert expected_schema_file_columns in log_output
        assert expected_sql_file_columns in log_output

    def test__constraints_wrong_column_data_types(
        self, project, string_type, int_type, schema_int_type, data_types
    ):
        for (sql_column_value, schema_data_type, error_data_type) in data_types:
            # Write parametrized data_type to sql file
            write_file(
                my_model_data_type_sql.format(sql_value=sql_column_value),
                "models",
                "my_model_data_type.sql",
            )

            # Write wrong data_type to corresponding schema file
            # Write integer type for all schema yaml values except when testing integer type itself
            wrong_schema_data_type = (
                schema_int_type
                if schema_data_type.upper() != schema_int_type.upper()
                else string_type
            )
            wrong_schema_error_data_type = (
                int_type if schema_data_type.upper() != schema_int_type.upper() else string_type
            )
            write_file(
                model_data_type_schema_yml.format(data_type=wrong_schema_data_type),
                "models",
                "constraints_schema.yml",
            )

            results, log_output = run_dbt_and_capture(
                ["run", "-s", "my_model_data_type"], expect_pass=False
            )
            manifest = get_manifest(project.project_root)
            model_id = "model.test.my_model_data_type"
            my_model_config = manifest.nodes[model_id].config
            contract_actual_config = my_model_config.contract

            assert contract_actual_config is True

            expected_compile_error = "Please ensure the name, data_type, order, and number of columns in your `yml` file match the columns in your SQL file."
            expected_sql_file_columns = (
                f"SQL File Columns: wrong_data_type_column_name {error_data_type}"
            )
            expected_schema_file_columns = (
                f"Schema File Columns: wrong_data_type_column_name {wrong_schema_error_data_type}"
            )

            assert expected_compile_error in log_output
            assert expected_schema_file_columns in log_output
            assert expected_sql_file_columns in log_output

    def test__constraints_correct_column_data_types(self, project, data_types):
        for (sql_column_value, schema_data_type, _) in data_types:
            # Write parametrized data_type to sql file
            write_file(
                my_model_data_type_sql.format(sql_value=sql_column_value),
                "models",
                "my_model_data_type.sql",
            )
            # Write correct data_type to corresponding schema file
            write_file(
                model_data_type_schema_yml.format(data_type=schema_data_type),
                "models",
                "constraints_schema.yml",
            )

            run_dbt(["run", "-s", "my_model_data_type"])

            manifest = get_manifest(project.project_root)
            model_id = "model.test.my_model_data_type"
            my_model_config = manifest.nodes[model_id].config
            contract_actual_config = my_model_config.contract

            assert contract_actual_config is True


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


class BaseTableConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }


class BaseViewConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }


class TestTableConstraintsColumnsEqual(BaseTableConstraintsColumnsEqual):
    pass


class TestViewConstraintsColumnsEqual(BaseViewConstraintsColumnsEqual):
    pass


class TestConstraintsRuntimeEnforcement(BaseConstraintsRuntimeEnforcement):
    pass
