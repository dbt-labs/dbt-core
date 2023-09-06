import pytest

from dbt.exceptions import DbtRuntimeError, Exception as DbtException
from dbt.tests.util import run_dbt_and_capture, run_dbt
from tests.functional.show.fixtures import (
    models__second_ephemeral_model,
    seeds__sample_seed,
    models__sample_model,
    models__sample_number_model,
    models__sample_number_model_with_nulls,
    models__second_model,
    models__ephemeral_model,
    schema_yml,
    models__sql_header,
    private_model_yml,
)


class TestShow:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_model.sql": models__sample_model,
            "sample_number_model.sql": models__sample_number_model,
            "sample_number_model_with_nulls.sql": models__sample_number_model_with_nulls,
            "second_model.sql": models__second_model,
            "ephemeral_model.sql": models__ephemeral_model,
            "sql_header.sql": models__sql_header,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"sample_seed.csv": seeds__sample_seed}

    def test_none(self, project):
        with pytest.raises(
            DbtRuntimeError, match="Either --select or --inline must be passed to show"
        ):
            run_dbt(["seed"])
            run_dbt(["show"])

    def test_select_model_text(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(["show", "--select", "second_model"])
        assert "Previewing node 'sample_model'" not in log_output
        assert "Previewing node 'second_model'" in log_output
        assert "col_one" in log_output
        assert "col_two" in log_output
        assert "answer" in log_output

    def test_select_multiple_model_text(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_model second_model"]
        )
        assert "Previewing node 'sample_model'" in log_output
        assert "sample_num" in log_output
        assert "sample_bool" in log_output

    def test_select_single_model_json(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_model", "--output", "json"]
        )
        assert "Previewing node 'sample_model'" not in log_output
        assert "sample_num" in log_output
        assert "sample_bool" in log_output

    def test_numeric_values(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_number_model", "--output", "json"]
        )
        assert "Previewing node 'sample_number_model'" not in log_output
        assert "1.0" not in log_output
        assert "1" in log_output
        assert "3.0" in log_output
        assert "4.3" in log_output
        assert "5" in log_output
        assert "5.0" not in log_output

    def test_numeric_values_with_nulls(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_number_model_with_nulls", "--output", "json"]
        )
        assert "Previewing node 'sample_number_model_with_nulls'" not in log_output
        assert "1.0" not in log_output
        assert "1" in log_output
        assert "3.0" in log_output
        assert "4.3" in log_output
        assert "5" in log_output
        assert "5.0" not in log_output

    def test_inline_pass(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--inline", "select * from {{ ref('sample_model') }}"]
        )
        assert "Previewing inline node" in log_output
        assert "sample_num" in log_output
        assert "sample_bool" in log_output

    def test_inline_fail(self, project):
        with pytest.raises(DbtException, match="Error parsing inline query"):
            run_dbt(["show", "--inline", "select * from {{ ref('third_model') }}"])

    def test_inline_fail_database_error(self, project):
        with pytest.raises(DbtRuntimeError, match="Database Error"):
            run_dbt(["show", "--inline", "slect asdlkjfsld;j"])

    def test_ephemeral_model(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(["show", "--select", "ephemeral_model"])
        assert "col_deci" in log_output

    def test_second_ephemeral_model(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--inline", models__second_ephemeral_model]
        )
        assert "col_hundo" in log_output

    @pytest.mark.parametrize(
        "args,expected",
        [
            ([], 5),  # default limit
            (["--limit", 3], 3),  # fetch 3 rows
            (["--limit", -1], 7),  # fetch all rows
        ],
    )
    def test_limit(self, project, args, expected):
        run_dbt(["build"])
        dbt_args = ["show", "--inline", models__second_ephemeral_model, *args]
        results, log_output = run_dbt_and_capture(dbt_args)
        assert len(results.results[0].agate_table) == expected

    def test_seed(self, project):
        (results, log_output) = run_dbt_and_capture(["show", "--select", "sample_seed"])
        assert "Previewing node 'sample_seed'" in log_output

    def test_sql_header(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(["show", "--select", "sql_header"])
        assert "Asia/Kolkata" in log_output


class TestShowModelVersions:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "sample_model.sql": models__sample_model,
            "sample_model_v2.sql": models__second_model,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"sample_seed.csv": seeds__sample_seed}

    def test_version_unspecified(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(["show", "--select", "sample_model"])
        assert "Previewing node 'sample_model.v1'" in log_output
        assert "Previewing node 'sample_model.v2'" in log_output

    def test_none(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(["show", "--select", "sample_model.v2"])
        assert "Previewing node 'sample_model.v1'" not in log_output
        assert "Previewing node 'sample_model.v2'" in log_output


class TestShowPrivateModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": private_model_yml,
            "private_model.sql": models__sample_model,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"sample_seed.csv": seeds__sample_seed}

    def test_version_unspecified(self, project):
        run_dbt(["build"])
        run_dbt(["show", "--inline", "select * from {{ ref('private_model') }}"])
