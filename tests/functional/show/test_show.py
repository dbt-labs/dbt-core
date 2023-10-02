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
    private_model_yml,
)


class ShowBase:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_model.sql": models__sample_model,
            "sample_number_model.sql": models__sample_number_model,
            "sample_number_model_with_nulls.sql": models__sample_number_model_with_nulls,
            "second_model.sql": models__second_model,
            "ephemeral_model.sql": models__ephemeral_model,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"sample_seed.csv": seeds__sample_seed}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])


class TestShowNone(ShowBase):
    def test_none(self, project):
        with pytest.raises(
            DbtRuntimeError, match="Either --select or --inline must be passed to show"
        ):
            run_dbt(["show"])


class TestShowSelectText(ShowBase):
    def test_select_model_text(self, project):
        run_dbt(["build"])
        (_, log_output) = run_dbt_and_capture(["show", "--select", "second_model"])
        assert "Previewing node 'sample_model'" not in log_output
        assert "Previewing node 'second_model'" in log_output
        assert "col_one" in log_output
        assert "col_two" in log_output
        assert "answer" in log_output


class TestShowMultiple(ShowBase):
    def test_select_multiple_model_text(self, project):
        run_dbt(["build"])
        (_, log_output) = run_dbt_and_capture(["show", "--select", "sample_model second_model"])
        assert "Previewing node 'sample_model'" in log_output
        assert "sample_num" in log_output
        assert "sample_bool" in log_output


class TestShowSingle(ShowBase):
    def test_select_single_model_json(self, project):
        run_dbt(["build"])
        (_, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_model", "--output", "json"]
        )
        assert "Previewing node 'sample_model'" not in log_output
        assert "sample_num" in log_output
        assert "sample_bool" in log_output


class TestShowNumeric(ShowBase):
    def test_numeric_values(self, project):
        run_dbt(["build"])
        (_, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_number_model", "--output", "json"]
        )
        # json log output needs the escapes removed for string matching
        log_output = log_output.replace("\\", "")
        assert "Previewing node 'sample_number_model'" not in log_output
        assert '"float_to_int_field": 1.0' not in log_output
        assert '"float_to_int_field": 1' in log_output
        assert '"float_field": 3.0' in log_output
        assert '"float_with_dec_field": 4.3' in log_output
        assert '"int_field": 5' in log_output
        assert '"int_field": 5.0' not in log_output


class TestShowNumericNulls(ShowBase):
    def test_numeric_values_with_nulls(self, project):
        run_dbt(["build"])
        (_, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_number_model_with_nulls", "--output", "json"]
        )
        # json log output needs the escapes removed for string matching
        log_output = log_output.replace("\\", "")
        assert "Previewing node 'sample_number_model_with_nulls'" not in log_output
        assert '"float_to_int_field": 1.0' not in log_output
        assert '"float_to_int_field": 1' in log_output
        assert '"float_field": 3.0' in log_output
        assert '"float_with_dec_field": 4.3' in log_output
        assert '"int_field": 5' in log_output
        assert '"int_field": 5.0' not in log_output


class TestShowInline(ShowBase):
    def test_inline_pass(self, project):
        run_dbt(["build"])
        (_, log_output) = run_dbt_and_capture(
            ["show", "--inline", "select * from {{ ref('sample_model') }}"]
        )
        assert "Previewing inline node" in log_output
        assert "sample_num" in log_output
        assert "sample_bool" in log_output


class TestShowInlineFail(ShowBase):
    def test_inline_fail(self, project):
        with pytest.raises(DbtException, match="Error parsing inline query"):
            run_dbt(["show", "--inline", "select * from {{ ref('third_model') }}"])


class TestShowInlineFailDB(ShowBase):
    def test_inline_fail_database_error(self, project):
        with pytest.raises(DbtRuntimeError, match="Database Error"):
            run_dbt(["show", "--inline", "slect asdlkjfsld;j"])


class TestShowEphemeral(ShowBase):
    def test_ephemeral_model(self, project):
        run_dbt(["build"])
        (_, log_output) = run_dbt_and_capture(["show", "--select", "ephemeral_model"])
        assert "col_deci" in log_output


class TestShowSecondEphemeral(ShowBase):
    def test_second_ephemeral_model(self, project):
        run_dbt(["build"])
        (_, log_output) = run_dbt_and_capture(["show", "--inline", models__second_ephemeral_model])
        assert "col_hundo" in log_output


class TestShowSeed(ShowBase):
    def test_seed(self, project):
        (_, log_output) = run_dbt_and_capture(["show", "--select", "sample_seed"])
        assert "Previewing node 'sample_seed'" in log_output


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
