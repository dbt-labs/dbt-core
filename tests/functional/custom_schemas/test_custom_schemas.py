import pytest

from dbt.exceptions import ParsingError
from dbt.tests.util import run_dbt

generate_schema_name_macro_sql = """
{% macro generate_schema_name(custom_schema_name, node) %}
    test_schema
{% endmacro %}
"""

# this macro returns none when custom_schema_name (from config schema) is unset
generate_schema_name_macro_null_return = """
{% macro generate_schema_name(custom_schema_name, node) %}
    {{ return(custom_schema_name) }}
{% endmacro %}
"""


class TestCustomSchema:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": "select 1 as id"}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "generate_schema_name_null_return.sql": generate_schema_name_macro_sql,
        }

    def test_custom_schema(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results.results[0].node.schema == "test_schema"


class TestCustomSchemaNullReturn:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_valid_schema_from_generate_schema_name": True,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": "select 1 as id"}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "generate_schema_name_null_return.sql": generate_schema_name_macro_null_return,
        }

    def test_custom_schema_null_return(self, project):
        with pytest.raises(ParsingError) as excinfo:
            run_dbt(["run"])
        assert (
            "Node 'model.test.model' has a schema set to None as a result of a generate_schema_name call."
            in str(excinfo.value)
        )


class TestCustomSchemaNullReturnLegacy:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_valid_schema_from_generate_schema_name": False,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": "select 1 as id",
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "generate_schema_name_null_return.sql": generate_schema_name_macro_null_return,
        }

    def test_custom_schema_null_return_legacy(self, project):
        manifest = run_dbt(["parse"], expect_pass=True)
        # This was buggy behavior (non-conformant to manifest schemas published in v12) but nonetheless legacy behavior
        assert manifest.nodes["model.test.model"].schema is None


# Should be updated to TestCustomSchemaNullReturn instead of TestCustomSchemaNullReturnLegacy once
# required_valid_schema_from_generate_schema_name flag is set to True by default
class TestCustomSchemaNullReturnDefault(TestCustomSchemaNullReturnLegacy):
    pass
