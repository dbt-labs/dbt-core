import pytest

from dbt.exceptions import ParsingError
from dbt.tests.util import get_manifest, run_dbt
from tests.functional.constraints.fixtures import (
    model_invalid_constraint_no_contract_schema_yml,
    model_invalid_constraint_type_schema_yml,
)

_model_sql = "select 1 as id"


class TestInvalidUnenforceableConstraintDefault:
    """With the default flag (False), an invalid constraint type on an unenforced model
    should parse successfully (the bad constraint is silently dropped and a Note is emitted)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": _model_sql,
            "schema.yml": model_invalid_constraint_type_schema_yml,
        }

    def test_invalid_constraint_filtered_out(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.my_model"]
        # The invalid constraint should have been filtered out
        assert node.constraints == []


class TestInvalidUnenforceableConstraintPartiallyFiltered:
    """Valid constraints survive; only the invalid one is dropped."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": _model_sql,
            "schema.yml": model_invalid_constraint_no_contract_schema_yml,
        }

    def test_valid_constraints_preserved(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.my_model"]
        assert len(node.constraints) == 1
        from dbt_common.contracts.constraints import ConstraintType

        assert node.constraints[0].type == ConstraintType.not_null


class TestInvalidUnenforceableConstraintFlagEnabled:
    """When require_valid_unenforced_constraints=True, an invalid constraint type on an
    unenforced model must raise a ParsingError."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_valid_unenforced_constraints": True,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": _model_sql,
            "schema.yml": model_invalid_constraint_type_schema_yml,
        }

    def test_invalid_constraint_raises(self, project):
        with pytest.raises(ParsingError) as exc_info:
            run_dbt(["parse"])
        assert "Invalid constraint type" in str(exc_info.value)
        assert "my_model" in str(exc_info.value)
