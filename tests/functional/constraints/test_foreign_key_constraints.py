import pytest

from dbt.exceptions import DbtRuntimeError
from dbt.tests.util import get_artifact, run_dbt
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)
from tests.functional.constraints.fixtures import (
    model_column_level_foreign_key_source_schema_yml,
    model_foreign_key_column_invalid_syntax_schema_yml,
    model_foreign_key_column_node_not_found_schema_yml,
    model_foreign_key_model_column_schema_yml,
    model_foreign_key_model_invalid_syntax_schema_yml,
    model_foreign_key_model_node_not_found_schema_yml,
    model_foreign_key_model_schema_yml,
    model_foreign_key_source_schema_yml,
)


class TestModelLevelForeignKeyConstraintToRef:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "constraints_schema.yml": model_foreign_key_model_schema_yml,
            "my_model.sql": "select 1 as id",
            "my_model_to.sql": "select 1 as id",
        }

    def test_model_level_fk_to(self, project, unique_schema):
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes["model.test.my_model"].constraints) == 1

        parsed_constraint = manifest.nodes["model.test.my_model"].constraints[0]
        assert parsed_constraint == ModelLevelConstraint(
            type=ConstraintType.foreign_key,
            columns=["id"],
            to="ref('my_model_to')",
            to_columns=["id"],
        )

        # Assert compilation renders to from 'ref' to relation identifer
        run_dbt(["compile"])
        manifest = get_artifact(project.project_root, "target", "manifest.json")
        assert len(manifest["nodes"]["model.test.my_model"]["constraints"]) == 1

        compiled_constraint = manifest["nodes"]["model.test.my_model"]["constraints"][0]
        assert compiled_constraint["to"] == f'"dbt"."{unique_schema}"."my_model_to"'
        # Other constraint fields should remain as parsed
        assert compiled_constraint["to_columns"] == parsed_constraint.to_columns
        assert compiled_constraint["columns"] == parsed_constraint.columns
        assert compiled_constraint["type"] == parsed_constraint.type


class TestModelLevelForeignKeyConstraintToSource:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "constraints_schema.yml": model_foreign_key_source_schema_yml,
            "my_model.sql": "select 1 as id",
            "my_model_to.sql": "select 1 as id",
        }

    def test_model_level_fk_to(self, project, unique_schema):
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes["model.test.my_model"].constraints) == 1

        parsed_constraint = manifest.nodes["model.test.my_model"].constraints[0]
        assert parsed_constraint == ModelLevelConstraint(
            type=ConstraintType.foreign_key,
            columns=["id"],
            to="source('test_source', 'test_table')",
            to_columns=["id"],
        )

        # Assert compilation renders to from 'ref' to relation identifer
        run_dbt(["compile"])
        manifest = get_artifact(project.project_root, "target", "manifest.json")
        assert len(manifest["nodes"]["model.test.my_model"]["constraints"]) == 1

        compiled_constraint = manifest["nodes"]["model.test.my_model"]["constraints"][0]
        assert compiled_constraint["to"] == '"dbt"."test_source"."test_table"'
        # Other constraint fields should remain as parsed
        assert compiled_constraint["to_columns"] == parsed_constraint.to_columns
        assert compiled_constraint["columns"] == parsed_constraint.columns
        assert compiled_constraint["type"] == parsed_constraint.type


class TestModelLevelForeignKeyConstraintRefNotFoundError:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "constraints_schema.yml": model_foreign_key_model_node_not_found_schema_yml,
            "my_model.sql": "select 1 as id",
            "my_model_to.sql": "select 1 as id",
        }

    def test_model_level_fk_to_doesnt_exist(self, project):
        with pytest.raises(DbtRuntimeError, match="not in the graph"):
            run_dbt(["compile"])


class TestModelLevelForeignKeyConstraintRefSyntaxError:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "constraints_schema.yml": model_foreign_key_model_invalid_syntax_schema_yml,
            "my_model.sql": "select 1 as id",
            "my_model_to.sql": "select 1 as id",
        }

    def test_model_level_fk_to(self, project):
        with pytest.raises(
            DbtRuntimeError,
            match="'model.test.my_model' defines a foreign key constraint 'to' expression which is not valid 'ref' or 'source' syntax",
        ):
            run_dbt(["compile"])


class TestColumnLevelForeignKeyConstraintToRef:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "constraints_schema.yml": model_foreign_key_model_column_schema_yml,
            "my_model.sql": "select 1 as id",
            "my_model_to.sql": "select 1 as id",
        }

    def test_column_level_fk_to(self, project, unique_schema):
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes["model.test.my_model"].columns["id"].constraints) == 1

        parsed_constraint = manifest.nodes["model.test.my_model"].columns["id"].constraints[0]
        assert parsed_constraint == ColumnLevelConstraint(
            type=ConstraintType.foreign_key, to="ref('my_model_to')", to_columns=["id"]
        )

        # Assert compilation renders to from 'ref' to relation identifer
        run_dbt(["compile"])
        manifest = get_artifact(project.project_root, "target", "manifest.json")
        assert len(manifest["nodes"]["model.test.my_model"]["columns"]["id"]["constraints"]) == 1

        compiled_constraint = manifest["nodes"]["model.test.my_model"]["columns"]["id"][
            "constraints"
        ][0]
        assert compiled_constraint["to"] == f'"dbt"."{unique_schema}"."my_model_to"'
        # Other constraint fields should remain as parsed
        assert compiled_constraint["to_columns"] == parsed_constraint.to_columns
        assert compiled_constraint["type"] == parsed_constraint.type


class TestColumnLevelForeignKeyConstraintToSource:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "constraints_schema.yml": model_column_level_foreign_key_source_schema_yml,
            "my_model.sql": "select 1 as id",
            "my_model_to.sql": "select 1 as id",
        }

    def test_model_level_fk_to(self, project, unique_schema):
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes["model.test.my_model"].columns["id"].constraints) == 1

        parsed_constraint = manifest.nodes["model.test.my_model"].columns["id"].constraints[0]
        assert parsed_constraint == ColumnLevelConstraint(
            type=ConstraintType.foreign_key,
            to="source('test_source', 'test_table')",
            to_columns=["id"],
        )

        # Assert compilation renders to from 'ref' to relation identifer
        run_dbt(["compile"])
        manifest = get_artifact(project.project_root, "target", "manifest.json")
        assert len(manifest["nodes"]["model.test.my_model"]["columns"]["id"]["constraints"]) == 1

        compiled_constraint = manifest["nodes"]["model.test.my_model"]["columns"]["id"][
            "constraints"
        ][0]
        assert compiled_constraint["to"] == '"dbt"."test_source"."test_table"'
        # # Other constraint fields should remain as parsed
        assert compiled_constraint["to_columns"] == parsed_constraint.to_columns
        assert compiled_constraint["type"] == parsed_constraint.type


class TestColumnLevelForeignKeyConstraintRefNotFoundError:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "constraints_schema.yml": model_foreign_key_column_node_not_found_schema_yml,
            "my_model.sql": "select 1 as id",
            "my_model_to.sql": "select 1 as id",
        }

    def test_model_level_fk_to_doesnt_exist(self, project):
        with pytest.raises(DbtRuntimeError, match="not in the graph"):
            run_dbt(["compile"])


class TestColumnLevelForeignKeyConstraintRefSyntaxError:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "constraints_schema.yml": model_foreign_key_column_invalid_syntax_schema_yml,
            "my_model.sql": "select 1 as id",
            "my_model_to.sql": "select 1 as id",
        }

    def test_model_level_fk_to(self, project):
        with pytest.raises(
            DbtRuntimeError,
            match="'model.test.my_model' defines a foreign key constraint 'to' expression which is not valid 'ref' or 'source' syntax",
        ):
            run_dbt(["compile"])
