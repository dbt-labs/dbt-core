import pytest

from dbt.artifacts.resources import RefArgs
from dbt.exceptions import CompilationError, ParsingError
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
        node_with_fk_constraint = manifest.nodes["model.test.my_model"]
        assert len(node_with_fk_constraint.constraints) == 1

        parsed_constraint = node_with_fk_constraint.constraints[0]
        assert parsed_constraint == ModelLevelConstraint(
            type=ConstraintType.foreign_key,
            columns=["id"],
            to="ref('my_model_to')",
            to_columns=["id"],
        )
        # Assert column-level constraint source included in node.depends_on
        assert node_with_fk_constraint.refs == [RefArgs("my_model_to")]
        assert node_with_fk_constraint.depends_on.nodes == ["model.test.my_model_to"]
        assert node_with_fk_constraint.sources == []

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
        node_with_fk_constraint = manifest.nodes["model.test.my_model"]
        assert len(node_with_fk_constraint.constraints) == 1

        parsed_constraint = node_with_fk_constraint.constraints[0]
        assert parsed_constraint == ModelLevelConstraint(
            type=ConstraintType.foreign_key,
            columns=["id"],
            to="source('test_source', 'test_table')",
            to_columns=["id"],
        )
        # Assert column-level constraint source included in node.depends_on
        assert node_with_fk_constraint.refs == []
        assert node_with_fk_constraint.depends_on.nodes == ["source.test.test_source.test_table"]
        assert node_with_fk_constraint.sources == [["test_source", "test_table"]]

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
        with pytest.raises(
            CompilationError, match="depends on a node named 'doesnt_exist' which was not found"
        ):
            run_dbt(["parse"])


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
            ParsingError,
            match="Invalid 'ref' or 'source' syntax on foreign key constraint 'to' on model my_model: invalid",
        ):
            run_dbt(["parse"])


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
        node_with_fk_constraint = manifest.nodes["model.test.my_model"]
        assert len(node_with_fk_constraint.columns["id"].constraints) == 1

        parsed_constraint = node_with_fk_constraint.columns["id"].constraints[0]
        # Assert column-level constraint parsed
        assert parsed_constraint == ColumnLevelConstraint(
            type=ConstraintType.foreign_key, to="ref('my_model_to')", to_columns=["id"]
        )
        # Assert column-level constraint ref included in node.depends_on
        assert node_with_fk_constraint.refs == [RefArgs(name="my_model_to")]
        assert node_with_fk_constraint.sources == []
        assert node_with_fk_constraint.depends_on.nodes == ["model.test.my_model_to"]

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
        node_with_fk_constraint = manifest.nodes["model.test.my_model"]
        assert len(node_with_fk_constraint.columns["id"].constraints) == 1

        parsed_constraint = node_with_fk_constraint.columns["id"].constraints[0]
        assert parsed_constraint == ColumnLevelConstraint(
            type=ConstraintType.foreign_key,
            to="source('test_source', 'test_table')",
            to_columns=["id"],
        )
        # Assert column-level constraint source included in node.depends_on
        assert node_with_fk_constraint.refs == []
        assert node_with_fk_constraint.depends_on.nodes == ["source.test.test_source.test_table"]
        assert node_with_fk_constraint.sources == [["test_source", "test_table"]]

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
        with pytest.raises(
            CompilationError, match="depends on a node named 'doesnt_exist' which was not found"
        ):
            run_dbt(["parse"])


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
            ParsingError,
            match="Invalid 'ref' or 'source' syntax on foreign key constraint 'to' on model my_model: invalid.",
        ):
            run_dbt(["parse"])
