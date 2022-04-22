import os
import pytest
import yaml
from dbt.exceptions import ParsingException

from dbt.tests.util import (
    run_dbt,
    update_config_file,
    check_relations_equal,
    check_table_does_not_exist,
)
from tests.functional.sources.common_source_setup import (
    BaseSourcesTest,
)
from tests.functional.sources.fixtures import (
    macros__macro_sql,
    malformed_models__schema_yml,
    malformed_models__descendant_model_sql,
    malformed_schema_tests__schema_yml,
    malformed_schema_tests__model_sql,
)


class SuccessfulSourcesTest(BaseSourcesTest):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        self.run_dbt_with_vars(project, ["seed"])
        os.environ["DBT_ENV_CUSTOM_ENV_key"] = "value"

        yield

        del os.environ["DBT_ENV_CUSTOM_ENV_key"]

    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro.sql": macros__macro_sql}

    def _create_schemas(self, project):
        schema = self.alternative_schema(project.test_schema)
        project.run_sql(f"drop schema if exists {schema} cascade")
        project.run_sql(f"create schema {schema}")

    def alternative_schema(self, test_schema):
        return test_schema + "_other"

    @pytest.fixture(scope="class", autouse=True)
    def createDummyTables(self, project):
        self._create_schemas(project)
        project.run_sql("create table {}.dummy_table (id int)".format(project.test_schema))
        project.run_sql(
            "create view {}.external_view as (select * from {}.dummy_table)".format(
                self.alternative_schema(project.test_schema), project.test_schema
            )
        )

    def run_dbt_with_vars(self, project, cmd, *args, **kwargs):
        vars_dict = {
            "test_run_schema": project.test_schema,
            "test_run_alt_schema": self.alternative_schema(project.test_schema),
            "test_loaded_at": project.adapter.quote("updated_at"),
        }
        cmd.extend(["--vars", yaml.safe_dump(vars_dict)])
        return run_dbt(cmd, *args, **kwargs)


class TestBasicSource(SuccessfulSourcesTest):
    def test_basic_source_def(self, project):
        results = self.run_dbt_with_vars(project, ["run"])
        assert len(results) == 4

        check_relations_equal(
            project.adapter, ["source", "descendant_model", "nonsource_descendant"]
        )
        check_relations_equal(project.adapter, ["expected_multi_source", "multi_source_model"])
        results = self.run_dbt_with_vars(project, ["test"])
        assert len(results) == 8


class TestSourceSelector(SuccessfulSourcesTest):
    def test_source_selector(self, project):
        # only one of our models explicitly depends upon a source
        results = self.run_dbt_with_vars(
            project, ["run", "--models", "source:test_source.test_table+"]
        )
        assert len(results) == 1
        check_relations_equal(project.adapter, ["source", "descendant_model"])
        check_table_does_not_exist(project.adapter, "nonsource_descendant")
        check_table_does_not_exist(project.adapter, "multi_source_model")

        # do the same thing, but with tags
        results = self.run_dbt_with_vars(
            project, ["run", "--models", "tag:my_test_source_table_tag+"]
        )
        assert len(results) == 1

        results = self.run_dbt_with_vars(
            project, ["test", "--models", "source:test_source.test_table+"]
        )
        assert len(results) == 6

        results = self.run_dbt_with_vars(
            project, ["test", "--models", "tag:my_test_source_table_tag+"]
        )
        assert len(results) == 6

        results = self.run_dbt_with_vars(project, ["test", "--models", "tag:my_test_source_tag+"])
        # test_table + other_test_table
        assert len(results) == 8

        results = self.run_dbt_with_vars(project, ["test", "--models", "tag:id_column"])
        # all 4 id column tests
        assert len(results) == 4


class TestEmptySource(SuccessfulSourcesTest):
    def test_empty_source_def(self, project):
        # sources themselves can never be selected, so nothing should be run
        results = self.run_dbt_with_vars(
            project, ["run", "--models", "source:test_source.test_table"]
        )
        check_table_does_not_exist(project.adapter, "nonsource_descendant")
        check_table_does_not_exist(project.adapter, "multi_source_model")
        check_table_does_not_exist(project.adapter, "descendant_model")
        assert len(results) == 0


class TestSourceDef(SuccessfulSourcesTest):
    def test_source_only_def(self, project):
        results = self.run_dbt_with_vars(project, ["run", "--models", "source:other_source+"])
        assert len(results) == 1
        check_relations_equal(project.adapter, ["expected_multi_source", "multi_source_model"])
        check_table_does_not_exist(project.adapter, "nonsource_descendant")
        check_table_does_not_exist(project.adapter, "descendant_model")

        results = self.run_dbt_with_vars(project, ["run", "--models", "source:test_source+"])
        assert len(results) == 2
        check_relations_equal(project.adapter, ["source", "descendant_model"])
        check_relations_equal(project.adapter, ["expected_multi_source", "multi_source_model"])
        check_table_does_not_exist(project.adapter, "nonsource_descendant")


class TestSourceChildrenParents(SuccessfulSourcesTest):
    def test_source_childrens_parents(self, project):
        results = self.run_dbt_with_vars(project, ["run", "--models", "@source:test_source"])
        assert len(results) == 2
        check_relations_equal(project.adapter, ["source", "descendant_model"])
        check_relations_equal(project.adapter, ["expected_multi_source", "multi_source_model"])
        check_table_does_not_exist(project.adapter, "nonsource_descendant")


class TestSourceRunOperation(SuccessfulSourcesTest):
    def test_run_operation_source(self, project):
        kwargs = '{"source_name": "test_source", "table_name": "test_table"}'
        self.run_dbt_with_vars(project, ["run-operation", "vacuum_source", "--args", kwargs])


class TestMalformedSources(BaseSourcesTest):
    # even seeds should fail, because parsing is what's raising
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": malformed_models__schema_yml,
            "descendant_model.sql": malformed_models__descendant_model_sql,
        }

    def test_malformed_schema_will_break_run(self, project):
        with pytest.raises(ParsingException):
            self.run_dbt_with_vars(project, ["seed"])


class TestRenderingInSourceTests(BaseSourcesTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": malformed_schema_tests__schema_yml,
            "model.sql": malformed_schema_tests__model_sql,
        }

    def test_render_in_source_tests(self, project):
        self.run_dbt_with_vars(project, ["seed"])
        self.run_dbt_with_vars(project, ["run"])
        # syntax error at or near "{", because the test isn't rendered
        self.run_dbt_with_vars(project, ["test"], expect_pass=False)


class TestUnquotedSources(SuccessfulSourcesTest):
    def test_catalog(self, project):
        new_quoting_config = {
            "quoting": {
                "identifier": False,
                "schema": False,
                "database": False,
            }
        }
        update_config_file(new_quoting_config, project.project_root, "dbt_project.yml")
        self.run_dbt_with_vars(project, ["run"])
        self.run_dbt_with_vars(project, ["docs", "generate"])
