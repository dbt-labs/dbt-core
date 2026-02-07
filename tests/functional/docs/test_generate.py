from unittest import mock

import pytest

from dbt.plugins.manifest import ModelNodeArgs, PluginNodes
from dbt.tests.util import get_manifest, run_dbt

sample_seed = """sample_num,sample_bool
1,true
2,false
3,true
"""

second_seed = """sample_num,sample_bool
4,true
5,false
6,true
"""

sample_config = """
sources:
  - name: my_source_schema
    schema: "{{ target.schema }}"
    tables:
      - name: sample_source
      - name: second_source
      - name: non_existent_source
      - name: source_from_seed
"""


class TestBaseGenerate:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as fun",
            "alt_model.sql": "select 1 as notfun",
            "sample_config.yml": sample_config,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "sample_seed.csv": sample_seed,
            "second_seed.csv": sample_seed,
        }


class TestGenerateManifestNotCompiled(TestBaseGenerate):
    def test_manifest_not_compiled(self, project):
        run_dbt(["docs", "generate", "--no-compile"])
        # manifest.json is written out in parsing now, but it
        # shouldn't be compiled because of the --no-compile flag
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        assert model_id in manifest.nodes
        assert manifest.nodes[model_id].compiled is False


class TestGenerateEmptyCatalog(TestBaseGenerate):
    def test_generate_empty_catalog(self, project):
        catalog = run_dbt(["docs", "generate", "--empty-catalog"])
        assert catalog.nodes == {}, "nodes should be empty"
        assert catalog.sources == {}, "sources should be empty"
        assert catalog.errors is None, "errors should be null"


class TestGenerateSelectLimitsCatalog(TestBaseGenerate):
    def test_select_limits_catalog(self, project):
        run_dbt(["run"])
        catalog = run_dbt(["docs", "generate", "--select", "my_model"])
        assert len(catalog.nodes) == 1
        assert "model.test.my_model" in catalog.nodes


class TestGenerateSelectLimitsNoMatch(TestBaseGenerate):
    def test_select_limits_no_match(self, project):
        run_dbt(["run"])
        catalog = run_dbt(["docs", "generate", "--select", "my_missing_model"])
        assert len(catalog.nodes) == 0
        assert len(catalog.sources) == 0


class TestGenerateCatalogWithSources(TestBaseGenerate):
    def test_catalog_with_sources(self, project):
        # populate sources other than non_existent_source
        project.run_sql("create table {}.sample_source (id int)".format(project.test_schema))
        project.run_sql("create table {}.second_source (id int)".format(project.test_schema))

        # build nodes
        run_dbt(["build"])

        catalog = run_dbt(["docs", "generate"])

        # 2 seeds + 2 models
        assert len(catalog.nodes) == 4
        # 2 sources (only ones that exist)
        assert len(catalog.sources) == 2


class TestGenerateCatalogWithExternalNodes(TestBaseGenerate):
    @mock.patch("dbt.plugins.get_plugin_manager")
    def test_catalog_with_external_node(self, get_plugin_manager, project):
        project.run_sql("create table {}.external_model (id int)".format(project.test_schema))

        run_dbt(["build"])

        external_nodes = PluginNodes()
        external_model_node = ModelNodeArgs(
            name="external_model",
            package_name="external_package",
            identifier="external_model",
            schema=project.test_schema,
            database="dbt",
        )
        external_nodes.add_model(external_model_node)
        get_plugin_manager.return_value.get_nodes.return_value = external_nodes
        catalog = run_dbt(["docs", "generate"])

        assert "model.external_package.external_model" in catalog.nodes


class TestGenerateSelectSource(TestBaseGenerate):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "sample_seed.csv": sample_seed,
            "second_seed.csv": sample_seed,
            "source_from_seed.csv": sample_seed,
        }

    def test_select_source(self, project):
        run_dbt(["build"])

        project.run_sql("create table {}.sample_source (id int)".format(project.test_schema))
        project.run_sql("create table {}.second_source (id int)".format(project.test_schema))

        # 2 existing sources, 1 selected
        catalog = run_dbt(
            ["docs", "generate", "--select", "source:test.my_source_schema.sample_source"]
        )
        assert len(catalog.sources) == 1
        assert "source.test.my_source_schema.sample_source" in catalog.sources
        # no nodes selected
        assert len(catalog.nodes) == 0

        # 2 existing sources sources, 1 selected that has relation as a seed
        catalog = run_dbt(
            ["docs", "generate", "--select", "source:test.my_source_schema.source_from_seed"]
        )
        assert len(catalog.sources) == 1
        assert "source.test.my_source_schema.source_from_seed" in catalog.sources
        # seed with same relation that was not selected not in catalog
        assert len(catalog.nodes) == 0


class TestGenerateSelectOverMaxSchemaMetadataRelations(TestBaseGenerate):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "sample_seed.csv": sample_seed,
            "second_seed.csv": sample_seed,
            "source_from_seed.csv": sample_seed,
        }

    def test_select_source(self, project):
        run_dbt(["build"])

        project.run_sql("create table {}.sample_source (id int)".format(project.test_schema))
        project.run_sql("create table {}.second_source (id int)".format(project.test_schema))

        with mock.patch.object(type(project.adapter), "MAX_SCHEMA_METADATA_RELATIONS", 1):
            # more relations than MAX_SCHEMA_METADATA_RELATIONS -> all sources and nodes correctly returned
            catalog = run_dbt(["docs", "generate"])
            assert len(catalog.sources) == 3
            assert len(catalog.nodes) == 5

            # full source selection respected
            catalog = run_dbt(["docs", "generate", "--select", "source:*"])
            assert len(catalog.sources) == 3
            assert len(catalog.nodes) == 0

            # full node selection respected
            catalog = run_dbt(["docs", "generate", "--exclude", "source:*"])
            assert len(catalog.sources) == 0
            assert len(catalog.nodes) == 5

            # granular source selection respected (> MAX_SCHEMA_METADATA_RELATIONS selected sources)
            catalog = run_dbt(
                [
                    "docs",
                    "generate",
                    "--select",
                    "source:test.my_source_schema.sample_source",
                    "source:test.my_source_schema.second_source",
                ]
            )
            assert len(catalog.sources) == 2
            assert len(catalog.nodes) == 0

            # granular node selection respected (> MAX_SCHEMA_METADATA_RELATIONS selected nodes)
            catalog = run_dbt(["docs", "generate", "--select", "my_model", "alt_model"])
            assert len(catalog.sources) == 0
            assert len(catalog.nodes) == 2


class TestGenerateSelectSeed(TestBaseGenerate):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "sample_seed.csv": sample_seed,
            "second_seed.csv": sample_seed,
            "source_from_seed.csv": sample_seed,
        }

    def test_select_seed(self, project):
        run_dbt(["build"])

        # 3 seeds, 1 selected
        catalog = run_dbt(["docs", "generate", "--select", "sample_seed"])
        assert len(catalog.nodes) == 1
        assert "seed.test.sample_seed" in catalog.nodes
        # no sources selected
        assert len(catalog.sources) == 0

        # 3 seeds, 1 selected that has same relation as a source
        catalog = run_dbt(["docs", "generate", "--select", "source_from_seed"])
        assert len(catalog.nodes) == 1
        assert "seed.test.source_from_seed" in catalog.nodes
        # source with same relation that was not selected not in catalog
        assert len(catalog.sources) == 0


source_with_column_descriptions_config = """
sources:
  - name: test_source
    schema: "{{ target.schema }}"
    tables:
      - name: source_table
        columns:
          - name: id
            description: "This is a YAML description for id column"
          - name: name
            # No description provided, should fall back to DB comment
          - name: value
            description: "This is a YAML description for value column"
"""


class TestGenerateSourceColumnDescriptions(TestBaseGenerate):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_config.yml": source_with_column_descriptions_config,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    def test_source_column_descriptions_from_db_and_yaml(self, project):
        # Create a source table with column comments in the database
        project.run_sql(
            """
            create table {schema}.source_table (
                id int,
                name varchar(100),
                value decimal,
                undocumented_column text
            )
            """.format(
                schema=project.test_schema
            )
        )

        # Add comments to columns in the database
        # Note: This uses PostgreSQL-style COMMENT syntax
        # The test framework typically uses PostgreSQL for tests
        project.run_sql(
            "COMMENT ON COLUMN {schema}.source_table.id IS 'DB comment for id'".format(
                schema=project.test_schema
            )
        )
        project.run_sql(
            "COMMENT ON COLUMN {schema}.source_table.name IS 'DB comment for name'".format(
                schema=project.test_schema
            )
        )
        project.run_sql(
            "COMMENT ON COLUMN {schema}.source_table.value IS 'DB comment for value'".format(
                schema=project.test_schema
            )
        )
        project.run_sql(
            "COMMENT ON COLUMN {schema}.source_table.undocumented_column IS 'DB comment for undocumented'".format(
                schema=project.test_schema
            )
        )

        # Generate docs
        catalog = run_dbt(["docs", "generate"])

        # Verify the source is in the catalog
        source_id = "source.test.test_source.source_table"
        assert source_id in catalog.sources

        source_catalog = catalog.sources[source_id]
        columns = source_catalog.columns

        # Test 1: Column with YAML description should use YAML (not DB comment)
        assert "id" in columns
        assert (
            columns["id"].comment == "This is a YAML description for id column"
        ), "YAML description should take priority over DB comment"

        # Test 2: Column without YAML description should use DB comment
        assert "name" in columns
        assert (
            columns["name"].comment == "DB comment for name"
        ), "DB comment should be used when YAML description is missing"

        # Test 3: Column with YAML description should use YAML (not DB comment)
        assert "value" in columns
        assert (
            columns["value"].comment == "This is a YAML description for value column"
        ), "YAML description should take priority over DB comment"

        # Test 4: Column not in YAML should use DB comment
        assert "undocumented_column" in columns
        assert (
            columns["undocumented_column"].comment == "DB comment for undocumented"
        ), "DB comment should be used for columns not documented in YAML"
