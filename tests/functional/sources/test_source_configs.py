import pytest
from dbt.contracts.graph.model_config import SourceConfig
from dbt.contracts.graph.unparsed import FreshnessThreshold, Time, TimePeriod, Quoting
from dbt.exceptions import CompilationException

from dbt.tests.util import run_dbt, update_config_file, get_manifest


class SourceConfigTests:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self):
        pytest.expected_config = SourceConfig(
            enabled=True,
            # TODO: uncomment all this once it's added to SourceConfig, throws error right now
            # quoting = Quoting(database=False, schema=False, identifier=False, column=False)
            # freshness = FreshnessThreshold(
            #     warn_after=Time(count=12, period=TimePeriod.hour),
            #     error_after=Time(count=24, period=TimePeriod.hour),
            #     filter=None
            #     )
            # loader = "a_loader"
            # loaded_at_field = some_column
            # database = custom_database
            # schema = custom_schema
            # # identifier = "seed"  #this doesnt seems to be supported now?
            # meta = {'languages': ['python']}
            # tags = ["important_tag"]
        )


models__schema_yml = """version: 2

sources:
  - name: test_source
    tables:
      - name: test_table
  - name: other_source
    tables:
      - name: test_table
"""


# Test enabled config in dbt_project.yml
# expect pass, already implemented
class TestSourceEnabledConfigProjectLevel(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "sources": {
                "test": {
                    "test_source": {
                        "enabled": True,
                    },
                }
            }
        }

    def test_enabled_source_config_dbt_project(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources

        new_enabled_config = {
            "sources": {
                "test": {
                    "test_source": {
                        "enabled": False,
                    },
                }
            }
        }
        update_config_file(new_enabled_config, project.project_root, "dbt_project.yml")
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)

        assert (
            "source.test.test_source.test_table" not in manifest.sources
        )  # or should it be there with enabled: false??
        assert "source.test.other_source.test_table" in manifest.sources


disabled_source_level__schema_yml = """version: 2

sources:
  - name: test_source
    config:
      enabled: False
    tables:
      - name: test_table
      - name: disabled_test_table
"""


# Test enabled config at sources level in yml file
# expect fail - not implemented
class TestConfigYamlSourceLevel(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": disabled_source_level__schema_yml,
        }

    @pytest.mark.xfail
    def test_source_config_yaml_source_level(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" not in manifest.sources
        assert "source.test.test_source.disabled_test_table" not in manifest.sources


disabled_source_table__schema_yml = """version: 2

sources:
  - name: test_source
    tables:
      - name: test_table
      - name: disabled_test_table
        config:
            enabled: False
"""


# Test enabled config at source table level in yaml file
# expect fail - not implemented
class TestConfigYamlSourceTable(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": disabled_source_table__schema_yml,
        }

    @pytest.mark.xfail
    def test_source_config_yaml_source_table(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        assert "source.test.test_source.disabled_test_table" not in manifest.sources


# Test all configs other than enabled in dbt_project.yml
# expect fail - not implemented
class TestAllConfigsProjectLevel(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": models__schema_yml}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "sources": {
                "enabled": True,
                "quoting": {
                    "database": False,
                    "schema": False,
                    "identifier": False,
                    "column": False,
                },
                "freshness": {
                    "error_after": {"count": 24, "period": "hour"},
                    "warn_after": {"count": 12, "period": "hour"},
                },
                "loader": "a_loader",
                "loaded_at_field": "some_column",
                "database": "custom_database",
                "schema": "custom_schema",
                # identifier: "seed"  #this doesnt seems to be supported now?
                "meta": {"languages": ["python"]},
                "tags": ["important_tag"],
            }
        }

    @pytest.mark.xfail
    def test_source_all_configs_dbt_project(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        config = manifest.sources.get("source.test.test_source.test_table").config

        assert isinstance(config, SourceConfig)

        assert config == pytest.expected_config


configs_source_level__schema_yml = """version: 2

sources:
  - name: test_source
    config:
        enabled: True,
        quoting:
            database: False
            schema: False
            identifier: False
            column: False
        freshness:
            error_after: {count: 24, period: hour}
            warn_after: {count: 12, period: hour}
        loader: "a_loader"
        loaded_at_field: some_column
        database: custom_database
        schema: custom_schema
        # identifier: "seed"  #this doesnt seems to be supported now?
        meta: {'languages': ['python']}
        tags: ["important_tag"]
    tables:
      - name: test_table
      - name: other_test_table
"""


# Test configs other than enabled at sources level in yaml file
# **currently passes since enabled is all that ends up in the
# node.config since it's the only thing implemented
class TestAllConfigsSourceLevel(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": configs_source_level__schema_yml}

    def test_source_all_configs_source_level(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        assert "source.test.test_source.other_test_table" in manifest.sources
        config_test_table = manifest.sources.get("source.test.test_source.test_table").config
        config_other_test_table = manifest.sources.get(
            "source.test.test_source.other_test_table"
        ).config

        assert isinstance(config_test_table, SourceConfig)
        assert isinstance(config_other_test_table, SourceConfig)

        assert config_test_table == config_other_test_table
        assert config_test_table == pytest.expected_config


configs_source_table__schema_yml = """version: 2

sources:
  - name: test_source
    tables:
      - name: test_table
        config:
            enabled: True,
            quoting:
                database: False
                schema: False
                identifier: False
                column: False
            freshness:
                error_after: {count: 24, period: hour}
                warn_after: {count: 12, period: hour}
            loader: "a_loader"
            loaded_at_field: some_column
            database: custom_database
            schema: custom_schema
            # identifier: "seed"  #this doesnt seems to be supported now?
            meta: {'languages': ['python']}
            tags: ["important_tag"]
      - name: other_test_table
"""


# Test configs other than enabled at source table level in yml file
# expect fail - not implemented
class TestSourceAllConfigsSourceTable(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": configs_source_table__schema_yml}

    @pytest.mark.xfail
    def test_source_all_configs_source_table(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        assert "source.test.test_source.other_test_table" in manifest.sources
        config_test_table = manifest.sources.get("source.test.test_source.test_table").config
        config_other_test_table = manifest.sources.get(
            "source.test.test_source.other_test_table"
        ).config

        assert isinstance(config_test_table, SourceConfig)
        assert isinstance(config_other_test_table, SourceConfig)

        assert config_test_table != config_other_test_table
        assert config_test_table == pytest.expected_config


all_configs_everywhere__schema_yml = """version: 2

sources:
  - name: test_source
    config:
        tags: "source_level_important_tag",
    tables:
      - name: test_table
        config:
            enabled: True,
           quoting:
                database: False
                schema: False
                identifier: False
                column: False
            freshness:
                error_after: {count: 24, period: hour}
                warn_after: {count: 12, period: hour}
            loader: "a_loader"
            loaded_at_field: some_column
            database: custom_database
            schema: custom_schema
            # identifier: "seed"  #this doesnt seems to be supported now?
            meta: {'languages': ['python']}
            tags: ["important_tag"]
      - name: other_test_table
"""


# Test inheritence - set configs atproject, source, and source-table level - expect source-table level to win
# expect fail - not implemented
class TestSourceConfigsInheritence1(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": all_configs_everywhere__schema_yml}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"sources": {"tags": "project_level_important_tag"}}

    @pytest.mark.xfail
    def test_source_all_configs_source_table(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        assert "source.test.test_source.other_test_table" in manifest.sources
        config_test_table = manifest.sources.get("source.test.test_source.test_table").config
        config_other_test_table = manifest.sources.get(
            "source.test.test_source.other_test_table"
        ).config

        assert isinstance(config_test_table, SourceConfig)
        assert isinstance(config_other_test_table, SourceConfig)

        assert config_test_table != config_other_test_table
        assert config_test_table == pytest.expected_config

        expected_source_level_config = SourceConfig(
            enabled=True,
            # "tags" = "source_level_important_tag"  #TODO: update after SourceConfigs gets updated
        )

        assert config_other_test_table == expected_source_level_config


all_configs_not_table_schema_yml = """version: 2

sources:
  - name: test_source
    config:
        enabled: True,
        quoting:
            database: False
            schema: False
            identifier: False
            column: False
        freshness:
            error_after: {count: 24, period: hour}
            warn_after: {count: 12, period: hour}
        loader: "a_loader"
        loaded_at_field: some_column
        database: custom_database
        schema: custom_schema
        # identifier: "seed"  #this doesnt seems to be supported now?
        meta: {'languages': ['python']}
        tags: ["important_tag"]
    tables:
      - name: test_table
      - name: other_test_table
"""


# Test inheritence - set configs at project and source level - expect source level to win
# expect fail - not implemented
class TestSourceConfigsInheritence2(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": all_configs_not_table_schema_yml}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"sources": {"tags": "project_level_important_tag"}}

    @pytest.mark.xfail
    def test_source_two_configs_source_level(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        assert "source.test.test_source.other_test_table" in manifest.sources
        config_test_table = manifest.sources.get("source.test.test_source.test_table").config
        config_other_test_table = manifest.sources.get(
            "source.test.test_source.other_test_table"
        ).config

        assert isinstance(config_test_table, SourceConfig)
        assert isinstance(config_other_test_table, SourceConfig)

        assert config_test_table == config_other_test_table
        assert config_test_table == pytest.expected_config


all_configs_everywhere__schema_yml = """version: 2

sources:
  - name: test_source
    tables:
      - name: test_table
        config:
            enabled: True,
            quoting:
                database: False
                schema: False
                identifier: False
                column: False
            freshness:
                error_after: {count: 24, period: hour}
                warn_after: {count: 12, period: hour}
            loader: "a_loader"
            loaded_at_field: some_column
            database: custom_database
            schema: custom_schema
            # identifier: "seed"  #this doesnt seems to be supported now?
            meta: {'languages': ['python']}
            tags: ["important_tag"]
      - name: other_test_table
"""


# Test inheritence - set configs at project and source-table level - expect source-table level to win
# expect fail - not implemented
class TestSourceConfigsInheritence3(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": all_configs_everywhere__schema_yml}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"sources": {"tags": "project_level_important_tag"}}

    @pytest.mark.xfail
    def test_source_two_configs_source_table(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        assert "source.test.test_source.other_test_table" in manifest.sources
        config_test_table = manifest.sources.get("source.test.test_source.test_table").config
        config_other_test_table = manifest.sources.get(
            "source.test.test_source.other_test_table"
        ).config

        assert isinstance(config_test_table, SourceConfig)
        assert isinstance(config_other_test_table, SourceConfig)

        assert config_test_table != config_other_test_table
        assert config_test_table == pytest.expected_config

        expected_project_level_config = SourceConfig(
            enabled=True,
            # tags = "project_level_important_tag",   # TODO: uncomment these once SourceConfig is updated
        )

        assert config_other_test_table == expected_project_level_config


configs_as_properties__schema_yml = """version: 2

sources:
  - name: test_source
    quoting:
        database: False
        schema: False
        identifier: False
        column: False
    freshness:
        error_after: {count: 24, period: hour}
        warn_after: {count: 12, period: hour}
    loader: "a_loader"
    loaded_at_field: some_column
    database: custom_database
    schema: custom_schema
    # identifier: "seed"  #this doesnt seems to be supported now?
    meta: {'languages': ['python']}
    tags: ["important_tag"]
    tables:
      - name: test_table
"""


# Check backwards compatibility of setting configs as properties at top level
# expect pass since the properties don't get copied to the node.config yet
class TestSourceBackwardsCompatibility(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": configs_as_properties__schema_yml}

    def test_source_configs_as_properties(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        config_test_table = manifest.sources.get("source.test.test_source.test_table").config

        # this is new functionality - but it currently passes since SourceConfig is not updated
        # and is commented out in the setup becuse it is not updated with new configs
        # even when properties are defined at teh top level they should end up on the node.config
        assert isinstance(config_test_table, SourceConfig)
        assert config_test_table == pytest.expected_config

        # this is existing functionality - should always pass
        properties_test_table = manifest.sources.get("source.test.test_source.test_table")
        assert properties_test_table.quoting == Quoting(
            database=False, schema=False, identifier=False, column=False
        )
        assert properties_test_table.freshness == FreshnessThreshold(
            warn_after=Time(count=12, period=TimePeriod.hour),
            error_after=Time(count=24, period=TimePeriod.hour),
            filter=None,
        )
        assert properties_test_table.loader == "a_loader"
        assert properties_test_table.loaded_at_field == "some_column"
        assert properties_test_table.database == "custom_database"
        assert properties_test_table.schema == "custom_schema"
        # assert properties_test_table.identifier == "seed"
        assert properties_test_table.meta == {}  # TODO: why is this blank
        assert properties_test_table.tags == ["important_tag"]


configs_properites__schema_yml = """version: 2

sources:
  - name: test_source
    quoting:
        database: False
        schema: False
        identifier: False
        column: False
    freshness:
        error_after: {count: 24, period: hour}
        warn_after: {count: 12, period: hour}
    loader: "a_loader"
    loaded_at_field: some_column
    database: custom_database
    schema: custom_schema
    # identifier: "seed"  #this doesnt seems to be supported now?
    meta: {'languages': ['python']}
    tags: ["important_tag"]
    config:
        enabled: True,
        quoting:
            database: False
            schema: False
            identifier: False
            column: False
        freshness:
            error_after: {count: 24, period: hour}
            warn_after: {count: 12, period: hour}
        loader: "a_loader"
        loaded_at_field: some_column
        database: custom_database
        schema: custom_schema
        # identifier: "seed"  #this doesnt seems to be supported now?
        meta: {'languages': ['python']}
        tags: ["important_tag"]
    tables:
      - name: test_table
      - name: other_test_table
"""


# Raise an error when properties are set at top level and also as configs
class TestErrorSourceConfigProperty(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": configs_properites__schema_yml}

    @pytest.mark.xfail
    def test_error_source_configs_properties(self, project):
        # TODO: update below with correct exception/text/etc.  This is placeholder.
        with pytest.raises(CompilationException) as exc:
            run_dbt(["compile"])

        assert "???" in str(exc.value)
