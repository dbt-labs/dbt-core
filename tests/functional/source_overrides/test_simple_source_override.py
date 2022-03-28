from datetime import datetime, timedelta
import pytest
from dbt.contracts.graph.model_config import SourceConfig


from dbt.tests.util import run_dbt, update_config_file, check_relations_equal
from dbt.tests.fixtures.project import write_project_files
from tests.functional.source_overrides.fixtures import (  # noqa: F401
    local_dependency,
    models__schema_yml,
    seeds__expected_result_csv,
    seeds__my_real_other_seed_csv,
    seeds__my_real_seed_csv,
)


class TestSourceOverride:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root, local_dependency):  # noqa: F811
        write_project_files(project_root, "local_dependency", local_dependency)
        pytest._id = 101

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": models__schema_yml}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_result.csv": seeds__expected_result_csv,
            "my_real_other_seed.csv": seeds__my_real_other_seed_csv,
            "my_real_seed.csv": seeds__my_real_seed_csv,
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "local": "local_dependency",
                },
            ]
        }

    def _set_updated_at_to(self, delta, project):
        insert_time = datetime.utcnow() + delta
        timestr = insert_time.strftime("%Y-%m-%d %H:%M:%S")
        # favorite_color,id,first_name,email,ip_address,updated_at

        quoted_columns = ",".join(
            project.adapter.quote(c)
            for c in ("favorite_color", "id", "first_name", "email", "ip_address", "updated_at")
        )
        insert_id = pytest._id
        pytest._id += 1

        kwargs = {
            "schema": project.test_schema,
            "time": timestr,
            "id": insert_id,
            "source": project.adapter.quote("snapshot_freshness_base"),
            "quoted_columns": quoted_columns,
        }

        raw_sql = """INSERT INTO {schema}.{source}
            ({quoted_columns})
        VALUES (
            'blue',{id},'Jake','abc@example.com','192.168.1.1','{time}'
        )""".format(
            **kwargs
        )

        project.run_sql(raw_sql)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "localdep": {
                    "enabled": False,
                    "keep": {
                        "enabled": True,
                    },
                },
                "quote_columns": False,
            },
            "sources": {
                "localdep": {
                    "my_other_source": {
                        "enabled": False,
                    }
                }
            },
        }

    def test_source_overrides(self, project):
        run_dbt(["deps"])

        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 5

        # There should be 7, as we disabled 1 test of the original 8
        test_results = run_dbt(["test"])
        assert len(test_results) == 7

        results = run_dbt(["run"])
        assert len(results) == 1

        check_relations_equal(project.adapter, ["expected_result", "my_model"])

        # set the updated_at field of this seed to last week
        self._set_updated_at_to(timedelta(days=-7), project)
        # if snapshot-freshness fails, freshness just didn't happen!
        results = run_dbt(["source", "snapshot-freshness"], expect_pass=False)
        # we disabled my_other_source, so we only run the one freshness check
        # in
        assert len(results) == 1
        # If snapshot-freshness passes, that means error_after was
        # applied from the source override but not the source table override
        self._set_updated_at_to(timedelta(days=-2), project)
        results = run_dbt(
            ["source", "snapshot-freshness"],
            expect_pass=False,
        )
        assert len(results) == 1

        self._set_updated_at_to(timedelta(hours=-12), project)
        results = run_dbt(["source", "snapshot-freshness"], expect_pass=True)
        assert len(results) == 1

        # update source to be enabled
        new_source_config = {
            "sources": {
                "localdep": {
                    "my_other_source": {
                        "enabled": True,
                    }
                }
            }
        }
        update_config_file(new_source_config, project.project_root, "dbt_project.yml")

        # enable my_other_source, snapshot freshness should fail due to the new
        # not-fresh source
        results = run_dbt(["source", "snapshot-freshness"], expect_pass=False)
        assert len(results) == 2


class SourceOverrideTest:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root, local_dependency):  # noqa: F811
        write_project_files(project_root, "local_dependency", local_dependency)

        pytest.expected_config = SourceConfig(
            enabled=True,
            # TODO: uncomment all this once it's added to SourceConfig, throws error right now
            # quoting = Quoting(database=True, schema=True, identifier=True, column=True)
            # freshness = FreshnessThreshold(
            #     warn_after=Time(count=1, period=TimePeriod.minute),
            #     error_after=Time(count=5, period=TimePeriod.minute),
            #     filter=None
            #     )
            # loader = "override_a_loader"
            # loaded_at_field = override_some_column
            # database = override_custom_database
            # schema = override_custom_schema
            # meta = {'languages': ['java']}
            # tags = ["override_important_tag"]
        )

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "sources": {
                "localdep": {
                    "my_source": {
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
                        "meta": {"languages": ["python"]},
                        "tags": ["important_tag"],
                    }
                }
            },
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "local": "local_dependency",
                },
            ]
        }


overrides_source_level__schema_yml = """
version: 2
sources:
  - name: my_source
    overrides: localdep
    config:
        quoting:
            database: True
            schema: True
            identifier: True
            column: True
        freshness:
            error_after: {count: 1, period: minute}
            warn_after: {count: 5, period: minute}
        loader: "override_a_loader"
        loaded_at_field: override_some_column
        database: override_custom_database
        schema: override_custom_schema
        meta: {'languages': ['java']}
        tags: ["override_important_tag"]
    tables:
      - name: my_table
      - name: my_other_table
      - name: snapshot_freshness
"""


# test overriding at the source level
# expect fail since these are no valid configs right now
class TestSourceLevelOverride(SourceOverrideTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": overrides_source_level__schema_yml}

    @pytest.mark.xfail
    def test_source_level_overrides(self, project):
        run_dbt(["deps"])

        # this currently fails because configs fail parsing under an override
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)

        assert "source.localdep.my_source.my_table" in manifest.sources
        assert "source.localdep.my_source.my_other_table" in manifest.sources
        assert "source.localdep.my_source.snapshot_freshness" in manifest.sources

        config_my_table = manifest.sources.get("source.localdep.my_source.my_table").config
        config_my_other_table = manifest.sources.get(
            "source.localdep.my_source.my_other_table"
        ).config
        config_snapshot_freshness_table = manifest.sources.get(
            "source.localdep.my_source.snapshot_freshness"
        ).config

        assert isinstance(config_my_table, SourceConfig)
        assert isinstance(config_my_other_table, SourceConfig)

        assert config_my_table == config_my_other_table
        assert config_my_table == config_snapshot_freshness_table
        assert config_my_table == pytest.expected_config


overrides_source_level__schema_yml = """
version: 2
sources:
  - name: my_source
    overrides: localdep
    tables:
      - name: my_table
      - name: my_other_table
        config:
            quoting:
                database: True
                schema: True
                identifier: True
                column: True
            freshness:
                error_after: {count: 1, period: minute}
                warn_after: {count: 5, period: minute}
            loader: "override_a_loader"
            loaded_at_field: override_some_column
            database: override_custom_database
            schema: override_custom_schema
            meta: {'languages': ['java']}
            tags: ["override_important_tag"]
      - name: snapshot_freshness
"""


# test overriding at the source table level
# expect fail since these are no valid configs right now
class TestSourceTableOverride(SourceOverrideTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": overrides_source_level__schema_yml}

    @pytest.mark.xfail
    def test_source_table_overrides(self, project):
        run_dbt(["deps"])

        # this currently fails because configs fail parsing under an override
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)

        assert "source.localdep.my_source.my_table" in manifest.sources
        assert "source.localdep.my_source.my_other_table" in manifest.sources
        assert "source.localdep.my_source.snapshot_freshness" in manifest.sources

        config_my_table = manifest.sources.get("source.localdep.my_source.my_table").config
        config_my_other_table = manifest.sources.get(
            "source.localdep.my_source.my_other_table"
        ).config
        config_snapshot_freshness_table = manifest.sources.get(
            "source.localdep.my_source.snapshot_freshness"
        ).config

        assert isinstance(config_my_table, SourceConfig)
        assert isinstance(config_my_other_table, SourceConfig)

        assert config_my_table != config_my_other_table
        assert config_my_table == config_snapshot_freshness_table

        assert config_my_other_table == pytest.expected_config
