from dbt.dataclass_schema import ValidationError
import pytest
import os

from dbt.exceptions import ParsingError
from dbt.tests.util import run_dbt, update_config_file, write_file, check_relations_equal
from tests.functional.configs.fixtures import BaseConfigProject, simple_snapshot


class TestConfigs(BaseConfigProject):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "tagged": {
                        # the model configs will override this
                        "materialized": "invalid",
                        # the model configs will append to these
                        "tags": ["tag_one"],
                    }
                },
            },
            "seeds": {
                "quote_columns": False,
            },
        }

    def test_config_layering(
        self,
        project,
    ):
        # run seed
        results = run_dbt(["seed"])
        assert len(results) == 1

        # test the project-level tag, and both config() call tags
        assert len(run_dbt(["run", "--model", "tag:tag_one"])) == 1
        assert len(run_dbt(["run", "--model", "tag:tag_two"])) == 1
        assert len(run_dbt(["run", "--model", "tag:tag_three"])) == 1
        check_relations_equal(project.adapter, ["seed", "model"])

        # make sure we overwrote the materialization properly
        tables = project.get_tables_in_schema()
        assert tables["model"] == "table"


# In addition to testing an alternative target-paths setting, it tests that
# the attribute is jinja rendered and that the context "modules" works.
class TestTargetConfigs(BaseConfigProject):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "target-path": "target_{{ modules.datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S') }}",
            "seeds": {
                "quote_columns": False,
            },
        }

    def test_alternative_target_paths(self, project):
        # chdir to a different directory to test creation of target directory under project_root
        os.chdir(project.profiles_dir)
        run_dbt(["seed"])

        target_path = ""
        for d in os.listdir(project.project_root):
            if os.path.isdir(os.path.join(project.project_root, d)) and d.startswith("target_"):
                target_path = d
        assert os.path.exists(os.path.join(project.project_root, target_path, "manifest.json"))


class TestInvalidTestsMaterializationProj(object):
    def test_tests_materialization_proj_config(self, project):
        config_patch = {"tests": {"materialized": "table"}}
        update_config_file(config_patch, project.project_root, "dbt_project.yml")
        tests_dir = os.path.join(project.project_root, "tests")
        write_file("select * from foo", tests_dir, "test.sql")

        with pytest.raises(ValidationError):
            run_dbt()


class TestInvalidSeedsMaterializationProj(object):
    def test_seeds_materialization_proj_config(self, project):
        config_patch = {"seeds": {"materialized": "table"}}
        update_config_file(config_patch, project.project_root, "dbt_project.yml")

        seeds_dir = os.path.join(project.project_root, "seeds")
        write_file("id1, id2\n1, 2", seeds_dir, "seed.csv")

        with pytest.raises(ValidationError):
            run_dbt()


class TestInvalidSeedsMaterializationSchema(object):
    def test_seeds_materialization_schema_config(self, project):
        seeds_dir = os.path.join(project.project_root, "seeds")
        write_file(
            "version: 2\nseeds:\n  - name: myseed\n    config:\n      materialized: table",
            seeds_dir,
            "schema.yml",
        )
        write_file("id1, id2\n1, 2", seeds_dir, "myseed.csv")

        with pytest.raises(ValidationError):
            run_dbt()


class TestInvalidSnapshotsMaterializationProj(object):
    def test_snapshots_materialization_proj_config(self, project):
        config_patch = {"snapshots": {"materialized": "table"}}
        update_config_file(config_patch, project.project_root, "dbt_project.yml")

        snapshots_dir = os.path.join(project.project_root, "snapshots")
        write_file(simple_snapshot, snapshots_dir, "mysnapshot.sql")

        with pytest.raises(ParsingError):
            run_dbt()


class TestInvalidSnapshotsMaterializationSchema(object):
    def test_snapshots_materialization_schema_config(self, project):
        snapshots_dir = os.path.join(project.project_root, "snapshots")
        write_file(
            "version: 2\nsnapshots:\n  - name: mysnapshot\n    config:\n      materialized: table",
            snapshots_dir,
            "schema.yml",
        )
        write_file(simple_snapshot, snapshots_dir, "mysnapshot.sql")

        with pytest.raises(ValidationError):
            run_dbt()
