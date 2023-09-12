import os
import shutil
from collections import Counter
from copy import deepcopy

import pytest

from dbt.exceptions import DbtRuntimeError
from dbt.tests.adapter.dbt_clone.fixtures import (
    seed_csv,
    table_model_sql,
    view_model_sql,
    ephemeral_model_sql,
    exposures_yml,
    schema_yml,
    snapshot_sql,
    get_schema_name_sql,
    macros_sql,
    infinite_macros_sql,
    custom_can_clone_tables_false_macros_sql,
)
from dbt.tests.util import run_dbt, run_dbt_and_capture


class BaseClone:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model_sql,
            "view_model.sql": view_model_sql,
            "ephemeral_model.sql": ephemeral_model_sql,
            "schema.yml": schema_yml,
            "exposures.yml": exposures_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
            "infinite_macros.sql": infinite_macros_sql,
            "get_schema_name.sql": get_schema_name_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot.sql": snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def other_schema(self, unique_schema):
        return unique_schema + "_other"

    @property
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "quote_columns": False,
                }
            }
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema, other_schema):
        outputs = {"default": dbt_profile_target, "otherschema": deepcopy(dbt_profile_target)}
        outputs["default"]["schema"] = unique_schema
        outputs["otherschema"]["schema"] = other_schema
        return {"test": {"outputs": outputs, "target": "default"}}

    def copy_state(self, project_root):
        state_path = os.path.join(project_root, "state")
        if not os.path.exists(state_path):
            os.makedirs(state_path)
        shutil.copyfile(
            f"{project_root}/target/manifest.json", f"{project_root}/state/manifest.json"
        )

    def run_and_save_state(self, project_root, with_snapshot=False):
        results = run_dbt(["seed"])
        assert len(results) == 1
        assert not any(r.node.deferred for r in results)
        results = run_dbt(["run"])
        assert len(results) == 2
        assert not any(r.node.deferred for r in results)
        results = run_dbt(["test"])
        assert len(results) == 2

        if with_snapshot:
            results = run_dbt(["snapshot"])
            assert len(results) == 1
            assert not any(r.node.deferred for r in results)

        # copy files
        self.copy_state(project_root)


# -- Below we define base classes for tests you import the one based on if your adapter uses dbt clone or not --
class BaseClonePossible(BaseClone):
    def test_can_clone_true(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root, with_snapshot=True)

        clone_args = [
            "clone",
            "--state",
            "state",
            "--target",
            "otherschema",
        ]

        results = run_dbt(clone_args)
        assert len(results) == 4

        schema_relations = project.adapter.list_relations(
            database=project.database, schema=other_schema
        )
        types = [r.type for r in schema_relations]
        count_types = Counter(types)
        assert count_types == Counter({"table": 3, "view": 1})

        # objects already exist, so this is a no-op
        results = run_dbt(clone_args)
        assert len(results) == 4
        assert all("no-op" in r.message.lower() for r in results)

        # recreate all objects
        results = run_dbt([*clone_args, "--full-refresh"])
        assert len(results) == 4

        # select only models this time
        results = run_dbt([*clone_args, "--resource-type", "model"])
        assert len(results) == 2
        assert all("no-op" in r.message.lower() for r in results)

    def test_clone_no_state(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root, with_snapshot=True)

        clone_args = [
            "clone",
            "--target",
            "otherschema",
        ]

        with pytest.raises(
            DbtRuntimeError,
            match="--state or --defer-state are required for deferral, but neither was provided",
        ):
            run_dbt(clone_args)


class BaseCloneNotPossible(BaseClone):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
            "my_can_clone_tables.sql": custom_can_clone_tables_false_macros_sql,
            "infinite_macros.sql": infinite_macros_sql,
            "get_schema_name.sql": get_schema_name_sql,
        }

    def test_can_clone_false(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root, with_snapshot=True)

        clone_args = [
            "clone",
            "--state",
            "state",
            "--target",
            "otherschema",
        ]

        results = run_dbt(clone_args)
        assert len(results) == 4

        schema_relations = project.adapter.list_relations(
            database=project.database, schema=other_schema
        )
        assert all(r.type == "view" for r in schema_relations)

        # objects already exist, so this is a no-op
        results = run_dbt(clone_args)
        assert len(results) == 4
        assert all("no-op" in r.message.lower() for r in results)

        # recreate all objects
        results = run_dbt([*clone_args, "--full-refresh"])
        assert len(results) == 4

        # select only models this time
        results = run_dbt([*clone_args, "--resource-type", "model"])
        assert len(results) == 2
        assert all("no-op" in r.message.lower() for r in results)


class TestPostgresCloneNotPossible(BaseCloneNotPossible):
    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_seeds"
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass


class TestCloneSameTargetAndState(BaseClone):
    def test_clone_same_target_and_state(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root)

        clone_args = [
            "clone",
            "--state",
            "target",
        ]

        results, output = run_dbt_and_capture(clone_args, expect_pass=False)
        assert "Warning: The state and target directories are the same: 'target'" in output
