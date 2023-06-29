import json
import os
import shutil
from copy import deepcopy

import pytest

from dbt.cli.exceptions import DbtUsageException
from dbt.contracts.results import RunStatus
from dbt.exceptions import DbtRuntimeError
from dbt.tests.util import run_dbt, write_file, rm_file
from tests.functional.defer_state.fixtures import (
    seed_csv,
    table_model_sql,
    changed_table_model_sql,
    view_model_sql,
    changed_view_model_sql,
    ephemeral_model_sql,
    changed_ephemeral_model_sql,
    schema_yml,
    exposures_yml,
    macros_sql,
    infinite_macros_sql,
    snapshot_sql,
    view_model_now_table_sql,
)


class BaseDeferState:
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


class TestDeferStateUnsupportedCommands(BaseDeferState):
    def test_unsupported_commands(self, project):
        # make sure these commands don"t work with --defer
        with pytest.raises(DbtUsageException):
            run_dbt(["seed", "--defer"])

    def test_no_state(self, project):
        # no "state" files present, snapshot fails
        with pytest.raises(DbtRuntimeError):
            run_dbt(["snapshot", "--state", "state", "--defer"])


class TestRunCompileState(BaseDeferState):
    def test_run_and_compile_defer(self, project):
        self.run_and_save_state(project.project_root)

        # defer test, it succeeds
        # Change directory to ensure that state directory is underneath
        # project directory.
        os.chdir(project.profiles_dir)
        results = run_dbt(["compile", "--state", "state", "--defer"])
        assert len(results.results) == 6
        assert results.results[0].node.name == "seed"


class TestSnapshotState(BaseDeferState):
    def test_snapshot_state_defer(self, project):
        self.run_and_save_state(project.project_root)
        # snapshot succeeds without --defer
        run_dbt(["snapshot"])
        # copy files
        self.copy_state(project.project_root)
        # defer test, it succeeds
        run_dbt(["snapshot", "--state", "state", "--defer"])
        # favor_state test, it succeeds
        run_dbt(["snapshot", "--state", "state", "--defer", "--favor-state"])


class TestRunDeferState(BaseDeferState):
    def test_run_and_defer(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root)

        # test tests first, because run will change things
        # no state, wrong schema, failure.
        run_dbt(["test", "--target", "otherschema"], expect_pass=False)

        # test generate docs
        # no state, wrong schema, empty nodes
        catalog = run_dbt(["docs", "generate", "--target", "otherschema"])
        assert not catalog.nodes

        # no state, run also fails
        run_dbt(["run", "--target", "otherschema"], expect_pass=False)

        # defer test, it succeeds
        results = run_dbt(
            ["test", "-m", "view_model+", "--state", "state", "--defer", "--target", "otherschema"]
        )

        # defer docs generate with state, catalog refers schema from the happy times
        catalog = run_dbt(
            [
                "docs",
                "generate",
                "-m",
                "view_model+",
                "--state",
                "state",
                "--defer",
                "--target",
                "otherschema",
            ]
        )
        assert other_schema not in catalog.nodes["seed.test.seed"].metadata.schema
        assert unique_schema in catalog.nodes["seed.test.seed"].metadata.schema

        # with state it should work though
        results = run_dbt(
            ["run", "-m", "view_model", "--state", "state", "--defer", "--target", "otherschema"]
        )
        assert other_schema not in results[0].node.compiled_code
        assert unique_schema in results[0].node.compiled_code

        with open("target/manifest.json") as fp:
            data = json.load(fp)
        assert data["nodes"]["seed.test.seed"]["deferred"]

        assert len(results) == 1


class TestRunDeferStateChangedModel(BaseDeferState):
    def test_run_defer_state_changed_model(self, project):
        self.run_and_save_state(project.project_root)

        # change "view_model"
        write_file(changed_view_model_sql, "models", "view_model.sql")

        # the sql here is just wrong, so it should fail
        run_dbt(
            ["run", "-m", "view_model", "--state", "state", "--defer", "--target", "otherschema"],
            expect_pass=False,
        )
        # but this should work since we just use the old happy model
        run_dbt(
            ["run", "-m", "table_model", "--state", "state", "--defer", "--target", "otherschema"],
            expect_pass=True,
        )

        # change "ephemeral_model"
        write_file(changed_ephemeral_model_sql, "models", "ephemeral_model.sql")
        # this should fail because the table model refs a broken ephemeral
        # model, which it should see
        run_dbt(
            ["run", "-m", "table_model", "--state", "state", "--defer", "--target", "otherschema"],
            expect_pass=False,
        )


class TestRunDeferStateIFFNotExists(BaseDeferState):
    def test_run_defer_iff_not_exists(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root)

        results = run_dbt(["seed", "--target", "otherschema"])
        assert len(results) == 1
        results = run_dbt(["run", "--state", "state", "--defer", "--target", "otherschema"])
        assert len(results) == 2

        # because the seed now exists in our "other" schema, we should prefer it over the one
        # available from state
        assert other_schema in results[0].node.compiled_code

        # this time with --favor-state: even though the seed now exists in our "other" schema,
        # we should still favor the one available from state
        results = run_dbt(
            ["run", "--state", "state", "--defer", "--favor-state", "--target", "otherschema"]
        )
        assert len(results) == 2
        assert other_schema not in results[0].node.compiled_code


class TestDeferStateDeletedUpstream(BaseDeferState):
    def test_run_defer_deleted_upstream(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root)

        # remove "ephemeral_model" + change "table_model"
        rm_file("models", "ephemeral_model.sql")
        write_file(changed_table_model_sql, "models", "table_model.sql")

        # ephemeral_model is now gone. previously this caused a
        # keyerror (dbt#2875), now it should pass
        run_dbt(
            ["run", "-m", "view_model", "--state", "state", "--defer", "--target", "otherschema"],
            expect_pass=True,
        )

        # despite deferral, we should use models just created in our schema
        results = run_dbt(["test", "--state", "state", "--defer", "--target", "otherschema"])
        assert other_schema in results[0].node.compiled_code

        # this time with --favor-state: prefer the models in the "other" schema, even though they exist in ours
        run_dbt(
            [
                "run",
                "-m",
                "view_model",
                "--state",
                "state",
                "--defer",
                "--favor-state",
                "--target",
                "otherschema",
            ],
            expect_pass=True,
        )
        results = run_dbt(["test", "--state", "state", "--defer", "--favor-state"])
        assert other_schema not in results[0].node.compiled_code


class TestDeferStateFlag(BaseDeferState):
    def test_defer_state_flag(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)

        # test that state deferral works correctly
        run_dbt(["compile", "--target-path", "target_compile"])
        write_file(view_model_now_table_sql, "models", "table_model.sql")

        results = run_dbt(["ls", "--select", "state:modified", "--state", "target_compile"])
        assert results == ["test.table_model"]

        run_dbt(["seed", "--target", "otherschema", "--target-path", "target_otherschema"])

        # this will fail because we haven't loaded the seed in the default schema
        run_dbt(
            [
                "run",
                "--select",
                "state:modified",
                "--defer",
                "--state",
                "target_compile",
                "--favor-state",
            ],
            expect_pass=False,
        )

        # this will fail because we haven't passed in --state
        with pytest.raises(
            DbtRuntimeError, match="Got a state selector method, but no comparison manifest"
        ):
            run_dbt(
                [
                    "run",
                    "--select",
                    "state:modified",
                    "--defer",
                    "--defer-state",
                    "target_otherschema",
                    "--favor-state",
                ],
                expect_pass=False,
            )

        # this will succeed because we've loaded the seed in other schema and are successfully deferring to it instead
        results = run_dbt(
            [
                "run",
                "--select",
                "state:modified",
                "--defer",
                "--state",
                "target_compile",
                "--defer-state",
                "target_otherschema",
                "--favor-state",
            ]
        )

        assert len(results.results) == 1
        assert results.results[0].status == RunStatus.Success
        assert results.results[0].node.name == "table_model"
        assert results.results[0].adapter_response["rows_affected"] == 2
