import os
import shutil

import pytest

from dbt.tests.util import run_dbt, write_file
from tests.functional.defer_state.fixtures import (
    ephemeral_model_sql,
    exposures_yml,
    infinite_macros_sql,
    macros_sql,
    schema_yml,
    seed_csv,
    table_model_sql,
    view_model_sql,
)


class BaseRunResultsState:
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

    @property
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "quote_columns": False,
                }
            }
        }

    def clear_state(self):
        shutil.rmtree("./state")

    def copy_state(self):
        if not os.path.exists("state"):
            os.makedirs("state")
        shutil.copyfile("target/manifest.json", "state/manifest.json")
        shutil.copyfile("target/run_results.json", "state/run_results.json")

    def run_and_save_state(self):
        run_dbt(["build"])
        self.copy_state()

    def rebuild_run_dbt(self, expect_pass=True):
        self.clear_state()
        run_dbt(["build"], expect_pass=expect_pass)
        self.copy_state()

    def update_view_model_bad_sql(self):
        # update view model to generate a failure case
        not_unique_sql = "select * from forced_error"
        write_file(not_unique_sql, "models", "view_model.sql")

    def update_view_model_failing_tests(self, with_dupes=True, with_nulls=False):
        # test failure on build tests
        # fail the unique test
        select_1 = "select 1 as id"
        select_stmts = [select_1]
        if with_dupes:
            select_stmts.append(select_1)
        if with_nulls:
            select_stmts.append("select null as id")
        failing_tests_sql = " union all ".join(select_stmts)
        write_file(failing_tests_sql, "models", "view_model.sql")

    def update_unique_test_severity_warn(self):
        # change the unique test severity from error to warn and reuse the same view_model.sql changes above
        new_config = schema_yml.replace("error", "warn")
        write_file(new_config, "models", "schema.yml")


class TestSeedRunResultsState(BaseRunResultsState):
    def test_seed_run_results_state(self, project):
        self.run_and_save_state()
        self.clear_state()
        run_dbt(["seed"])
        self.copy_state()
        results = run_dbt(
            ["ls", "--resource-type", "seed", "--select", "result:success", "--state", "./state"],
            expect_pass=True,
        )
        assert len(results) == 1
        assert results[0] == "test.seed"

        results = run_dbt(["ls", "--select", "result:success", "--state", "./state"])
        assert len(results) == 1
        assert results[0] == "test.seed"

        results = run_dbt(["ls", "--select", "result:success+", "--state", "./state"])
        assert len(results) == 7
        assert set(results) == {
            "test.seed",
            "test.table_model",
            "test.view_model",
            "test.ephemeral_model",
            "test.not_null_view_model_id",
            "test.unique_view_model_id",
            "exposure:test.my_exposure",
        }

        # add a new faulty row to the seed
        changed_seed_contents = seed_csv + "\n" + "\\\3,carl"
        write_file(changed_seed_contents, "seeds", "seed.csv")

        self.clear_state()
        run_dbt(["seed"], expect_pass=False)
        self.copy_state()

        results = run_dbt(
            ["ls", "--resource-type", "seed", "--select", "result:error", "--state", "./state"],
            expect_pass=True,
        )
        assert len(results) == 1
        assert results[0] == "test.seed"

        results = run_dbt(["ls", "--select", "result:error", "--state", "./state"])
        assert len(results) == 1
        assert results[0] == "test.seed"

        results = run_dbt(["ls", "--select", "result:error+", "--state", "./state"])
        assert len(results) == 7
        assert set(results) == {
            "test.seed",
            "test.table_model",
            "test.view_model",
            "test.ephemeral_model",
            "test.not_null_view_model_id",
            "test.unique_view_model_id",
            "exposure:test.my_exposure",
        }


class TestBuildRunResultsState(BaseRunResultsState):
    def test_build_run_results_state(self, project):
        self.run_and_save_state()
        results = run_dbt(["build", "--select", "result:error", "--state", "./state"])
        assert len(results) == 0

        self.update_view_model_bad_sql()
        self.rebuild_run_dbt(expect_pass=False)

        results = run_dbt(
            ["build", "--select", "result:error", "--state", "./state"], expect_pass=False
        )
        assert len(results) == 3
        nodes = set([elem.node.name for elem in results])
        assert nodes == {"view_model", "not_null_view_model_id", "unique_view_model_id"}

        results = run_dbt(["ls", "--select", "result:error", "--state", "./state"])
        assert len(results) == 3
        assert set(results) == {
            "test.view_model",
            "test.not_null_view_model_id",
            "test.unique_view_model_id",
        }

        results = run_dbt(
            ["build", "--select", "result:error+", "--state", "./state"], expect_pass=False
        )
        assert len(results) == 4
        nodes = set([elem.node.name for elem in results])
        assert nodes == {
            "table_model",
            "view_model",
            "not_null_view_model_id",
            "unique_view_model_id",
        }

        results = run_dbt(["ls", "--select", "result:error+", "--state", "./state"])
        assert len(results) == 6  # includes exposure
        assert set(results) == {
            "test.table_model",
            "test.view_model",
            "test.ephemeral_model",
            "test.not_null_view_model_id",
            "test.unique_view_model_id",
            "exposure:test.my_exposure",
        }

        self.update_view_model_failing_tests()
        self.rebuild_run_dbt(expect_pass=False)

        results = run_dbt(
            ["build", "--select", "result:fail", "--state", "./state"], expect_pass=False
        )
        assert len(results) == 1
        assert results[0].node.name == "unique_view_model_id"

        results = run_dbt(["ls", "--select", "result:fail", "--state", "./state"])
        assert len(results) == 1
        assert results[0] == "test.unique_view_model_id"

        results = run_dbt(
            ["build", "--select", "result:fail+", "--state", "./state"], expect_pass=False
        )
        assert len(results) == 1
        nodes = set([elem.node.name for elem in results])
        assert nodes == {"unique_view_model_id"}

        results = run_dbt(["ls", "--select", "result:fail+", "--state", "./state"])
        assert len(results) == 1
        assert set(results) == {"test.unique_view_model_id"}

        self.update_unique_test_severity_warn()
        self.rebuild_run_dbt(expect_pass=True)

        results = run_dbt(
            ["build", "--select", "result:warn", "--state", "./state"], expect_pass=True
        )
        assert len(results) == 1
        assert results[0].node.name == "unique_view_model_id"

        results = run_dbt(["ls", "--select", "result:warn", "--state", "./state"])
        assert len(results) == 1
        assert results[0] == "test.unique_view_model_id"

        results = run_dbt(
            ["build", "--select", "result:warn+", "--state", "./state"], expect_pass=True
        )
        assert len(results) == 1
        nodes = set([elem.node.name for elem in results])
        assert nodes == {"unique_view_model_id"}

        results = run_dbt(["ls", "--select", "result:warn+", "--state", "./state"])
        assert len(results) == 1
        assert set(results) == {"test.unique_view_model_id"}


class TestRunRunResultsState(BaseRunResultsState):
    def test_run_run_results_state(self, project):
        self.run_and_save_state()
        results = run_dbt(
            ["run", "--select", "result:success", "--state", "./state"], expect_pass=True
        )
        assert len(results) == 2
        assert results[0].node.name == "view_model"
        assert results[1].node.name == "table_model"

        # clear state and rerun upstream view model to test + operator
        self.clear_state()
        run_dbt(["run", "--select", "view_model"], expect_pass=True)
        self.copy_state()
        results = run_dbt(
            ["run", "--select", "result:success+", "--state", "./state"], expect_pass=True
        )
        assert len(results) == 2
        assert results[0].node.name == "view_model"
        assert results[1].node.name == "table_model"

        # check we are starting from a place with 0 errors
        results = run_dbt(["run", "--select", "result:error", "--state", "./state"])
        assert len(results) == 0

        self.update_view_model_bad_sql()
        self.clear_state()
        run_dbt(["run"], expect_pass=False)
        self.copy_state()

        # test single result selector on error
        results = run_dbt(
            ["run", "--select", "result:error", "--state", "./state"], expect_pass=False
        )
        assert len(results) == 1
        assert results[0].node.name == "view_model"

        # test + operator selection on error
        results = run_dbt(
            ["run", "--select", "result:error+", "--state", "./state"], expect_pass=False
        )
        assert len(results) == 2
        assert results[0].node.name == "view_model"
        assert results[1].node.name == "table_model"

        # single result selector on skipped. Expect this to pass becase underlying view already defined above
        results = run_dbt(
            ["run", "--select", "result:skipped", "--state", "./state"], expect_pass=True
        )
        assert len(results) == 1
        assert results[0].node.name == "table_model"

        # add a downstream model that depends on table_model for skipped+ selector
        downstream_model_sql = "select * from {{ref('table_model')}}"
        write_file(downstream_model_sql, "models", "table_model_downstream.sql")

        self.clear_state()
        run_dbt(["run"], expect_pass=False)
        self.copy_state()

        results = run_dbt(
            ["run", "--select", "result:skipped+", "--state", "./state"], expect_pass=True
        )
        assert len(results) == 2
        assert results[0].node.name == "table_model"
        assert results[1].node.name == "table_model_downstream"


class TestTestRunResultsState(BaseRunResultsState):
    def test_test_run_results_state(self, project):
        self.run_and_save_state()
        # run passed nodes
        results = run_dbt(
            ["test", "--select", "result:pass", "--state", "./state"], expect_pass=True
        )
        assert len(results) == 2
        nodes = set([elem.node.name for elem in results])
        assert nodes == {"unique_view_model_id", "not_null_view_model_id"}

        # run passed nodes with + operator
        results = run_dbt(
            ["test", "--select", "result:pass+", "--state", "./state"], expect_pass=True
        )
        assert len(results) == 2
        nodes = set([elem.node.name for elem in results])
        assert nodes == {"unique_view_model_id", "not_null_view_model_id"}

        self.update_view_model_failing_tests()
        self.rebuild_run_dbt(expect_pass=False)

        # test with failure selector
        results = run_dbt(
            ["test", "--select", "result:fail", "--state", "./state"], expect_pass=False
        )
        assert len(results) == 1
        assert results[0].node.name == "unique_view_model_id"

        # test with failure selector and + operator
        results = run_dbt(
            ["test", "--select", "result:fail+", "--state", "./state"], expect_pass=False
        )
        assert len(results) == 1
        assert results[0].node.name == "unique_view_model_id"

        self.update_unique_test_severity_warn()
        # rebuild - expect_pass = True because we changed the error to a warning this time around
        self.rebuild_run_dbt(expect_pass=True)

        # test with warn selector
        results = run_dbt(
            ["test", "--select", "result:warn", "--state", "./state"], expect_pass=True
        )
        assert len(results) == 1
        assert results[0].node.name == "unique_view_model_id"

        # test with warn selector and + operator
        results = run_dbt(
            ["test", "--select", "result:warn+", "--state", "./state"], expect_pass=True
        )
        assert len(results) == 1
        assert results[0].node.name == "unique_view_model_id"


class TestConcurrentSelectionRunResultsState(BaseRunResultsState):
    def test_concurrent_selection_run_run_results_state(self, project):
        self.run_and_save_state()
        results = run_dbt(
            ["run", "--select", "state:modified+", "result:error+", "--state", "./state"]
        )
        assert len(results) == 0

        self.update_view_model_bad_sql()
        self.clear_state()
        run_dbt(["run"], expect_pass=False)
        self.copy_state()

        # add a new failing dbt model
        bad_sql = "select * from forced_error"
        write_file(bad_sql, "models", "table_model_modified_example.sql")

        results = run_dbt(
            ["run", "--select", "state:modified+", "result:error+", "--state", "./state"],
            expect_pass=False,
        )
        assert len(results) == 3
        nodes = set([elem.node.name for elem in results])
        assert nodes == {"view_model", "table_model_modified_example", "table_model"}


class TestConcurrentSelectionTestRunResultsState(BaseRunResultsState):
    def test_concurrent_selection_test_run_results_state(self, project):
        self.run_and_save_state()
        # create failure test case for result:fail selector
        self.update_view_model_failing_tests(with_nulls=True)

        # run dbt build again to trigger test errors
        self.rebuild_run_dbt(expect_pass=False)

        # get the failures from
        results = run_dbt(
            [
                "test",
                "--select",
                "result:fail",
                "--exclude",
                "not_null_view_model_id",
                "--state",
                "./state",
            ],
            expect_pass=False,
        )
        assert len(results) == 1
        nodes = set([elem.node.name for elem in results])
        assert nodes == {"unique_view_model_id"}


class TestConcurrentSelectionBuildRunResultsState(BaseRunResultsState):
    def test_concurrent_selectors_build_run_results_state(self, project):
        self.run_and_save_state()
        results = run_dbt(
            ["build", "--select", "state:modified+", "result:error+", "--state", "./state"]
        )
        assert len(results) == 0

        self.update_view_model_bad_sql()
        self.rebuild_run_dbt(expect_pass=False)

        # add a new failing dbt model
        bad_sql = "select * from forced_error"
        write_file(bad_sql, "models", "table_model_modified_example.sql")

        results = run_dbt(
            ["build", "--select", "state:modified+", "result:error+", "--state", "./state"],
            expect_pass=False,
        )
        assert len(results) == 5
        nodes = set([elem.node.name for elem in results])
        assert nodes == {
            "table_model_modified_example",
            "view_model",
            "table_model",
            "not_null_view_model_id",
            "unique_view_model_id",
        }

        self.update_view_model_failing_tests()

        # create error model case for result:error selector
        more_bad_sql = "select 1 as id from not_exists"
        write_file(more_bad_sql, "models", "error_model.sql")

        # create something downstream from the error model to rerun
        downstream_model_sql = "select * from {{ ref('error_model') }} )"
        write_file(downstream_model_sql, "models", "downstream_of_error_model.sql")

        # regenerate build state
        self.rebuild_run_dbt(expect_pass=False)

        # modify model again to trigger the state:modified selector
        bad_again_sql = "select * from forced_anothererror"
        write_file(bad_again_sql, "models", "table_model_modified_example.sql")

        results = run_dbt(
            [
                "build",
                "--select",
                "state:modified+",
                "result:error+",
                "result:fail+",
                "--state",
                "./state",
            ],
            expect_pass=False,
        )
        assert len(results) == 4
        nodes = set([elem.node.name for elem in results])
        assert nodes == {
            "error_model",
            "downstream_of_error_model",
            "table_model_modified_example",
            "unique_view_model_id",
        }
