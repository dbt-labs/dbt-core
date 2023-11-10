import pytest
import os
import shutil
from copy import deepcopy
from dbt.tests.util import (
    run_dbt,
    write_file,
    get_manifest,
    get_artifact,
    write_config_file,
)
from dbt.exceptions import DuplicateResourceNameError
from fixtures import (
    my_model_vars_sql,
    my_model_a_sql,
    my_model_b_sql,
    test_my_model_yml,
    datetime_test,
    my_incremental_model_sql,
    event_sql,
    test_my_model_incremental_yml,
)


class TestUnitTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_vars_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_yml + datetime_test,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"my_test": "my_test_var"}}

    def test_basic(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        # Select by model name
        results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5

        # Test select by test name
        results = run_dbt(["unit-test", "--select", "test_name:test_my_model_string_concat"])
        assert len(results) == 1

        # Select, method not specified
        results = run_dbt(["unit-test", "--select", "test_my_model_overrides"])
        assert len(results) == 1

        # Select using tag
        results = run_dbt(["unit-test", "--select", "tag:test_this"])
        assert len(results) == 1

        # Partial parsing... remove test
        write_file(test_my_model_yml, project.project_root, "models", "test_my_model.yml")
        results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 4

        # Partial parsing... put back removed test
        write_file(
            test_my_model_yml + datetime_test, project.project_root, "models", "test_my_model.yml"
        )
        results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5

        manifest = get_manifest(project.project_root)
        assert len(manifest.unit_tests) == 5
        # Every unit test has a depends_on to the model it tests
        for unit_test_definition in manifest.unit_tests.values():
            assert unit_test_definition.depends_on.nodes[0] == "model.test.my_model"

        # We should have a UnitTestNode for every test, plus two input models for each test
        unit_test_manifest = get_artifact(
            project.project_root, "target", "unit_test_manifest.json"
        )
        assert len(unit_test_manifest["nodes"]) == 15

        # Check for duplicate unit test name
        # this doesn't currently pass with partial parsing because of the root problem
        # described in https://github.com/dbt-labs/dbt-core/issues/8982
        write_file(
            test_my_model_yml + datetime_test + datetime_test,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(DuplicateResourceNameError):
            run_dbt(["run", "--no-partial-parse", "--select", "my_model"])


class TestUnitTestIncrementalModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_incremental_model.sql": my_incremental_model_sql,
            "events.sql": event_sql,
            "test_my_incremental_model.yml": test_my_model_incremental_yml,
        }

    def test_basic(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        # Select by model name
        results = run_dbt(["unit-test", "--select", "my_incremental_model"], expect_pass=True)
        assert len(results) == 2


class UnitTestState:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_vars_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"my_test": "my_test_var"}}

    def copy_state(self, project_root):
        state_path = os.path.join(project_root, "state")
        if not os.path.exists(state_path):
            os.makedirs(state_path)
        shutil.copyfile(
            f"{project_root}/target/manifest.json", f"{project_root}/state/manifest.json"
        )
        shutil.copyfile(
            f"{project_root}/target/run_results.json", f"{project_root}/state/run_results.json"
        )


class TestUnitTestStateModified(UnitTestState):
    def test_state_modified(self, project):
        run_dbt(["run"])
        run_dbt(["unit-test"], expect_pass=False)
        self.copy_state(project.project_root)

        # no changes
        results = run_dbt(["unit-test", "--select", "state:modified", "--state", "state"])
        assert len(results) == 0

        # change unit test definition
        with_changes = test_my_model_yml.replace("{string_c: ab}", "{string_c: bc}")
        write_config_file(with_changes, project.project_root, "models", "test_my_model.yml")
        results = run_dbt(
            ["unit-test", "--select", "state:modified", "--state", "state"], expect_pass=False
        )
        assert len(results) == 1

        # change underlying model logic
        write_config_file(test_my_model_yml, project.project_root, "models", "test_my_model.yml")
        write_file(
            my_model_vars_sql.replace("a+b as c,", "a + b as c,"),
            project.project_root,
            "models",
            "my_model.sql",
        )
        results = run_dbt(
            ["unit-test", "--select", "state:modified", "--state", "state"], expect_pass=False
        )
        assert len(results) == 4


class TestUnitTestRetry(UnitTestState):
    def test_unit_test_retry(self, project):
        run_dbt(["run"])
        run_dbt(["unit-test"], expect_pass=False)
        self.copy_state(project.project_root)

        results = run_dbt(["retry"], expect_pass=False)
        assert len(results) == 1


class TestUnitTestDeferState(UnitTestState):
    @pytest.fixture(scope="class")
    def other_schema(self, unique_schema):
        return unique_schema + "_other"

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema, other_schema):
        outputs = {"default": dbt_profile_target, "otherschema": deepcopy(dbt_profile_target)}
        outputs["default"]["schema"] = unique_schema
        outputs["otherschema"]["schema"] = other_schema
        return {"test": {"outputs": outputs, "target": "default"}}

    def test_unit_test_defer_state(self, project):
        run_dbt(["run", "--target", "otherschema"])
        self.copy_state(project.project_root)
        results = run_dbt(["unit-test", "--defer", "--state", "state"], expect_pass=False)
        assert len(results) == 4
        assert sorted([r.status for r in results]) == ["fail", "pass", "pass", "pass"]
