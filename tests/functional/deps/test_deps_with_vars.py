"""Test that dbt deps works when vars are used in dbt_project.yml without defaults.

The key behavior being tested:
- dbt deps uses lenient mode (require_vars=False) and succeeds even with missing vars
- dbt run/compile/build/debug use strict mode (require_vars=True) and show the right error messages

Expected behavior from reviewer's scenario:
1. dbt deps succeeds (doesn't need vars)
2. dbt run fails with error "Required var 'X' not found"
3. dbt run --vars succeeds when vars provided
"""

import os

import pytest
import yaml

from dbt.tests.util import run_dbt
from dbt_common.exceptions import CompilationError

# Simple model for testing
model_sql = """
select 1 as id
"""


# Test 1: Happy path - deps with defaults
class TestDepsSucceedsWithVarDefaults:
    """Test that dbt deps succeeds when vars have default values"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # config: +dataset: "{{ var('my_dataset', 'default') }}"
        return {"models": {"test_project": {"+dataset": "dqm_{{ var('my_dataset', 'default') }}"}}}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": []}

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    def test_deps_succeeds(self, project):
        # run: dbt deps
        # assert: succeeds
        results = run_dbt(["deps"])
        assert results is None or results == []


# Test 2: Happy path - run with defaults
class TestRunSucceedsWithVarDefaults:
    """Test that dbt run succeeds when vars have default values"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # config: +materialized: "{{ var('my_var', 'view') }}"
        return {"models": {"test_project": {"+materialized": "{{ var('my_var', 'view') }}"}}}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": []}

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    def test_run_succeeds(self, project):
        # run: dbt run
        # assert: succeeds
        results = run_dbt(["run"])
        assert len(results) == 1


# Test 3: Happy path - run with explicit vars
class TestRunSucceedsWithExplicitVars:
    """Test that dbt run succeeds when vars provided via --vars"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # config: +materialized: "{{ var('my_var', 'view') }}"
        return {
            "models": {
                "test_project": {"+materialized": "{{ var('my_materialization', 'view') }}"}
            }
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": []}

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    def test_run_succeeds_with_vars(self, project):
        # run: dbt run --vars '{"my_var": "table"}'
        # assert: succeeds
        results = run_dbt(["run", "--vars", '{"my_materialization": "table"}'])
        assert len(results) == 1


# Test 4: Run fails Wwith the right error message
class TestRunFailsWithMissingVar:
    """Test dbt run fails with right error'"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # config: start with default for setup
        return {
            "models": {
                "test_project": {"+materialized": "{{ var('example_materialized', 'view') }}"}
            }
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": []}

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    def test_run_fails_with_error(self, project):
        # IN TEST: dynamically remove default
        project_yml_path = os.path.join(project.project_root, "dbt_project.yml")
        with open(project_yml_path, "r") as f:
            project_config = yaml.safe_load(f)

        project_config["models"]["test_project"][
            "+materialized"
        ] = "{{ var('example_materialized') }}"

        with open(project_yml_path, "w") as f:
            yaml.dump(project_config, f)

        # run: dbt run
        # assert: fails with "Required var 'X' not found"
        try:
            run_dbt(["run"], expect_pass=False)
            assert False, "Expected run to fail with missing required var"
        except CompilationError as e:
            error_msg = str(e)
            # ✅ Verify error message
            assert "example_materialized" in error_msg, "Error should mention var name"
            assert (
                "Required var" in error_msg or "not found" in error_msg
            ), "Error should say 'Required var' or 'not found'"


# Test 5: compile also fails with the correct error
class TestCompileFailsWithMissingVar:
    """Test dbt compile fails with error for missing vars"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # config: start with simple hardcoded value
        return {"models": {"test_project": {"+materialized": "view"}}}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": []}

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    def test_compile_fails_with_error(self, project):
        # IN TEST: dynamically add var without default
        project_yml_path = os.path.join(project.project_root, "dbt_project.yml")
        with open(project_yml_path, "r") as f:
            project_config = yaml.safe_load(f)

        project_config["models"]["test_project"][
            "+materialized"
        ] = "{{ var('compile_var_no_default') }}"

        with open(project_yml_path, "w") as f:
            yaml.dump(project_config, f)

        # run: dbt compile
        # assert: fails with "Required var 'X' not found"
        try:
            run_dbt(["compile"], expect_pass=False)
            assert False, "Expected compile to fail with missing var"
        except CompilationError as e:
            error_msg = str(e)
            assert "compile_var_no_default" in error_msg
            assert "Required var" in error_msg or "not found" in error_msg


# Test 6: deps succeeds even when var missing
class TestDepsSucceedsEvenWhenVarMissing:
    """Test dbt deps succeeds even when var has no default"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # config: start with default for setup
        return {
            "models": {"test_project": {"+materialized": "{{ var('deps_test_var', 'view') }}"}}
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": []}

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    def test_deps_still_succeeds(self, project):
        # run: dbt deps (succeeds)
        results = run_dbt(["deps"])
        assert results is None or results == []

        # IN TEST: modify config to remove var default
        project_yml_path = os.path.join(project.project_root, "dbt_project.yml")
        with open(project_yml_path, "r") as f:
            project_config = yaml.safe_load(f)

        project_config["models"]["test_project"]["+materialized"] = "{{ var('deps_test_var') }}"

        with open(project_yml_path, "w") as f:
            yaml.dump(project_config, f)

        # run: dbt deps again (still succeeds - lenient mode)
        results = run_dbt(["deps"])
        assert results is None or results == []

        # run: dbt run (fails - strict mode)
        try:
            run_dbt(["run"], expect_pass=False)
            assert False, "Expected run to fail with missing var"
        except CompilationError as e:
            error_msg = str(e)
            assert "deps_test_var" in error_msg
            assert "Required var" in error_msg or "not found" in error_msg


# Test 7: build also fails
class TestBuildFailsWithMissingVar:
    """Test dbt build fails with error for missing vars"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # config: start with simple hardcoded value
        return {"models": {"test_project": {"+materialized": "view"}}}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": []}

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    def test_build_fails_with_error(self, project):
        # IN TEST: dynamically add var without default
        project_yml_path = os.path.join(project.project_root, "dbt_project.yml")
        with open(project_yml_path, "r") as f:
            project_config = yaml.safe_load(f)

        project_config["models"]["test_project"][
            "+materialized"
        ] = "{{ var('build_var_no_default') }}"

        with open(project_yml_path, "w") as f:
            yaml.dump(project_config, f)

        # run: dbt build
        # assert: fails with "Required var 'X' not found"
        try:
            run_dbt(["build"], expect_pass=False)
            assert False, "Expected build to fail with missing var"
        except CompilationError as e:
            error_msg = str(e)
            assert "build_var_no_default" in error_msg
            assert "Required var" in error_msg or "not found" in error_msg


# Test 8: debug with defaults
class TestDebugSucceedsWithVarDefaults:
    """Test dbt debug succeeds when vars have defaults (no regression)"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # config: +materialized: "{{ var('debug_var', 'view') }}"
        return {"models": {"test_project": {"+materialized": "{{ var('debug_var', 'view') }}"}}}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": []}

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    def test_debug_succeeds(self, project):
        # run: dbt debug
        # assert: succeeds (no regression)
        run_dbt(["debug"])


# Test 9: debug fails like run/compile (strict mode)
class TestDebugFailsWithMissingVar:
    """Test dbt debug fails with error (strict mode like run/compile)"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # config: start with default for setup
        return {"models": {"test_project": {"+materialized": "{{ var('debug_var', 'view') }}"}}}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": []}

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    def test_debug_fails_with_error(self, project):
        # First verify debug works with default
        run_dbt(["debug"])

        # IN TEST: dynamically remove default
        project_yml_path = os.path.join(project.project_root, "dbt_project.yml")
        with open(project_yml_path, "r") as f:
            project_config = yaml.safe_load(f)

        project_config["models"]["test_project"]["+materialized"] = "{{ var('debug_var') }}"

        with open(project_yml_path, "w") as f:
            yaml.dump(project_config, f)

        # run: dbt debug
        # assert: fails with "Required var 'X' not found"
        try:
            run_dbt(["debug"], expect_pass=False)
            assert False, "Expected debug to fail with missing var"
        except CompilationError as e:
            error_msg = str(e)
            assert "debug_var" in error_msg
            assert "Required var" in error_msg or "not found" in error_msg
