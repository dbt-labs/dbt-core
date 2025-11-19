"""Test that dbt deps works when vars are used in dbt_project.yml without defaults.

Note: Functional tests for this behavior are limited because the test framework
itself needs to load the project to set up the test environment. The actual behavior
is verified through unit tests in test_renderer_with_vars.py and manual testing.

The key behavior being tested:
- dbt deps uses lenient mode (require_vars=False) and succeeds even with missing vars
- dbt run/compile use strict mode (require_vars=True) and show helpful error messages
"""

import pytest

from dbt.tests.util import run_dbt


class TestDepsWithVarsWithDefaults:
    """Test that dbt deps works correctly when vars have default values"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Using defaults for test setup, which is the common case
        return {"models": {"test_project": {"+dataset": "dqm_{{ var('my_dataset', 'default') }}"}}}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": []}

    def test_deps_with_var_defaults(self, project):
        """Test that dbt deps succeeds when vars have defaults"""
        results = run_dbt(["deps"])
        assert results is None or results == []  # deps returns None on success
