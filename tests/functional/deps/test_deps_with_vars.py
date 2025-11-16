"""Test that dbt deps works when vars are used in dbt_project.yml without defaults."""

import pytest

from dbt.tests.util import run_dbt

# dbt_project.yml that uses a var without a default value in model configs
dbt_project_yml = """
name: 'test_project'
version: '1.0'
config-version: 2

profile: 'test'

models:
  test_project:
    +dataset: "dqm_{{ var('my_dataset') }}"
"""

# A simple packages.yml to test deps
packages_yml = """
packages: []
"""


class TestDepsWithRequiredVar:
    """Test that dbt deps succeeds even when a required var is used in dbt_project.yml"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Return the project config as a dict
        return {"models": {"test_project": {"+dataset": "dqm_{{ var('my_dataset') }}"}}}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": []}

    def test_deps_with_required_var_succeeds(self, project):
        """Test that dbt deps succeeds when a var without a default is in dbt_project.yml"""
        # This should not raise an error even though 'my_dataset' var is not provided
        results = run_dbt(["deps"])
        assert results is None or results == []  # deps returns None on success


class TestDepsWithRequiredVarInConfig:
    """Test that dbt deps succeeds with various var configurations"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test_project": {
                    "+schema": "{{ var('my_schema') }}",
                    "+dataset": "dqm_{{ var('my_dataset') }}",
                }
            }
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": []}

    def test_deps_with_multiple_required_vars(self, project):
        """Test that dbt deps succeeds with multiple required vars"""
        # This should not raise an error
        results = run_dbt(["deps"])
        assert results is None or results == []
