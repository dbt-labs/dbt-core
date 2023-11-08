import os
import subprocess
import pytest

from dbt.tests.util import run_dbt_and_capture

# we use a macro to print the value and check the logs when testing
on_run_start_macro_assert_git_branch = """
{% macro assert_git_branch_name() %}
    {{ log("git branch name: " ~ git_branch, 1) }}
{% endmacro %}
"""


class TestContextGitValues:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "assert_git_branch_name.sql": on_run_start_macro_assert_git_branch,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-start": "{{ assert_git_branch_name() }}",
        }

    def test_git_values(self, project):
        os.chdir(project.project_root)
        # Initialize a new git repository
        subprocess.run(["git", "init"], check=True)
        subprocess.run(["git", "config", "user.email", "no-mail@dbtlabs.com"], check=True)
        subprocess.run(["git", "config", "user.name", "dbt Labs"], check=True)
        subprocess.run(["git", "checkout", "-b" "new_branch_for_testing"], check=True)
        subprocess.run(["git", "add", "*"], check=True)
        subprocess.run(["git", "commit", "-m", "commit to git"], check=True)

        _, run_logs = run_dbt_and_capture(["run"])
        assert "git branch name: new_branch_for_testing" in run_logs
