import pytest

from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import run_dbt, write_file

models__parent_success_sql = """
{{ config(materialized='table') }}

select 1 as id
"""

models__parent_error_sql = """
{{ config(materialized='table') }}

select 1 / 0 as id
"""

models__child_sql = """
{{ config(materialized='table') }}

select * from {{ ref('parent') }}
"""

schema_skip_children_yml = """
models:
  - name: parent
    config:
      on_error: skip_children
"""

schema_continue_yml = """
models:
  - name: parent
    config:
      on_error: continue
"""

models__parent1_success_sql = """
{{ config(materialized='table') }}

select 1 as id
"""

models__parent1_error_sql = """
{{ config(materialized='table') }}

select 1 / 0 as id
"""

models__parent2_success_sql = """
{{ config(materialized='table') }}

select 2 as id
"""

models__parent2_error_sql = """
{{ config(materialized='table') }}

select 1 / 0 as id
"""

models__child_two_parents_sql = """
{{ config(materialized='table') }}

select id from {{ ref('parent1') }}
union all
select id from {{ ref('parent2') }}
"""

schema_two_parents_yml = """
models:
  - name: parent1
    config:
      on_error: skip_children
  - name: parent2
    config:
      on_error: continue
"""


class TestOnErrorUnspecifiedSkipsChildren:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent.sql": models__parent_error_sql,
            "child.sql": models__child_sql,
        }

    def test_downstream_skipped_when_on_error_unspecified(self, project, models):
        res = run_dbt(["run"], expect_pass=False)
        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.test.parent") is RunStatus.Error
        assert status_by_id.get("model.test.child") is RunStatus.Skipped


class TestOnErrorSkipChildrenSkipsChildren:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent.sql": models__parent_error_sql,
            "child.sql": models__child_sql,
            "schema.yml": schema_skip_children_yml,
        }

    def test_downstream_skipped_when_on_error_skip_children(self, project, models):
        res = run_dbt(["run"], expect_pass=False)
        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.test.parent") is RunStatus.Error
        assert status_by_id.get("model.test.child") is RunStatus.Skipped


class TestOnErrorContinueRunsChildren:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent.sql": models__parent_success_sql,
            "child.sql": models__child_sql,
            "schema.yml": schema_continue_yml,
        }

    def test_children_executed_when_on_error_continue(self, project, models):
        run_dbt(["run"])  # let the model be populated once.
        write_file(
            models__parent_error_sql, project.project_root, "models", "parent.sql"
        )  # now make it fail

        res = run_dbt(["run"], expect_pass=False)

        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.test.parent") is RunStatus.Error
        assert status_by_id.get("model.test.child") is RunStatus.Success


class TestOnErrorBothParentsSuccess:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent1.sql": models__parent1_success_sql,
            "parent2.sql": models__parent2_success_sql,
            "child.sql": models__child_two_parents_sql,
            "schema.yml": schema_two_parents_yml,
        }

    def test_both_parents_success_child_success(self, project, models):
        res = run_dbt(["run"])
        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.test.parent1") is RunStatus.Success
        assert status_by_id.get("model.test.parent2") is RunStatus.Success
        assert status_by_id.get("model.test.child") is RunStatus.Success


class TestOnErrorSkipParentFailsAndContinueParentSuccess:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent1.sql": models__parent1_error_sql,
            "parent2.sql": models__parent2_success_sql,
            "child.sql": models__child_two_parents_sql,
            "schema.yml": schema_two_parents_yml,
        }

    def test_parent1_fail_parent2_success_child_skipped(self, project, models):
        res = run_dbt(["run"], expect_pass=False)

        status_by_id = {r.node.unique_id: r.status for r in res.results}

        assert status_by_id.get("model.test.parent1") is RunStatus.Error
        assert status_by_id.get("model.test.parent2") is RunStatus.Success
        assert status_by_id.get("model.test.child") is RunStatus.Skipped


class TestOnErrorSkipParentSuccessAndContinueParentFails:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent1.sql": models__parent1_success_sql,
            "parent2.sql": models__parent2_success_sql,
            "child.sql": models__child_two_parents_sql,
            "schema.yml": schema_two_parents_yml,
        }

    def test_parent1_success_parent2_fail_child_success(self, project, models):
        run_dbt(["run"])
        write_file(models__parent2_error_sql, project.project_root, "models", "parent2.sql")

        res = run_dbt(["run"], expect_pass=False)

        status_by_id = {r.node.unique_id: r.status for r in res.results}

        assert status_by_id.get("model.test.parent1") is RunStatus.Success
        assert status_by_id.get("model.test.parent2") is RunStatus.Error
        assert status_by_id.get("model.test.child") is RunStatus.Success


class TestOnErrorBothParentsFail:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent1.sql": models__parent1_error_sql,
            "parent2.sql": models__parent2_error_sql,
            "child.sql": models__child_two_parents_sql,
            "schema.yml": schema_two_parents_yml,
        }

    def test_both_parents_fail_child_skipped(self, project, models):
        res = run_dbt(["run"], expect_pass=False)

        status_by_id = {r.node.unique_id: r.status for r in res.results}

        assert status_by_id.get("model.test.parent1") is RunStatus.Error
        assert status_by_id.get("model.test.parent2") is RunStatus.Error
        assert status_by_id.get("model.test.child") is RunStatus.Skipped


class TestOnErrorIgnoredWhenFailFastIsSet:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent.sql": models__parent_success_sql,
            "child.sql": models__child_sql,
            "schema.yml": schema_continue_yml,
        }

    def test_ignored_when_fail_fast_is_set(self, project, models):
        run_dbt(["run"])  # let the model be populated once.
        write_file(
            models__parent_error_sql, project.project_root, "models", "parent.sql"
        )  # now make it fail

        res = run_dbt(["run", "--fail-fast"], expect_pass=False)

        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.test.parent") is RunStatus.Error
        assert status_by_id.get("model.test.child") is RunStatus.Skipped
