import pytest

from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import run_dbt, write_file
from tests.functional.run_on_error.fixtures import (
    child_sql,
    child_two_parents_sql,
    dep_schema_continue_yml,
    dep_schema_parent1_yml,
    dep_schema_skip_children_yml,
    models__parent1_error_sql,
    models__parent1_success_sql,
    models__parent2_error_sql,
    models__parent2_success_sql,
    models__parent_error_sql,
    models__parent_success_sql,
    run_with_deps,
    schema_parent2_yml,
    write_dependency,
)


class TestOnErrorUnspecifiedSkipsChildrenCrossProject:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "on_error_dep"}]}

    @pytest.fixture(scope="class", autouse=True)
    def setup_dependency(self, project):
        write_dependency(
            project.project_root,
            {"parent.sql": models__parent_error_sql},
            schema_yml=None,
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {"child.sql": child_sql}

    def test_downstream_skipped_when_on_error_unspecified(self, project, models):
        res = run_with_deps(expect_pass=False)
        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.on_error_dep.parent") is RunStatus.Error
        assert status_by_id.get("model.test.child") is RunStatus.Skipped


class TestOnErrorSkipChildrenSkipsChildrenCrossProject:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "on_error_dep"}]}

    @pytest.fixture(scope="class", autouse=True)
    def setup_dependency(self, project):
        write_dependency(
            project.project_root,
            {"parent.sql": models__parent_error_sql},
            schema_yml=dep_schema_skip_children_yml,
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {"child.sql": child_sql}

    def test_downstream_skipped_when_on_error_skip_children(self, project, models):
        res = run_with_deps(expect_pass=False)
        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.on_error_dep.parent") is RunStatus.Error
        assert status_by_id.get("model.test.child") is RunStatus.Skipped


class TestOnErrorContinueRunsChildrenCrossProject:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "on_error_dep"}]}

    @pytest.fixture(scope="class", autouse=True)
    def setup_dependency(self, project):
        write_dependency(
            project.project_root,
            {"parent.sql": models__parent_success_sql},
            schema_yml=dep_schema_continue_yml,
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {"child.sql": child_sql}

    def test_children_executed_when_on_error_continue(self, project, models):
        run_with_deps(expect_pass=True)
        write_file(
            models__parent_error_sql,
            project.project_root,
            "on_error_dep",
            "models",
            "parent.sql",
        )
        res = run_with_deps(expect_pass=False)
        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.on_error_dep.parent") is RunStatus.Error
        assert status_by_id.get("model.test.child") is RunStatus.Success


class TestOnErrorBothParentsSuccessCrossProject:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "on_error_dep"}]}

    @pytest.fixture(scope="class", autouse=True)
    def setup_dependency(self, project):
        write_dependency(
            project.project_root,
            {"parent1.sql": models__parent1_success_sql},
            schema_yml=dep_schema_parent1_yml,
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent2.sql": models__parent2_success_sql,
            "child.sql": child_two_parents_sql,
            "schema.yml": schema_parent2_yml,
        }

    def test_both_parents_success_child_success(self, project, models):
        res = run_with_deps(expect_pass=True)
        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.on_error_dep.parent1") is RunStatus.Success
        assert status_by_id.get("model.test.parent2") is RunStatus.Success
        assert status_by_id.get("model.test.child") is RunStatus.Success


class TestOnErrorSkipParentFailsAndContinueParentSuccessCrossProject:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "on_error_dep"}]}

    @pytest.fixture(scope="class", autouse=True)
    def setup_dependency(self, project):
        write_dependency(
            project.project_root,
            {"parent1.sql": models__parent1_error_sql},
            schema_yml=dep_schema_parent1_yml,
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent2.sql": models__parent2_success_sql,
            "child.sql": child_two_parents_sql,
            "schema.yml": schema_parent2_yml,
        }

    def test_parent1_fail_parent2_success_child_skipped(self, project, models):
        res = run_with_deps(expect_pass=False)
        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.on_error_dep.parent1") is RunStatus.Error
        assert status_by_id.get("model.test.parent2") is RunStatus.Success
        assert status_by_id.get("model.test.child") is RunStatus.Skipped


class TestOnErrorSkipParentSuccessAndContinueParentFailsCrossProject:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "on_error_dep"}]}

    @pytest.fixture(scope="class", autouse=True)
    def setup_dependency(self, project):
        write_dependency(
            project.project_root,
            {"parent1.sql": models__parent1_success_sql},
            schema_yml=dep_schema_parent1_yml,
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent2.sql": models__parent2_success_sql,
            "child.sql": child_two_parents_sql,
            "schema.yml": schema_parent2_yml,
        }

    def test_parent1_success_parent2_fail_child_success(self, project, models):
        run_with_deps(expect_pass=True)
        write_file(
            models__parent2_error_sql,
            project.project_root,
            "models",
            "parent2.sql",
        )
        res = run_with_deps(expect_pass=False)
        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.on_error_dep.parent1") is RunStatus.Success
        assert status_by_id.get("model.test.parent2") is RunStatus.Error
        assert status_by_id.get("model.test.child") is RunStatus.Success


class TestOnErrorBothParentsFailCrossProject:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "on_error_dep"}]}

    @pytest.fixture(scope="class", autouse=True)
    def setup_dependency(self, project):
        write_dependency(
            project.project_root,
            {"parent1.sql": models__parent1_error_sql},
            schema_yml=dep_schema_parent1_yml,
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent2.sql": models__parent2_error_sql,
            "child.sql": child_two_parents_sql,
            "schema.yml": schema_parent2_yml,
        }

    def test_both_parents_fail_child_skipped(self, project, models):
        res = run_with_deps(expect_pass=False)
        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.on_error_dep.parent1") is RunStatus.Error
        assert status_by_id.get("model.test.parent2") is RunStatus.Error
        assert status_by_id.get("model.test.child") is RunStatus.Skipped


class TestOnErrorIgnoredWhenFailFastIsSetCrossProject:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "on_error_dep"}]}

    @pytest.fixture(scope="class", autouse=True)
    def setup_dependency(self, project):
        write_dependency(
            project.project_root,
            {"parent.sql": models__parent_success_sql},
            schema_yml=dep_schema_continue_yml,
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {"child.sql": child_sql}

    def test_ignored_when_fail_fast_is_set(self, project, models):
        run_with_deps(expect_pass=True)
        write_file(
            models__parent_error_sql,
            project.project_root,
            "on_error_dep",
            "models",
            "parent.sql",
        )
        run_dbt(["deps"])
        res = run_dbt(["run", "--fail-fast"], expect_pass=False)
        status_by_id = {r.node.unique_id: r.status for r in res.results}
        assert status_by_id.get("model.on_error_dep.parent") is RunStatus.Error
        assert status_by_id.get("model.test.child") is RunStatus.Skipped
