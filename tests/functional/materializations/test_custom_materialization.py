import pytest

from dbt.tests.util import run_dbt


models__model_sql = """
{{ config(materialized='view') }}
select 1 as id

"""


@pytest.fixture(scope="class")
def models():
    return {"model.sql": models__model_sql}


class TestOverrideAdapterDependency:
    # make sure that if there's a dependency with an adapter-specific
    # materialization, we honor that materialization
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "override-view-adapter-dep"}]}

    def test_adapter_dependency(self, project, override_view_adapter_dep):
        run_dbt(["deps"])
        # this should error because the override is buggy
        run_dbt(["run"], expect_pass=False)


class TestOverrideDefaultDependency:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "override-view-default-dep"}]}

    def test_default_dependency(self, project, override_view_default_dep):
        run_dbt(["deps"])
        # this should error because the override is buggy
        run_dbt(["run"], expect_pass=False)


class TestOverrideAdapterDependencyPassing:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "override-view-adapter-pass-dep"}]}

    def test_default_dependency(self, project, override_view_adapter_pass_dep):
        run_dbt(["deps"])
        # this should pass because the override is ok
        run_dbt(["run"])


class TestOverrideAdapterLocal:
    # make sure that the local default wins over the dependency
    # adapter-specific

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "override-view-adapter-pass-dep"}]}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"macro-paths": ["override-view-adapter-macros"]}

    def test_default_dependency(
        self, project, override_view_adapter_pass_dep, override_view_adapter_macros
    ):
        run_dbt(["deps"])
        # this should error because the override is buggy
        run_dbt(["run"], expect_pass=False)


class TestOverrideDefaultReturn:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"macro-paths": ["override-view-return-no-relation"]}

    def test_default_dependency(self, project, override_view_return_no_relation):
        run_dbt(["deps"])
        results = run_dbt(["run"], expect_pass=False)
        assert "did not explicitly return a list of relations" in results[0].message
