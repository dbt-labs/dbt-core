import pytest

from dbt.tests.util import run_dbt
from dbt import deprecations

models__model_sql = """
{{ config(materialized='view') }}
select 1 as id

"""


models_custom_materialization__model_sql = """
{{ config(materialized='custom_materialization') }}
select 1 as id

"""


@pytest.fixture(scope="class")
def models():
    return {"model.sql": models__model_sql}


@pytest.fixture(scope="class")
def set_up_deprecations():
    deprecations.reset_deprecations()
    assert deprecations.active_deprecations == set()


class TestOverrideAdapterDependency:
    # make sure that if there's a dependency with an adapter-specific
    # materialization, we honor that materialization
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "override-view-adapter-dep"}]}

    def test_adapter_dependency(self, project, override_view_adapter_dep, set_up_deprecations):
        run_dbt(["deps"])
        # this should error because the override is buggy
        run_dbt(["run"], expect_pass=False)

        # overriding a built-in materialization scoped to adapter from package is deprecated
        assert deprecations.active_deprecations == {"package-materialization-override"}


class TestOverrideAdapterDependencyDeprecated:
    # make sure that if there's a dependency with an adapter-specific
    # materialization, we honor that materialization
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "override-view-adapter-dep"}]}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_explicit_package_overrides_for_builtin_materializations": True,
            },
        }

    def test_adapter_dependency_deprecate_overrides(
        self, project, override_view_adapter_dep, set_up_deprecations
    ):
        run_dbt(["deps"])
        # this should pass because the override is buggy and unused
        run_dbt(["run"])

        # no deprecation warning -- flag used correctly
        assert deprecations.active_deprecations == set()


class TestOverrideAdapterDependencyLegacy:
    # make sure that if there's a dependency with an adapter-specific
    # materialization, we honor that materialization
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "override-view-adapter-dep"}]}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_explicit_package_overrides_for_builtin_materializations": False,
            },
        }

    def test_adapter_dependency(self, project, override_view_adapter_dep, set_up_deprecations):
        run_dbt(["deps"])
        # this should error because the override is buggy
        run_dbt(["run"], expect_pass=False)

        # overriding a built-in materialization scoped to adapter from package is deprecated
        assert deprecations.active_deprecations == {"package-materialization-override"}


class TestOverrideDefaultDependency:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "override-view-default-dep"}]}

    def test_default_dependency(self, project, override_view_default_dep, set_up_deprecations):
        run_dbt(["deps"])
        # this should error because the override is buggy
        run_dbt(["run"], expect_pass=False)

        # overriding a built-in materialization from package is deprecated
        assert deprecations.active_deprecations == {"package-materialization-override"}


class TestOverrideDefaultDependencyDeprecated:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "override-view-default-dep"}]}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_explicit_package_overrides_for_builtin_materializations": True,
            },
        }

    def test_default_dependency_deprecated(
        self, project, override_view_default_dep, set_up_deprecations
    ):
        run_dbt(["deps"])
        # this should pass because the override is buggy and unused
        run_dbt(["run"])

        # overriding a built-in materialization from package is deprecated
        assert deprecations.active_deprecations == set()


class TestOverrideDefaultDependencyLegacy:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "override-view-default-dep"}]}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_explicit_package_overrides_for_builtin_materializations": False,
            },
        }

    def test_default_dependency(self, project, override_view_default_dep, set_up_deprecations):
        run_dbt(["deps"])
        # this should error because the override is buggy
        run_dbt(["run"], expect_pass=False)

        # overriding a built-in materialization from package is deprecated
        assert deprecations.active_deprecations == {"package-materialization-override"}


root_view_override_macro = """
{% materialization view, default %}
 {{ return(view_default_override.materialization_view_default()) }}
{% endmaterialization %}
"""


class TestOverrideDefaultDependencyRootOverride:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "override-view-default-dep"}]}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"my_view.sql": root_view_override_macro}

    def test_default_dependency_with_root_override(
        self, project, override_view_default_dep, set_up_deprecations
    ):
        run_dbt(["deps"])
        # this should error because the override is buggy
        run_dbt(["run"], expect_pass=False)

        # using an package-overriden built-in materialization in a root matereialization is _not_ deprecated
        assert deprecations.active_deprecations == set()


class TestCustomMaterializationDependency:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models_custom_materialization__model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "custom-materialization-dep"}]}

    def test_custom_materialization_deopendency(
        self, project, custom_materialization_dep, set_up_deprecations
    ):
        run_dbt(["deps"])
        # custom materilization is valid
        run_dbt(["run"])

        # using a custom materialization is from an installed package is _not_ deprecated
        assert deprecations.active_deprecations == set()


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
