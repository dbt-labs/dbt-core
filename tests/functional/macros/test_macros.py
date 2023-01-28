import pytest
import shutil

import dbt.exceptions

from pathlib import Path

from dbt.tests.util import (
    run_dbt,
    check_relations_equal,
)

from tests.functional.macros.fixtures import (
    models__dep_macro,
    models__with_undefined_macro,
    models__local_macro,
    models__ref_macro,
    models__override_get_columns_macros,
    models__deprecated_adapter_macro_model,
    macros__my_macros,
    macros__no_default_macros,
    macros__override_get_columns_macros,
    macros__package_override_get_columns_macros,
    macros__deprecated_adapter_macro,
)


class TestMacros:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(project.test_data_dir / Path("seed.sql"))

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dep_macro.sql": models__dep_macro,
            "local_macro.sql": models__local_macro,
            "ref_macro.sql": models__ref_macro,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"my_macros.sql": macros__my_macros}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "git": "https://github.com/dbt-labs/dbt-integration-project",
                    "revision": "dbt/1.0.0",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "vars": {
                "test": {
                    "test": "DUMMY",
                },
            },
            "macro-paths": ["macros"],
        }

    def test_working_macros(self, project):
        run_dbt(["deps"])
        results = run_dbt()
        assert len(results) == 6

        check_relations_equal(project.adapter, ["expected_dep_macro", "dep_macro"])
        check_relations_equal(project.adapter, ["expected_local_macro", "local_macro"])


class TestInvalidMacros:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dep_macro.sql": models__dep_macro,
            "local_macro.sql": models__local_macro,
            "ref_macro.sql": models__ref_macro,
        }

    def test_invalid_macro(self, project):
        run_dbt(expect_pass=False)


class TestAdapterMacroNoDestination:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models__with_undefined_macro}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"my_macros.sql": macros__no_default_macros}

    def test_invalid_macro(self, project):
        with pytest.raises(dbt.exceptions.CompilationError) as exc:
            run_dbt()

        assert "In dispatch: No macro named 'dispatch_to_nowhere' found" in str(exc.value)


class TestMacroOverrideBuiltin:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models__override_get_columns_macros}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"macros.sql": macros__override_get_columns_macros}

    def test_overrides(self, project):
        # the first time, the model doesn't exist
        run_dbt()
        run_dbt()


class TestMacroOverridePackage:
    """
    The macro in `override-postgres-get-columns-macros` should override the
    `get_columns_in_relation` macro by default.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models__override_get_columns_macros}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"macros.sql": macros__package_override_get_columns_macros}

    def test_overrides(self, project):
        # the first time, the model doesn't exist
        run_dbt()
        run_dbt()


class TestMacroNotOverridePackage:
    """
    The macro in `override-postgres-get-columns-macros` does NOT override the
    `get_columns_in_relation` macro because we tell dispatch to not look at the
    postgres macros.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models__override_get_columns_macros}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"macros.sql": macros__package_override_get_columns_macros}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "dispatch": [{"macro_namespace": "dbt", "search_order": ["dbt"]}],
        }

    def test_overrides(self, project):
        # the first time, the model doesn't exist
        run_dbt(expect_pass=False)
        run_dbt(expect_pass=False)


class TestDispatchMacroOverrideBuiltin(TestMacroOverrideBuiltin):
    # test the same functionality as above, but this time,
    # dbt.get_columns_in_relation will dispatch to a default__ macro
    # from an installed package, per dispatch config search_order
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        shutil.copytree(
            project.test_dir / Path("package_macro_overrides"),
            project.project_root / Path("package_macro_overrides"),
        )

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "dispatch": [
                {
                    "macro_namespace": "dbt",
                    "search_order": ["test", "package_macro_overrides", "dbt"],
                }
            ],
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "local": "./package_macro_overrides",
                },
            ]
        }

    def test_overrides(self, project):
        run_dbt(["deps"])
        run_dbt()
        run_dbt()


class TestAdapterMacroDeprecated:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models__deprecated_adapter_macro_model}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro.sql": macros__deprecated_adapter_macro}

    def test_invalid_macro(self, project):
        with pytest.raises(dbt.exceptions.CompilationError) as exc:
            run_dbt()

        assert 'The "adapter_macro" macro has been deprecated' in str(exc.value)
