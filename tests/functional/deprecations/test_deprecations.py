import os
from collections import defaultdict
from unittest import mock

import pytest
import yaml

import dbt_common
from dbt import deprecations
from dbt.clients.registry import _get_cached
from dbt.events.types import (
    CustomKeyInConfigDeprecation,
    CustomKeyInObjectDeprecation,
    DeprecationsSummary,
    DuplicateYAMLKeysDeprecation,
    GenericJSONSchemaValidationDeprecation,
    PackageRedirectDeprecation,
)
from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file
from dbt_common.exceptions import EventCompilationError
from tests.functional.deprecations.fixtures import (
    bad_name_yaml,
    custom_key_in_config_yaml,
    custom_key_in_object_yaml,
    deprecated_model_exposure_yaml,
    duplicate_keys_yaml,
    invalid_deprecation_date_yaml,
    models_trivial__model_sql,
)
from tests.utils import EventCatcher


class TestConfigPathDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"already_exists.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "data-paths": ["data"],
            "log-path": "customlogs",
            "target-path": "customtarget",
        }

    def test_data_path(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        run_dbt(["debug"])
        expected = {
            "project-config-data-paths",
            "project-config-log-path",
            "project-config-target-path",
        }
        for deprecation in expected:
            assert deprecation in deprecations.active_deprecations

    def test_data_path_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        with pytest.raises(dbt_common.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "debug"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "The `data-paths` config has been renamed"
        assert expected_msg in exc_str


class TestPackageInstallPathDeprecation:
    @pytest.fixture(scope="class")
    def models_trivial(self):
        return {"model.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"config-version": 2, "clean-targets": ["dbt_modules"]}

    def test_package_path(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        run_dbt(["clean"])
        assert "install-packages-path" in deprecations.active_deprecations

    def test_package_path_not_set(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        with pytest.raises(dbt_common.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "clean"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "path has changed from `dbt_modules` to `dbt_packages`."
        assert expected_msg in exc_str


class TestPackageRedirectDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"already_exists.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"package": "fishtown-analytics/dbt_utils", "version": "0.7.0"}]}

    def test_package_redirect(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        run_dbt(["deps"])
        assert "package-redirect" in deprecations.active_deprecations

    # if this test comes before test_package_redirect it will raise an exception as expected
    def test_package_redirect_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        with pytest.raises(dbt_common.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "deps"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "The `fishtown-analytics/dbt_utils` package is deprecated in favor of `dbt-labs/dbt_utils`"
        assert expected_msg in exc_str


class TestDeprecatedModelExposure:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_trivial__model_sql,
            "exposure.yml": deprecated_model_exposure_yaml,
        }

    def test_exposure_with_deprecated_model(self, project):
        run_dbt(["parse"])


class TestExposureNameDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models_trivial__model_sql, "bad_name.yml": bad_name_yaml}

    def test_exposure_name(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        run_dbt(["parse"])
        assert "exposure-name" in deprecations.active_deprecations

    def test_exposure_name_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        with pytest.raises(dbt_common.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "--no-partial-parse", "parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "Starting in v1.3, the 'name' of an exposure should contain only letters, numbers, and underscores."
        assert expected_msg in exc_str


class TestProjectFlagsMovedDeprecation:
    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        return {
            "config": {"send_anonymous_usage_stats": False},
        }

    @pytest.fixture(scope="class")
    def dbt_project_yml(self, project_root, project_config_update):
        project_config = {
            "name": "test",
            "profile": "test",
        }
        write_file(yaml.safe_dump(project_config), project_root, "dbt_project.yml")
        return project_config

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as fun"}

    def test_profile_config_deprecation(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)

        _, logs = run_dbt_and_capture(["parse"])

        assert (
            "User config should be moved from the 'config' key in profiles.yml to the 'flags' key in dbt_project.yml."
            in logs
        )
        assert "project-flags-moved" in deprecations.active_deprecations


class TestProjectFlagsMovedDeprecationQuiet(TestProjectFlagsMovedDeprecation):
    def test_profile_config_deprecation(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)

        _, logs = run_dbt_and_capture(["--quiet", "parse"])

        assert (
            "User config should be moved from the 'config' key in profiles.yml to the 'flags' key in dbt_project.yml."
            not in logs
        )
        assert "project-flags-moved" in deprecations.active_deprecations


class TestProjectFlagsMovedDeprecationWarnErrorOptions(TestProjectFlagsMovedDeprecation):
    def test_profile_config_deprecation(self, project):
        deprecations.reset_deprecations()
        with pytest.raises(EventCompilationError):
            run_dbt(["--warn-error-options", "{'include': 'all'}", "parse"])

        with pytest.raises(EventCompilationError):
            run_dbt(
                ["--warn-error-options", "{'include': ['ProjectFlagsMovedDeprecation']}", "parse"]
            )

        _, logs = run_dbt_and_capture(
            ["--warn-error-options", "{'silence': ['ProjectFlagsMovedDeprecation']}", "parse"]
        )
        assert (
            "User config should be moved from the 'config' key in profiles.yml to the 'flags' key in dbt_project.yml."
            not in logs
        )


class TestShowAllDeprecationsFlag:
    @pytest.fixture(scope="class")
    def models(self):
        return {"already_exists.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "fishtown-analytics/dbt_utils", "version": "0.7.0"},
                {"package": "calogica/dbt_date", "version": "0.10.0"},
            ]
        }

    @pytest.fixture(scope="class")
    def event_catcher(self) -> EventCatcher:
        return EventCatcher(event_to_catch=PackageRedirectDeprecation)

    def test_package_redirect(self, project, event_catcher: EventCatcher):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        run_dbt(["deps"], callbacks=[event_catcher.catch])
        assert "package-redirect" in deprecations.active_deprecations
        assert deprecations.active_deprecations["package-redirect"] == 2
        assert len(event_catcher.caught_events) == 1

        deprecations.reset_deprecations()
        _get_cached.cache = {}
        event_catcher.flush()
        run_dbt(["deps", "--show-all-deprecations"], callbacks=[event_catcher.catch])
        assert "package-redirect" in deprecations.active_deprecations
        assert deprecations.active_deprecations["package-redirect"] == 2
        assert len(event_catcher.caught_events) == 2


class TestDeprecationSummary:
    @pytest.fixture(scope="class")
    def models(self):
        return {"already_exists.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "fishtown-analytics/dbt_utils", "version": "0.7.0"},
                {"package": "calogica/dbt_date", "version": "0.10.0"},
            ]
        }

    @pytest.fixture(scope="class")
    def event_catcher(self) -> EventCatcher:
        return EventCatcher(event_to_catch=DeprecationsSummary)

    def test_package_redirect(self, project, event_catcher: EventCatcher):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        run_dbt(["deps"], callbacks=[event_catcher.catch])
        assert "package-redirect" in deprecations.active_deprecations
        assert deprecations.active_deprecations["package-redirect"] == 2
        assert len(event_catcher.caught_events) == 1
        for summary in event_catcher.caught_events[0].data.summaries:  # type: ignore
            found_summary = False
            if summary.event_name == "PackageRedirectDeprecation":
                assert (
                    summary.occurrences == 2
                ), f"Expected 2 occurrences of PackageRedirectDeprecation, got {summary.occurrences}"
                found_summary = True

        assert found_summary, "Expected to find PackageRedirectDeprecation in deprecations summary"


class TestDeprecatedInvalidDeprecationDate:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models_trivial.sql": models_trivial__model_sql,
            "models.yml": invalid_deprecation_date_yaml,
        }

    @mock.patch.dict(os.environ, {"DBT_ENV_PRIVATE_RUN_JSONSCHEMA_VALIDATIONS": "True"})
    def test_deprecated_invalid_deprecation_date(self, project):
        event_catcher = EventCatcher(GenericJSONSchemaValidationDeprecation)
        try:
            run_dbt(["parse", "--no-partial-parse"], callbacks=[event_catcher.catch])
        except:  # noqa
            assert (
                True
            ), "Expected an exception to be raised, because a model object can't be created with a deprecation_date as an int"

        assert len(event_catcher.caught_events) == 1
        assert (
            "1 is not of type 'string', 'null' in file" in event_catcher.caught_events[0].info.msg
        )


class TestDuplicateYAMLKeysInSchemaFiles:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models_trivial.sql": models_trivial__model_sql,
            "models.yml": duplicate_keys_yaml,
        }

    def test_duplicate_yaml_keys_in_schema_files(self, project):
        event_catcher = EventCatcher(DuplicateYAMLKeysDeprecation)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 1
        assert (
            "Duplicate key 'models' in \"<unicode string>\", line 6, column 1 in file"
            in event_catcher.caught_events[0].info.msg
        )


class TestCustomKeyInConfigDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models_trivial.sql": models_trivial__model_sql,
            "models.yml": custom_key_in_config_yaml,
        }

    @mock.patch.dict(os.environ, {"DBT_ENV_PRIVATE_RUN_JSONSCHEMA_VALIDATIONS": "True"})
    def test_duplicate_yaml_keys_in_schema_files(self, project):
        event_catcher = EventCatcher(CustomKeyInConfigDeprecation)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 1
        assert (
            "Custom key `my_custom_key` found in `config` at path `models[0].config`"
            in event_catcher.caught_events[0].info.msg
        )


class TestCustomKeyInObjectDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models_trivial.sql": models_trivial__model_sql,
            "models.yml": custom_key_in_object_yaml,
        }

    @mock.patch.dict(os.environ, {"DBT_ENV_PRIVATE_RUN_JSONSCHEMA_VALIDATIONS": "True"})
    def test_custom_key_in_object_deprecation(self, project):
        event_catcher = EventCatcher(CustomKeyInObjectDeprecation)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 1
        assert (
            "Custom key `'my_custom_property'` found at `models[0]` in file"
            in event_catcher.caught_events[0].info.msg
        )


class TestJsonschemaValidationDeprecationsArentRunWithoutEnvVar:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models_trivial.sql": models_trivial__model_sql,
            "models.yml": custom_key_in_object_yaml,
        }

    def test_jsonschema_validation_deprecations_arent_run_without_env_var(self, project):
        event_catcher = EventCatcher(CustomKeyInObjectDeprecation)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 0
