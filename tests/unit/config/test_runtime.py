import os
from argparse import Namespace
from unittest import mock

import dbt.config
import dbt.exceptions
from dbt import tracking
from dbt.contracts.project import PackageConfig
from dbt.flags import set_from_args
from dbt.tests.util import safe_set_invocation_context
from tests.unit.config import (
    BaseConfigTest,
    empty_profile_renderer,
    project_from_config_norender,
    temp_cd,
)


class TestRuntimeConfig(BaseConfigTest):
    def get_project(self):
        return project_from_config_norender(
            self.default_project_data,
            project_root=self.project_dir,
            verify_version=self.args.version_check,
        )

    def get_profile(self):
        renderer = empty_profile_renderer()
        return dbt.config.Profile.from_raw_profiles(
            self.default_profile_data, self.default_project_data["profile"], renderer
        )

    def from_parts(self, exc=None):
        with self.assertRaisesOrReturns(exc) as err:
            project = self.get_project()
            profile = self.get_profile()

            result = dbt.config.RuntimeConfig.from_parts(project, profile, self.args)

        if exc is None:
            return result
        else:
            return err

    def test_from_parts(self):
        project = self.get_project()
        profile = self.get_profile()
        config = dbt.config.RuntimeConfig.from_parts(project, profile, self.args)

        self.assertEqual(config.cli_vars, {})
        self.assertEqual(config.to_profile_info(), profile.to_profile_info())
        # we should have the default quoting set in the full config, but not in
        # the project
        # TODO(jeb): Adapters must assert that quoting is populated?
        expected_project = project.to_project_config()
        self.assertEqual(expected_project["quoting"], {})

        expected_project["quoting"] = {
            "database": True,
            "identifier": True,
            "schema": True,
        }
        self.assertEqual(config.to_project_config(), expected_project)

    def test_str(self):
        project = self.get_project()
        profile = self.get_profile()
        config = dbt.config.RuntimeConfig.from_parts(project, profile, {})

        # to make sure nothing terrible happens
        str(config)

    def test_supported_version(self):
        self.default_project_data["require-dbt-version"] = ">0.0.0"
        conf = self.from_parts()
        self.assertEqual(set(x.to_version_string() for x in conf.dbt_version), {">0.0.0"})

    def test_unsupported_version(self):
        self.default_project_data["require-dbt-version"] = ">99999.0.0"
        raised = self.from_parts(dbt.exceptions.DbtProjectError)
        self.assertIn("This version of dbt is not supported", str(raised.exception))

    def test_unsupported_version_no_check(self):
        self.default_project_data["require-dbt-version"] = ">99999.0.0"
        self.args.version_check = False
        set_from_args(self.args, None)
        conf = self.from_parts()
        self.assertEqual(set(x.to_version_string() for x in conf.dbt_version), {">99999.0.0"})

    def test_supported_version_range(self):
        self.default_project_data["require-dbt-version"] = [">0.0.0", "<=99999.0.0"]
        conf = self.from_parts()
        self.assertEqual(
            set(x.to_version_string() for x in conf.dbt_version), {">0.0.0", "<=99999.0.0"}
        )

    def test_unsupported_version_range(self):
        self.default_project_data["require-dbt-version"] = [">0.0.0", "<=0.0.1"]
        raised = self.from_parts(dbt.exceptions.DbtProjectError)
        self.assertIn("This version of dbt is not supported", str(raised.exception))

    def test_unsupported_version_range_bad_config(self):
        self.default_project_data["require-dbt-version"] = [">0.0.0", "<=0.0.1"]
        self.default_project_data["some-extra-field-not-allowed"] = True
        raised = self.from_parts(dbt.exceptions.DbtProjectError)
        self.assertIn("This version of dbt is not supported", str(raised.exception))

    def test_unsupported_version_range_no_check(self):
        self.default_project_data["require-dbt-version"] = [">0.0.0", "<=0.0.1"]
        self.args.version_check = False
        set_from_args(self.args, None)
        conf = self.from_parts()
        self.assertEqual(
            set(x.to_version_string() for x in conf.dbt_version), {">0.0.0", "<=0.0.1"}
        )

    def test_impossible_version_range(self):
        self.default_project_data["require-dbt-version"] = [">99999.0.0", "<=0.0.1"]
        raised = self.from_parts(dbt.exceptions.DbtProjectError)
        self.assertIn(
            "The package version requirement can never be satisfied", str(raised.exception)
        )

    def test_unsupported_version_extra_config(self):
        self.default_project_data["some-extra-field-not-allowed"] = True
        raised = self.from_parts(dbt.exceptions.DbtProjectError)
        self.assertIn("Additional properties are not allowed", str(raised.exception))

    def test_archive_not_allowed(self):
        self.default_project_data["archive"] = [
            {
                "source_schema": "a",
                "target_schema": "b",
                "tables": [
                    {
                        "source_table": "seed",
                        "target_table": "archive_actual",
                        "updated_at": "updated_at",
                        "unique_key": """id || '-' || first_name""",
                    },
                ],
            }
        ]
        with self.assertRaises(dbt.exceptions.DbtProjectError):
            self.get_project()

    def test__warn_for_unused_resource_config_paths_empty(self):
        project = self.from_parts()
        dbt.flags.WARN_ERROR = True
        try:
            project.warn_for_unused_resource_config_paths(
                {
                    "models": frozenset(
                        (
                            ("my_test_project", "foo", "bar"),
                            ("my_test_project", "foo", "baz"),
                        )
                    )
                },
                [],
            )
        finally:
            dbt.flags.WARN_ERROR = False

    @mock.patch.object(tracking, "active_user")
    def test_get_metadata(self, mock_user):
        project = self.get_project()
        profile = self.get_profile()
        config = dbt.config.RuntimeConfig.from_parts(project, profile, self.args)

        mock_user.id = "cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf"
        set_from_args(Namespace(SEND_ANONYMOUS_USAGE_STATS=False), None)

        metadata = config.get_metadata()
        # ensure user_id and send_anonymous_usage_stats are set correctly
        self.assertEqual(metadata.user_id, mock_user.id)
        self.assertFalse(metadata.send_anonymous_usage_stats)


class TestRuntimeConfigWithConfigs(BaseConfigTest):
    def setUp(self):
        self.profiles_dir = "/invalid-profiles-path"
        self.project_dir = "/invalid-root-path"
        super().setUp()
        self.default_project_data["project-root"] = self.project_dir
        self.default_project_data["models"] = {
            "enabled": True,
            "my_test_project": {
                "foo": {
                    "materialized": "view",
                    "bar": {
                        "materialized": "table",
                    },
                },
                "baz": {
                    "materialized": "table",
                },
            },
        }
        self.used = {
            "models": frozenset(
                (
                    ("my_test_project", "foo", "bar"),
                    ("my_test_project", "foo", "baz"),
                )
            )
        }

    def get_project(self):
        return project_from_config_norender(
            self.default_project_data, project_root=self.project_dir, verify_version=True
        )

    def get_profile(self):
        renderer = empty_profile_renderer()
        return dbt.config.Profile.from_raw_profiles(
            self.default_profile_data, self.default_project_data["profile"], renderer
        )

    def from_parts(self, exc=None):
        with self.assertRaisesOrReturns(exc) as err:
            project = self.get_project()
            profile = self.get_profile()

            result = dbt.config.RuntimeConfig.from_parts(project, profile, self.args)

        if exc is None:
            return result
        else:
            return err

    def test__warn_for_unused_resource_config_paths(self):
        project = self.from_parts()
        with mock.patch("dbt.config.runtime.warn_or_error") as warn_or_error_patch:
            project.warn_for_unused_resource_config_paths(self.used, [])
            warn_or_error_patch.assert_called_once()
            event = warn_or_error_patch.call_args[0][0]
            assert type(event).__name__ == "UnusedResourceConfigPath"
            msg = event.message()
            expected_msg = "- models.my_test_project.baz"
            assert expected_msg in msg


class TestRuntimeConfigFiles(BaseConfigTest):
    def test_from_args(self):
        with temp_cd(self.project_dir):
            config = dbt.config.RuntimeConfig.from_args(self.args)
        self.assertEqual(config.version, "0.0.1")
        self.assertEqual(config.profile_name, "default")
        # on osx, for example, these are not necessarily equal due to /private
        self.assertTrue(os.path.samefile(config.project_root, self.project_dir))
        self.assertEqual(config.model_paths, ["models"])
        self.assertEqual(config.macro_paths, ["macros"])
        self.assertEqual(config.seed_paths, ["seeds"])
        self.assertEqual(config.test_paths, ["tests"])
        self.assertEqual(config.analysis_paths, ["analyses"])
        self.assertEqual(
            set(config.docs_paths), set(["models", "seeds", "snapshots", "analyses", "macros"])
        )
        self.assertEqual(config.asset_paths, [])
        self.assertEqual(config.target_path, "target")
        self.assertEqual(config.clean_targets, ["target"])
        self.assertEqual(config.log_path, "logs")
        self.assertEqual(config.packages_install_path, "dbt_packages")
        self.assertEqual(config.quoting, {"database": True, "identifier": True, "schema": True})
        self.assertEqual(config.models, {})
        self.assertEqual(config.on_run_start, [])
        self.assertEqual(config.on_run_end, [])
        self.assertEqual(config.seeds, {})
        self.assertEqual(config.packages, PackageConfig(packages=[]))
        self.assertEqual(config.project_name, "my_test_project")


class TestVariableRuntimeConfigFiles(BaseConfigTest):
    def setUp(self):
        super().setUp()
        self.default_project_data.update(
            {
                "version": "{{ var('cli_version') }}",
                "name": "blah",
                "profile": "{{ env_var('env_value_profile') }}",
                "on-run-end": [
                    "{{ env_var('env_value_profile') }}",
                ],
                "models": {
                    "foo": {
                        "post-hook": "{{ env_var('env_value_profile') }}",
                    },
                    "bar": {
                        # just gibberish, make sure it gets interpreted
                        "materialized": "{{ env_var('env_value_profile') }}",
                    },
                },
                "seeds": {
                    "foo": {
                        "post-hook": "{{ env_var('env_value_profile') }}",
                    },
                    "bar": {
                        # just gibberish, make sure it gets interpreted
                        "materialized": "{{ env_var('env_value_profile') }}",
                    },
                },
            }
        )
        self.write_project(self.default_project_data)

    def test_cli_and_env_vars(self):
        self.args.target = "cli-and-env-vars"
        self.args.vars = {"cli_value_host": "cli-postgres-host", "cli_version": "0.1.2"}
        self.args.project_dir = self.project_dir
        set_from_args(self.args, None)
        with mock.patch.dict(os.environ, self.env_override):
            safe_set_invocation_context()  # reset invocation context with new env
            config = dbt.config.RuntimeConfig.from_args(self.args)

        self.assertEqual(config.version, "0.1.2")
        self.assertEqual(config.project_name, "blah")
        self.assertEqual(config.profile_name, "default")
        self.assertEqual(config.credentials.host, "cli-postgres-host")
        self.assertEqual(config.credentials.user, "env-postgres-user")
        # make sure hooks are not interpreted
        self.assertEqual(config.on_run_end, ["{{ env_var('env_value_profile') }}"])
        self.assertEqual(config.models["foo"]["post-hook"], "{{ env_var('env_value_profile') }}")
        self.assertEqual(config.models["bar"]["materialized"], "default")  # rendered!
        self.assertEqual(config.seeds["foo"]["post-hook"], "{{ env_var('env_value_profile') }}")
        self.assertEqual(config.seeds["bar"]["materialized"], "default")  # rendered!
