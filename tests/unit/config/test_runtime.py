import os
import tempfile
from argparse import Namespace
from typing import Any, Dict
from unittest import mock

import pytest
from pytest_mock import MockerFixture

import dbt.config
import dbt.exceptions
from dbt import tracking
from dbt.config.profile import Profile
from dbt.config.project import Project
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.project import PackageConfig
from dbt.events.types import UnusedResourceConfigPath
from dbt.flags import set_from_args
from dbt.tests.util import safe_set_invocation_context
from dbt_common.events.event_manager_client import add_callback_to_manager
from tests.unit.config import BaseConfigTest, temp_cd
from tests.utils import EventCatcher


class TestRuntimeConfig:
    @pytest.fixture
    def args(self) -> Namespace:
        return Namespace(
            profiles_dir=tempfile.mkdtemp(),
            cli_vars={},
            version_check=True,
            project_dir=tempfile.mkdtemp(),
            target=None,
            threads=None,
            profile=None,
        )

    def test_str(self, profile: Profile, project: Project) -> None:
        config = dbt.config.RuntimeConfig.from_parts(project, profile, {})

        # to make sure nothing terrible happens
        str(config)

    def test_from_parts(self, args: Namespace, profile: Profile, project: Project):
        config = dbt.config.RuntimeConfig.from_parts(project, profile, args)

        assert config.cli_vars == {}
        assert config.to_profile_info() == profile.to_profile_info()
        # we should have the default quoting set in the full config, but not in
        # the project
        # TODO(jeb): Adapters must assert that quoting is populated?
        expected_project = project.to_project_config()
        assert expected_project["quoting"] == {}

        expected_project["quoting"] = {
            "database": True,
            "identifier": True,
            "schema": True,
        }
        assert config.to_project_config() == expected_project

    def test_get_metadata(self, mocker: MockerFixture, runtime_config: RuntimeConfig) -> None:
        mock_user = mocker.patch.object(tracking, "active_user")
        mock_user.id = "cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf"
        set_from_args(Namespace(SEND_ANONYMOUS_USAGE_STATS=False), None)

        metadata = runtime_config.get_metadata()
        # ensure user_id and send_anonymous_usage_stats are set correctly
        assert metadata.user_id == mock_user.id
        assert not metadata.send_anonymous_usage_stats

    @pytest.fixture
    def used_fqns(self) -> Dict[str, Any]:
        return {"models": frozenset((("my_test_project", "foo", "bar"),))}

    def test_warn_for_unused_resource_config_paths(
        self,
        runtime_config: RuntimeConfig,
        used_fqns: Dict[str, Any],
    ):
        catcher = EventCatcher(event_to_catch=UnusedResourceConfigPath)
        add_callback_to_manager(catcher.catch)

        runtime_config.models = {
            "my_test_project": {
                "foo": {
                    "materialized": "view",
                    "bar": {
                        "materialized": "table",
                    },
                    "baz": {
                        "materialized": "table",
                    },
                }
            }
        }

        runtime_config.warn_for_unused_resource_config_paths(used_fqns, [])
        len(catcher.caught_events) == 1
        expected_msg = "models.my_test_project.foo.baz"
        assert expected_msg in str(catcher.caught_events[0].data)

    def test_warn_for_unused_resource_config_paths_empty_models(
        self,
        runtime_config: RuntimeConfig,
        used_fqns: Dict[str, Any],
    ) -> None:
        catcher = EventCatcher(event_to_catch=UnusedResourceConfigPath)
        add_callback_to_manager(catcher.catch)

        # models should already be empty, but lets ensure it
        runtime_config.models = {}

        runtime_config.warn_for_unused_resource_config_paths(used_fqns, ())
        assert len(catcher.caught_events) == 0


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
