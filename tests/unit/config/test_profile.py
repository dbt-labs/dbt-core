import os
from copy import deepcopy
from unittest import mock

import dbt.config
import dbt.exceptions
from dbt.adapters.postgres import PostgresCredentials
from dbt.flags import set_from_args
from dbt.tests.util import safe_set_invocation_context
from tests.unit.config import (
    BaseConfigTest,
    empty_profile_renderer,
    project_from_config_norender,
)


class TestProfile(BaseConfigTest):
    def from_raw_profiles(self):
        renderer = empty_profile_renderer()
        return dbt.config.Profile.from_raw_profiles(self.default_profile_data, "default", renderer)

    def test_from_raw_profiles(self):
        profile = self.from_raw_profiles()
        self.assertEqual(profile.profile_name, "default")
        self.assertEqual(profile.target_name, "postgres")
        self.assertEqual(profile.threads, 7)
        self.assertTrue(isinstance(profile.credentials, PostgresCredentials))
        self.assertEqual(profile.credentials.type, "postgres")
        self.assertEqual(profile.credentials.host, "postgres-db-hostname")
        self.assertEqual(profile.credentials.port, 5555)
        self.assertEqual(profile.credentials.user, "db_user")
        self.assertEqual(profile.credentials.password, "db_pass")
        self.assertEqual(profile.credentials.schema, "postgres-schema")
        self.assertEqual(profile.credentials.database, "postgres-db-name")

    def test_missing_type(self):
        del self.default_profile_data["default"]["outputs"]["postgres"]["type"]
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            self.from_raw_profiles()
        self.assertIn("type", str(exc.exception))
        self.assertIn("postgres", str(exc.exception))
        self.assertIn("default", str(exc.exception))

    def test_bad_type(self):
        self.default_profile_data["default"]["outputs"]["postgres"]["type"] = "invalid"
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            self.from_raw_profiles()
        self.assertIn("Credentials", str(exc.exception))
        self.assertIn("postgres", str(exc.exception))
        self.assertIn("default", str(exc.exception))

    def test_invalid_credentials(self):
        del self.default_profile_data["default"]["outputs"]["postgres"]["host"]
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            self.from_raw_profiles()
        self.assertIn("Credentials", str(exc.exception))
        self.assertIn("postgres", str(exc.exception))
        self.assertIn("default", str(exc.exception))

    def test_missing_target(self):
        profile = self.default_profile_data["default"]
        del profile["target"]
        profile["outputs"]["default"] = profile["outputs"]["postgres"]
        profile = self.from_raw_profiles()
        self.assertEqual(profile.profile_name, "default")
        self.assertEqual(profile.target_name, "default")
        self.assertEqual(profile.credentials.type, "postgres")

    def test_extra_path(self):
        self.default_project_data.update(
            {
                "model-paths": ["models"],
                "source-paths": ["other-models"],
            }
        )
        with self.assertRaises(dbt.exceptions.DbtProjectError) as exc:
            project_from_config_norender(self.default_project_data, project_root=self.project_dir)

        self.assertIn("source-paths and model-paths", str(exc.exception))
        self.assertIn("cannot both be defined.", str(exc.exception))

    def test_profile_invalid_project(self):
        renderer = empty_profile_renderer()
        with self.assertRaises(dbt.exceptions.DbtProjectError) as exc:
            dbt.config.Profile.from_raw_profiles(
                self.default_profile_data, "invalid-profile", renderer
            )

        self.assertEqual(exc.exception.result_type, "invalid_project")
        self.assertIn("Could not find", str(exc.exception))
        self.assertIn("invalid-profile", str(exc.exception))

    def test_profile_invalid_target(self):
        renderer = empty_profile_renderer()
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            dbt.config.Profile.from_raw_profiles(
                self.default_profile_data, "default", renderer, target_override="nope"
            )

        self.assertIn("nope", str(exc.exception))
        self.assertIn("- postgres", str(exc.exception))
        self.assertIn("- with-vars", str(exc.exception))

    def test_no_outputs(self):
        renderer = empty_profile_renderer()

        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            dbt.config.Profile.from_raw_profiles(
                {"some-profile": {"target": "blah"}}, "some-profile", renderer
            )
        self.assertIn("outputs not specified", str(exc.exception))
        self.assertIn("some-profile", str(exc.exception))

    def test_neq(self):
        profile = self.from_raw_profiles()
        self.assertNotEqual(profile, object())

    def test_eq(self):
        renderer = empty_profile_renderer()
        profile = dbt.config.Profile.from_raw_profiles(
            deepcopy(self.default_profile_data), "default", renderer
        )

        other = dbt.config.Profile.from_raw_profiles(
            deepcopy(self.default_profile_data), "default", renderer
        )
        self.assertEqual(profile, other)

    def test_invalid_env_vars(self):
        self.env_override["env_value_port"] = "hello"
        with mock.patch.dict(os.environ, self.env_override):
            with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
                safe_set_invocation_context()
                renderer = empty_profile_renderer()
                dbt.config.Profile.from_raw_profile_info(
                    self.default_profile_data["default"],
                    "default",
                    renderer,
                    target_override="with-vars",
                )
        self.assertIn("Could not convert value 'hello' into type 'number'", str(exc.exception))


class TestProfileFile(BaseConfigTest):
    def from_raw_profile_info(self, raw_profile=None, profile_name="default", **kwargs):
        if raw_profile is None:
            raw_profile = self.default_profile_data["default"]
        renderer = empty_profile_renderer()
        kw = {
            "raw_profile": raw_profile,
            "profile_name": profile_name,
            "renderer": renderer,
        }
        kw.update(kwargs)
        return dbt.config.Profile.from_raw_profile_info(**kw)

    def from_args(self, project_profile_name="default", **kwargs):
        kw = {
            "project_profile_name": project_profile_name,
            "renderer": empty_profile_renderer(),
            "threads_override": self.args.threads,
            "target_override": self.args.target,
            "profile_name_override": self.args.profile,
        }
        kw.update(kwargs)
        return dbt.config.Profile.render(**kw)

    def test_profile_simple(self):
        profile = self.from_args()
        from_raw = self.from_raw_profile_info()

        self.assertEqual(profile.profile_name, "default")
        self.assertEqual(profile.target_name, "postgres")
        self.assertEqual(profile.threads, 7)
        self.assertTrue(isinstance(profile.credentials, PostgresCredentials))
        self.assertEqual(profile.credentials.type, "postgres")
        self.assertEqual(profile.credentials.host, "postgres-db-hostname")
        self.assertEqual(profile.credentials.port, 5555)
        self.assertEqual(profile.credentials.user, "db_user")
        self.assertEqual(profile.credentials.password, "db_pass")
        self.assertEqual(profile.credentials.schema, "postgres-schema")
        self.assertEqual(profile.credentials.database, "postgres-db-name")
        self.assertEqual(profile, from_raw)

    def test_profile_override(self):
        self.args.profile = "other"
        self.args.threads = 3
        set_from_args(self.args, None)
        profile = self.from_args()
        from_raw = self.from_raw_profile_info(
            self.default_profile_data["other"],
            "other",
            threads_override=3,
        )

        self.assertEqual(profile.profile_name, "other")
        self.assertEqual(profile.target_name, "other-postgres")
        self.assertEqual(profile.threads, 3)
        self.assertTrue(isinstance(profile.credentials, PostgresCredentials))
        self.assertEqual(profile.credentials.type, "postgres")
        self.assertEqual(profile.credentials.host, "other-postgres-db-hostname")
        self.assertEqual(profile.credentials.port, 4444)
        self.assertEqual(profile.credentials.user, "other_db_user")
        self.assertEqual(profile.credentials.password, "other_db_pass")
        self.assertEqual(profile.credentials.schema, "other-postgres-schema")
        self.assertEqual(profile.credentials.database, "other-postgres-db-name")
        self.assertEqual(profile, from_raw)

    def test_env_vars(self):
        self.args.target = "with-vars"
        with mock.patch.dict(os.environ, self.env_override):
            safe_set_invocation_context()  # reset invocation context with new env
            profile = self.from_args()
            from_raw = self.from_raw_profile_info(target_override="with-vars")

        self.assertEqual(profile.profile_name, "default")
        self.assertEqual(profile.target_name, "with-vars")
        self.assertEqual(profile.threads, 1)
        self.assertEqual(profile.credentials.type, "postgres")
        self.assertEqual(profile.credentials.host, "env-postgres-host")
        self.assertEqual(profile.credentials.port, 6543)
        self.assertEqual(profile.credentials.user, "env-postgres-user")
        self.assertEqual(profile.credentials.password, "env-postgres-pass")
        self.assertEqual(profile, from_raw)

    def test_env_vars_env_target(self):
        self.default_profile_data["default"]["target"] = "{{ env_var('env_value_target') }}"
        self.write_profile(self.default_profile_data)
        self.env_override["env_value_target"] = "with-vars"
        with mock.patch.dict(os.environ, self.env_override):
            safe_set_invocation_context()  # reset invocation context with new env
            profile = self.from_args()
            from_raw = self.from_raw_profile_info(target_override="with-vars")

        self.assertEqual(profile.profile_name, "default")
        self.assertEqual(profile.target_name, "with-vars")
        self.assertEqual(profile.threads, 1)
        self.assertEqual(profile.credentials.type, "postgres")
        self.assertEqual(profile.credentials.host, "env-postgres-host")
        self.assertEqual(profile.credentials.port, 6543)
        self.assertEqual(profile.credentials.user, "env-postgres-user")
        self.assertEqual(profile.credentials.password, "env-postgres-pass")
        self.assertEqual(profile, from_raw)

    def test_invalid_env_vars(self):
        self.env_override["env_value_port"] = "hello"
        self.args.target = "with-vars"
        with mock.patch.dict(os.environ, self.env_override):
            with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
                safe_set_invocation_context()  # reset invocation context with new env
                self.from_args()

        self.assertIn("Could not convert value 'hello' into type 'number'", str(exc.exception))

    def test_cli_and_env_vars(self):
        self.args.target = "cli-and-env-vars"
        self.args.vars = {"cli_value_host": "cli-postgres-host"}
        renderer = dbt.config.renderer.ProfileRenderer({"cli_value_host": "cli-postgres-host"})
        with mock.patch.dict(os.environ, self.env_override):
            safe_set_invocation_context()  # reset invocation context with new env
            profile = self.from_args(renderer=renderer)
            from_raw = self.from_raw_profile_info(
                target_override="cli-and-env-vars",
                renderer=renderer,
            )

        self.assertEqual(profile.profile_name, "default")
        self.assertEqual(profile.target_name, "cli-and-env-vars")
        self.assertEqual(profile.threads, 1)
        self.assertEqual(profile.credentials.type, "postgres")
        self.assertEqual(profile.credentials.host, "cli-postgres-host")
        self.assertEqual(profile.credentials.port, 6543)
        self.assertEqual(profile.credentials.user, "env-postgres-user")
        self.assertEqual(profile.credentials.password, "env-postgres-pass")
        self.assertEqual(profile, from_raw)

    def test_no_profile(self):
        with self.assertRaises(dbt.exceptions.DbtProjectError) as exc:
            self.from_args(project_profile_name=None)
        self.assertIn("no profile was specified", str(exc.exception))

    def test_empty_profile(self):
        self.write_empty_profile()
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            self.from_args()
        self.assertIn("profiles.yml is empty", str(exc.exception))

    def test_profile_with_empty_profile_data(self):
        renderer = empty_profile_renderer()
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            dbt.config.Profile.from_raw_profiles(
                self.default_profile_data, "empty_profile_data", renderer
            )
        self.assertIn("Profile empty_profile_data in profiles.yml is empty", str(exc.exception))


class TestSecondaryProfiles(BaseConfigTest):
    def test_secondary_profiles_basic(self):
        profile_data_with_secondary = deepcopy(self.default_profile_data)
        profile_data_with_secondary["default"]["outputs"]["postgres"]["secondary_profiles"] = [
            {
                "secondary_profile_1": {
                    "target": "secondary_target_1",
                    "outputs": {
                        "secondary_target_1": {
                            "type": "postgres",
                            "host": "secondary-host",
                            "port": 1234,
                            "user": "secondary_user",
                            "password": "secondary_password",
                            "schema": "secondary_schema",
                            "database": "secondary_db",
                        }
                    },
                }
            }
        ]
        renderer = empty_profile_renderer()
        profile = dbt.config.Profile.from_raw_profiles(
            profile_data_with_secondary, "default", renderer
        )

        self.assertIn("secondary_profile_1", profile.secondary_profiles)
        secondary_profile = profile.secondary_profiles["secondary_profile_1"]
        self.assertEqual(secondary_profile.profile_name, "secondary_profile_1")
        self.assertEqual(secondary_profile.target_name, "secondary_target_1")
        self.assertTrue(isinstance(secondary_profile.credentials, PostgresCredentials))
        self.assertEqual(secondary_profile.credentials.host, "secondary-host")
        self.assertEqual(secondary_profile.credentials.port, 1234)
        self.assertEqual(secondary_profile.credentials.database, "secondary_db")

    def test_secondary_profiles_override_threads(self):
        profile_data_with_secondary = deepcopy(self.default_profile_data)
        profile_data_with_secondary["default"]["outputs"]["postgres"]["secondary_profiles"] = [
            {
                "secondary_profile_1": {
                    "target": "secondary_target_1",
                    "outputs": {
                        "secondary_target_1": {
                            "type": "postgres",
                            "host": "secondary-host",
                            "port": 1234,
                            "user": "secondary_user",
                            "password": "secondary_password",
                            "schema": "secondary_schema",
                            "database": "secondary_db",
                            "threads": 5,  # threads defined in secondary profile
                        }
                    },
                }
            }
        ]
        renderer = empty_profile_renderer()
        profile = dbt.config.Profile.from_raw_profiles(
            profile_data_with_secondary, "default", renderer, threads_override=10
        )  # override threads to 10

        self.assertIn("secondary_profile_1", profile.secondary_profiles)
        secondary_profile = profile.secondary_profiles["secondary_profile_1"]
        # threads_override should take precedence over threads defined in secondary profile
        self.assertEqual(secondary_profile.threads, 10)
        self.assertEqual(
            profile.threads, 10
        )  # primary profile should also have overridden threads

    def test_secondary_profiles_multiple_outputs(self):
        profile_data_with_multiple_outputs = deepcopy(self.default_profile_data)
        profile_data_with_multiple_outputs["default"]["outputs"]["postgres"][
            "secondary_profiles"
        ] = [
            {
                "secondary_profile_1": {
                    "target": "secondary_target_2",
                    "outputs": {
                        "secondary_target_1": {
                            "type": "postgres",
                            "host": "secondary-host",
                            "port": 1234,
                            "user": "secondary_user",
                            "password": "secondary_password",
                            "schema": "secondary_schema",
                            "database": "secondary_db",
                        },
                        "secondary_target_2": {
                            "type": "postgres",
                            "host": "another-secondary-host",
                            "port": 5678,
                            "user": "another_secondary_user",
                            "password": "another_secondary_password",
                            "schema": "another_secondary_schema",
                            "database": "another_secondary_db",
                        },
                    },
                }
            }
        ]
        renderer = empty_profile_renderer()
        profile = dbt.config.Profile.from_raw_profiles(
            profile_data_with_multiple_outputs, "default", renderer
        )
        self.assertIn("secondary_profile_1", profile.secondary_profiles)
        secondary_profile = profile.secondary_profiles["secondary_profile_1"]
        self.assertEqual(secondary_profile.profile_name, "secondary_profile_1")
        self.assertEqual(secondary_profile.target_name, "secondary_target_2")

    def test_secondary_profiles_duplicate_names(self):
        profile_data_with_duplicate_secondary = deepcopy(self.default_profile_data)
        profile_data_with_duplicate_secondary["default"]["outputs"]["postgres"][
            "secondary_profiles"
        ] = [
            {
                "secondary_profile_1": {
                    "target": "secondary_target_1",
                    "outputs": {
                        "secondary_target_1": {
                            "type": "postgres",
                            "host": "secondary-host",
                            "port": 1234,
                            "user": "secondary_user",
                            "password": "secondary_password",
                            "schema": "secondary_schema",
                            "database": "secondary_db",
                        }
                    },
                }
            },
            {
                "secondary_profile_1": {  # Duplicate name
                    "target": "secondary_target_2",
                    "outputs": {
                        "secondary_target_2": {
                            "type": "postgres",
                            "host": "another-secondary-host",
                            "port": 5678,
                            "user": "another_secondary_user",
                            "password": "another_secondary_password",
                            "schema": "another_secondary_schema",
                            "database": "another_secondary_db",
                        }
                    },
                }
            },
        ]
        renderer = empty_profile_renderer()
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            dbt.config.Profile.from_raw_profiles(
                profile_data_with_duplicate_secondary, "default", renderer
            )
        self.assertIn(
            "Secondary profile 'secondary_profile_1' is already defined", str(exc.exception)
        )

    def test_secondary_profiles_nested_secondary(self):
        profile_data_with_nested_secondary = deepcopy(self.default_profile_data)
        profile_data_with_nested_secondary["default"]["outputs"]["postgres"][
            "secondary_profiles"
        ] = [
            {
                "secondary_profile_1": {
                    "target": "secondary_target_1",
                    "outputs": {
                        "secondary_target_1": {
                            "type": "postgres",
                            "host": "secondary-host",
                            "port": 1234,
                            "user": "secondary_user",
                            "password": "secondary_password",
                            "schema": "secondary_schema",
                            "database": "secondary_db",
                            "secondary_profiles": [  # Nested secondary profiles - should be disallowed
                                {
                                    "nested_secondary_profile": {
                                        "target": "nested_target",
                                        "outputs": {
                                            "nested_target": {
                                                "type": "postgres",
                                                "host": "nested-host",
                                                "port": 9012,
                                                "user": "nested_user",
                                                "password": "nested_password",
                                                "schema": "nested_schema",
                                                "database": "nested_db",
                                            }
                                        },
                                    }
                                }
                            ],
                        }
                    },
                }
            }
        ]
        renderer = empty_profile_renderer()
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            dbt.config.Profile.from_raw_profiles(
                profile_data_with_nested_secondary, "default", renderer
            )
        self.assertIn(
            "Secondary profile 'secondary_profile_1' cannot have nested secondary profiles",
            str(exc.exception),
        )

    def test_secondary_profiles_no_outputs(self):
        profile_data_with_no_outputs = deepcopy(self.default_profile_data)
        profile_data_with_no_outputs["default"]["outputs"]["postgres"]["secondary_profiles"] = [
            {
                "secondary_profile_1": {
                    "target": "secondary_target_1",
                }
            }
        ]
        renderer = empty_profile_renderer()
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            dbt.config.Profile.from_raw_profiles(profile_data_with_no_outputs, "default", renderer)
        self.assertIn("outputs not specified", str(exc.exception))
        self.assertIn("secondary_profile_1", str(exc.exception))

    def test_secondary_profiles_no_target_single_output(self):
        profile_data_with_no_target = deepcopy(self.default_profile_data)
        profile_data_with_no_target["default"]["outputs"]["postgres"]["secondary_profiles"] = [
            {
                "secondary_profile_1": {
                    "outputs": {
                        "secondary_target_1": {
                            "type": "postgres",
                            "host": "secondary-host",
                            "port": 1234,
                            "user": "secondary_user",
                            "password": "secondary_password",
                            "schema": "secondary_schema",
                            "database": "secondary_db",
                        }
                    }
                }
            }
        ]
        renderer = empty_profile_renderer()

        profile = dbt.config.Profile.from_raw_profiles(
            profile_data_with_no_target, "default", renderer
        )
        self.assertIn("secondary_profile_1", profile.secondary_profiles)
        secondary_profile = profile.secondary_profiles["secondary_profile_1"]
        self.assertEqual(secondary_profile.profile_name, "secondary_profile_1")
        self.assertEqual(secondary_profile.target_name, "secondary_target_1")

    def test_secondary_profiles_no_target_multiple_outputs(self):
        profile_data_with_duplicate_secondary = deepcopy(self.default_profile_data)
        profile_data_with_duplicate_secondary["default"]["outputs"]["postgres"][
            "secondary_profiles"
        ] = [
            {
                "secondary_profile_1": {
                    "outputs": {
                        "secondary_target_1": {
                            "type": "postgres",
                            "host": "secondary-host",
                            "port": 1234,
                            "user": "secondary_user",
                            "password": "secondary_password",
                            "schema": "secondary_schema",
                            "database": "secondary_db",
                        },
                        "secondary_target_2": {
                            "type": "postgres",
                            "host": "another-secondary-host",
                            "port": 5678,
                            "user": "another_secondary_user",
                            "password": "another_secondary_password",
                            "schema": "another_secondary_schema",
                            "database": "another_secondary_db",
                        },
                    }
                }
            }
        ]
        renderer = empty_profile_renderer()
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            dbt.config.Profile.from_raw_profiles(
                profile_data_with_duplicate_secondary, "default", renderer
            )
        self.assertIn(
            "The profile 'secondary_profile_1' does not have a target named 'default'",
            str(exc.exception),
        )

    def test_secondary_profiles_no_host(self):
        profile_data_with_no_host = deepcopy(self.default_profile_data)
        profile_data_with_no_host["default"]["outputs"]["postgres"]["secondary_profiles"] = [
            {
                "secondary_profile_1": {
                    "target": "secondary_target_1",
                    "outputs": {
                        "secondary_target_1": {
                            "type": "postgres",
                            "port": 1234,
                            "user": "secondary_user",
                            "password": "secondary_password",
                            "schema": "secondary_schema",
                            "database": "secondary_db",
                        }
                    },
                }
            }
        ]
        renderer = empty_profile_renderer()
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            dbt.config.Profile.from_raw_profiles(profile_data_with_no_host, "default", renderer)
        self.assertIn("'host' is a required property", str(exc.exception))
