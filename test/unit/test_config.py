from copy import deepcopy
from contextlib import contextmanager
import json
import os
import shutil
import tempfile
import unittest

import mock
import yaml

import dbt.config
import dbt.exceptions
from dbt.contracts.connection import PostgresCredentials, RedshiftCredentials
from dbt.contracts.project import PackageConfig


@contextmanager
def temp_cd(path):
    current_path = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(current_path)


model_config = {
    'my_package_name': {
        'enabled': True,
        'adwords': {
            'adwords_ads': {
                'materialized': 'table',
                'enabled': True,
                'schema': 'analytics'
            }
        },
        'snowplow': {
            'snowplow_sessions': {
                'sort': 'timestamp',
                'materialized': 'incremental',
                'dist': 'user_id',
                'sql_where': 'created_at > (select max(created_at) from {{ this }})',
                'unique_key': 'id'
            },
            'base': {
                'snowplow_events': {
                    'sort': ['timestamp', 'userid'],
                    'materialized': 'table',
                    'sort_type': 'interleaved',
                    'dist': 'userid'
                }
            }
        }
    }
}

model_fqns = frozenset((
    ('my_package_name', 'snowplow', 'snowplow_sessions'),
    ('my_package_name', 'snowplow', 'base', 'snowplow_events'),
    ('my_package_name', 'adwords', 'adwords_ads'),
))


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self.base_dir = tempfile.mkdtemp()
        self.profiles_path = os.path.join(self.base_dir, 'profiles.yml')

    def set_up_empty_config(self):
        with open(self.profiles_path, 'w') as f:
            f.write(yaml.dump({}))

    def set_up_config_options(self, **kwargs):
        config = {
            'config': kwargs
        }

        with open(self.profiles_path, 'w') as f:
            f.write(yaml.dump(config))

    def tearDown(self):
        try:
            shutil.rmtree(self.base_dir)
        except:
            pass

    def test__implicit_opt_in(self):
        self.set_up_empty_config()
        config = dbt.config.read_config(self.base_dir)
        self.assertTrue(dbt.config.send_anonymous_usage_stats(config))

    def test__explicit_opt_out(self):
        self.set_up_config_options(send_anonymous_usage_stats=False)
        config = dbt.config.read_config(self.base_dir)
        self.assertFalse(dbt.config.send_anonymous_usage_stats(config))

    def test__explicit_opt_in(self):
        self.set_up_config_options(send_anonymous_usage_stats=True)
        config = dbt.config.read_config(self.base_dir)
        self.assertTrue(dbt.config.send_anonymous_usage_stats(config))

    def test__implicit_colors(self):
        self.set_up_empty_config()
        config = dbt.config.read_config(self.base_dir)
        self.assertTrue(dbt.config.colorize_output(config))

    def test__explicit_opt_out(self):
        self.set_up_config_options(use_colors=False)
        config = dbt.config.read_config(self.base_dir)
        self.assertFalse(dbt.config.colorize_output(config))

    def test__explicit_opt_in(self):
        self.set_up_config_options(use_colors=True)
        config = dbt.config.read_config(self.base_dir)
        self.assertTrue(dbt.config.colorize_output(config))


class Args(object):
    def __init__(self, profiles_dir=None, threads=None, profile=None, cli_vars=None):
        self.profile = profile
        if threads is not None:
            self.threads = threads
        if profiles_dir is not None:
            self.profiles_dir = profiles_dir
        if cli_vars is not None:
            self.vars = cli_vars


class BaseConfigTest(unittest.TestCase):
    """Subclass this, and before calling the superclass setUp, set
    profiles_dir.
    """
    def setUp(self):
        self.default_project_data = {
            'version': '0.0.1',
            'name': 'my_test_project',
            'profile': 'default',
        }
        self.default_profile_data = {
            'default': {
                'outputs': {
                    'postgres': {
                        'type': 'postgres',
                        'host': 'postgres-db-hostname',
                        'port': 5555,
                        'user': 'db_user',
                        'pass': 'db_pass',
                        'dbname': 'postgres-db-name',
                        'schema': 'postgres-schema',
                        'threads': 7,
                    },
                    'redshift': {
                        'type': 'redshift',
                        'host': 'redshift-db-hostname',
                        'port': 5555,
                        'user': 'db_user',
                        'pass': 'db_pass',
                        'dbname': 'redshift-db-name',
                        'schema': 'redshift-schema',
                    },
                    'with-vars': {
                        'type': "{{ env_var('env_value_type') }}",
                        'host': "{{ env_var('env_value_host') }}",
                        'port': "{{ env_var('env_value_port') }}",
                        'user': "{{ env_var('env_value_user') }}",
                        'pass': "{{ env_var('env_value_pass') }}",
                        'dbname': "{{ env_var('env_value_dbname') }}",
                        'schema': "{{ env_var('env_value_schema') }}",
                    },
                    'cli-and-env-vars': {
                        'type': "{{ env_var('env_value_type') }}",
                        'host': "{{ var('cli_value_host') }}",
                        'port': "{{ env_var('env_value_port') }}",
                        'user': "{{ env_var('env_value_user') }}",
                        'pass': "{{ env_var('env_value_pass') }}",
                        'dbname': "{{ env_var('env_value_dbname') }}",
                        'schema': "{{ env_var('env_value_schema') }}",
                    }
                },
                'target': 'postgres',
            },
            'other': {
                'outputs': {
                    'other-postgres': {
                        'type': 'postgres',
                        'host': 'other-postgres-db-hostname',
                        'port': 4444,
                        'user': 'other_db_user',
                        'pass': 'other_db_pass',
                        'dbname': 'other-postgres-db-name',
                        'schema': 'other-postgres-schema',
                        'threads': 2,
                    }
                },
                'target': 'other-postgres',
            }
        }
        self.args = Args(profiles_dir=self.profiles_dir, cli_vars='{}')
        self.env_override = {
            'env_value_type': 'postgres',
            'env_value_host': 'env-postgres-host',
            'env_value_port': '6543',
            'env_value_user': 'env-postgres-user',
            'env_value_pass': 'env-postgres-pass',
            'env_value_dbname': 'env-postgres-dbname',
            'env_value_schema': 'env-postgres-schema',
            'env_value_project': 'blah',
        }


class BaseFileTest(BaseConfigTest):
    def setUp(self):
        self.project_dir = os.path.normpath(tempfile.mkdtemp())
        self.profiles_dir = os.path.normpath(tempfile.mkdtemp())
        super(BaseFileTest, self).setUp()

    def tearDown(self):
        try:
            shutil.rmtree(self.project_dir)
        except EnvironmentError:
            pass
        try:
            shutil.rmtree(self.profiles_dir)
        except EnvironmentError:
            pass

    def proejct_path(self, name):
        return os.path.join(self.project_dir, name)

    def profile_path(self, name):
        return os.path.join(self.profiles_dir, name)

    def write_project(self, project_data=None):
        if project_data is None:
            project_data = self.project_data
        with open(self.proejct_path('dbt_project.yml'), 'w') as fp:
            yaml.dump(project_data, fp)

    def write_packages(self, package_data):
        with open(self.proejct_path('packages.yml'), 'w') as fp:
            yaml.dump(package_data, fp)

    def write_profile(self, profile_data=None):
        if profile_data is None:
            profile_data = self.profile_data
        with open(self.profile_path('profiles.yml'), 'w') as fp:
            yaml.dump(profile_data, fp)


class TestProfile(BaseConfigTest):
    def setUp(self):
        self.profiles_dir = '/invalid-path'
        super(TestProfile, self).setUp()

    def from_raw_profiles(self):
        return dbt.config.Profile.from_raw_profiles(
            self.default_profile_data, 'default', {}
        )

    def test_from_raw_profiles(self):
        profile = self.from_raw_profiles()
        self.assertEqual(profile.profile_name, 'default')
        self.assertEqual(profile.target_name, 'postgres')
        self.assertEqual(profile.threads, 7)
        self.assertTrue(profile.send_anonymous_usage_stats)
        self.assertTrue(profile.use_colors)
        self.assertTrue(isinstance(profile.credentials, PostgresCredentials))
        self.assertEqual(profile.credentials.type, 'postgres')
        self.assertEqual(profile.credentials.host, 'postgres-db-hostname')
        self.assertEqual(profile.credentials.port, 5555)
        self.assertEqual(profile.credentials.user, 'db_user')
        self.assertEqual(profile.credentials.password, 'db_pass')
        self.assertEqual(profile.credentials.schema, 'postgres-schema')
        self.assertEqual(profile.credentials.dbname, 'postgres-db-name')

    def test_config_override(self):
        self.default_profile_data['config'] = {
            'send_anonymous_usage_stats': False,
            'use_colors': False
        }
        profile = self.from_raw_profiles()
        self.assertEqual(profile.profile_name, 'default')
        self.assertEqual(profile.target_name, 'postgres')
        self.assertFalse(profile.send_anonymous_usage_stats)
        self.assertFalse(profile.use_colors)

    def test_partial_config_override(self):
        self.default_profile_data['config'] = {
            'send_anonymous_usage_stats': False,
        }
        profile = self.from_raw_profiles()
        self.assertEqual(profile.profile_name, 'default')
        self.assertEqual(profile.target_name, 'postgres')
        self.assertFalse(profile.send_anonymous_usage_stats)
        self.assertTrue(profile.use_colors)

    def test_missing_type(self):
        del self.default_profile_data['default']['outputs']['postgres']['type']
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            self.from_raw_profiles()
        self.assertIn('type', str(exc.exception))
        self.assertIn('postgres', str(exc.exception))
        self.assertIn('default', str(exc.exception))

    def test_bad_type(self):
        self.default_profile_data['default']['outputs']['postgres']['type'] = 'invalid'
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            self.from_raw_profiles()
        self.assertIn('Credentials', str(exc.exception))
        self.assertIn('postgres', str(exc.exception))
        self.assertIn('default', str(exc.exception))

    def test_invalid_credentials(self):
        del self.default_profile_data['default']['outputs']['postgres']['host']
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            self.from_raw_profiles()
        self.assertIn('Credentials', str(exc.exception))
        self.assertIn('postgres', str(exc.exception))
        self.assertIn('default', str(exc.exception))

    def test_missing_target(self):
        profile = self.default_profile_data['default']
        del profile['target']
        profile['outputs']['default'] = profile['outputs']['postgres']
        profile = self.from_raw_profiles()
        self.assertEqual(profile.profile_name, 'default')
        self.assertEqual(profile.target_name, 'default')
        self.assertEqual(profile.credentials.type, 'postgres')

    def test_profile_invalid_project(self):
        with self.assertRaises(dbt.exceptions.DbtProjectError) as exc:
            dbt.config.Profile.from_raw_profiles(
                self.default_profile_data, 'invalid-profile', {}
            )

        self.assertEqual(exc.exception.result_type, 'invalid_project')
        self.assertIn('Could not find', str(exc.exception))
        self.assertIn('invalid-profile', str(exc.exception))

    def test_profile_invalid_target(self):
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            dbt.config.Profile.from_raw_profiles(
                self.default_profile_data, 'default', {},
                target_override='nope'
            )

        self.assertIn('nope', str(exc.exception))
        self.assertIn('- postgres', str(exc.exception))
        self.assertIn('- redshift', str(exc.exception))
        self.assertIn('- with-vars', str(exc.exception))

    def test_no_outputs(self):
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            dbt.config.Profile.from_raw_profiles(
                {'some-profile': {'target': 'blah'}}, 'some-profile', {}
            )
        self.assertIn('outputs not specified', str(exc.exception))
        self.assertIn('some-profile', str(exc.exception))

    def test_neq(self):
        profile = self.from_raw_profiles()
        self.assertNotEqual(profile, object())

    def test_eq(self):
        profile = dbt.config.Profile.from_raw_profiles(
            deepcopy(self.default_profile_data), 'default', {}
        )

        other = dbt.config.Profile.from_raw_profiles(
            deepcopy(self.default_profile_data), 'default', {}
        )
        self.assertEqual(profile, other)

    def test_invalid_env_vars(self):
        self.env_override['env_value_port'] = 'hello'
        with mock.patch.dict(os.environ, self.env_override):
            with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
                dbt.config.Profile.from_raw_profile_info(
                    self.default_profile_data['default'],
                    'default',
                    {},
                    target_override='with-vars'
                )
        self.assertIn("not of type 'integer'", str(exc.exception))


class TestProfileFile(BaseFileTest):
    def setUp(self):
        super(TestProfileFile, self).setUp()
        self.write_profile(self.default_profile_data)

    def from_raw_profile_info(self, raw_profile=None, profile_name='default', **kwargs):
        if raw_profile is None:
            raw_profile = self.default_profile_data['default']
        kw = {
            'raw_profile': raw_profile,
            'profile_name': profile_name,
            'cli_vars': {},
        }
        kw.update(kwargs)
        return dbt.config.Profile.from_raw_profile_info(**kw)

    def from_args(self, project_profile_name='default', **kwargs):
        kw = {
            'args': self.args,
            'project_profile_name': project_profile_name,
            'cli_vars': {},
        }
        kw.update(kwargs)
        return dbt.config.Profile.from_args(**kw)


    def test_profile_simple(self):
        profile = self.from_args()
        from_raw = self.from_raw_profile_info()

        self.assertEqual(profile.profile_name, 'default')
        self.assertEqual(profile.target_name, 'postgres')
        self.assertEqual(profile.threads, 7)
        self.assertTrue(profile.send_anonymous_usage_stats)
        self.assertTrue(profile.use_colors)
        self.assertTrue(isinstance(profile.credentials, PostgresCredentials))
        self.assertEqual(profile.credentials.type, 'postgres')
        self.assertEqual(profile.credentials.host, 'postgres-db-hostname')
        self.assertEqual(profile.credentials.port, 5555)
        self.assertEqual(profile.credentials.user, 'db_user')
        self.assertEqual(profile.credentials.password, 'db_pass')
        self.assertEqual(profile.credentials.schema, 'postgres-schema')
        self.assertEqual(profile.credentials.dbname, 'postgres-db-name')
        self.assertEqual(profile, from_raw)

    def test_profile_override(self):
        self.args.profile = 'other'
        self.args.threads = 3
        profile = self.from_args()
        from_raw = self.from_raw_profile_info(
                self.default_profile_data['other'],
                'other',
                threads_override=3,
            )

        self.assertEqual(profile.profile_name, 'other')
        self.assertEqual(profile.target_name, 'other-postgres')
        self.assertEqual(profile.threads, 3)
        self.assertTrue(profile.send_anonymous_usage_stats)
        self.assertTrue(profile.use_colors)
        self.assertTrue(isinstance(profile.credentials, PostgresCredentials))
        self.assertEqual(profile.credentials.type, 'postgres')
        self.assertEqual(profile.credentials.host, 'other-postgres-db-hostname')
        self.assertEqual(profile.credentials.port, 4444)
        self.assertEqual(profile.credentials.user, 'other_db_user')
        self.assertEqual(profile.credentials.password, 'other_db_pass')
        self.assertEqual(profile.credentials.schema, 'other-postgres-schema')
        self.assertEqual(profile.credentials.dbname, 'other-postgres-db-name')
        self.assertEqual(profile, from_raw)

    def test_target_override(self):
        self.args.target = 'redshift'
        profile = self.from_args()
        from_raw = self.from_raw_profile_info(
                target_override='redshift'
            )

        self.assertEqual(profile.profile_name, 'default')
        self.assertEqual(profile.target_name, 'redshift')
        self.assertEqual(profile.threads, 1)
        self.assertTrue(profile.send_anonymous_usage_stats)
        self.assertTrue(profile.use_colors)
        self.assertTrue(isinstance(profile.credentials, RedshiftCredentials))
        self.assertEqual(profile.credentials.type, 'redshift')
        self.assertEqual(profile.credentials.host, 'redshift-db-hostname')
        self.assertEqual(profile.credentials.port, 5555)
        self.assertEqual(profile.credentials.user, 'db_user')
        self.assertEqual(profile.credentials.password, 'db_pass')
        self.assertEqual(profile.credentials.schema, 'redshift-schema')
        self.assertEqual(profile.credentials.dbname, 'redshift-db-name')
        self.assertEqual(profile, from_raw)

    def test_env_vars(self):
        self.args.target = 'with-vars'
        with mock.patch.dict(os.environ, self.env_override):
            profile = self.from_args()
            from_raw = self.from_raw_profile_info(
                target_override='with-vars'
            )

        self.assertEqual(profile.profile_name, 'default')
        self.assertEqual(profile.target_name, 'with-vars')
        self.assertEqual(profile.threads, 1)
        self.assertTrue(profile.send_anonymous_usage_stats)
        self.assertTrue(profile.use_colors)
        self.assertEqual(profile.credentials.type, 'postgres')
        self.assertEqual(profile.credentials.host, 'env-postgres-host')
        self.assertEqual(profile.credentials.port, 6543)
        self.assertEqual(profile.credentials.user, 'env-postgres-user')
        self.assertEqual(profile.credentials.password, 'env-postgres-pass')
        self.assertEqual(profile, from_raw)

    def test_env_vars_env_target(self):
        self.default_profile_data['default']['target'] = "{{ env_var('env_value_target') }}"
        self.write_profile(self.default_profile_data)
        self.env_override['env_value_target'] = 'with-vars'
        with mock.patch.dict(os.environ, self.env_override):
            profile = self.from_args()
            from_raw = self.from_raw_profile_info(
                target_override='with-vars'
            )

        self.assertEqual(profile.profile_name, 'default')
        self.assertEqual(profile.target_name, 'with-vars')
        self.assertEqual(profile.threads, 1)
        self.assertTrue(profile.send_anonymous_usage_stats)
        self.assertTrue(profile.use_colors)
        self.assertEqual(profile.credentials.type, 'postgres')
        self.assertEqual(profile.credentials.host, 'env-postgres-host')
        self.assertEqual(profile.credentials.port, 6543)
        self.assertEqual(profile.credentials.user, 'env-postgres-user')
        self.assertEqual(profile.credentials.password, 'env-postgres-pass')
        self.assertEqual(profile, from_raw)

    def test_invalid_env_vars(self):
        self.env_override['env_value_port'] = 'hello'
        self.args.target = 'with-vars'
        with mock.patch.dict(os.environ, self.env_override):
            with self.assertRaises(dbt.config.DbtProfileError) as exc:
                self.from_args()

        self.assertIn("not of type 'integer'", str(exc.exception))

    def test_cli_and_env_vars(self):
        self.args.target = 'cli-and-env-vars'
        self.args.vars = '{"cli_value_host": "cli-postgres-host"}'
        with mock.patch.dict(os.environ, self.env_override):
            profile = self.from_args(cli_vars=None)
            from_raw = self.from_raw_profile_info(
                target_override='cli-and-env-vars',
                cli_vars={'cli_value_host': 'cli-postgres-host'},
            )

        self.assertEqual(profile.profile_name, 'default')
        self.assertEqual(profile.target_name, 'cli-and-env-vars')
        self.assertEqual(profile.threads, 1)
        self.assertTrue(profile.send_anonymous_usage_stats)
        self.assertTrue(profile.use_colors)
        self.assertEqual(profile.credentials.type, 'postgres')
        self.assertEqual(profile.credentials.host, 'cli-postgres-host')
        self.assertEqual(profile.credentials.port, 6543)
        self.assertEqual(profile.credentials.user, 'env-postgres-user')
        self.assertEqual(profile.credentials.password, 'env-postgres-pass')
        self.assertEqual(profile, from_raw)

    def test_no_profile(self):
        with self.assertRaises(dbt.exceptions.DbtProjectError) as exc:
            self.from_args(project_profile_name=None)
        self.assertIn('no profile was specified', str(exc.exception))


class TestProject(BaseConfigTest):
    def setUp(self):
        self.profiles_dir = '/invalid-profiles-path'
        self.project_dir = '/invalid-root-path'
        super(TestProject, self).setUp()
        self.default_project_data['project-root'] = self.project_dir

    def test_defaults(self):
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        self.assertEqual(project.project_name, 'my_test_project')
        self.assertEqual(project.version, '0.0.1')
        self.assertEqual(project.profile_name, 'default')
        self.assertEqual(project.project_root, '/invalid-root-path')
        self.assertEqual(project.source_paths, ['models'])
        self.assertEqual(project.macro_paths, ['macros'])
        self.assertEqual(project.data_paths, ['data'])
        self.assertEqual(project.test_paths, ['test'])
        self.assertEqual(project.analysis_paths, [])
        self.assertEqual(project.docs_paths, ['models'])
        self.assertEqual(project.target_path, 'target')
        self.assertEqual(project.clean_targets, ['target'])
        self.assertEqual(project.log_path, 'logs')
        self.assertEqual(project.modules_path, 'dbt_modules')
        self.assertEqual(project.quoting, {})
        self.assertEqual(project.models, {})
        self.assertEqual(project.on_run_start, [])
        self.assertEqual(project.on_run_end, [])
        self.assertEqual(project.archive, [])
        self.assertEqual(project.seeds, {})
        self.assertEqual(project.packages, PackageConfig(packages=[]))
        # just make sure str() doesn't crash anything, that's always
        # embarrassing
        str(project)

    def test_eq(self):
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        other = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        self.assertEqual(project, other)

    def test_neq(self):
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        self.assertNotEqual(project, object())

    def test_implicit_overrides(self):
        self.default_project_data.update({
            'source-paths': ['other-models'],
            'target-path': 'other-target',
        })
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        self.assertEqual(project.docs_paths, ['other-models'])
        self.assertEqual(project.clean_targets, ['other-target'])

    def test_hashed_name(self):
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        self.assertEqual(project.hashed_name(), '754cd47eac1d6f50a5f7cd399ec43da4')

    def test_all_overrides(self):
        self.default_project_data.update({
            'source-paths': ['other-models'],
            'macro-paths': ['other-macros'],
            'data-paths': ['other-data'],
            'test-paths': ['other-test'],
            'analysis-paths': ['analysis'],
            'docs-paths': ['docs'],
            'target-path': 'other-target',
            'clean-targets': ['another-target'],
            'log-path': 'other-logs',
            'modules-path': 'other-dbt_modules',
            'quoting': {'identifier': False},
            'models': {
                'pre-hook': ['{{ logging.log_model_start_event() }}'],
                'post-hook': ['{{ logging.log_model_end_event() }}'],
                'my_test_project': {
                    'first': {
                        'enabled': False,
                        'sub': {
                            'enabled': True,
                        }
                    },
                    'second': {
                        'materialized': 'table',
                    },
                },
                'third_party': {
                    'third': {
                        'materialized': 'view',
                    },
                },
            },
            'on-run-start': [
                '{{ logging.log_run_start_event() }}',
            ],
            'on-run-end': [
                '{{ logging.log_run_end_event() }}',
            ],
            'archive': [
                {
                    'source_schema': 'my_schema',
                    'target_schema': 'my_other_schema',
                    'tables': [
                        {
                            'source_table': 'my_table',
                            'target_Table': 'my_table_archived',
                            'updated_at': 'updated_at_field',
                            'unique_key': 'table_id',
                        },
                    ],
                },
            ],
            'seeds': {
                'my_test_project': {
                    'enabled': True,
                    'schema': 'seed_data',
                    'post-hook': 'grant select on {{ this }} to bi_user',
                },
            },
        })
        packages = {
            'packages': [
                {
                    'local': 'foo',
                },
                {
                    'git': 'git@example.com:fishtown-analytics/dbt-utils.git',
                    'revision': 'test-rev'
                },
            ],
        }
        project = dbt.config.Project.from_project_config(
            self.default_project_data, packages
        )
        self.assertEqual(project.project_name, 'my_test_project')
        self.assertEqual(project.version, '0.0.1')
        self.assertEqual(project.profile_name, 'default')
        self.assertEqual(project.project_root, '/invalid-root-path')
        self.assertEqual(project.source_paths, ['other-models'])
        self.assertEqual(project.macro_paths, ['other-macros'])
        self.assertEqual(project.data_paths, ['other-data'])
        self.assertEqual(project.test_paths, ['other-test'])
        self.assertEqual(project.analysis_paths, ['analysis'])
        self.assertEqual(project.docs_paths, ['docs'])
        self.assertEqual(project.target_path, 'other-target')
        self.assertEqual(project.clean_targets, ['another-target'])
        self.assertEqual(project.log_path, 'other-logs')
        self.assertEqual(project.modules_path, 'other-dbt_modules')
        self.assertEqual(project.quoting, {'identifier': False})
        self.assertEqual(project.models, {
            'pre-hook': ['{{ logging.log_model_start_event() }}'],
            'post-hook': ['{{ logging.log_model_end_event() }}'],
            'my_test_project': {
                'first': {
                    'enabled': False,
                    'sub': {
                        'enabled': True,
                    }
                },
                'second': {
                    'materialized': 'table',
                },
            },
            'third_party': {
                'third': {
                    'materialized': 'view',
                },
            },
        })
        self.assertEqual(project.on_run_start, ['{{ logging.log_run_start_event() }}'])
        self.assertEqual(project.on_run_end, ['{{ logging.log_run_end_event() }}'])
        self.assertEqual(project.archive, [{
            'source_schema': 'my_schema',
            'target_schema': 'my_other_schema',
            'tables': [
                {
                    'source_table': 'my_table',
                    'target_Table': 'my_table_archived',
                    'updated_at': 'updated_at_field',
                    'unique_key': 'table_id',
                },
            ],
        }])
        self.assertEqual(project.seeds, {
            'my_test_project': {
                'enabled': True,
                'schema': 'seed_data',
                'post-hook': 'grant select on {{ this }} to bi_user',
            },
        })
        self.assertEqual(project.packages, PackageConfig(packages=[
            {
                'local': 'foo',
            },
            {
                'git': 'git@example.com:fishtown-analytics/dbt-utils.git',
                'revision': 'test-rev'
            },
        ]))
        str(project)
        json.dumps(project.to_project_config())

    def test_string_run_hooks(self):
        self.default_project_data.update({
            'on-run-start': '{{ logging.log_run_start_event() }}',
            'on-run-end': '{{ logging.log_run_end_event() }}',
        })
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        self.assertEqual(
            project.on_run_start,
            ['{{ logging.log_run_start_event() }}']
        )
        self.assertEqual(
            project.on_run_end,
            ['{{ logging.log_run_end_event() }}']
        )

    def test_invalid_project_name(self):
        self.default_project_data['name'] = 'invalid-project-name'
        with self.assertRaises(dbt.exceptions.DbtProjectError) as exc:
            dbt.config.Project.from_project_config(self.default_project_data)

        self.assertIn('invalid-project-name', str(exc.exception))

    def test_no_project(self):
        with self.assertRaises(dbt.exceptions.DbtProjectError) as exc:
            dbt.config.Project.from_project_root(self.project_dir, {})

        self.assertIn('no dbt_project.yml', str(exc.exception))

    def test__no_unused_resource_config_paths(self):
        self.default_project_data.update({
            'models': model_config,
            'seeds': {},
        })
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )

        resource_fqns = {'models': model_fqns}
        # import ipdb;ipdb.set_trace()
        unused = project.get_unused_resource_config_paths(resource_fqns, [])
        self.assertEqual(len(unused), 0)

    def test__unused_resource_config_paths(self):
        self.default_project_data.update({
            'models': model_config['my_package_name'],
            'seeds': {},
        })
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )

        resource_fqns = {'models': model_fqns}
        unused = project.get_unused_resource_config_paths(resource_fqns, [])
        self.assertEqual(len(unused), 3)

    def test__get_unused_resource_config_paths_empty(self):
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        unused = project.get_unused_resource_config_paths({'models': frozenset((
            ('my_test_project', 'foo', 'bar'),
            ('my_test_project', 'foo', 'baz'),
        ))}, [])
        self.assertEqual(len(unused), 0)

    @mock.patch.object(dbt.config, 'logger')
    def test__warn_for_unused_resource_config_paths_empty(self, mock_logger):
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        unused = project.warn_for_unused_resource_config_paths({'models': frozenset((
            ('my_test_project', 'foo', 'bar'),
            ('my_test_project', 'foo', 'baz'),
        ))}, [])
        mock_logger.info.assert_not_called()

    def test_none_values(self):
        self.default_project_data.update({
            'models': None,
            'seeds': None,
            'archive': None,
            'on-run-end': None,
            'on-run-start': None,
        })
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        self.assertEqual(project.models, {})
        self.assertEqual(project.on_run_start, [])
        self.assertEqual(project.on_run_end, [])
        self.assertEqual(project.archive, [])
        self.assertEqual(project.seeds, {})

    def test_nested_none_values(self):
        self.default_project_data.update({
            'models': {'vars': None, 'pre-hook': None, 'post-hook': None},
            'seeds': {'vars': None, 'pre-hook': None, 'post-hook': None, 'column_types': None},
        })
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        self.assertEqual(project.models, {'vars': {}, 'pre-hook': [], 'post-hook': []})
        self.assertEqual(project.seeds, {'vars': {}, 'pre-hook': [], 'post-hook': [], 'column_types': {}})

    def test_cycle(self):
        models = {}
        models['models'] = models
        self.default_project_data.update({
            'models': models,
        })
        with self.assertRaises(dbt.exceptions.DbtProjectError):
            dbt.config.Project.from_project_config(
                self.default_project_data
            )


class TestProjectWithConfigs(BaseConfigTest):
    def setUp(self):
        self.profiles_dir = '/invalid-profiles-path'
        self.project_dir = '/invalid-root-path'
        super(TestProjectWithConfigs, self).setUp()
        self.default_project_data['project-root'] = self.project_dir
        self.default_project_data['models'] = {
            'enabled': True,
            'my_test_project': {
                'foo': {
                    'materialized': 'view',
                    'bar': {
                        'materialized': 'table',
                    }
                },
                'baz': {
                    'materialized': 'table',
                }
            }
        }
        self.used = {'models': frozenset((
            ('my_test_project', 'foo', 'bar'),
            ('my_test_project', 'foo', 'baz'),
        ))}

    def test__get_unused_resource_config_paths(self):
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        unused = project.get_unused_resource_config_paths(self.used, [])
        self.assertEqual(len(unused), 1)
        self.assertEqual(unused[0], ('models', 'my_test_project', 'baz'))

    @mock.patch.object(dbt.config, 'logger')
    def test__warn_for_unused_resource_config_paths(self, mock_logger):
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        unused = project.warn_for_unused_resource_config_paths(self.used, [])
        mock_logger.info.assert_called_once()

    @mock.patch.object(dbt.config, 'logger')
    def test__warn_for_unused_resource_config_paths_disabled(self, mock_logger):
        project = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        unused = project.get_unused_resource_config_paths(
            self.used,
            frozenset([('my_test_project', 'baz')])
        )

        self.assertEqual(len(unused), 0)



class TestProjectFile(BaseFileTest):
    def setUp(self):
        super(TestProjectFile, self).setUp()
        self.write_project(self.default_project_data)
        # and after the fact, add the project root
        self.default_project_data['project-root'] = self.project_dir

    def test_from_project_root(self):
        project = dbt.config.Project.from_project_root(self.project_dir, {})
        from_config = dbt.config.Project.from_project_config(
            self.default_project_data
        )
        self.assertEqual(project, from_config)
        self.assertEqual(project.version, "0.0.1")
        self.assertEqual(project.project_name, 'my_test_project')

    def test_with_invalid_package(self):
        self.write_packages({'invalid': ['not a package of any kind']})
        with self.assertRaises(dbt.exceptions.DbtProjectError) as exc:
            dbt.config.Project.from_project_root(self.project_dir, {})


class TestVariableProjectFile(BaseFileTest):
    def setUp(self):
        super(TestVariableProjectFile, self).setUp()
        self.default_project_data['version'] = "{{ var('cli_version') }}"
        self.default_project_data['name'] = "{{ env_var('env_value_project') }}"
        self.write_project(self.default_project_data)
        # and after the fact, add the project root
        self.default_project_data['project-root'] = self.project_dir

    def test_cli_and_env_vars(self):
        cli_vars = '{"cli_version": "0.1.2"}'
        with mock.patch.dict(os.environ, self.env_override):
            project = dbt.config.Project.from_project_root(
                self.project_dir,
                cli_vars
            )

        self.assertEqual(project.version, "0.1.2")
        self.assertEqual(project.project_name, 'blah')


class TestRuntimeConfig(BaseConfigTest):
    def setUp(self):
        self.profiles_dir = '/invalid-profiles-path'
        self.project_dir = '/invalid-root-path'
        super(TestRuntimeConfig, self).setUp()
        self.default_project_data['project-root'] = self.project_dir

    def get_project(self):
        return dbt.config.Project.from_project_config(
            self.default_project_data
        )

    def get_profile(self):
        return dbt.config.Profile.from_raw_profiles(
            self.default_profile_data, self.default_project_data['profile'], {}
        )

    def test_from_parts(self):
        project = self.get_project()
        profile = self.get_profile()
        config = dbt.config.RuntimeConfig.from_parts(project, profile, {})

        self.assertEqual(config.cli_vars, {})
        self.assertEqual(config.to_profile_info(), profile.to_profile_info())
        # we should have the default quoting set in the full config, but not in
        # the project
        # TODO(jeb): Adapters must assert that quoting is populated?
        expected_project = project.to_project_config()
        self.assertEqual(expected_project['quoting'], {})

        expected_project['quoting'] = {
            'database': True,
            'identifier': True,
            'schema': True,
        }
        self.assertEqual(config.to_project_config(), expected_project)

    def test_str(self):
        project = self.get_project()
        profile = self.get_profile()
        config = dbt.config.RuntimeConfig.from_parts(project, profile, {})

        # to make sure nothing terrible happens
        str(config)

    def test_validate_fails(self):
        project = self.get_project()
        profile = self.get_profile()
        # invalid - must be boolean
        profile.use_colors = None
        with self.assertRaises(dbt.exceptions.DbtProjectError):
            dbt.config.RuntimeConfig.from_parts(project, profile, {})


class TestRuntimeConfigFiles(BaseFileTest):
    def setUp(self):
        super(TestRuntimeConfigFiles, self).setUp()
        self.write_profile(self.default_profile_data)
        self.write_project(self.default_project_data)
        # and after the fact, add the project root
        self.default_project_data['project-root'] = self.project_dir

    def test_from_args(self):
        with temp_cd(self.project_dir):
            config = dbt.config.RuntimeConfig.from_args(self.args)
        self.assertEqual(config.project_name, 'my_test_project')
        self.assertEqual(config.version, '0.0.1')
        self.assertEqual(config.profile_name, 'default')
        # on osx, for example, these are not necessarily equal due to /private
        self.assertTrue(os.path.samefile(config.project_root,
                                         self.project_dir))
        self.assertEqual(config.source_paths, ['models'])
        self.assertEqual(config.macro_paths, ['macros'])
        self.assertEqual(config.data_paths, ['data'])
        self.assertEqual(config.test_paths, ['test'])
        self.assertEqual(config.analysis_paths, [])
        self.assertEqual(config.docs_paths, ['models'])
        self.assertEqual(config.target_path, 'target')
        self.assertEqual(config.clean_targets, ['target'])
        self.assertEqual(config.log_path, 'logs')
        self.assertEqual(config.modules_path, 'dbt_modules')
        self.assertEqual(config.quoting, {'database': True, 'identifier': True, 'schema': True})
        self.assertEqual(config.models, {})
        self.assertEqual(config.on_run_start, [])
        self.assertEqual(config.on_run_end, [])
        self.assertEqual(config.archive, [])
        self.assertEqual(config.seeds, {})
        self.assertEqual(config.packages, PackageConfig(packages=[]))


class TestVariableRuntimeConfigFiles(BaseFileTest):
    def setUp(self):
        super(TestVariableRuntimeConfigFiles, self).setUp()
        self.default_project_data.update({
            'version': "{{ var('cli_version') }}",
            'name': "{{ env_var('env_value_project') }}",
            'on-run-end': [
                "{{ env_var('env_value_project') }}",
            ],
            'models': {
                'foo': {
                    'post-hook': "{{ env_var('env_value_target') }}",
                },
                'bar': {
                    # just gibberish, make sure it gets interpreted
                    'materialized': "{{ env_var('env_value_project') }}",
                }
            },
            'seeds': {
                'foo': {
                    'post-hook': "{{ env_var('env_value_target') }}",
                },
                'bar': {
                    # just gibberish, make sure it gets interpreted
                    'materialized': "{{ env_var('env_value_project') }}",
                }
            },
        })
        self.write_project(self.default_project_data)
        self.write_profile(self.default_profile_data)
        # and after the fact, add the project root
        self.default_project_data['project-root'] = self.project_dir

    def test_cli_and_env_vars(self):
        self.args.target = 'cli-and-env-vars'
        self.args.vars = '{"cli_value_host": "cli-postgres-host", "cli_version": "0.1.2"}'
        with mock.patch.dict(os.environ, self.env_override), temp_cd(self.project_dir):
            config = dbt.config.RuntimeConfig.from_args(self.args)

        self.assertEqual(config.version, "0.1.2")
        self.assertEqual(config.project_name, 'blah')
        self.assertEqual(config.credentials.host, 'cli-postgres-host')
        self.assertEqual(config.credentials.user, 'env-postgres-user')
        # make sure hooks are not interpreted
        self.assertEqual(config.on_run_end, ["{{ env_var('env_value_project') }}"])
        self.assertEqual(config.models['foo']['post-hook'], "{{ env_var('env_value_target') }}")
        self.assertEqual(config.models['bar']['materialized'], 'blah')
        self.assertEqual(config.seeds['foo']['post-hook'], "{{ env_var('env_value_target') }}")
        self.assertEqual(config.seeds['bar']['materialized'], 'blah')

