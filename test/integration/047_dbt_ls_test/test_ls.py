from test.integration.base import DBTIntegrationTest, use_profile
import os
from dbt.logger import log_manager

import json


class TestStrictUndefined(DBTIntegrationTest):

    @property
    def schema(self):
        return 'dbt_ls_047'

    @staticmethod
    def dir(value):
        return os.path.normpath(value)

    @property
    def models(self):
        return self.dir('models')

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'analysis-paths': [self.dir('analyses')],
            'snapshot-paths': [self.dir('snapshots')],
            'macro-paths': [self.dir('macros')],
            'data-paths': [self.dir('data')],
            'test-paths': [self.dir('tests')],
            'seeds': {
                'quote_columns': False,
            },
        }

    def setUp(self):
        super().setUp()
        self.maxDiff = None

    def run_dbt_ls(self, args=None, expect_pass=True):
        log_manager.stdout_console()
        full_args = ['ls']
        if args is not None:
            full_args = full_args + args

        result = self.run_dbt(args=full_args, expect_pass=expect_pass,
                              strict=False, parser=False)

        log_manager.stdout_console()
        return result

    def assertEqualJSON(self, json_str, expected):
        self.assertEqual(json.loads(json_str), expected)

    def expect_given_output(self, args, expectations):
        for key, values in expectations.items():
            ls_result = self.run_dbt_ls(args + ['--output', key])
            if not isinstance(values, (list, tuple)):
                values = [values]
            self.assertEqual(len(ls_result), len(values))
            for got, expected in zip(ls_result, values):
                if key == 'json':
                    self.assertEqualJSON(got, expected)
                else:
                    self.assertEqual(got, expected)

    def expect_snapshot_output(self):
        expectations = {
            'name': 'my_snapshot',
            'selector': 'test.snapshot.my_snapshot',
            'json': {
                'name': 'my_snapshot',
                'package_name': 'test',
                'depends_on': {'nodes': [], 'macros': []},
                'tags': [],
                'config': {
                    'enabled': True,
                    'materialized': 'snapshot',
                    'post-hook': [],
                    'tags': [],
                    'pre-hook': [],
                    'quoting': {},
                    'vars': {},
                    'column_types': {},
                    'persist_docs': {},
                    'target_database': self.default_database,
                    'target_schema': self.unique_schema(),
                    'unique_key': 'id',
                    'strategy': 'timestamp',
                    'updated_at': 'updated_at',
                    'full_refresh': None,
                    'database': None,
                    'schema': None,
                    'alias': None,
                    'check_cols': None,
                },
                'alias': 'my_snapshot',
                'resource_type': 'snapshot',
            },
            'path': self.dir('snapshots/snapshot.sql'),
        }
        self.expect_given_output(['--resource-type', 'snapshot'], expectations)

    def expect_analyses_output(self):
        expectations = {
            'name': 'a',
            'selector': 'test.analysis.a',
            'json': {
                'name': 'a',
                'package_name': 'test',
                'depends_on': {'nodes': [], 'macros': []},
                'tags': [],
                'config': {
                    'enabled': True,
                    'materialized': 'view',
                    'post-hook': [],
                    'tags': [],
                    'pre-hook': [],
                    'quoting': {},
                    'vars': {},
                    'column_types': {},
                    'persist_docs': {},
                    'full_refresh': None,
                    'database': None,
                    'schema': None,
                    'alias': None,
                },
                'alias': 'a',
                'resource_type': 'analysis',
            },
            'path': self.dir('analyses/a.sql'),
        }
        self.expect_given_output(['--resource-type', 'analysis'], expectations)

    def expect_model_output(self):
        expectations = {
            'name': ('ephemeral', 'incremental', 'inner', 'outer'),
            'selector': ('test.ephemeral', 'test.incremental', 'test.sub.inner', 'test.outer'),
            'json': (
                {
                    'name': 'ephemeral',
                    'package_name': 'test',
                    'depends_on': {'nodes': [], 'macros': []},
                    'tags': [],
                    'config': {
                        'enabled': True,
                        'materialized': 'ephemeral',
                        'post-hook': [],
                        'tags': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {},
                        'column_types': {},
                        'persist_docs': {},
                        'full_refresh': None,
                        'database': None,
                        'schema': None,
                        'alias': None,
                    },
                    'alias': 'ephemeral',
                    'resource_type': 'model',
                },
                {
                    'name': 'incremental',
                    'package_name': 'test',
                    'depends_on': {'nodes': ['seed.test.seed'], 'macros': ['macro.dbt.is_incremental']},
                    'tags': [],
                    'config': {
                        'enabled': True,
                        'materialized': 'incremental',
                        'post-hook': [],
                        'tags': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {},
                        'column_types': {},
                        'persist_docs': {},
                        'full_refresh': None,
                        'incremental_strategy': 'delete+insert',
                        'database': None,
                        'schema': None,
                        'alias': None,
                    },
                    'alias': 'incremental',
                    'resource_type': 'model',
                },
                {
                    'name': 'inner',
                    'package_name': 'test',
                    'depends_on': {'nodes': ['model.test.outer'], 'macros': []},
                    'tags': [],
                    'config': {
                        'enabled': True,
                        'materialized': 'view',
                        'post-hook': [],
                        'tags': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {},
                        'column_types': {},
                        'persist_docs': {},
                        'full_refresh': None,
                        'database': None,
                        'schema': None,
                        'alias': None,
                    },
                    'alias': 'inner',
                    'resource_type': 'model',
                },
                {
                    'name': 'outer',
                    'package_name': 'test',
                    'depends_on': {'nodes': ['model.test.ephemeral'], 'macros': []},
                    'tags': [],
                    'config': {
                        'enabled': True,
                        'materialized': 'view',
                        'post-hook': [],
                        'tags': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {},
                        'column_types': {},
                        'persist_docs': {},
                        'full_refresh': None,
                        'database': None,
                        'schema': None,
                        'alias': None,
                    },
                    'alias': 'outer',
                    'resource_type': 'model',
                },
            ),
            'path': (self.dir('models/ephemeral.sql'), self.dir('models/incremental.sql'), self.dir('models/sub/inner.sql'), self.dir('models/outer.sql')),
        }
        self.expect_given_output(['--resource-type', 'model'], expectations)

    # Do not include ephemeral model - it was not selected
    def expect_model_ephemeral_output(self):
        expectations = {
            'name': ('outer'),
            'selector': ('test.outer'),
            'json': (
                {
                    'name': 'outer',
                    'package_name': 'test',
                    'depends_on': {'nodes': [], 'macros': []},
                    'tags': [],
                    'config': {
                        'enabled': True,
                        'materialized': 'view',
                        'post-hook': [],
                        'tags': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {},
                        'column_types': {},
                        'persist_docs': {},
                        'full_refresh': None,
                        'database': None,
                        'schema': None,
                        'alias': None,
                    },
                    'alias': 'outer',
                    'resource_type': 'model',
                },
            ),
            'path': (self.dir('models/outer.sql'), ),
        }
        self.expect_given_output(['--model', 'outer'], expectations)

    def expect_source_output(self):
        expectations = {
            'name': 'my_source.my_table',
            'selector': 'source:test.my_source.my_table',
            'json': {
                'config': {
                    'enabled': True,
                },
                'package_name': 'test',
                'name': 'my_table',
                'source_name': 'my_source',
                'resource_type': 'source',
                'tags': [],
            },
            'path': self.dir('models/schema.yml'),
        }
        # should we do this --select automatically for a user if if 'source' is
        # in the resource types and there is no '--select' or '--exclude'?
        self.expect_given_output(['--resource-type', 'source', '--select', 'source:*'], expectations)

    def expect_seed_output(self):
        expectations = {
            'name': 'seed',
            'selector': 'test.seed',
            'json': {
                'name': 'seed',
                'package_name': 'test',
                'depends_on': {'nodes': [], 'macros': []},
                'tags': [],
                'config': {
                    'enabled': True,
                    'materialized': 'seed',
                    'post-hook': [],
                    'tags': [],
                    'pre-hook': [],
                    'quoting': {},
                    'vars': {},
                    'column_types': {},
                    'persist_docs': {},
                    'quote_columns': False,
                    'full_refresh': None,
                    'database': None,
                    'schema': None,
                    'alias': None,
                },
                'alias': 'seed',
                'resource_type': 'seed',
            },
            'path': self.dir('data/seed.csv'),
        }
        self.expect_given_output(['--resource-type', 'seed'], expectations)

    def expect_test_output(self):
        expectations = {
            'name': ('not_null_outer_id', 't', 'unique_outer_id'),
            'selector': ('test.schema_test.not_null_outer_id', 'test.data_test.t', 'test.schema_test.unique_outer_id'),
            'json': (
                {
                    'name': 'not_null_outer_id',
                    'package_name': 'test',
                    'depends_on': {'nodes': ['model.test.outer'], 'macros': ['macro.dbt.test_not_null']},
                    'tags': ['schema'],
                    'config': {
                        'enabled': True,
                        'materialized': 'test',
                        'post-hook': [],
                        'severity': 'ERROR',
                        'tags': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {},
                        'column_types': {},
                        'persist_docs': {},
                        'full_refresh': None,
                        'database': None,
                        'schema': None,
                        'alias': None,
                    },
                    'alias': 'not_null_outer_id',
                    'resource_type': 'test',
                },
                {
                    'name': 't',
                    'package_name': 'test',
                    'depends_on': {'nodes': [], 'macros': []},
                    'tags': ['data'],
                    'config': {
                        'enabled': True,
                        'materialized': 'test',
                        'post-hook': [],
                        'severity': 'ERROR',
                        'tags': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {},
                        'column_types': {},
                        'persist_docs': {},
                        'full_refresh': None,
                        'database': None,
                        'schema': None,
                        'alias': None,
                    },
                    'alias': 't',
                    'resource_type': 'test',
                },
                {
                    'name': 'unique_outer_id',
                    'package_name': 'test',
                    'depends_on': {'nodes': ['model.test.outer'], 'macros': ['macro.dbt.test_unique']},
                    'tags': ['schema'],
                    'config': {
                        'enabled': True,
                        'materialized': 'test',
                        'post-hook': [],
                        'severity': 'ERROR',
                        'tags': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {},
                        'column_types': {},
                        'persist_docs': {},
                        'full_refresh': None,
                        'database': None,
                        'schema': None,
                        'alias': None,
                    },
                    'alias': 'unique_outer_id',
                    'resource_type': 'test',
                },
            ),
            'path': (self.dir('models/schema.yml'), self.dir('tests/t.sql'), self.dir('models/schema.yml')),
        }
        self.expect_given_output(['--resource-type', 'test'], expectations)

    def expect_all_output(self):
        # tests have their type inserted into their fqn, after the package
        # but models don't! they just have (package.name)
        # sources are like models - (package.source_name.table_name)
        expected_default = {
            'test.ephemeral',
            'test.incremental',
            'test.snapshot.my_snapshot',
            'test.sub.inner',
            'test.outer',
            'test.seed',
            'source:test.my_source.my_table',
            'test.schema_test.not_null_outer_id',
            'test.schema_test.unique_outer_id',
            'test.data_test.t',
        }
        # analyses have their type inserted into their fqn like tests
        expected_all = expected_default | {'test.analysis.a'}

        results = self.run_dbt_ls(['--resource-type', 'all', '--select', '*', 'source:*'])
        self.assertEqual(set(results), expected_all)

        results = self.run_dbt_ls(['--select', '*', 'source:*'])
        self.assertEqual(set(results), expected_default)

        results = self.run_dbt_ls(['--resource-type', 'default', '--select', '*', 'source:*'])
        self.assertEqual(set(results), expected_default)

        results = self.run_dbt_ls

    def expect_select(self):
        results = self.run_dbt_ls(['--resource-type', 'test', '--select', 'outer'])
        self.assertEqual(set(results), {'test.schema_test.not_null_outer_id', 'test.schema_test.unique_outer_id'})

        self.run_dbt_ls(['--resource-type', 'test', '--select', 'inner'], expect_pass=True)

        results = self.run_dbt_ls(['--resource-type', 'test', '--select', '+inner'])
        self.assertEqual(set(results), {'test.schema_test.not_null_outer_id', 'test.schema_test.unique_outer_id'})

        results = self.run_dbt_ls(['--resource-type', 'model', '--select', 'outer+'])
        self.assertEqual(set(results), {'test.outer', 'test.sub.inner'})

        results = self.run_dbt_ls(['--resource-type', 'model', '--exclude', 'inner'])
        self.assertEqual(set(results), {'test.ephemeral', 'test.outer', 'test.incremental'})

        results = self.run_dbt_ls(['--select', 'config.incremental_strategy:delete+insert'])
        self.assertEqual(set(results), {'test.incremental'})

        self.run_dbt_ls(['--select', 'config.incremental_strategy:insert_overwrite'], expect_pass=True)


    @use_profile('postgres')
    def test_postgres_ls(self):
        self.expect_snapshot_output()
        self.expect_analyses_output()
        self.expect_model_output()
        self.expect_source_output()
        self.expect_seed_output()
        self.expect_test_output()
        self.expect_select()
        self.expect_all_output()
