import json
import os
from datetime import datetime, timedelta

import yaml

from dbt.exceptions import CompilationException
import dbt.tracking
import dbt.version
from test.integration.base import DBTIntegrationTest, use_profile, AnyFloat, \
    AnyStringWith


class BaseSourcesTest(DBTIntegrationTest):
    @property
    def schema(self):
        return "sources_042"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            'quoting': {'database': True, 'schema': True, 'identifier': True},
            'seeds': {
                'quote_columns': True,
            },
        }

    def setUp(self):
        super().setUp()
        os.environ['DBT_TEST_SCHEMA_NAME_VARIABLE'] = 'test_run_schema'

    def tearDown(self):
        del os.environ['DBT_TEST_SCHEMA_NAME_VARIABLE']
        super().tearDown()

    def run_dbt_with_vars(self, cmd, *args, **kwargs):
        vars_dict = {
            'test_run_schema': self.unique_schema(),
            'test_loaded_at': self.adapter.quote('updated_at'),
        }
        cmd.extend(['--vars', yaml.safe_dump(vars_dict)])
        return self.run_dbt(cmd, *args, **kwargs)


class SuccessfulSourcesTest(BaseSourcesTest):
    def setUp(self):
        super().setUp()
        self.run_dbt_with_vars(['seed'])
        self.maxDiff = None
        self._id = 101
        # this is the db initial value
        self.last_inserted_time = "2016-09-19T14:45:51+00:00"
        os.environ['DBT_ENV_CUSTOM_ENV_key'] = 'value'

    def tearDown(self):
        super().tearDown()
        del os.environ['DBT_ENV_CUSTOM_ENV_key']

    def _set_updated_at_to(self, delta):
        insert_time = datetime.utcnow() + delta
        timestr = insert_time.strftime("%Y-%m-%d %H:%M:%S")
        # favorite_color,id,first_name,email,ip_address,updated_at
        insert_id = self._id
        self._id += 1
        raw_sql = """INSERT INTO {schema}.{source}
            ({quoted_columns})
        VALUES (
            'blue',{id},'Jake','abc@example.com','192.168.1.1','{time}'
        )"""
        quoted_columns = ','.join(
            self.adapter.quote(c) for c in
            ('favorite_color', 'id', 'first_name',
             'email', 'ip_address', 'updated_at')
        )
        self.run_sql(
            raw_sql,
            kwargs={
                'schema': self.unique_schema(),
                'time': timestr,
                'id': insert_id,
                'source': self.adapter.quote('source'),
                'quoted_columns': quoted_columns,
            }
        )
        self.last_inserted_time = insert_time.strftime(
            "%Y-%m-%dT%H:%M:%S+00:00")


class TestSources(SuccessfulSourcesTest):
    @property
    def project_config(self):
        cfg = super().project_config
        cfg.update({
            'macro-paths': ['macros'],
        })
        return cfg

    def _create_schemas(self):
        super()._create_schemas()
        self._create_schema_named(self.default_database,
                                  self.alternative_schema())

    def alternative_schema(self):
        return self.unique_schema() + '_other'

    def setUp(self):
        super().setUp()
        self.run_sql(
            'create table {}.dummy_table (id int)'.format(self.unique_schema())
        )
        self.run_sql(
            'create view {}.external_view as (select * from {}.dummy_table)'
            .format(self.alternative_schema(), self.unique_schema())
        )

    def run_dbt_with_vars(self, cmd, *args, **kwargs):
        vars_dict = {
            'test_run_schema': self.unique_schema(),
            'test_run_alt_schema': self.alternative_schema(),
            'test_loaded_at': self.adapter.quote('updated_at'),
        }
        cmd.extend(['--vars', yaml.safe_dump(vars_dict)])
        return self.run_dbt(cmd, *args, **kwargs)

    @use_profile('postgres')
    def test_postgres_basic_source_def(self):
        results = self.run_dbt_with_vars(['run'])
        self.assertEqual(len(results), 4)
        self.assertManyTablesEqual(
            ['source', 'descendant_model', 'nonsource_descendant'],
            ['expected_multi_source', 'multi_source_model'])
        results = self.run_dbt_with_vars(['test'])
        self.assertEqual(len(results), 6)

    @use_profile('postgres')
    def test_postgres_source_selector(self):
        # only one of our models explicitly depends upon a source
        results = self.run_dbt_with_vars([
            'run',
            '--models',
            'source:test_source.test_table+'
        ])
        self.assertEqual(len(results), 1)
        self.assertTablesEqual('source', 'descendant_model')
        self.assertTableDoesNotExist('nonsource_descendant')
        self.assertTableDoesNotExist('multi_source_model')

        # do the same thing, but with tags
        results = self.run_dbt_with_vars([
            'run',
            '--models',
            'tag:my_test_source_table_tag+'
        ])
        self.assertEqual(len(results), 1)

        results = self.run_dbt_with_vars([
            'test',
            '--models',
            'source:test_source.test_table+'
        ])
        self.assertEqual(len(results), 4)

        results = self.run_dbt_with_vars([
            'test', '--models', 'tag:my_test_source_table_tag+'
        ])
        self.assertEqual(len(results), 4)

        results = self.run_dbt_with_vars([
            'test', '--models', 'tag:my_test_source_tag+'
        ])
        # test_table + other_test_table
        self.assertEqual(len(results), 6)

        results = self.run_dbt_with_vars([
            'test', '--models', 'tag:id_column'
        ])
        # all 4 id column tests
        self.assertEqual(len(results), 4)

    @use_profile('postgres')
    def test_postgres_empty_source_def(self):
        # sources themselves can never be selected, so nothing should be run
        results = self.run_dbt_with_vars([
            'run',
            '--models',
            'source:test_source.test_table'
        ])
        self.assertTableDoesNotExist('nonsource_descendant')
        self.assertTableDoesNotExist('multi_source_model')
        self.assertTableDoesNotExist('descendant_model')
        self.assertEqual(len(results), 0)

    @use_profile('postgres')
    def test_postgres_source_only_def(self):
        results = self.run_dbt_with_vars([
            'run', '--models', 'source:other_source+'
        ])
        self.assertEqual(len(results), 1)
        self.assertTablesEqual('expected_multi_source', 'multi_source_model')
        self.assertTableDoesNotExist('nonsource_descendant')
        self.assertTableDoesNotExist('descendant_model')

        results = self.run_dbt_with_vars([
            'run', '--models', 'source:test_source+'
        ])
        self.assertEqual(len(results), 2)
        self.assertManyTablesEqual(
            ['source', 'descendant_model'],
            ['expected_multi_source', 'multi_source_model'])
        self.assertTableDoesNotExist('nonsource_descendant')

    @use_profile('postgres')
    def test_postgres_source_childrens_parents(self):
        results = self.run_dbt_with_vars([
            'run', '--models', '@source:test_source'
        ])
        self.assertEqual(len(results), 2)
        self.assertManyTablesEqual(
            ['source', 'descendant_model'],
            ['expected_multi_source', 'multi_source_model'],
        )
        self.assertTableDoesNotExist('nonsource_descendant')

    @use_profile('postgres')
    def test_postgres_run_operation_source(self):
        kwargs = '{"source_name": "test_source", "table_name": "test_table"}'
        self.run_dbt_with_vars([
            'run-operation', 'vacuum_source', '--args', kwargs
        ])


class TestSourceFreshness(SuccessfulSourcesTest):

    def _assert_freshness_results(self, path, state):
        self.assertTrue(os.path.exists(path))
        with open(path) as fp:
            data = json.load(fp)

        assert set(data) == {'metadata', 'results', 'elapsed_time'}
        assert 'generated_at' in data['metadata']
        assert isinstance(data['elapsed_time'], float)
        self.assertBetween(data['metadata']['generated_at'],
                           self.freshness_start_time)
        assert data['metadata']['dbt_schema_version'] == 'https://schemas.getdbt.com/dbt/sources/v2.json'
        assert data['metadata']['dbt_version'] == dbt.version.__version__
        assert data['metadata']['invocation_id'] == dbt.tracking.active_user.invocation_id
        key = 'key'
        if os.name == 'nt':
            key = key.upper()
        assert data['metadata']['env'] == {key: 'value'}

        last_inserted_time = self.last_inserted_time

        self.assertEqual(len(data['results']), 1)

        self.assertEqual(data['results'], [
            {
                'unique_id': 'source.test.test_source.test_table',
                'max_loaded_at': last_inserted_time,
                'snapshotted_at': AnyStringWith(),
                'max_loaded_at_time_ago_in_s': AnyFloat(),
                'status': state,
                'criteria': {
                    'filter': None,
                    'warn_after': {'count': 10, 'period': 'hour'},
                    'error_after': {'count': 18, 'period': 'hour'},
                },
                'adapter_response': {},
                'thread_id': AnyStringWith('Thread-'),
                'execution_time': AnyFloat(),
                'timing': [
                    {
                        'name': 'compile',
                        'started_at': AnyStringWith(),
                        'completed_at': AnyStringWith(),
                    },
                    {
                        'name': 'execute',
                        'started_at': AnyStringWith(),
                        'completed_at': AnyStringWith(),
                    }
                ]
            }
        ])

    def _run_source_freshness(self):
        # test_source.test_table should have a loaded_at field of `updated_at`
        # and a freshness of warn_after: 10 hours, error_after: 18 hours
        # by default, our data set is way out of date!
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/error_source.json'],
            expect_pass=False
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'error')
        self._assert_freshness_results('target/error_source.json', 'error')

        self._set_updated_at_to(timedelta(hours=-12))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/warn_source.json'],
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'warn')
        self._assert_freshness_results('target/warn_source.json', 'warn')

        self._set_updated_at_to(timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/pass_source.json'],
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'pass')
        self._assert_freshness_results('target/pass_source.json', 'pass')

    @use_profile('postgres')
    def test_postgres_source_freshness(self):
        self._run_source_freshness()

    @use_profile('postgres')
    def test_postgres_source_snapshot_freshness(self):
        """Ensures that the deprecated command `source snapshot-freshness`
        aliases to `source freshness` command.
        """
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'snapshot-freshness', '-o', 'target/error_source.json'],
            expect_pass=False
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'error')
        self._assert_freshness_results('target/error_source.json', 'error')

        self._set_updated_at_to(timedelta(hours=-12))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'snapshot-freshness', '-o', 'target/warn_source.json'],
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'warn')
        self._assert_freshness_results('target/warn_source.json', 'warn')

        self._set_updated_at_to(timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'snapshot-freshness', '-o', 'target/pass_source.json'],
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'pass')
        self._assert_freshness_results('target/pass_source.json', 'pass')

    @use_profile('postgres')
    def test_postgres_source_freshness_selection_select(self):
        """Tests node selection using the --select argument."""
        self._set_updated_at_to(timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        # select source directly
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '--select',
                'source:test_source.test_table', '-o', 'target/pass_source.json'],
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'pass')
        self._assert_freshness_results('target/pass_source.json', 'pass')

    @use_profile('postgres')
    def test_postgres_source_freshness_selection_exclude(self):
        """Tests node selection using the --select argument. It 'excludes' the
        only source in the project so it should return no results."""
        self._set_updated_at_to(timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        # exclude source directly
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '--exclude',
                'source:test_source.test_table', '-o', 'target/exclude_source.json'],
        )
        self.assertEqual(len(results), 0)

    @use_profile('postgres')
    def test_postgres_source_freshness_selection_graph_operation(self):
        """Tests node selection using the --select argument with graph
        operations. `+descendant_model` == select all nodes `descendant_model`
        depends on.
        """
        self._set_updated_at_to(timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        # select model ancestors
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '--select',
                '+descendant_model', '-o', 'target/ancestor_source.json']
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'pass')
        self._assert_freshness_results('target/ancestor_source.json', 'pass')


class TestSourceFreshnessErrors(SuccessfulSourcesTest):
    @property
    def models(self):
        return "error_models"

    @use_profile('postgres')
    def test_postgres_error(self):
        results = self.run_dbt_with_vars(
            ['source', 'freshness'],
            expect_pass=False
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'runtime error')


class TestSourceFreshnessFilter(SuccessfulSourcesTest):
    @property
    def models(self):
        return 'filtered_models'

    @use_profile('postgres')
    def test_postgres_all_records(self):
        # all records are filtered out
        self.run_dbt_with_vars(
            ['source', 'freshness'], expect_pass=False)
        # we should insert a record with #101 that's fresh, but will still fail
        # because the filter excludes it
        self._set_updated_at_to(timedelta(hours=-2))
        self.run_dbt_with_vars(
            ['source', 'freshness'], expect_pass=False)

        # we should now insert a record with #102 that's fresh, and the filter
        # includes it
        self._set_updated_at_to(timedelta(hours=-2))
        results = self.run_dbt_with_vars(
            ['source', 'freshness'], expect_pass=True)


class TestMalformedSources(BaseSourcesTest):
    # even seeds should fail, because parsing is what's raising
    @property
    def models(self):
        return "malformed_models"

    @use_profile('postgres')
    def test_postgres_malformed_schema_will_break_run(self):
        with self.assertRaises(CompilationException):
            self.run_dbt_with_vars(['seed'])


class TestRenderingInSourceTests(BaseSourcesTest):
    @property
    def models(self):
        return "malformed_schema_tests"

    @use_profile('postgres')
    def test_postgres_render_in_source_tests(self):
        self.run_dbt_with_vars(['seed'])
        self.run_dbt_with_vars(['run'])
        # syntax error at or near "{", because the test isn't rendered
        self.run_dbt_with_vars(['test'], expect_pass=False)


class TestUnquotedSources(SuccessfulSourcesTest):
    @property
    def project_config(self):
        cfg = super().project_config
        cfg['quoting'] = {
            'identifier': False,
            'schema': False,
            'database': False,
        }
        return cfg

    @use_profile('postgres')
    def test_postgres_catalog(self):
        self.run_dbt_with_vars(['run'])
        self.run_dbt_with_vars(['docs', 'generate'])
