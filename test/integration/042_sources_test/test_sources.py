from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, use_profile
from datetime import datetime, timedelta
import os

class BaseSourcesTest(DBTIntegrationTest):
    @property
    def schema(self):
        return "sources_042"

    @property
    def models(self):
        return "test/integration/042_sources_test/models"

    @property
    def project_config(self):
        return {
            'data-paths': ['test/integration/042_sources_test/data'],
        }

    def setUp(self):
        super(BaseSourcesTest, self).setUp()
        os.environ['DBT_TEST_SCHEMA_NAME_VARIABLE'] = 'test_run_schema'
        self.run_dbt_with_vars(['seed'])

    def tearDown(self):
        del os.environ['DBT_TEST_SCHEMA_NAME_VARIABLE']
        super(BaseSourcesTest, self).tearDown()

    def run_dbt_with_vars(self, cmd, *args, **kwargs):
        cmd.extend(['--vars',
                    '{{test_run_schema: {}}}'.format(self.unique_schema())])
        return self.run_dbt(cmd, *args, **kwargs)


class TestSources(BaseSourcesTest):
    @use_profile('postgres')
    def test_postgres_basic_source_def(self):
        results = self.run_dbt_with_vars(['run'])
        self.assertEqual(len(results), 3)
        self.assertManyTablesEqual(
            ['source', 'descendant_model', 'nonsource_descendant'],
            ['expected_multi_source', 'multi_source_model'])
        results = self.run_dbt_with_vars(['test'])
        self.assertEqual(len(results), 4)

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
        results = self.run_dbt_with_vars([
            'test',
            '--models',
            'source:test_source.test_table+'
        ])
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


class TestSourceFreshness(BaseSourcesTest):
    def setUp(self):
        super(TestSourceFreshness, self).setUp()
        self._id = 100

    # test_source.test_table should have a loaded_at field of `updated_at`
    # and a freshness of warn_after: 10 hours, error_after: 18 hours
    # by default, our data set is way out of date!
    def _set_updated_at_to(self, delta):
        timestr = (datetime.utcnow() + delta).strftime("%Y-%m-%d %H:%M:%S")
        #favorite_color,id,first_name,email,ip_address,updated_at
        insert_id = self._id
        self._id += 1
        raw_sql = """INSERT INTO {schema}.source
            (favorite_color,id,first_name,email,ip_address,updated_at)
        VALUES (
            'blue',{id},'Jake','abc@example.com','192.168.1.1','{time}'
        )"""
        self.run_sql(
            raw_sql,
            kwargs={
                'schema': self.unique_schema(),
                'time': timestr,
                'id': insert_id
            }
        )

    def _run_source_freshness(self):
        results = self.run_dbt_with_vars(
            ['--single-threaded', 'source', 'snapshot-freshness'],
            expect_pass=False
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'error')
        self.assertTrue(results[0].failed)

        self._set_updated_at_to(timedelta(hours=-12))
        results = self.run_dbt_with_vars(
            ['source', 'snapshot-freshness'],
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'warn')
        self.assertFalse(results[0].failed)

        self._set_updated_at_to(timedelta(hours=-2))
        results = self.run_dbt_with_vars(
            ['source', 'snapshot-freshness'],
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'pass')
        self.assertFalse(results[0].failed)

    @use_profile('postgres')
    def test_postgres_source_freshness(self):
        self._run_source_freshness()

    @use_profile('snowflake')
    def test_snowflake_source_freshness(self):
        self._run_source_freshness()

    @use_profile('redshift')
    def test_redshift_source_freshness(self):
        self._run_source_freshness()

    @use_profile('bigquery')
    def test_bigquery_source_freshness(self):
        self._run_source_freshness()
