from test.integration.base import DBTIntegrationTest, use_profile
import yaml


class TestBaseCaching(DBTIntegrationTest):
    @property
    def schema(self):
        return "caching_038"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'test-paths': ['tests']
        }

    @use_profile('postgres')
    def test_postgres_adapter_methods(self):
        self.run_dbt(['compile'])  # trigger any compile-time issues
        self.run_dbt()
        self.assertTablesEqual('model', 'expected')

    @use_profile('redshift')
    def test_redshift_adapter_methods(self):
        self.run_dbt(['compile'])  # trigger any compile-time issues
        self.run_dbt()
        self.assertTablesEqual('model', 'expected')

    @use_profile('snowflake')
    def test_snowflake_adapter_methods(self):
        self.run_dbt(['compile'])  # trigger any compile-time issues
        self.run_dbt()
        self.assertTablesEqual('MODEL', 'EXPECTED')

    @use_profile('bigquery')
    def test_bigquery_adapter_methods(self):
        self.run_dbt(['compile'])  # trigger any compile-time issues
        self.run_dbt()
        self.assertTablesEqual('model', 'expected')


class TestRenameRelation(DBTIntegrationTest):
    @property
    def schema(self):
        return "rename_relation_054"

    @property
    def models(self):
        return 'bigquery-models'

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'source-paths': ['models']
        }

    @use_profile('bigquery')
    def test_bigquery_adapter_methods(self):
        self.run_dbt(['compile'])  # trigger any compile-time issues
        self.run_sql_file("seed_bq.sql")
        self.run_dbt(['seed'])
        rename_relation_args = yaml.safe_dump({
            'from_name': 'seed',
            'to_name': 'renamed_seed',
        })
        self.run_dbt(['run-operation', 'rename_named_relation', '--args', rename_relation_args])
        self.run_dbt()
