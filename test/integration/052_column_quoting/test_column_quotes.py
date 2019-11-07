from test.integration.base import DBTIntegrationTest,  use_profile
import os


class TestColumnQuoting(DBTIntegrationTest):
    @property
    def schema(self):
        return 'dbt_column_quoting_052'

    @staticmethod
    def dir(value):
        return os.path.normpath(value)

    @property
    def models(self):
        return self.dir('models')

    def _run_columnn_quotes(self, strategy='delete+insert'):
        strategy_vars = '{{"strategy": "{}"}}'.format(strategy)
        self.run_dbt(['seed', '--vars', strategy_vars])
        self.run_dbt(['run', '--vars', strategy_vars])
        self.run_dbt(['run', '--vars', strategy_vars])

    @use_profile('postgres')
    def test_postgres_column_quotes(self):
        self._run_columnn_quotes()

    @use_profile('redshift')
    def test_redshift_column_quotes(self):
        self._run_columnn_quotes()

    @use_profile('snowflake')
    def test_snowflake_column_quotes(self):
        self._run_columnn_quotes()

    @use_profile('bigquery')
    def test_bigquery_column_quotes(self):
        self._run_columnn_quotes()

    @use_profile('snowflake')
    def test_snowflake_column_quotes_merged(self):
        self._run_columnn_quotes(strategy='merge')

    @use_profile('bigquery')
    def test_bigquery_column_quotes_merged(self):
        self._run_columnn_quotes(strategy='merge')
