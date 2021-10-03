from test.integration.base import DBTIntegrationTest,  use_profile
import os


class BaseColumnQuotingTest(DBTIntegrationTest):
    def column_quoting(self):
        raise NotImplementedError('column_quoting not implemented')

    @property
    def schema(self):
        return 'dbt_column_quoting_052'

    @staticmethod
    def dir(value):
        return os.path.normpath(value)

    def _run_columnn_quotes(self, strategy='delete+insert'):
        strategy_vars = '{{"strategy": "{}"}}'.format(strategy)
        self.run_dbt(['seed', '--vars', strategy_vars])
        self.run_dbt(['run', '--vars', strategy_vars])
        self.run_dbt(['run', '--vars', strategy_vars])


class TestColumnQuotingDefault(BaseColumnQuotingTest):
    @property
    def project_config(self):
        return {
            'config-version': 2
        }

    @property
    def models(self):
        return self.dir('models')

    def run_dbt(self, *args, **kwargs):
        return super().run_dbt(*args, **kwargs)

    @use_profile('postgres')
    def test_postgres_column_quotes(self):
        self._run_columnn_quotes()


class TestColumnQuotingDisabled(BaseColumnQuotingTest):
    @property
    def models(self):
        return self.dir('models-unquoted')

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seeds': {
                'quote_columns': False,
            },
        }

    @use_profile('postgres')
    def test_postgres_column_quotes(self):
        self._run_columnn_quotes()


class TestColumnQuotingEnabled(BaseColumnQuotingTest):
    @property
    def models(self):
        return self.dir('models')

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seeds': {
                'quote_columns': True,
            },
        }

    @use_profile('postgres')
    def test_postgres_column_quotes(self):
        self._run_columnn_quotes()
