import os

from test.integration.base import DBTIntegrationTest, use_profile


class TestLateBindingView(DBTIntegrationTest):
    @property
    def schema(self):
        return 'late_binding_view_034'

    @staticmethod
    def dir(path):
        return os.path.normpath(path)

    @property
    def models(self):
        return self.dir("models")

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': [self.dir('seed')],
            'seeds': {
                'quote_columns': False,
            }
        }

    @use_profile('redshift')
    def test__redshift_late_binding_view_query(self):
        self.assertEqual(len(self.run_dbt(["seed"])), 1)
        self.assertEqual(len(self.run_dbt()), 1)
        # remove the table. Use 'cascade' here so that if late-binding views
        # didn't work as advertised, the following dbt run will fail.
        drop = 'drop table if exists {}.seed cascade'.format(
            self.unique_schema()
        )
        self.run_sql(drop)
        self.assertEqual(len(self.run_dbt()), 1)
