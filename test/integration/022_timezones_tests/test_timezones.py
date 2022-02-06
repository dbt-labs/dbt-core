from freezegun import freeze_time
from test.integration.base import DBTIntegrationTest, use_profile


class TestTimezones(DBTIntegrationTest):

    @property
    def schema(self):
        return "timezones_022"

    @property
    def models(self):
        return "models"

    @property
    def profile_config(self):
        return {
            'test': {
                'outputs': {
                    'dev': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': self.database_host,
                        'port': 5432,
                        'user': "root",
                        'pass': "password",
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    },
                },
                'target': 'dev'
            }
        }

    @property
    def query(self):
        return """
            select
              run_started_at_est,
              run_started_at_utc
            from {schema}.timezones
        """.format(schema=self.unique_schema())

    @freeze_time("2017-01-01 03:00:00", tz_offset=0)
    @use_profile('postgres')
    def test_postgres_run_started_at(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 1)
        result = self.run_sql(self.query, fetch='all')[0]
        est, utc = result
        self.assertEqual(utc, '2017-01-01 03:00:00+00:00')
        self.assertEqual(est, '2016-12-31 22:00:00-05:00')
