from test.integration.base import DBTIntegrationTest, use_profile


class TestColumnTypes(DBTIntegrationTest):
    @property
    def schema(self):
        return '056_column_types'

    def run_and_test(self):
        self.assertEqual(len(self.run_dbt(['run'])), 1)
        self.assertEqual(len(self.run_dbt(['test'])), 1)


class TestPostgresColumnTypes(TestColumnTypes):
    @property
    def models(self):
        return 'pg_models'

    @use_profile('postgres')
    def test_postgres_column_types(self):
        self.run_and_test()


class TestRedshiftColumnTypes(TestColumnTypes):
    @property
    def models(self):
        return 'rs_models'

    @use_profile('redshift')
    def test_redshift_column_types(self):
        self.run_and_test()


class TestSnowflakeColumnTypes(TestColumnTypes):
    @property
    def models(self):
        return 'sf_models'

    @use_profile('snowflake')
    def test_snowflake_column_types(self):
        self.run_and_test()


class TestBigQueryColumnTypes(TestColumnTypes):
    @property
    def models(self):
        return 'bq_models'

    @use_profile('bigquery')
    def test_bigquery_column_types(self):
        self.run_and_test()
