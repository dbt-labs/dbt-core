from tests.integration.base import DBTIntegrationTest, use_profile


class TestCaseSensitiveModelBigQueryRun(DBTIntegrationTest):
    @property
    def schema(self):
        return "bigquery_test"

    @property
    def models(self):
        return "case-sensitive-models"

    @use_profile('bigquery')
    def test__bigquery_double_run_fails(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 1)
        self.run_dbt(expect_pass=False)


class TestCaseSensitiveSchemaBigQueryRun(TestCaseSensitiveModelBigQueryRun):
    # same test, but the schema is funky instead of the model name
    @property
    def schema(self):
        return "bigquery_test"

    def unique_schema(self):
        schema = self.schema

        to_return = "{}_{}".format(self.prefix, schema)
        return to_return

    @property
    def models(self):
        return "case-sensitive-schemas"
