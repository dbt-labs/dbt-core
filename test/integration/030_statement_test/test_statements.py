from test.integration.base import DBTIntegrationTest, use_profile


class TestStatements(DBTIntegrationTest):

    @property
    def schema(self):
        return "statements_030"

    @staticmethod
    def dir(path):
        return "test/integration/030_statement_test/" + path.lstrip("/")

    @property
    def models(self):
        return self.dir("models")

    @use_profile("postgres")
    def test_postgres_statements(self):
        self.use_default_project({"data-paths": [self.dir("seed")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 2)
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("statement_actual","statement_expected")

    @use_profile("snowflake")
    def test_snowflake_statements(self):
        self.use_default_project({"data-paths": [self.dir("seed")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 2)
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        self.assertManyTablesEqual(["STATEMENT_ACTUAL", "STATEMENT_EXPECTED"])


class TestStatementsBigquery(DBTIntegrationTest):

    @property
    def schema(self):
        return "statements_030"

    @staticmethod
    def dir(path):
        return "test/integration/030_statement_test/" + path.lstrip("/")

    @property
    def models(self):
        return self.dir("models-bq")

    @use_profile("bigquery")
    def test_bigquery_statements(self):
        self.use_default_project({"data-paths": [self.dir("seed")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 2)
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("statement_actual","statement_expected")

