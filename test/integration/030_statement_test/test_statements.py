from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest


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

    @attr(type="postgres")
    def test_postgres_statements(self):
        self.use_profile("postgres")
        self.use_default_project({"data-paths": [self.dir("seed")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 2)
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("statement_actual","statement_expected")

    @attr(type="snowflake")
    def test_snowflake_statements(self):
        self.use_profile("postgres")
        self.use_default_project({"data-paths": [self.dir("seed")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 2)
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        self.assertManyTablesEqual(["STATEMENT_ACTUAL", "STATEMENT_EXPECTED"])

    @attr(type="bigquery")
    def test_bigquery_statements(self):
        self.use_profile("postgres")
        self.use_default_project({"data-paths": [self.dir("seed")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 2)
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("statement_actual","statement_expected")

