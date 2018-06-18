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

        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertTablesEqual("statement_actual","statement_expected")

    @attr(type="snowflake")
    def test_snowflake_statements(self):
        self.use_profile("postgres")
        self.use_default_project({"data-paths": [self.dir("seed")]})

        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertTablesEqual("statement_actual","statement_expected")

    @attr(type="bigquery")
    def test_bigquery_statements(self):
        self.use_profile("postgres")
        self.use_default_project({"data-paths": [self.dir("seed")]})

        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertTablesEqual("statement_actual","statement_expected")

