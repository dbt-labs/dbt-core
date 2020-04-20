from test.integration.base import DBTIntegrationTest, use_profile


class TestStatements(DBTIntegrationTest):

    @property
    def schema(self):
        return "statements_030"

    @staticmethod
    def dir(path):
        return path.lstrip("/")

    @property
    def models(self):
        return self.dir("models")

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seeds': {
                'quote_columns': False,
            }
        }

    @use_profile("postgres")
    def test_postgres_statements(self):
        self.use_default_project({"data-paths": [self.dir("seed")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 2)
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("statement_actual", "statement_expected")

    @use_profile("snowflake")
    def test_snowflake_statements(self):
        self.use_default_project({"data-paths": [self.dir("snowflake-seed")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 2)
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        self.assertManyTablesEqual(["STATEMENT_ACTUAL", "STATEMENT_EXPECTED"])

    @use_profile("presto")
    def test_presto_statements(self):
        self.use_default_project({"data-paths": [self.dir("seed")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 2)
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("statement_actual", "statement_expected")


class TestStatementsBigquery(DBTIntegrationTest):

    @property
    def schema(self):
        return "statements_030"

    @staticmethod
    def dir(path):
        return path.lstrip("/")

    @property
    def models(self):
        return self.dir("models-bq")

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seeds': {
                'quote_columns': False,
            }
        }

    @use_profile("bigquery")
    def test_bigquery_statements(self):
        self.use_default_project({"data-paths": [self.dir("seed")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 2)
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("statement_actual", "statement_expected")
