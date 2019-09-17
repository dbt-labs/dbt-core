from test.integration.base import DBTIntegrationTest, use_profile


class TestInvalidDisabledModels(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "invalid_models_011"

    @property
    def models(self):
        return "models-2"

    @use_profile('postgres')
    def test_postgres_view_with_incremental_attributes(self):
        with self.assertRaises(RuntimeError) as exc:
            self.run_dbt()

        self.assertIn('enabled', str(exc.exception))


class TestInvalidModelReference(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "invalid_models_011"

    @property
    def models(self):
        return "models-3"

    @use_profile('postgres')
    def test_postgres_view_with_incremental_attributes(self):
        with self.assertRaises(RuntimeError) as exc:
            self.run_dbt()

        self.assertIn('which was not found', str(exc.exception))
