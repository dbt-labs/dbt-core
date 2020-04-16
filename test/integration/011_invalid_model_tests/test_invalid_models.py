from test.integration.base import DBTIntegrationTest, use_profile
import os


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


class TestInvalidMacroCall(DBTIntegrationTest):
    @property
    def schema(self):
        return "invalid_models_011"

    @property
    def models(self):
        return "models-4"

    @staticmethod
    def dir(path):
        return path.lstrip("/")

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': [self.dir('bad-macros')],
        }

    @use_profile('postgres')
    def test_postgres_call_invalid(self):
        with self.assertRaises(Exception) as exc:
            self.run_dbt(['compile'])


        macro_path = os.path.join('bad-macros', 'macros.sql')
        model_path = os.path.join('models-4', 'bad_macro.sql')

        self.assertIn(f'> in macro some_macro ({macro_path})', str(exc.exception))
        self.assertIn(f'> called by model bad_macro ({model_path})', str(exc.exception))
