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
        return "models-invalid-disabled"

    @use_profile('postgres')
    def test_postgres_view_with_incremental_attributes(self):
        with self.assertRaises(RuntimeError) as exc:
            self.run_dbt()

        self.assertIn('enabled', str(exc.exception))


class TestDisabledModelReference(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "invalid_models_011"

    @property
    def models(self):
        return "models-disabled"

    @use_profile('postgres')
    def test_postgres_view_with_incremental_attributes(self):
        with self.assertRaises(RuntimeError) as exc:
            self.run_dbt()

        self.assertIn('which is disabled', str(exc.exception))


class TestMissingModelReference(DBTIntegrationTest):
    @property
    def schema(self):
        return "invalid_models_011"

    @property
    def models(self):
        return "models-not-found"

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
        return "models-invalid-macro"

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
        model_path = os.path.join('models-invalid-macro', 'bad_macro.sql')

        self.assertIn(f'> in macro some_macro ({macro_path})', str(exc.exception))
        self.assertIn(f'> called by model bad_macro ({model_path})', str(exc.exception))


class TestInvalidDisabledSource(DBTIntegrationTest):
    def setUp(self):
        super().setUp()
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "invalid_models_011"

    @property
    def models(self):
        return 'sources-disabled'

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'sources': {
                'test': {
                    'enabled': False,
                }
            }
        }

    @use_profile('postgres')
    def test_postgres_source_disabled(self):
        with self.assertRaises(RuntimeError) as exc:
            self.run_dbt()

        self.assertIn('which is disabled', str(exc.exception))


class TestInvalidMissingSource(DBTIntegrationTest):
    def setUp(self):
        super().setUp()
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "invalid_models_011"

    @property
    def models(self):
        return 'sources-missing'

    @use_profile('postgres')
    def test_postgres_source_missing(self):
        with self.assertRaises(RuntimeError) as exc:
            self.run_dbt()

        self.assertIn('which was not found', str(exc.exception))
