import os
from dbt.exceptions import CompilationException
from test.integration.base import DBTIntegrationTest, use_profile


class TestDuplicateMacroEnabledSameFile(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_macro_025"

    @property
    def models(self):
        return "models-3"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['macros-bad-same']
        }

    @use_profile('postgres')
    def test_postgres_duplicate_macros(self):
        with self.assertRaises(CompilationException) as exc:
            self.run_dbt(['compile'])
        self.assertIn('some_macro', str(exc.exception))
        self.assertIn(os.path.join('macros-bad-same', 'macros.sql'), str(exc.exception))


class TestDuplicateMacroEnabledDifferentFiles(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_macro_025"

    @property
    def models(self):
        return "models-3"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['macros-bad-separate']
        }

    @use_profile('postgres')
    def test_postgres_duplicate_macros(self):
        with self.assertRaises(CompilationException) as exc:
            self.run_dbt(['compile'])
        self.assertIn('some_macro', str(exc.exception))
        self.assertIn(os.path.join('macros-bad-separate', 'one.sql'), str(exc.exception))
        self.assertIn(os.path.join('macros-bad-separate', 'two.sql'), str(exc.exception))
