from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest
import os


class TestAnalyses(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_analyses_019"

    @property
    def models(self):
        return "test/integration/019_analysis_tests/models"

    def analysis_path(self):
        return "test/integration/019_analysis_tests/analysis"

    @property
    def project_config(self):
        return {
            "analysis-paths": [self.analysis_path()]
        }

    @attr(type='postgres')
    def test_analyses(self):
        compiled_analysis_path = os.path.normpath('target/compiled/test/analysis')
        path_1 = os.path.join(compiled_analysis_path, 'analysis.sql')
        path_2 = os.path.join(compiled_analysis_path, 'raw_stuff.sql')

        self.run_dbt(['clean'])
        self.assertFalse(os.path.exists(compiled_analysis_path))
        results = self.run_dbt(["compile"])
        self.assertEqual(len(results), 3)

        self.assertTrue(os.path.exists(path_1))
        self.assertTrue(os.path.exists(path_2))

        with open(path_1) as fp:
            self.assertEqual(
                fp.read().strip(),
                'select * from "{}"."my_model"'.format(self.unique_schema())
            )
        with open(path_2) as fp:
            self.assertEqual(
                fp.read().strip(),
                '{% invalid jinja stuff %}'
            )

