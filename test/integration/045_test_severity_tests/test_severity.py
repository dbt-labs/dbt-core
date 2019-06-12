from test.integration.base import DBTIntegrationTest, use_profile

class TestSeverity(DBTIntegrationTest):
    @property
    def schema(self):
        return "severity_045"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'data-paths': ['data'],
        }

    def run_dbt_with_vars(self, cmd, *args, **kwargs):
        cmd.extend(['--vars',
                    '{{test_run_schema: {}}}'.format(self.unique_schema())])
        return self.run_dbt(cmd, *args, **kwargs)

    @use_profile('postgres')
    def test_postgres_severity_warnings(self):
        self.run_dbt_with_vars(['seed'], strict=False)
        self.run_dbt_with_vars(['run'], strict=False)
        results = self.run_dbt_with_vars(['test'], strict=False)
        self.assertEqual(len(results), 2)
        self.assertFalse(results[0].fail)
        self.assertEqual(results[0].status, 2)
        self.assertFalse(results[1].fail)
        self.assertEqual(results[1].status, 2)

    @use_profile('postgres')
    def test_postgres_severity_warnings_errors(self):
        self.run_dbt_with_vars(['seed'], strict=False)
        self.run_dbt_with_vars(['run'], strict=False)
        results = self.run_dbt_with_vars(['test'], expect_pass=False)
        self.assertEqual(len(results), 2)
        self.assertTrue(results[0].fail)
        self.assertEqual(results[0].status, 2)
        self.assertTrue(results[1].fail)
        self.assertEqual(results[1].status, 2)
