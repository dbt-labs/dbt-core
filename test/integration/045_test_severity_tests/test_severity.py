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
            'config-version': 2,
            'data-paths': ['data'],
            'test-paths': ['tests'],
            'seeds': {
                'quote_columns': False,
            },
        }

    def run_dbt_with_vars(self, cmd, strict_var, *args, **kwargs):
        cmd.extend(['--vars',
                    '{{test_run_schema: {}, strict: {}}}'.format(self.unique_schema(), strict_var)])
        return self.run_dbt(cmd, *args, **kwargs)

    @use_profile('postgres')
    def test_postgres_severity_warnings(self):
        self.run_dbt_with_vars(['seed'], 'false', strict=False)
        self.run_dbt_with_vars(['run'], 'false', strict=False)
        results = self.run_dbt_with_vars(
            ['test', '--schema'], 'false', strict=False)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].status, 'warn')
        self.assertEqual(results[0].message, 2)
        self.assertEqual(results[1].status, 'warn')
        self.assertEqual(results[1].message, 2)

    @use_profile('postgres')
    def test_postgres_severity_rendered_errors(self):
        self.run_dbt_with_vars(['seed'], 'false', strict=False)
        self.run_dbt_with_vars(['run'], 'false', strict=False)
        results = self.run_dbt_with_vars(
            ['test', '--schema'], 'true', strict=False, expect_pass=False)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].status, 'fail')
        self.assertEqual(results[0].message, 2)
        self.assertEqual(results[1].status, 'fail')
        self.assertEqual(results[1].message, 2)

    @use_profile('postgres')
    def test_postgres_severity_warnings_strict(self):
        self.run_dbt_with_vars(['seed'], 'false', strict=False)
        self.run_dbt_with_vars(['run'], 'false', strict=False)
        results = self.run_dbt_with_vars(
            ['test', '--schema'], 'false', expect_pass=False)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].status, 'fail')
        self.assertEqual(results[0].message, 2)
        self.assertEqual(results[1].status, 'fail')
        self.assertEqual(results[1].message, 2)

    @use_profile('postgres')
    def test_postgres_data_severity_warnings(self):
        self.run_dbt_with_vars(['seed'], 'false', strict=False)
        self.run_dbt_with_vars(['run'], 'false', strict=False)
        results = self.run_dbt_with_vars(
            ['test', '--data'], 'false', strict=False)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'warn')
        self.assertEqual(results[0].message, 2)

    @use_profile('postgres')
    def test_postgres_data_severity_rendered_errors(self):
        self.run_dbt_with_vars(['seed'], 'false', strict=False)
        self.run_dbt_with_vars(['run'], 'false', strict=False)
        results = self.run_dbt_with_vars(
            ['test', '--data'], 'true', strict=False, expect_pass=False)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'fail')
        self.assertEqual(results[0].message, 2)

    @use_profile('postgres')
    def test_postgres_data_severity_warnings_strict(self):
        self.run_dbt_with_vars(['seed'], 'false', strict=False)
        self.run_dbt_with_vars(['run'], 'false', strict=False)
        results = self.run_dbt_with_vars(
            ['test', '--data'], 'false', expect_pass=False)
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].status, 'fail')
        self.assertEqual(results[0].message, 2)
