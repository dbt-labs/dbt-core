import json
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
            'seed-paths': ['seeds'],
            'test-paths': ['tests'],
            'seeds': {
                'quote_columns': False,
            },
        }

    def run_dbt_with_vars(self, cmd, strict_var, *args, **kwargs):
        cmd.extend(['--vars',
                    '{{test_run_schema: {}, strict: {}}}'.format(self.unique_schema(), strict_var)])
        return self.run_dbt_and_capture(cmd, *args, **kwargs)

    @use_profile('postgres')
    def test_postgres_severity_warnings(self):
        self.run_dbt_with_vars(['seed'], 'false')
        self.run_dbt_with_vars(['run'], 'false')
        results, _ = self.run_dbt_with_vars(
            ['test', '--select', 'test_type:generic'], 'false')
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].status, 'warn')
        self.assertEqual(results[0].failures, 2)
        self.assertEqual(results[1].status, 'warn')
        self.assertEqual(results[1].failures, 2)

    @use_profile('postgres')
    def test_postgres_severity_rendered_errors(self):
        self.run_dbt_with_vars(['seed'], 'false')
        self.run_dbt_with_vars(['run'], 'false')
        results, _ = self.run_dbt_with_vars(
            ['test', '--select', 'test_type:generic'], 'true', expect_pass=False)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].status, 'fail')
        self.assertEqual(results[0].failures, 2)
        self.assertEqual(results[1].status, 'fail')
        self.assertEqual(results[1].failures, 2)

    @use_profile('postgres')
    def test_postgres_severity_warnings_strict(self):
        self.run_dbt_with_vars(['seed'], 'false')
        self.run_dbt_with_vars(['run'], 'false')
        results, _ = self.run_dbt_with_vars(
            ['test', '--select', 'test_type:generic'], 'false', expect_pass=True)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].status, 'warn')
        self.assertEqual(results[0].failures, 2)
        self.assertEqual(results[1].status, 'warn')
        self.assertEqual(results[1].failures, 2)

    @use_profile('postgres')
    def test_postgres_data_severity_warnings(self):
        self.run_dbt_with_vars(['seed'], 'false')
        self.run_dbt_with_vars(['run'], 'false')
        results, _ = self.run_dbt_with_vars(
            ['test', '--select', 'test_type:singular'], 'false')
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'warn')
        self.assertEqual(results[0].failures, 2)

    @use_profile('postgres')
    def test_postgres_data_severity_rendered_errors(self):
        self.run_dbt_with_vars(['seed'], 'false')
        self.run_dbt_with_vars(['run'], 'false')
        results, _ = self.run_dbt_with_vars(
            ['test', '--select', 'test_type:singular'], 'true', expect_pass=False)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'fail')
        self.assertEqual(results[0].failures, 2)

    @use_profile('postgres')
    def test_postgres_data_severity_warnings_strict(self):
        self.run_dbt_with_vars(['seed'], 'false')
        self.run_dbt_with_vars(['run'], 'false')
        results, _ = self.run_dbt_with_vars(
            ['test', '--select', 'test_type:singular'], 'false', expect_pass=True)
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].status, 'fail')
        self.assertEqual(results[0].failures, 2)

    def parse_json_logs(self, log_output):
        parsed_logs = []
        for line in log_output.split('\n'):
            try:
                log = json.loads(line)
            except ValueError:
                continue
            parsed_logs.append(log)
        return parsed_logs

    @use_profile('postgres')
    def test_postgres_data_severity_warnings_json(self):
        self.run_dbt_with_vars(['seed'], 'false')
        self.run_dbt_with_vars(['run'], 'false')
        results, log_output = self.run_dbt_with_vars(
            ['--log-format', 'json', 'test', '--select', 'test_type:singular'],
            'false', expect_pass=True)
        levels = [x['level'] for x in self.parse_json_logs(log_output)]
        self.assertNotIn('error', levels)
        self.assertIn('warn', levels)
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].status, 'fail')
        self.assertEqual(results[0].failures, 2)

    @use_profile('postgres')
    def test_postgres_data_severity_warnings_json_strict(self):
        self.run_dbt_with_vars(['seed'], 'false')
        self.run_dbt_with_vars(['run'], 'false')
        results, log_output = self.run_dbt_with_vars(
            ['--log-format', 'json', 'test', '--select', 'test_type:singular'],
            'true', expect_pass=False)
        levels = [x['level'] for x in self.parse_json_logs(log_output)]
        self.assertNotIn('warn', levels)
        self.assertIn('error', levels)
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].status, 'fail')
        self.assertEqual(results[0].failures, 2)
