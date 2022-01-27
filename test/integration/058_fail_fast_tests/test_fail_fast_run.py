from test.integration.base import DBTIntegrationTest, use_profile
from dbt.exceptions import FailFastException


class TestFastFailingDuringRun(DBTIntegrationTest):
    @property
    def schema(self):
        return "fail_fast_058"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "on-run-start": "create table if not exists {{ target.schema }}.audit (model text)",
            'models': {
                'test': {
                    'pre-hook': [
                        {
                            # we depend on non-deterministic nature of tasks execution
                            # there is possibility to run next task in-between
                            # first task failure and adapter connections cancellations
                            # if you encounter any problems with these tests please report
                            # the sleep command with random time minimize the risk
                            'sql': "select pg_sleep(random())",
                            'transaction': False
                        },
                        {
                            'sql': "insert into {{ target.schema }}.audit values ('{{ this }}')",
                            'transaction': False
                        }
                    ],
                }
            }
        }

    @property
    def models(self):
        return "models"

    def check_audit_table(self, count=1):
        query = "select * from {schema}.audit".format(schema=self.unique_schema())

        vals = self.run_sql(query, fetch='all')
        self.assertFalse(len(vals) == count, 'Execution was not stopped before run end')


    @use_profile('postgres')
    def test_postgres_fail_fast_run(self):
        with self.assertRaises(FailFastException):
            self.run_dbt(['run', '--threads', '1', '--fail-fast'])
            self.check_audit_table()


class FailFastFromConfig(TestFastFailingDuringRun):

    @property
    def profile_config(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False,
                'fail_fast': True,
            }
        }

    @use_profile('postgres')
    def test_postgres_fail_fast_run_user_config(self):
        with self.assertRaises(FailFastException):
            self.run_dbt(['run', '--threads', '1'])
            self.check_audit_table()
