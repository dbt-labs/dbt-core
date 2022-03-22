import pytest

from dbt.tests.util import run_dbt
from tests.functional.fail_fast.fixtures import models, project_files  # noqa: F401
from dbt.exceptions import FailFastException


def check_audit_table(project, count=1):
    query = "select * from {schema}.audit".format(schema=project.test_schema)

    vals = project.run_sql(query, fetch="all")
    assert not (len(vals) == count), "Execution was not stopped before run end"


class TestFastFailingDuringRun:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "on-run-start": "create table if not exists {{ target.schema }}.audit (model text)",
            "models": {
                "test": {
                    "pre-hook": [
                        {
                            # we depend on non-deterministic nature of tasks execution
                            # there is possibility to run next task in-between
                            # first task failure and adapter connections cancellations
                            # if you encounter any problems with these tests please report
                            # the sleep command with random time minimize the risk
                            "sql": "select pg_sleep(random())",
                            "transaction": False,
                        },
                        {
                            "sql": "insert into {{ target.schema }}.audit values ('{{ this }}')",
                            "transaction": False,
                        },
                    ],
                }
            },
        }

    def test_fail_fast_run(
        self,
        project,
    ):
        with pytest.raises(FailFastException):
            run_dbt(["run", "--threads", "1", "--fail-fast"])
            check_audit_table(project)


class TestFailFastFromConfig(TestFastFailingDuringRun):
    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        return {
            "config": {
                "send_anonymous_usage_stats": False,
                "fail_fast": True,
            }
        }

    def test_fail_fast_run_user_config(
        self,
        project,
    ):
        with pytest.raises(FailFastException):
            run_dbt(["run", "--threads", "1"])
            check_audit_table(project)
