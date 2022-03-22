import pytest
from pytest import mark
import os

from dbt.tests.util import run_dbt
from tests.functional.permission.fixtures import models, project_files  # noqa: F401


# postgres sometimes fails with an internal error if you run these tests too close together.
def postgres_error(err, *args):
    msg = str(err)
    if "tuple concurrently updated" in msg:
        return True
    return False


@mark.flaky(rerun_filter=postgres_error)
class TestPermissions:
    @pytest.fixture(autouse=True)
    def setUp(self, project):
        path = os.path.join(project.test_data_dir, "seed.sql")
        project.run_sql_file(path)

    @pytest.fixture(scope="class")
    def profiles_config_update(self, unique_schema):
        return {
            "test": {
                "outputs": {
                    "default": {
                        "type": "postgres",
                        "threads": 4,
                        "host": "localhost",
                        "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
                        "user": os.getenv("POSTGRES_TEST_USER", "root"),
                        "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
                        "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                        "schema": unique_schema,
                    },
                    "noaccess": {
                        "type": "postgres",
                        "threads": 4,
                        "host": "localhost",
                        "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
                        "user": "noaccess",
                        "pass": "password",
                        "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                        "schema": unique_schema + "_alt",  # Should this be the same unique_schema?
                    },
                },
            }
        }

    def test_no_create_schema_permissions(
        self,
        project,
    ):
        # the noaccess user does not have permissions to create a schema -- this should fail
        project.run_sql('drop schema if exists "{}" cascade'.format(project.test_schema))
        with pytest.raises(RuntimeError):
            run_dbt(["run", "--target", "noaccess"], expect_pass=False)

    def test_create_schema_permissions(
        self,
        project,
    ):
        # now it should work!
        # breakpoint()
        project.run_sql("grant create on database {} to noaccess".format(project.database))
        project.run_sql(
            'grant usage, create on schema "{}" to noaccess'.format(project.test_schema)
        )
        project.run_sql(
            'grant select on all tables in schema "{}" to noaccess'.format(project.test_schema)
        )
        results = run_dbt(["run", "--target", "noaccess"])
        assert len(results) == 1
