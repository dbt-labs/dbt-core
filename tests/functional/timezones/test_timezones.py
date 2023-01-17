import os
import pytest
from freezegun import freeze_time

from dbt.tests.util import run_dbt


model_sql = """
{{
    config(
        materialized='table'
    )
}}

select
    '{{ run_started_at.astimezone(modules.pytz.timezone("America/New_York")) }}' as run_started_at_est,
    '{{ run_started_at }}' as run_started_at_utc
"""


class TestTimezones:
    @pytest.fixture(scope="class")
    def models(self):
        return {"timezones.sql": model_sql}

    @pytest.fixture(scope="class")
    def dbt_profile_data(self, unique_schema):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "postgres",
                        "threads": 1,
                        "host": "localhost",
                        "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
                        "user": os.getenv("POSTGRES_TEST_USER", "root"),
                        "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
                        "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                        "schema": unique_schema,
                    },
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def query(self, project):
        return """
            select
              run_started_at_est,
              run_started_at_utc
            from {schema}.timezones
        """.format(
            schema=project.test_schema
        )

    @freeze_time("2022-01-01 03:00:00", tz_offset=0)
    def test_run_started_at(self, project, query):
        results = run_dbt(["run"])

        assert len(results) == 1

        result = project.run_sql(query, fetch="all")[0]
        est, utc = result

        assert utc == "2022-01-01 03:00:00+00:00"
        assert est == "2021-12-31 22:00:00-05:00"
