import pytest
import re
from dbt.tests.util import check_relation_has_expected_schema, run_dbt

_MODEL_CURRENT_TIMESTAMP = """
select {{ current_timestamp() }} as current_timestamp,
       {{ current_timestamp_in_utc_backcompat() }} as current_timestamp_in_utc_backcompat,
       {{ current_timestamp_backcompat() }} as current_timestamp_backcompat
"""

_MODEL_EXPECTED_SQL = """
select now() as current_timestamp,
       (current_timestamp at time zone 'utc')::TIMESTAMP as current_timestamp_in_utc_backcompat,
       current_timestamp::TIMESTAMP as current_timestamp_backcompat
"""


class BaseCurrentTimestamps:
    @pytest.fixture(scope="class")
    def models(self):
        return {"get_current_timestamp.sql": _MODEL_CURRENT_TIMESTAMP}

    # any adapters that don't want to check can set expected schema to None
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _MODEL_EXPECTED_SQL

    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "current_timestamp": "timestamp with time zone",
            "current_timestamp_in_utc_backcompat": "timestamp without time zone",
            "current_timestamp_backcompat": "timestamp without time zone",
        }

    def test_current_timestamps(self, project, models, expected_schema, expected_sql):
        results = run_dbt(["run"])
        assert len(results) == 1
        check_relation_has_expected_schema(
            project.adapter,
            relation_name="get_current_timestamp",
            expected_schema=expected_schema,
        )

        if expected_sql:
            generated_sql = results.results[0].node.compiled_code
            generated_sql_check = re.sub(r"\s+", "", generated_sql).lower()
            expected_sql_check = re.sub(r"\s+", "", expected_sql).lower()
            assert (
                expected_sql_check == generated_sql_check
            ), f"generated sql did not match expected: {generated_sql}"


class TestCurrentTimestamps(BaseCurrentTimestamps):
    pass
