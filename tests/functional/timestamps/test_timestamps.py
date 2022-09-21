import pytest
from dbt.tests.util import check_relation_has_expected_schema, run_dbt

_MODEL_CURRENT_TIMESTAMP = """
SELECT {{current_timestamp()}} as current_timestamp,
       {{current_timestamp_in_utc()}} as current_timestamp_in_utc
"""


class TestCurrentTimestamps:
    @pytest.fixture(scope="class")
    def models(self):
        return {"get_current_timestamp.sql": _MODEL_CURRENT_TIMESTAMP}

    def test_current_timestamps(self, project, models):
        results = run_dbt(["run"])
        assert len(results) == 1
        check_relation_has_expected_schema(
            project.adapter,
            relation_name="get_current_timestamp",
            expected_schema={
                "current_timestamp": "timestamp with time zone",
                "current_timestamp_in_utc": "timestamp without time zone",
            },
        )
