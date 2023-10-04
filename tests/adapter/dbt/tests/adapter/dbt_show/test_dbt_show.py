import pytest
from dbt.tests.util import run_dbt

from dbt.tests.adapter.dbt_show.fixtures import (
    models__sql_header,
    models__ephemeral_model,
    models__second_ephemeral_model,
    models__sample_model,
    seeds__sample_seed,
)


# -- Below we define base classes for tests you import based on if your adapter supports dbt show or not --
class BaseShowLimit:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_model.sql": models__sample_model,
            "ephemeral_model.sql": models__ephemeral_model,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"sample_seed.csv": seeds__sample_seed}

    @pytest.mark.parametrize(
        "args,expected",
        [
            ([], 5),  # default limit
            (["--limit", 3], 3),  # fetch 3 rows
            (["--limit", -1], 7),  # fetch all rows
        ],
    )
    def test_limit(self, project, args, expected):
        run_dbt(["build"])
        dbt_args = ["show", "--inline", models__second_ephemeral_model, *args]
        results = run_dbt(dbt_args)
        assert len(results.results[0].agate_table) == expected
        # ensure limit was injected in compiled_code when limit specified in command args
        limit = results.args.get("limit")
        if limit > 0:
            assert f"limit {limit}" in results.results[0].node.compiled_code


class BaseShowSqlHeader:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sql_header.sql": models__sql_header,
        }

    def test_sql_header(self, project):
        run_dbt(["show", "--select", "sql_header", "--vars", "timezone: Asia/Kolkata"])


class TestPostgresShowSqlHeader(BaseShowSqlHeader):
    pass


class TestPostgresShowLimit(BaseShowLimit):
    pass
