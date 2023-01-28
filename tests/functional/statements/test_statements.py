import pathlib
import pytest

from dbt.tests.util import run_dbt, check_relations_equal, write_file
from tests.functional.statements.fixtures import (
    models__statement_actual,
    seeds__statement_actual,
    seeds__statement_expected,
)


class TestStatements:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        # put seeds in 'seed' not 'seeds' directory
        (pathlib.Path(project.project_root) / "seed").mkdir(parents=True, exist_ok=True)
        write_file(seeds__statement_actual, project.project_root, "seed", "seed.csv")
        write_file(
            seeds__statement_expected, project.project_root, "seed", "statement_expected.csv"
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {"statement_actual.sql": models__statement_actual}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
            "seed-paths": ["seed"],
        }

    def test_postgres_statements(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 2
        results = run_dbt()
        assert len(results) == 1

        check_relations_equal(project.adapter, ["statement_actual", "statement_expected"])
