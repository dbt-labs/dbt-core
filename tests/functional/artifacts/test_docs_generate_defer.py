import os
import shutil
import pytest
from dbt.tests.util import run_dbt

model_sql = """
select 1 as id
"""


class TestDocsGenerateDefer:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}

    def copy_state(self):
        assert not os.path.exists("state")
        os.makedirs("state")
        shutil.copyfile("target/manifest.json", "state/manifest.json")

    def test_generate_defer(
        self,
        project,
    ):
        results = run_dbt(["run"])
        assert len(results) == 1

        # copy state files
        self.copy_state()

        # defer test, it succeeds
        results = run_dbt(["docs", "generate", "--state", "./state", "--defer"])
        assert results.nodes["model.test.model"]
