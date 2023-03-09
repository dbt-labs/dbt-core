import os

from dbt.tests.util import run_dbt, write_artifact


first_file_diff = {
    "deleted": [],
    "changed": [],
    "added": [{"path": "models/model_one.sql", "content": "select 1 as fun"}],
}


second_file_diff = {
    "deleted": [],
    "changed": [],
    "added": [{"path": "models/model_two.sql", "content": "select 123 as notfun"}],
}


class TestFileDiffs:
    def test_file_diffs(self, project):

        os.environ["DBT_PP_FILE_DIFF_TEST"] = "true"

        run_dbt(["deps"])
        run_dbt(["seed"])

        # We start with an empty project
        results = run_dbt()

        write_artifact(first_file_diff, "file_diff.json")
        results = run_dbt()
        assert len(results) == 1

        write_artifact(second_file_diff, "file_diff.json")
        results = run_dbt()
        assert len(results) == 2
