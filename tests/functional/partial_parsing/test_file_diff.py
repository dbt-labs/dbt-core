import os
import pytest

from dbt.tests.util import run_dbt, write_artifact, write_file
from tests.functional.partial_parsing.fixtures import model_one_sql, model_two_sql


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


class TestFileDiffPaths:
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


class TestFileDiffs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
        }

    def test_no_file_diffs(self, project):
        # We start with a project with one model
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes) == 1

        # add a model file
        write_file(model_two_sql, project.project_root, "models", "model_two.sql")

        # parse without computing a file diff
        manifest = run_dbt(["--partial-parse", "--no-partial-parse-file-diff", "parse"])
        assert len(manifest.nodes) == 1

        # default behaviour - parse with computing a file diff
        manifest = run_dbt(["--partial-parse", "parse"])
        assert len(manifest.nodes) == 2
