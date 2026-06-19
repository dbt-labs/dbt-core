import os

import pytest

from dbt.tests.util import get_manifest, run_dbt, write_artifact, write_file
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


# A model authored on Windows (CRLF) and delivered through a file diff must
# hash the same as the normalized on-disk (LF) form, otherwise state:modified
# is falsely triggered for cross-platform manifests (#11473). The on-disk read
# path already normalizes via load_source_file(); this guards that the file
# diff path (ReadFilesFromDiff) stays consistent with it.
crlf_model_content = "select 1 as fun\r\nunion all\r\nselect 2 as fun\r\n"

crlf_file_diff = {
    "deleted": [],
    "changed": [],
    "added": [{"path": "models/crlf_model.sql", "content": crlf_model_content}],
}


class TestFileDiffLineEndings:
    def test_file_diff_normalizes_line_endings(self, project):
        from dbt.artifacts.resources.base import FileHash
        from dbt.parser.read_files import normalize_file_contents

        os.environ["DBT_PP_FILE_DIFF_TEST"] = "true"

        run_dbt(["deps"])
        run_dbt(["seed"])

        # Start from an empty project (writes the baseline manifest).
        run_dbt()

        # Deliver a new model via the file diff with CRLF line endings.
        write_artifact(crlf_file_diff, "file_diff.json")
        run_dbt()

        manifest = get_manifest(project.project_root)
        node = next(n for n in manifest.nodes.values() if n.name == "crlf_model")

        lf_content = crlf_model_content.replace("\r\n", "\n")
        # CRLF content delivered via the file diff hashes the same as its
        # normalized LF equivalent (and as the on-disk read path), so it is not
        # falsely reported as state:modified.
        assert node.checksum == FileHash.from_contents(normalize_file_contents(crlf_model_content))
        assert node.checksum == FileHash.from_contents(normalize_file_contents(lf_content))
        # And specifically not the raw CRLF-sensitive hash (the pre-fix behavior).
        assert node.checksum != FileHash.from_contents(crlf_model_content)
