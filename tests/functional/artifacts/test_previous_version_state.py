import pytest
import os
import shutil
from dbt.tests.util import run_dbt
from dbt.exceptions import IncompatibleSchemaException

# This is a *very* simple project, with just one model in it.
models__my_model_sql = """
select 1 as id
"""

# SETUP: Using this project, we have run past minor versions of dbt
# to generate each contracted version of `manifest.json`.

# Whenever we bump the manifest version, we should add a new entry for that version,
# generated from this same project, and add a new comparison test below.
# You can generate the manifest using the generate_latest_manifest() method below.

# TEST: Then, using the *current* version of dbt (this branch),
# we will perform a `--state` comparison against those older manifests.

# Some comparisons should succeed, where we expect backward/forward compatibility.

# Comparisons against older versions should fail, because the structure of the
# WritableManifest class has changed in ways that prevent successful deserialization
# of older JSON manifests.


class TestPreviousVersionState:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": models__my_model_sql}

    # Use this method when generating a new manifest version for the first time.
    # Once generated, we shouldn't need to re-generate or modify the manifest.
    def generate_latest_manifest(
        self,
        project,
        current_manifest_version,
    ):
        run_dbt(["list"])
        source_path = os.path.join(project.project_root, "target/manifest.json")
        state_path = os.path.join(project.test_data_dir, f"state/{current_manifest_version}")
        target_path = os.path.join(state_path, "manifest.json")
        os.makedirs(state_path, exist_ok=True)
        shutil.copyfile(source_path, target_path)

    # The actual test method. Run `dbt list --select state:modified --state ...`
    # once for each past manifest version. They all have the same content, but different
    # schema/structure, only some of which are forward-compatible with the
    # current WriteableManifest class.
    def compare_previous_state(
        self,
        project,
        compare_manifest_version,
        expect_pass,
    ):
        state_path = os.path.join(project.test_data_dir, f"state/{compare_manifest_version}")
        cli_args = [
            "list",
            "--select",
            "state:modified",
            "--state",
            state_path,
        ]
        if expect_pass:
            results = run_dbt(cli_args, expect_pass=expect_pass)
            assert len(results) == 0
        else:
            with pytest.raises(IncompatibleSchemaException):
                run_dbt(cli_args, expect_pass=expect_pass)

    def test_compare_state_v6(self, project):
        # self.generate_latest_manifest(project, "v6")
        self.compare_previous_state(project, "v6", True)

    def test_compare_state_v5(self, project):
        self.compare_previous_state(project, "v5", True)

    def test_compare_state_v4(self, project):
        self.compare_previous_state(project, "v4", True)

    def test_compare_state_v3(self, project):
        self.compare_previous_state(project, "v3", False)

    def test_compare_state_v2(self, project):
        self.compare_previous_state(project, "v2", False)

    def test_compare_state_v1(self, project):
        self.compare_previous_state(project, "v1", False)
