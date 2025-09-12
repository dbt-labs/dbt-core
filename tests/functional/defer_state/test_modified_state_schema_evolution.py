import json
import os
import shutil

from dbt.tests.util import run_dbt


class TestModifiedStateSchemaEvolution:
    def update_state(self, test_data_dir):
        run_dbt(["parse"])

        shutil.copyfile("target/manifest.json", os.path.join(test_data_dir, "manifest.json"))

        # Set dbt_version to PREVIOUS to trigger manifest upgrades on state:modified
        state_manifest_path = os.path.join(test_data_dir, "manifest.json")
        with open(state_manifest_path, "r") as f:
            manifest = json.load(f)
            manifest["metadata"]["dbt_version"] = "PREVIOUS"

        with open(state_manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

    def test_modified_state_schema_evolution(self, happy_path_project):
        # Uncomment this line when happy_path_project is updated
        # If the happy_path_project needs to be updated in order to
        # test schema evolutions not introducing state:modified false positives,
        # make sure to update state off of main so that the functional changes of the
        # schema evolution branch do not get reflected in the 'previous' state.
        # self.update_state(happy_path_project.test_data_dir)

        results = run_dbt(
            ["ls", "--select", "state:modified", "--state", happy_path_project.test_data_dir]
        )

        # No false positives when no project changes are made
        assert len(results) == 0
