import pytest
import os
import shutil
from dbt.tests.util import run_dbt, get_manifest
from dbt.exceptions import IncompatibleSchemaError
from dbt.contracts.graph.manifest import WritableManifest

# This project must have one of each kind of node type, plus disabled versions, for
# test coverage to be effective.
models__my_model_sql = """
select 1 as id
"""

seeds__my_seed_csv = """
id,value
4,2
"""

docs__somedoc_md = """
{% docs somedoc %}
Testing, testing
{% enddocs %}
"""

macros__dummy_test_sql = """
{% test nothing(model) %}

-- a silly test to make sure that table-level tests show up in the manifest
-- without a column_name field
select 0

{% endtest %}
"""

snapshot__snapshot_seed_sql = """
{% snapshot snapshot_seed %}
{{
    config(
      unique_key='id',
      strategy='check',
      check_cols='all',
      target_schema=schema,
    )
}}
select * from {{ ref('my_seed') }}
{% endsnapshot %}
"""

tests__just_my_sql = """
{{ config(tags = ['data_test_tag']) }}

select * from {{ ref('my_model') }}
where false
"""

analyses__a_sql = """
select 4 as id
"""

# Use old attribute names (v1.0-1.2) to test forward/backward compatibility with the rename in v1.3
models__schema_yml = """
version: 2
models:
  - name: my_model
    description: "Example model"
    columns:
     - name: id
       tests:
       - not_null

metrics:
  - name: my_metric
    label: Count records
    model: ref('my_model')

    type: count
    sql: "*"
    timestamp: updated_at
    time_grains: [day]
sources:
  - name: my_source
    description: "My source"
    loader: a_loader
    tables:
      - name: my_table
        description: "My table"
        identifier: my_seed
exposures:
  - name: simple_exposure
    type: dashboard
    depends_on:
      - ref('my_model')
      - source('my_source', 'my_table')
    owner:
      email: something@example.com
"""

# SETUP: Using this project, we have run past minor versions of dbt
# to generate each contracted version of `manifest.json`.

# Whenever we bump the manifest version, we should add a new entry for that version
# into `data`, generated from this same project, and update the CURRENT_EXPECTED_MANIFEST_VERSION.
# You can generate the manifest using the generate_latest_manifest() method below.

# TEST: Then, using the *current* version of dbt (this branch),
# we will perform a `--state` comparison against those older manifests.

# Some comparisons should succeed, where we expect backward/forward compatibility.

# Comparisons against older versions should fail, because the structure of the
# WritableManifest class has changed in ways that prevent successful deserialization
# of older JSON manifests.


class TestPreviousVersionState:
    CURRENT_EXPECTED_MANIFEST_VERSION = 8

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": models__my_model_sql,
            "schema.yml": models__schema_yml,
            "somedoc.md": docs__somedoc_md,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": seeds__my_seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot_seed.sql": snapshot__snapshot_seed_sql}

    @pytest.fixture(scope="class")
    def tests(self):
        return {"just_my.sql": tests__just_my_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"dummy_test.sql": macros__dummy_test_sql}

    @pytest.fixture(scope="class")
    def analyses(self):
        return {"a.sql": analyses__a_sql}

    def test_project(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        # model, snapshot, seed, singular test, generic test, analysis
        assert len(manifest.nodes) == 6
        assert len(manifest.sources) == 1
        assert len(manifest.exposures) == 1
        assert len(manifest.metrics) == 1
        assert "macro.test.test_nothing" in manifest.macros

    # Use this method when generating a new manifest version for the first time.
    # Once generated, we shouldn't need to re-generate or modify the manifest.
    def generate_latest_manifest(
        self,
        project,
        current_manifest_version,
    ):
        run_dbt(["list"])
        source_path = os.path.join(project.project_root, "target/manifest.json")
        state_path = os.path.join(project.test_data_dir, f"state/v{current_manifest_version}")
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
        state_path = os.path.join(project.test_data_dir, f"state/v{compare_manifest_version}")
        cli_args = [
            "list",
            "--select",
            "state:modified",
            "--state",
            state_path,
        ]
        if expect_pass:
            results = run_dbt(cli_args, expect_pass=expect_pass)
            # We have different numbers of nodes in older versions
            if compare_manifest_version < 7:
                assert len(results) == 4
            else:
                assert len(results) == 1
        else:
            with pytest.raises(IncompatibleSchemaError):
                run_dbt(cli_args, expect_pass=expect_pass)

    def test_compare_state_current(self, project):
        current_schema_version = WritableManifest.dbt_schema_version.version
        assert (
            current_schema_version == self.CURRENT_EXPECTED_MANIFEST_VERSION
        ), "Sounds like you've bumped the manifest version and need to update this test!"
        # If we need a newly generated manifest, uncomment the following line and commit the result
        # self.generate_latest_manifest(project, current_schema_version)
        self.compare_previous_state(project, current_schema_version, True)

    def test_backwards_compatible_versions(self, project):
        # manifest schema version 4 and greater should always be forward compatible
        for schema_version in range(4, self.CURRENT_EXPECTED_MANIFEST_VERSION):
            self.compare_previous_state(project, schema_version, True)

    def test_nonbackwards_compatible_versions(self, project):
        # schema versions 1, 2, 3 are all not forward compatible
        for schema_version in range(1, 4):
            self.compare_previous_state(project, schema_version, False)
