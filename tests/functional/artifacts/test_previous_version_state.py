import json
import os
import shutil

import pytest

from dbt.contracts.graph.manifest import WritableManifest, get_manifest_schema_version
from dbt.exceptions import IncompatibleSchemaError
from dbt.tests.util import run_dbt, get_manifest

# This project must have one of each kind of node type, plus disabled versions, for
# test coverage to be complete.
models__my_model_sql = """
select 1 as id
"""

models__disabled_model_sql = """
{{ config(enabled=False) }}
select 2 as id
"""

seeds__my_seed_csv = """
id,value
4,2
"""

seeds__disabled_seed_csv = """
id,value
6,4
"""

docs__somedoc_md = """
{% docs somedoc %}
Testing, testing
{% enddocs %}
"""

macros__do_nothing_sql = """
{% macro do_nothing(foo2, bar2) %}
    select
        '{{ foo2 }}' as foo2,
        '{{ bar2 }}' as bar2
{% endmacro %}
"""

macros__dummy_test_sql = """
{% test check_nothing(model) %}
-- a silly test to make sure that table-level tests show up in the manifest
-- without a column_name field

select 0

{% endtest %}
"""

macros__disabled_dummy_test_sql = """
{% test disabled_check_nothing(model) %}
-- a silly test to make sure that table-level tests show up in the manifest
-- without a column_name field

{{ config(enabled=False) }}
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

snapshot__disabled_snapshot_seed_sql = """
{% snapshot disabled_snapshot_seed %}
{{
    config(
      unique_key='id',
      strategy='check',
      check_cols='all',
      target_schema=schema,
      enabled=False,
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

tests__disabled_just_my_sql = """
{{ config(enabled=False) }}

select * from {{ ref('my_model') }}
where false
"""

analyses__a_sql = """
select 4 as id
"""

analyses__disabled_a_sql = """
{{ config(enabled=False) }}
select 9 as id
"""

metricflow_time_spine_sql = """
SELECT to_date('02/20/2023', 'mm/dd/yyyy') as date_day
"""

# Use old attribute names (v1.0-1.2) to test forward/backward compatibility with the rename in v1.3
models__schema_yml = """
version: 2
models:
  - name: my_model
    description: "Example model"
    tests:
      - check_nothing
      - disabled_check_nothing
    columns:
     - name: id
       tests:
       - not_null

semantic_models:
  - name: semantic_people
    model: ref('my_model')
    dimensions:
      - name: favorite_color
        type: categorical
      - name: created_at
        type: TIME
        type_params:
          time_granularity: day
    measures:
      - name: years_tenure
        agg: SUM
        expr: tenure
      - name: people
        agg: count
        expr: id
      - name: customers
        agg: count
        expr: id
    entities:
      - name: id
        type: primary
    defaults:
      agg_time_dimension: created_at

metrics:
  - name: my_metric
    label: Count records
    type: simple
    type_params:
      measure: customers
  - name: disabled_metric
    label: Count records
    config:
        enabled: False
    type: simple
    type_params:
      measure: customers

sources:
  - name: my_source
    description: "My source"
    loader: a_loader
    tables:
      - name: my_table
        description: "My table"
        identifier: my_seed
      - name: disabled_table
        description: "Disabled table"
        config:
           enabled: False

exposures:
  - name: simple_exposure
    type: dashboard
    depends_on:
      - ref('my_model')
      - source('my_source', 'my_table')
    owner:
      email: something@example.com
  - name: disabled_exposure
    type: dashboard
    config:
      enabled: False
    depends_on:
      - ref('my_model')
    owner:
      email: something@example.com

seeds:
  - name: disabled_seed
    config:
      enabled: False
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


# We are creating enabled versions of every node type that might be in the manifest,
# plus disabled versions for types that support it (everything except macros and docs).


class TestPreviousVersionState:
    CURRENT_EXPECTED_MANIFEST_VERSION = 11

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": models__my_model_sql,
            "schema.yml": models__schema_yml,
            "somedoc.md": docs__somedoc_md,
            "disabled_model.sql": models__disabled_model_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": seeds__my_seed_csv,
            "disabled_seed.csv": seeds__disabled_seed_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot_seed.sql": snapshot__snapshot_seed_sql,
            "disabled_snapshot_seed.sql": snapshot__disabled_snapshot_seed_sql,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "just_my.sql": tests__just_my_sql,
            "disabled_just_my.sql": tests__disabled_just_my_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "do_nothing.sql": macros__do_nothing_sql,
            "dummy_test.sql": macros__dummy_test_sql,
            "disabled_dummy_test.sql": macros__disabled_dummy_test_sql,
        }

    @pytest.fixture(scope="class")
    def analyses(self):
        return {
            "a.sql": analyses__a_sql,
            "disabled_al.sql": analyses__disabled_a_sql,
        }

    def test_project(self, project):
        # This is mainly used to test changes to the test project in isolation from
        # the other noise.
        results = run_dbt(["run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        # model, snapshot, seed, singular test, generic test, analysis
        assert len(manifest.nodes) == 8
        assert len(manifest.sources) == 1
        assert len(manifest.exposures) == 1
        assert len(manifest.metrics) == 1
        # disabled model, snapshot, seed, singular test, generic test, analysis, source, exposure, metric
        assert len(manifest.disabled) == 9
        assert "macro.test.do_nothing" in manifest.macros

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
    # current WritableManifest class.
    def compare_previous_state(
        self,
        project,
        compare_manifest_version,
        expect_pass,
        num_results,
    ):
        state_path = os.path.join(project.test_data_dir, f"state/v{compare_manifest_version}")
        cli_args = [
            "list",
            "--resource-types",
            "model",
            "--select",
            "state:modified",
            "--state",
            state_path,
        ]
        if expect_pass:
            results = run_dbt(cli_args, expect_pass=expect_pass)
            assert len(results) == num_results
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
        self.compare_previous_state(project, current_schema_version, True, 0)

    def test_backwards_compatible_versions(self, project):
        # manifest schema version 4 and greater should always be forward compatible
        for schema_version in range(4, self.CURRENT_EXPECTED_MANIFEST_VERSION):
            self.compare_previous_state(project, schema_version, True, 1)

    def test_nonbackwards_compatible_versions(self, project):
        # schema versions 1, 2, 3 are all not forward compatible
        for schema_version in range(1, 4):
            self.compare_previous_state(project, schema_version, False, 0)

    def test_get_manifest_schema_version(self, project):
        for schema_version in range(1, self.CURRENT_EXPECTED_MANIFEST_VERSION):
            manifest_path = os.path.join(
                project.test_data_dir, f"state/v{schema_version}/manifest.json"
            )
            manifest = json.load(open(manifest_path))

            manifest_version = get_manifest_schema_version(manifest)
            assert manifest_version == schema_version
