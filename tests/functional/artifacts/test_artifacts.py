import pytest
import os
from datetime import datetime
import dbt

from dbt.tests.util import run_dbt, get_artifact, check_datetime_between
from tests.functional.artifacts.expected_manifest import (
    expected_seeded_manifest,
    expected_references_manifest,
)
from tests.functional.artifacts.expected_run_results import (
    expected_run_results,
    expected_references_run_results,
)

models__schema_yml = """
version: 2

models:
  - name: model
    description: "The test model"
    docs:
      show: false
    columns:
      - name: id
        description: The user ID number
        tests:
          - unique
          - not_null
      - name: first_name
        description: The user's first name
      - name: email
        description: The user's email
      - name: ip_address
        description: The user's IP address
      - name: updated_at
        description: The last time this user's email was updated
    tests:
      - test.nothing

  - name: second_model
    description: "The second test model"
    docs:
      show: false
    columns:
      - name: id
        description: The user ID number
      - name: first_name
        description: The user's first name
      - name: email
        description: The user's email
      - name: ip_address
        description: The user's IP address
      - name: updated_at
        description: The last time this user's email was updated


sources:
  - name: my_source
    description: "My source"
    loader: a_loader
    schema: "{{ var('test_schema') }}"
    tables:
      - name: my_table
        description: "My table"
        identifier: seed
        quoting:
          identifier: True
        columns:
          - name: id
            description: "An ID field"


exposures:
  - name: simple_exposure
    type: dashboard
    depends_on:
      - ref('model')
      - source('my_source', 'my_table')
    owner:
      email: something@example.com
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('model')
      - ref('second_model')
    owner:
      email: something@example.com
      name: Some name
    description: >
      A description of the complex exposure
    maturity: medium
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']
    url: http://example.com/notebook/1
"""

models__second_model_sql = """
{{
    config(
        materialized='view',
        schema='test',
    )
}}

select * from {{ ref('seed') }}
"""

models__readme_md = """
This is a readme.md file with {{ invalid-ish jinja }} in it
"""

models__model_sql = """
{{
    config(
        materialized='view',
    )
}}

select * from {{ ref('seed') }}
"""

seed__schema_yml = """
version: 2
seeds:
  - name: seed
    description: "The test seed"
    columns:
      - name: id
        description: The user ID number
      - name: first_name
        description: The user's first name
      - name: email
        description: The user's email
      - name: ip_address
        description: The user's IP address
      - name: updated_at
        description: The last time this user's email was updated
"""

seed__seed_csv = """id,first_name,email,ip_address,updated_at
1,Larry,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
"""

macros__schema_yml = """
version: 2
macros:
  - name: test_nothing
    description: "{{ doc('macro_info') }}"
    meta:
      some_key: 100
    arguments:
      - name: model
        type: Relation
        description: "{{ doc('macro_arg_info') }}"
"""

macros__macro_md = """
{% docs macro_info %}
My custom test that I wrote that does nothing
{% enddocs %}

{% docs macro_arg_info %}
The model for my custom test
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
      target_schema=var('alternate_schema')
    )
}}
select * from {{ ref('seed') }}
{% endsnapshot %}
"""

ref_models__schema_yml = """
version: 2

models:
  - name: ephemeral_summary
    description: "{{ doc('ephemeral_summary') }}"
    columns: &summary_columns
      - name: first_name
        description: "{{ doc('summary_first_name') }}"
      - name: ct
        description: "{{ doc('summary_count') }}"
  - name: view_summary
    description: "{{ doc('view_summary') }}"
    columns: *summary_columns

sources:
  - name: my_source
    description: "{{ doc('source_info') }}"
    loader: a_loader
    schema: "{{ var('test_schema') }}"
    quoting:
      database: False
      identifier: False
    tables:
      - name: my_table
        description: "{{ doc('table_info') }}"
        identifier: seed
        quoting:
          identifier: True
        columns:
          - name: id
            description: "{{ doc('column_info') }}"

exposures:
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('view_summary')
    owner:
      email: something@example.com
      name: Some name
    description: "{{ doc('notebook_info') }}"
    maturity: medium
    url: http://example.com/notebook/1
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']

"""

ref_models__view_summary_sql = """
{{
  config(
    materialized = "view"
  )
}}

select first_name, ct from {{ref('ephemeral_summary')}}
order by ct asc

"""

ref_models__ephemeral_summary_sql = """
{{
  config(
    materialized = "table"
  )
}}

select first_name, count(*) as ct from {{ref('ephemeral_copy')}}
group by first_name
order by first_name asc

"""

ref_models__ephemeral_copy_sql = """
{{
  config(
    materialized = "ephemeral"
  )
}}

select * from {{ source("my_source", "my_table") }}

"""

ref_models__docs_md = """
{% docs ephemeral_summary %}
A summmary table of the ephemeral copy of the seed data
{% enddocs %}

{% docs summary_first_name %}
The first name being summarized
{% enddocs %}

{% docs summary_count %}
The number of instances of the first name
{% enddocs %}

{% docs view_summary %}
A view of the summary of the ephemeral copy of the seed data
{% enddocs %}

{% docs source_info %}
My source
{% enddocs %}

{% docs table_info %}
My table
{% enddocs %}

{% docs column_info %}
An ID field
{% enddocs %}

{% docs notebook_info %}
A description of the complex exposure
{% enddocs %}

"""


def verify_metadata(metadata, dbt_schema_version, start_time):
    assert "generated_at" in metadata
    check_datetime_between(metadata["generated_at"], start=start_time)
    assert "dbt_version" in metadata
    assert metadata["dbt_version"] == dbt.version.__version__
    assert "dbt_schema_version" in metadata
    assert metadata["dbt_schema_version"] == dbt_schema_version
    assert metadata["invocation_id"] == dbt.tracking.active_user.invocation_id
    key = "env_key"
    if os.name == "nt":
        key = key.upper()
    assert metadata["env"] == {key: "env_value"}


def verify_manifest(project, expected_manifest, start_time):
    manifest_path = os.path.join(project.project_root, "target", "manifest.json")
    assert os.path.exists(manifest_path)

    manifest = get_artifact(manifest_path)

    manifest_keys = {
        "nodes",
        "sources",
        "macros",
        "parent_map",
        "child_map",
        "metrics",
        "docs",
        "metadata",
        "docs",
        "disabled",
        "exposures",
        "selectors",
    }

    assert set(manifest.keys()) == manifest_keys

    for key in manifest_keys:
        if key == "macros":
            verify_manifest_macros(manifest, expected_manifest.get("macros"))
        elif key == "metadata":
            metadata = manifest["metadata"]
            verify_metadata(
                metadata, "https://schemas.getdbt.com/dbt/manifest/v5.json", start_time
            )
            assert (
                "project_id" in metadata
                and metadata["project_id"] == "098f6bcd4621d373cade4e832627b4f6"
            )
            assert (
                "send_anonymous_usage_stats" in metadata
                and metadata["send_anonymous_usage_stats"] is False
            )
            assert "adapter_type" in metadata and metadata["adapter_type"] == project.adapter_type
        elif key in ["nodes", "sources", "exposures", "metrics", "disabled", "docs"]:
            for unique_id, node in expected_manifest[key].items():
                assert unique_id in manifest[key]
                assert manifest[key][unique_id] == node
        else:  # ['docs', 'parent_map', 'child_map', 'selectors']
            assert manifest[key] == expected_manifest[key]


def verify_manifest_macros(manifest, expected=None):
    assert "macros" in manifest
    if expected:
        for unique_id, expected_macro in expected.items():
            assert unique_id in manifest["macros"]
            actual_macro = manifest["macros"][unique_id]
            assert expected_macro == actual_macro


def verify_run_results(project, expected_run_results, start_time):
    run_results_path = os.path.join(project.project_root, "target", "run_results.json")
    run_results = get_artifact(run_results_path)
    assert "metadata" in run_results
    verify_metadata(
        run_results["metadata"], "https://schemas.getdbt.com/dbt/run-results/v4.json", start_time
    )
    assert "elapsed_time" in run_results
    assert run_results["elapsed_time"] > 0
    assert isinstance(run_results["elapsed_time"], float)
    assert "args" in run_results
    # sort the results so we can make reasonable assertions
    run_results["results"].sort(key=lambda r: r["unique_id"])
    assert run_results["results"] == expected_run_results
    set(run_results) == {"elapsed_time", "results", "metadata"}


class BaseVerifyProject:
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        alternate_schema_name = project.test_schema + "_test"
        project.create_test_schema(schema_name=alternate_schema_name)
        os.environ["DBT_ENV_CUSTOM_ENV_env_key"] = "env_value"
        run_dbt(["seed"])
        yield
        del os.environ["DBT_ENV_CUSTOM_ENV_env_key"]

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"schema.yml": seed__schema_yml, "seed.csv": seed__seed_csv}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "schema.yml": macros__schema_yml,
            "macro.md": macros__macro_md,
            "dummy_test.sql": macros__dummy_test_sql,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot_seed.sql": snapshot__snapshot_seed_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        alternate_schema = unique_schema + "_test"
        return {
            "asset-paths": ["assets", "invalid-asset-paths"],
            "vars": {
                "test_schema": unique_schema,
                "alternate_schema": alternate_schema,
            },
            "seeds": {
                "quote_columns": True,
            },
            "quoting": {"identifier": False},
        }


class TestVerifyArtifacts(BaseVerifyProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "second_model.sql": models__second_model_sql,
            "readme.md": models__readme_md,
            "model.sql": models__model_sql,
        }

    # Test generic "docs generate" command
    def test_run_and_generate(self, project):
        start_time = datetime.utcnow()
        results = run_dbt(["compile"])
        assert len(results) == 7
        verify_manifest(project, expected_seeded_manifest(project, quote_model=False), start_time)
        verify_run_results(project, expected_run_results(), start_time)


class TestVerifyArtifactsReferences(BaseVerifyProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": ref_models__schema_yml,
            "view_summary.sql": ref_models__view_summary_sql,
            "ephemeral_summary.sql": ref_models__ephemeral_summary_sql,
            "ephemeral_copy.sql": ref_models__ephemeral_copy_sql,
            "docs.md": ref_models__docs_md,
        }

    def test_references(self, project):
        start_time = datetime.utcnow()
        results = run_dbt(["compile"])
        assert len(results) == 4
        verify_manifest(project, expected_references_manifest(project), start_time)
        verify_run_results(project, expected_references_run_results(), start_time)
