import os

import pytest

from dbt.tests.util import get_manifest, rm_file, run_dbt, write_file

os.environ["DBT_PP_TEST"] = "true"

colors_sql = """
    select 'green' as first, 'red' as second, 'blue' as third
"""

another_v1_sql = """
select * from {{ ref("colors") }}
"""

another_ref_sql = """
select * from {{ ref("another") }}
"""

colors_yml = """
models:
  - name: colors
    description: "a list of colors"
  - name: another
    description: "another model"
    versions:
      - v: 1
"""

colors_alt_yml = """
models:
  - name: colors
    description: "a list of colors"
  - name: another
    description: "YET another model"
    versions:
      - v: 1
"""

foo_model_sql = """
select 1 as id
"""

another_ref_yml = """
models:
  - name: another_ref
    description: "model with reference to another ref"
  - name: foo_model
    description: "some random model"
"""

another_ref_alt_yml = """
models:
  - name: another_ref
    description: "model with reference to another ref"
  - name: foo_model
    description: "some random other model"
"""


class TestSchemaFileOrder:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "colors.sql": colors_sql,
            "colors.yml": colors_yml,
            "another_v1.sql": another_v1_sql,
            "another_ref.sql": another_ref_sql,
            "foo_model.sql": foo_model_sql,
            "another_ref.yml": another_ref_yml,
        }

    def test_schema_file_order(self, project):

        # initial run
        results = run_dbt(["run"])
        assert len(results) == 4

        manifest = get_manifest(project.project_root)
        model_id = "model.test.another_ref"
        model = manifest.nodes.get(model_id)
        assert model.description == "model with reference to another ref"

        write_file(colors_alt_yml, project.project_root, "models", "colors.yml")
        write_file(another_ref_alt_yml, project.project_root, "models", "another_ref.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 4
        manifest = get_manifest(project.project_root)
        model = manifest.nodes.get(model_id)
        assert model.name == "another_ref"
        # The description here would be '' without the bug fix
        assert model.description == "model with reference to another ref"


foo_sql = """
select 1 c
"""

bar_sql = """
select 1 c
"""

bar_with_ref_sql = """
select * from {{ ref('foo') }}
"""

foo_v2_sql = """
select 1 c
"""

schema_yml = """
# models/schema.yml
models:
  - name: foo
    latest_version: 1
    versions:
      - v: 1
      - v: 2
"""

foo_yml = """
# models/foo.yml
models:
  - name: foo
"""

bar_yml = """
# models/bar.yml
models:
  - name: bar
    columns:
      - name: c
        tests:
          - relationships:
              to: ref('foo')
              field: c
"""

foo_alt_yml = """
# models/foo.yml
models:
  - name: foo
    latest_version: 1
    versions:
      - v: 1
      - v: 2
"""


class TestNewVersionedSchemaFile:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "foo.sql": foo_sql,
            "bar.sql": bar_with_ref_sql,
        }

    def test_schema_file_order_new_versions(self, project):
        # This tests that when a model referring to an existing model
        # which has had a version added in a yaml file has been re-parsed
        # in order to fix the depends_on to the correct versioned model

        # initial run
        results = run_dbt(["compile"])
        assert len(results) == 2

        write_file(foo_v2_sql, project.project_root, "models", "foo_v2.sql")
        write_file(schema_yml, project.project_root, "models", "schema.yml")

        results = run_dbt(["compile"])


class TestMoreNewVersionedSchemaFile:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "foo.sql": foo_sql,
            "bar.sql": bar_sql,
            "foo.yml": foo_yml,
            "bar.yml": bar_yml,
        }

    def test_more_schema_file_new_versions(self, project):

        # initial run
        results = run_dbt(["compile"])
        assert len(results) == 3

        rm_file(project.project_root, "models", "foo.sql")
        write_file(foo_sql, project.project_root, "models", "foo_v1.sql")
        write_file(foo_sql, project.project_root, "models", "foo_v2.sql")
        write_file(foo_alt_yml, project.project_root, "models", "foo.yml")

        results = run_dbt(["compile"])


sources_yml = """
sources:
  - name: top_source
    tables:
      - name: abcd
      - name: efgh
      - name: ijkl
"""

abcd_sql = """
select * from {{ source("top_source", "abcd") }}
"""

efgh_sql = """
select * from {{ source("top_source", "efgh") }}
"""

ijkl_sql = """
select * from {{ source("top_source", "ijkl") }}
"""

models_yml = """
models:
  - name: abcd
    description: "abcd model"
  - name: efgh
    description: "efgh model"
  - name: ijkl
    description: "ijkl model"
"""

append_sources_yml = """
      - name: mnop
"""

append_models_yml = """
  - name: mnop
    description: "mnop model"
"""

mnop_sql = """
select * from {{ source("top_source", "mnop") }}
"""


class TestSourcesAndSchemaFiles:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sources.yml": sources_yml,
            "abcd.sql": abcd_sql,
            "efgh.sql": efgh_sql,
            "ijkl.sql": ijkl_sql,
            "_models.yml": models_yml,
        }

    def test_schema_file_order_new_versions(self, project):

        # initial run
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes) == 3

        write_file(models_yml + append_models_yml, project.project_root, "models", "_models.yml")
        write_file(mnop_sql, project.project_root, "models", "mnop.sql")
        write_file(sources_yml + append_sources_yml, project.project_root, "models", "sources.yml")

        manifest = run_dbt(["parse"])
        assert len(manifest.nodes) == 4

        # Without the fix the three original nodes will all be missing the
        # the patch updates, including description, so description will be ""
        for node in manifest.nodes.values():
            assert node.description == f"{node.name} model"
