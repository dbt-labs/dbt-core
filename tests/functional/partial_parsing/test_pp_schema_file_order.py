import os
from pathlib import Path

import pytest

from dbt.exceptions import ParsingError
from dbt.tests.util import get_manifest, run_dbt, run_dbt_and_capture, write_file

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
        assert model.description == "model with reference to another ref"
