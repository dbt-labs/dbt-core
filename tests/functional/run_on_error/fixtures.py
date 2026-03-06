import yaml

from dbt.tests.fixtures.project import write_project_files
from dbt.tests.util import run_dbt

models__parent_success_sql = """
{{ config(materialized='table') }}

select 1 as id
"""

models__parent_error_sql = """
{{ config(materialized='table') }}

select 1 / 0 as id
"""

models__child_sql = """
{{ config(materialized='table') }}

select * from {{ ref('parent') }}
"""

schema_skip_children_yml = """
models:
  - name: parent
    config:
      on_error: skip_children
"""

schema_continue_yml = """
models:
  - name: parent
    config:
      on_error: continue
"""

models__parent1_success_sql = """
{{ config(materialized='table') }}

select 1 as id
"""

models__parent1_error_sql = """
{{ config(materialized='table') }}

select 1 / 0 as id
"""

models__parent2_success_sql = """
{{ config(materialized='table') }}

select 2 as id
"""

models__parent2_error_sql = """
{{ config(materialized='table') }}

select 1 / 0 as id
"""

models__child_two_parents_sql = """
{{ config(materialized='table') }}

select id from {{ ref('parent1') }}
union all
select id from {{ ref('parent2') }}
"""

schema_two_parents_yml = """
models:
  - name: parent1
    config:
      on_error: skip_children
  - name: parent2
    config:
      on_error: continue
"""

dep_parent_success_sql = """
{{ config(materialized='table') }}

select 1 as id
"""

dep_parent_error_sql = """
{{ config(materialized='table') }}

select 1 / 0 as id
"""

dep_parent1_success_sql = """
{{ config(materialized='table') }}

select 1 as id
"""

dep_parent1_error_sql = """
{{ config(materialized='table') }}

select 1 / 0 as id
"""

dep_parent2_success_sql = """
{{ config(materialized='table') }}

select 2 as id
"""

dep_parent2_error_sql = """
{{ config(materialized='table') }}

select 1 / 0 as id
"""

dep_schema_skip_children_yml = """
models:
  - name: parent
    config:
      on_error: skip_children
"""

dep_schema_continue_yml = """
models:
  - name: parent
    config:
      on_error: continue
"""

dep_schema_parent1_yml = """
models:
  - name: parent1
    config:
      on_error: skip_children
"""

schema_parent2_yml = """
models:
  - name: parent2
    config:
      on_error: continue
"""

child_sql = """
{{ config(materialized='table') }}

select * from {{ ref('on_error_dep', 'parent') }}
"""

child_two_parents_sql = """
{{ config(materialized='table') }}

select id from {{ ref('on_error_dep', 'parent1') }}
union all
select id from {{ ref('parent2') }}
"""


def write_dependency(project_root, models_dict, schema_yml=None):
    dep_models = dict(models_dict)
    if schema_yml:
        dep_models["schema.yml"] = schema_yml
    write_project_files(
        project_root,
        "on_error_dep",
        {
            "dbt_project.yml": yaml.safe_dump(
                {
                    "name": "on_error_dep",
                    "version": "1.0",
                    "config-version": 2,
                    "model-paths": ["models"],
                }
            ),
            "packages.yml": yaml.safe_dump({"packages": []}),
            "models": dep_models,
        },
    )


def run_with_deps(*args, expect_pass=True):
    run_dbt(["deps"])
    run_args = ["run"]
    if args:
        run_args.extend(args)
    return run_dbt(run_args, *args, expect_pass=expect_pass)
