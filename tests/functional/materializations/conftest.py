import pytest
from dbt.tests.fixtures.project import write_project_files


override_view_adapter_pass_dep__dbt_project_yml = """
name: view_adapter_override
version: '1.0'
macro-paths: ['macros']
config-version: 2

"""

override_view_adapter_pass_dep__macros__override_view_sql = """
{# copy+pasting the default view impl #}
{% materialization view, default %}

  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set backup_identifier = model['name'] + '__dbt_backup' -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database,
                                                type='view') -%}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema, database=database, type='view') -%}

  /*
     This relation (probably) doesn't exist yet. If it does exist, it's a leftover from
     a previous run, and we're going to try to drop it immediately. At the end of this
     materialization, we're going to rename the "old_relation" to this identifier,
     and then we're going to drop it. In order to make sure we run the correct one of:
       - drop view ...
       - drop table ...

     We need to set the type of this relation to be the type of the old_relation, if it exists,
     or else "view" as a sane default if it does not. Note that if the old_relation does not
     exist, then there is nothing to move out of the way and subsequentally drop. In that case,
     this relation will be effectively unused.
  */
  {%- set backup_relation_type = 'view' if old_relation is none else old_relation.type -%}
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema, database=database,
                                                type=backup_relation_type) -%}

  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exists for some reason
  {{ adapter.drop_relation(intermediate_relation) }}
  {{ adapter.drop_relation(backup_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ create_view_as(intermediate_relation, sql) }}
  {%- endcall %}

  -- cleanup
  -- move the existing view out of the way
  {% if old_relation is not none %}
    {{ adapter.rename_relation(target_relation, backup_relation) }}
  {% endif %}
  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}

"""

override_view_adapter_macros__override_view_sql = """
{%- materialization view, adapter='postgres' -%}
{{ exceptions.raise_compiler_error('intentionally raising an error in the postgres view materialization') }}
{%- endmaterialization -%}

{# copy+pasting the default view impl #}
{% materialization view, default %}

  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set backup_identifier = model['name'] + '__dbt_backup' -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database,
                                                type='view') -%}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema, database=database, type='view') -%}

  /*
     This relation (probably) doesn't exist yet. If it does exist, it's a leftover from
     a previous run, and we're going to try to drop it immediately. At the end of this
     materialization, we're going to rename the "old_relation" to this identifier,
     and then we're going to drop it. In order to make sure we run the correct one of:
       - drop view ...
       - drop table ...

     We need to set the type of this relation to be the type of the old_relation, if it exists,
     or else "view" as a sane default if it does not. Note that if the old_relation does not
     exist, then there is nothing to move out of the way and subsequentally drop. In that case,
     this relation will be effectively unused.
  */
  {%- set backup_relation_type = 'view' if old_relation is none else old_relation.type -%}
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema, database=database,
                                                type=backup_relation_type) -%}

  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exists for some reason
  {{ adapter.drop_relation(intermediate_relation) }}
  {{ adapter.drop_relation(backup_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ create_view_as(intermediate_relation, sql) }}
  {%- endcall %}

  -- cleanup
  -- move the existing view out of the way
  {% if old_relation is not none %}
    {{ adapter.rename_relation(target_relation, backup_relation) }}
  {% endif %}
  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}

"""

override_view_adapter_dep__dbt_project_yml = """
name: view_adapter_override
version: '1.0'
macro-paths: ['macros']
config-version: 2

"""

override_view_adapter_dep__macros__override_view_sql = """
{%- materialization view, adapter='postgres' -%}
{{ exceptions.raise_compiler_error('intentionally raising an error in the postgres view materialization') }}
{%- endmaterialization -%}

{# copy+pasting the default view impl #}
{% materialization view, default %}

  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set backup_identifier = model['name'] + '__dbt_backup' -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database,
                                                type='view') -%}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema, database=database, type='view') -%}

  /*
     This relation (probably) doesn't exist yet. If it does exist, it's a leftover from
     a previous run, and we're going to try to drop it immediately. At the end of this
     materialization, we're going to rename the "old_relation" to this identifier,
     and then we're going to drop it. In order to make sure we run the correct one of:
       - drop view ...
       - drop table ...

     We need to set the type of this relation to be the type of the old_relation, if it exists,
     or else "view" as a sane default if it does not. Note that if the old_relation does not
     exist, then there is nothing to move out of the way and subsequentally drop. In that case,
     this relation will be effectively unused.
  */
  {%- set backup_relation_type = 'view' if old_relation is none else old_relation.type -%}
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema, database=database,
                                                type=backup_relation_type) -%}

  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exists for some reason
  {{ adapter.drop_relation(intermediate_relation) }}
  {{ adapter.drop_relation(backup_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ create_view_as(intermediate_relation, sql) }}
  {%- endcall %}

  -- cleanup
  -- move the existing view out of the way
  {% if old_relation is not none %}
    {{ adapter.rename_relation(target_relation, backup_relation) }}
  {% endif %}
  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}

"""

override_view_default_dep__dbt_project_yml = """
name: view_default_override
config-version: 2
version: '1.0'
macro-paths: ['macros']

"""

override_view_default_dep__macros__default_view_sql = """
{%- materialization view, default -%}
{{ exceptions.raise_compiler_error('intentionally raising an error in the default view materialization') }}
{%- endmaterialization -%}

"""

override_view_return_no_relation__dbt_project_yml = """
name: view_adapter_override
version: 2
macro-paths: ['macros']
config-version: 2

"""

override_view_return_no_relation__macros__override_view_sql = """
{# copy+pasting the default view impl #}
{% materialization view, default %}

  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set backup_identifier = model['name'] + '__dbt_backup' -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database,
                                                type='view') -%}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema, database=database, type='view') -%}

  /*
     This relation (probably) doesn't exist yet. If it does exist, it's a leftover from
     a previous run, and we're going to try to drop it immediately. At the end of this
     materialization, we're going to rename the "old_relation" to this identifier,
     and then we're going to drop it. In order to make sure we run the correct one of:
       - drop view ...
       - drop table ...

     We need to set the type of this relation to be the type of the old_relation, if it exists,
     or else "view" as a sane default if it does not. Note that if the old_relation does not
     exist, then there is nothing to move out of the way and subsequentally drop. In that case,
     this relation will be effectively unused.
  */
  {%- set backup_relation_type = 'view' if old_relation is none else old_relation.type -%}
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema, database=database,
                                                type=backup_relation_type) -%}

  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exists for some reason
  {{ adapter.drop_relation(intermediate_relation) }}
  {{ adapter.drop_relation(backup_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ create_view_as(intermediate_relation, sql) }}
  {%- endcall %}

  -- cleanup
  -- move the existing view out of the way
  {% if old_relation is not none %}
    {{ adapter.rename_relation(target_relation, backup_relation) }}
  {% endif %}
  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {# do not return anything! #}
  {# {{ return({'relations': [target_relation]}) }} #}

{%- endmaterialization -%}
"""


@pytest.fixture(scope="class")
def override_view_adapter_pass_dep(project_root):
    files = {
        "dbt_project.yml": override_view_adapter_pass_dep__dbt_project_yml,
        "macros": {"override_view.sql": override_view_adapter_pass_dep__macros__override_view_sql},
    }
    write_project_files(project_root, "override-view-adapter-pass-dep", files)


@pytest.fixture(scope="class")
def override_view_adapter_macros(project_root):
    files = {"override_view.sql": override_view_adapter_macros__override_view_sql}
    write_project_files(project_root, "override-view-adapter-macros", files)


@pytest.fixture(scope="class")
def override_view_adapter_dep(project_root):
    files = {
        "dbt_project.yml": override_view_adapter_dep__dbt_project_yml,
        "macros": {"override_view.sql": override_view_adapter_dep__macros__override_view_sql},
    }
    write_project_files(project_root, "override-view-adapter-dep", files)


@pytest.fixture(scope="class")
def override_view_default_dep(project_root):
    files = {
        "dbt_project.yml": override_view_default_dep__dbt_project_yml,
        "macros": {"default_view.sql": override_view_default_dep__macros__default_view_sql},
    }
    write_project_files(project_root, "override-view-default-dep", files)


@pytest.fixture(scope="class")
def override_view_return_no_relation(project_root):
    files = {
        "dbt_project.yml": override_view_return_no_relation__dbt_project_yml,
        "macros": {
            "override_view.sql": override_view_return_no_relation__macros__override_view_sql
        },
    }
    write_project_files(project_root, "override-view-return-no-relation", files)
