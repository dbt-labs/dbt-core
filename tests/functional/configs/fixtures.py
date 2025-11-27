import pytest

models__schema_yml = """
version: 2
sources:
  - name: raw
    database: "{{ target.database }}"
    schema: "{{ target.schema }}"
    tables:
      - name: 'seed'
        identifier: "{{ var('seed_name', 'invalid') }}"
        columns:
          - name: id
            data_tests:
              - unique:
                  enabled: "{{ var('enabled_direct', None) | as_native }}"
              - accepted_values:
                  enabled: "{{ var('enabled_direct', None) | as_native }}"
                  severity: "{{ var('severity_direct', None) | as_native }}"
                  values: [1,2]

models:
  - name: model
    columns:
      - name: id
        data_tests:
          - unique
          - accepted_values:
              values: [1,2,3,4]

"""

models__untagged_sql = """
{{
    config(materialized='table')
}}

select id, value from {{ source('raw', 'seed') }}

"""

models__tagged__model_sql = """
{{
    config(
        materialized='view',
        tags=['tag_two'],
    )
}}

{{
    config(
        materialized='table',
        tags=['tag_three'],
    )
}}

select 4 as id, 2 as value

"""

seeds__seed_csv = """id,value
4,2
"""

tests__failing_sql = """

select 1 as fun

"""

tests__sleeper_agent_sql = """
{{ config(
    enabled = var('enabled_direct', False),
    severity = var('severity_direct', 'WARN')
) }}

select 1 as fun

"""

my_model = """
select 1 as user
"""

my_model_2 = """
select * from {{ ref('my_model') }}
"""

my_model_3 = """
select * from {{ ref('my_model_2') }}
"""

my_model_2_disabled = """
{{ config(enabled=false) }}
select * from {{ ref('my_model') }}
"""

my_model_3_disabled = """
{{ config(enabled=false) }}
select * from {{ ref('my_model_2') }}
"""

my_model_2_enabled = """
{{ config(enabled=true) }}
select * from {{ ref('my_model') }}
"""

my_model_3_enabled = """
{{ config(enabled=true) }}
select * from {{ ref('my_model') }}
"""

schema_all_disabled_yml = """
version: 2
models:
  - name: my_model
  - name: my_model_2
    config:
      enabled: false
  - name: my_model_3
    config:
      enabled: false
"""

schema_explicit_enabled_yml = """
version: 2
models:
  - name: my_model
  - name: my_model_2
    config:
      enabled: true
  - name: my_model_3
    config:
      enabled: true
"""

schema_partial_disabled_yml = """
version: 2
models:
  - name: my_model
  - name: my_model_2
    config:
      enabled: false
  - name: my_model_3
"""

schema_partial_enabled_yml = """
version: 2
models:
  - name: my_model
  - name: my_model_2
    config:
      enabled: True
  - name: my_model_3
"""

schema_invalid_enabled_yml = """
version: 2
models:
  - name: my_model
    config:
      enabled: True and False
  - name: my_model_3
"""

simple_snapshot = """{% snapshot mysnapshot %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at='updated_at'
        )
    }}

    select * from dummy

{% endsnapshot %}"""


class BaseConfigProject:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "untagged.sql": models__untagged_sql,
            "tagged": {"model.sql": models__tagged__model_sql},
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "failing.sql": tests__failing_sql,
            "sleeper_agent.sql": tests__sleeper_agent_sql,
        }


# ========================================
# Fixtures for set_sql_header tests
# ========================================

# Fixtures for Issue #2793: Basic ref() in set_sql_header
models__test_tmp_1 = """
{{
    config(materialized="table")
}}

select 1 as key, 100 as value
"""

models__test_tmp_2 = """
{{
    config(materialized="table")
}}

{% call set_sql_header(config) %}
    select * from {{ ref('test_tmp_1') }};
{% endcall %}

select * from {{ ref('test_tmp_1') }}
"""


# Fixtures for Issue #3264: is_incremental() in set_sql_header
models__incremental_header = """
{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

{% call set_sql_header(config) %}
{% if is_incremental() %}
    -- This should only appear on incremental runs
    select 1 as incremental_marker;
{% else %}
    -- This should only appear on full refresh
    select 0 as full_refresh_marker;
{% endif %}
{% endcall %}

select 1 as id, 'initial' as status

{% if is_incremental() %}
union all
select 2 as id, 'incremental' as status
{% endif %}
"""


# Fixtures for Issue #4692: Nested macro calls
macros__custom_ref_macro = """
{% macro get_ref_in_macro(model_name) %}
    {{ return(ref(model_name)) }}
{% endmacro %}
"""

models__base_model = """
select 1 as id, 'base' as name
"""

models__nested_macro_header = """
{% call set_sql_header(config) %}
    select * from {{ get_ref_in_macro('base_model') }};
{% endcall %}

select * from {{ ref('base_model') }}
"""


# Fixtures for Issue #6058: source() in set_sql_header
seeds__source_seed = """id,name
1,alice
2,bob
"""

sources__schema_yml = """
version: 2

sources:
  - name: test_source
    schema: "{{ target.schema }}"
    tables:
      - name: source_seed
"""

models__source_in_header = """
{% call set_sql_header(config) %}
    select count(*) from {{ source('test_source', 'source_seed') }};
{% endcall %}

select * from {{ source('test_source', 'source_seed') }}
"""

# NOTE: source_in_set_block is NOT using set_sql_header, so it's out of scope for issue #2793
# It uses {% set %} blocks which is a different pattern with different resolution behavior
# Left here for reference but not tested
models__source_in_set_block = """
{% set my_source_query %}
    select count(*) from {{ source('test_source', 'source_seed') }}
{% endset %}

{{
    config(
        pre_hook=my_source_query
    )
}}

select * from {{ source('test_source', 'source_seed') }}
"""


# Fixtures for Issue #7151: this with custom generate_alias_name
macros__custom_alias = """
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name is not none -%}
        {{ return(custom_alias_name | trim) }}
    {%- else -%}
        {{ return('custom_' ~ node.name) }}
    {%- endif -%}
{%- endmacro %}
"""

models__this_with_alias = """
{% call set_sql_header(config) %}
    -- Reference to current model using this
    select 'header: ' || '{{ this }}' as header_this;
{% endcall %}

select '{{ this }}' as body_this, 1 as id
"""


# Fixtures for multiple refs in set_sql_header
models__ref_a = """
select 1 as id, 'a' as source
"""

models__ref_b = """
select 2 as id, 'b' as source
"""

models__multiple_refs_header = """
{% call set_sql_header(config) %}
    select * from {{ ref('ref_a') }};
    select * from {{ ref('ref_b') }};
{% endcall %}

select * from {{ ref('ref_a') }}
union all
select * from {{ ref('ref_b') }}
"""


# Fixtures for combination of ref, source, and this
models__combination_header = """
{% call set_sql_header(config) %}
    -- Using ref
    select count(*) from {{ ref('base_model') }};
    -- Using source
    select count(*) from {{ source('test_source', 'source_seed') }};
    -- Using this (should be current model)
    select '{{ this }}' as current_model;
{% endcall %}

select * from {{ ref('base_model') }}
"""


# Fixtures for different materializations
models__view_with_header = """
{{
    config(materialized='view')
}}

{% call set_sql_header(config) %}
    select 1 as view_header;
{% endcall %}

select * from {{ ref('base_model') }}
"""

models__ephemeral_with_header = """
{{
    config(materialized='ephemeral')
}}

{% call set_sql_header(config) %}
    select 1 as ephemeral_header;
{% endcall %}

select * from {{ ref('base_model') }}
"""


# Fixtures for Issue #2921: ref() with custom database/schema
models__custom_schema_model = """
{{
    config(
        schema='custom_schema'
    )
}}

select 1 as id, 'custom_schema' as source
"""

models__ref_custom_schema = """
{% call set_sql_header(config) %}
    -- Reference model with custom schema
    select count(*) from {{ ref('custom_schema_model') }};
{% endcall %}

select * from {{ ref('custom_schema_model') }}
"""


# Fixtures for comparison and boolean operators in set_sql_header
models__conditional_header = """
{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

{% call set_sql_header(config) %}
    {% if is_incremental() and var('enable_optimization', false) %}
        -- Boolean AND: Only runs on incremental with optimization enabled
        select 'incremental_and_optimized' as header_status, '{{ this }}' as target_table;
    {% endif %}
    {% if var('threshold', 0) > 50 %}
        -- Comparison operator: Only runs when threshold > 50
        select 'threshold_exceeded' as header_status, '{{ this }}' as target_table;
    {% endif %}
    {% if not is_incremental() or var('force_refresh', false) %}
        -- Boolean NOT and OR: Runs on full refresh or when force_refresh is true
        select 'full_refresh_or_forced' as header_status, '{{ this }}' as target_table;
    {% endif %}
{% endcall %}

select 1 as id, 'data' as value
{% if is_incremental() %}
union all
select 2 as id, 'incremental' as value
{% endif %}
"""
