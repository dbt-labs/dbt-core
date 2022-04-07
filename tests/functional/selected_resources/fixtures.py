on_run_start_macro_assert_selected_models_expected_list = """
{% macro assert_selected_models_expected_list(expected_list) %}

  {% if execute and (expected_list is not none) %}

    {% set sorted_selected_resources = selected_resources | sort %}
    {% set sorted_expected_list = expected_list | sort %}

    {% if sorted_selected_resources != sorted_expected_list %}
      {{ exceptions.raise_compiler_error("FAIL: sorted_selected_resources" ~ sorted_selected_resources ~ " is different from " ~ sorted_expected_list) }}
    {% endif %}

  {% endif %}

{% endmacro %}
"""


my_model1 = """
select 1 as id
"""

my_model2 = """
select * from {{ ref('model1') }}
"""

my_snapshot = """
{% snapshot cc_all_snapshot %}
    {{ config(
        check_cols='all', unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select * from {{ ref('model2') }}
{% endsnapshot %}
"""
