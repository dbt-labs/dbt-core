{% macro databricks__get_incremental_default_sql(arg_dict) %}
  {{ return(get_incremental_merge_sql(arg_dict)) }}
{% endmacro %}

{% macro databricks__get_incremental_append_sql(arg_dict) %}
  {% do return(get_insert_into_sql(arg_dict["temp_relation"], arg_dict["target_relation"])) %}
{% endmacro %}

{% macro databricks__get_incremental_replace_where_sql(arg_dict) %}
  {% do return(get_replace_where_sql(arg_dict)) %}
{% endmacro %}

{% macro get_incremental_replace_where_sql(arg_dict) %}

  {{ return(adapter.dispatch('get_incremental_replace_where_sql', 'dbt')(arg_dict)) }}

{% endmacro %}

{% macro databricks__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) %}
    {{ return(get_insert_overwrite_sql(source, target)) }}
{% endmacro %}


{% macro get_insert_overwrite_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert overwrite table {{ target_relation }}
    {{ partition_cols(label="partition") }}
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}

{% macro get_replace_where_sql(args_dict) -%}
    {%- set predicates = args_dict['incremental_predicates'] -%}
    {%- set target_relation = args_dict['target_relation'] -%}
    {%- set temp_relation = args_dict['temp_relation'] -%}

    insert into {{ target_relation }}
    {% if predicates %}
      {% if predicates is sequence and predicates is not string %}
    replace where {{ predicates | join(' and ') }}
      {% else %}
    replace where {{ predicates }}
      {% endif %}
    {% endif %}
    table {{ temp_relation }}

{% endmacro %}

{% macro get_insert_into_sql(source_relation, target_relation) %}
    {%- set source_columns = adapter.get_columns_in_relation(source_relation) | map(attribute="quoted") | list -%}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) | map(attribute="quoted") | list -%}
    {{ insert_into_sql_impl(target_relation, dest_columns, source_relation, source_columns) }}
{% endmacro %}

{% macro insert_into_sql_impl(target_relation, dest_columns, source_relation, source_columns) %}
    {%- set common_columns = [] -%}
    {%- for dest_col in dest_columns -%}
      {%- if dest_col in source_columns -%}
        {%- do common_columns.append(dest_col) -%}
      {%- else -%}
        {%- do common_columns.append('DEFAULT') -%}
      {%- endif -%}
    {%- endfor -%}
    {%- set dest_cols_csv = dest_columns | join(', ') -%}
    {%- set source_cols_csv = common_columns | join(', ') -%}
insert into table {{ target_relation }} ({{ dest_cols_csv }})
select {{source_cols_csv}} from {{ source_relation }}
{%- endmacro %}

{% macro databricks__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
  {# need dest_columns for merge_exclude_columns, default to use "*" #}

  {%- set target_alias = config.get('target_alias', 'DBT_INTERNAL_DEST') -%}
  {%- set source_alias = config.get('source_alias', 'DBT_INTERNAL_SOURCE') -%}

  {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set source_columns = (adapter.get_columns_in_relation(source) | map(attribute='quoted') | list)-%}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set merge_with_schema_evolution = (config.get('merge_with_schema_evolution') | lower == 'true') -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
  {%- set skip_matched_step = (config.get('skip_matched_step') | lower == 'true') -%}
  {%- set skip_not_matched_step = (config.get('skip_not_matched_step') | lower == 'true') -%}

  {%- set matched_condition = config.get('matched_condition') -%}
  {%- set not_matched_condition = config.get('not_matched_condition') -%}

  {%- set not_matched_by_source_action = config.get('not_matched_by_source_action') -%}
  {%- set not_matched_by_source_condition = config.get('not_matched_by_source_condition') -%}

  {%- set not_matched_by_source_action_trimmed = not_matched_by_source_action | lower | trim(' \n\t') %}
  {%- set not_matched_by_source_action_is_set = (
      not_matched_by_source_action_trimmed == 'delete'
      or not_matched_by_source_action_trimmed.startswith('update')
    )
  %}
  
  
  {% if unique_key %}
      {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
          {% for key in unique_key %}
              {% set this_key_match %}
                  {{ source_alias }}.{{ key }} <=> {{ target_alias }}.{{ key }}
              {% endset %}
              {% do predicates.append(this_key_match) %}
          {% endfor %}
      {% else %}
          {% set unique_key_match %}
              {{ source_alias }}.{{ unique_key }} <=> {{ target_alias }}.{{ unique_key }}
          {% endset %}
          {% do predicates.append(unique_key_match) %}
      {% endif %}
  {% else %}
      {% do predicates.append('FALSE') %}
  {% endif %}

    merge
        {%- if merge_with_schema_evolution %}
        with schema evolution
        {%- endif %}
    into
        {{ target }} as {{ target_alias }}
    using
        {{ source }} as {{ source_alias }}
    on
        {{ predicates | join('\n    and ') }}
    {%- if not skip_matched_step %}
    when matched
        {%- if matched_condition %}
        and ({{ matched_condition }})
        {%- endif %}
        then update set
            {{ get_merge_update_set(update_columns, on_schema_change, source_columns, source_alias) }}
    {%- endif %}
    {%- if not skip_not_matched_step %}
    when not matched
        {%- if not_matched_condition %}
        and ({{ not_matched_condition }})
        {%- endif %}
        then insert
            {{ get_merge_insert(on_schema_change, source_columns, source_alias) }}
    {%- endif %}
    {%- if not_matched_by_source_action_is_set %}
    when not matched by source
        {%- if not_matched_by_source_condition %}
        and ({{ not_matched_by_source_condition }})
        {%- endif %}
        then {{ not_matched_by_source_action }}
    {%- endif %}
{% endmacro %}

{% macro get_merge_update_set(update_columns, on_schema_change, source_columns, source_alias='DBT_INTERNAL_SOURCE') %}
  {%- if update_columns -%}
    {%- for column_name in update_columns -%}
      {{ column_name }} = {{ source_alias }}.{{ column_name }}{%- if not loop.last %}, {% endif -%}
    {%- endfor %}
  {%- elif on_schema_change == 'ignore' -%}
    *
  {%- else -%}
    {%- for column in source_columns -%}
      {{ column }} = {{ source_alias }}.{{ column }}{%- if not loop.last %}, {% endif -%}
    {%- endfor %}
  {%- endif -%}
{% endmacro %}

{% macro get_merge_insert(on_schema_change, source_columns, source_alias='DBT_INTERNAL_SOURCE') %}
  {%- if on_schema_change == 'ignore' -%}
    *
  {%- else -%}
    ({{ source_columns | join(", ") }}) VALUES (
    {%- for column in source_columns -%}
      {{ source_alias }}.{{ column }}{%- if not loop.last %}, {% endif -%}
    {%- endfor %})
  {%- endif -%}
{% endmacro %}

{% macro databricks__get_incremental_microbatch_sql(arg_dict) %}
  {%- set incremental_predicates = [] if arg_dict.get('incremental_predicates') is none else arg_dict.get('incremental_predicates') -%}
  {%- set event_time = model.config.event_time -%}
  {%- set start_time = config.get("__dbt_internal_microbatch_event_time_start") -%}
  {%- set end_time = config.get("__dbt_internal_microbatch_event_time_end") -%}
  {%- if start_time -%}
    {%- do incremental_predicates.append("cast(" ~ event_time ~ " as TIMESTAMP) >= '" ~ start_time ~ "'") -%}
  {%- endif -%}
  {%- if end_time -%}
    {%- do incremental_predicates.append("cast(" ~ event_time ~ " as TIMESTAMP) < '" ~ end_time ~ "'") -%}
  {%- endif -%}
  {%- do arg_dict.update({'incremental_predicates': incremental_predicates}) -%}
  {{ return(get_replace_where_sql(arg_dict)) }}
{% endmacro %}