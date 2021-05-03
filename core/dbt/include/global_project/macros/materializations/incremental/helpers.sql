
{% macro incremental_upsert(tmp_relation, target_relation, unique_key=none, statement_name="main", incremental_predicates=none) %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    {%- if unique_key is not none -%}
    delete
    from {{ target_relation }}
    where ({{ unique_key }}) in (
        select ({{ unique_key }})
        from {{ tmp_relation }}
    )
    {% if incremental_predicates %} and {{ incremental_predicates | join(' and ') }} {% endif %};
    {%- endif %}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}
    );
{%- endmacro %}
