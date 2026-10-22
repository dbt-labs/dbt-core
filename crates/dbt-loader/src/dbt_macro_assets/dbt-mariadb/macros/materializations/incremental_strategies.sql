{#- MariaDB rejects an alias on single-table DELETE, and the multi-table form
    with an alias fails without a default database. So delete without an
    alias, and point DBT_INTERNAL_DEST references in incremental_predicates at
    the fully qualified target. -#}
{% macro mariadb__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is string %}
        {% set unique_key = [unique_key] %}
        {% endif %}

        {%- set unique_key_str = unique_key|join(', ') -%}

        delete from {{ target }}
        where ({{ unique_key_str }}) in (
            select distinct {{ unique_key_str }}
            from {{ source }} as DBT_INTERNAL_SOURCE
        )
        {%- if incremental_predicates %}
            {% for predicate in incremental_predicates %}
                and {{ predicate | replace('DBT_INTERNAL_DEST.', target ~ '.') }}
            {% endfor %}
        {%- endif -%};

    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}
