
{%- macro get_max_sql(relation, field, require_partition_filter=False) -%}

    select max({{ field }}) as start_ts
    from {{ relation }}
    {% if require_partition_filter -%}
    where {{ field }} is null or {{ field }} is not null
    {%- endif %}

{%- endmacro -%}


{%- macro get_most_recent_record(relation, field, require_partition_filter=False) -%}

    {%- set result = run_query(get_max_sql(relation, field, require_partition_filter)) -%}

    {% if execute %}
        {% set start_ts = result.columns['start_ts'].values()[0] %}
    {% else %}
        {% set start_ts = '' %}
    {% endif %}

    {{ return(start_ts) }}

{%- endmacro -%}
