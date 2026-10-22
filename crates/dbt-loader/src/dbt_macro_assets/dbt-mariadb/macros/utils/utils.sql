{#- MariaDB treats `||` as logical OR (unless PIPES_AS_CONCAT is set) and only
    accepts CHAR, not VARCHAR, as a CAST target, so the Postgres-flavored
    defaults need MariaDB spellings. -#}

{% macro mariadb__concat(fields) -%}
    concat({{ fields|join(', ') }})
{%- endmacro %}

{% macro mariadb__hash(field) -%}
    md5(cast({{ field }} as char))
{%- endmacro %}

{% macro mariadb__snapshot_hash_arguments(args) -%}
    md5(concat_ws('|', {%- for arg in args -%}
        coalesce(cast({{ arg }} as char), '')
        {%- if not loop.last %}, {% endif -%}
    {%- endfor -%}))
{%- endmacro %}

{% macro mariadb__cast_bool_to_text(field) -%}
    case when {{ field }} then 'true' when not {{ field }} then 'false' end
{%- endmacro %}

{% macro mariadb__safe_cast(field, type) -%}
    {#- MariaDB CAST returns NULL (with a warning) instead of failing. -#}
    cast({{ field }} as {{ type }})
{%- endmacro %}

{% macro mariadb__dateadd(datepart, interval, from_date_or_timestamp) -%}
    date_add({{ from_date_or_timestamp }}, interval {{ interval }} {{ datepart }})
{%- endmacro %}

{% macro mariadb__datediff(first_date, second_date, datepart) -%}
    timestampdiff({{ datepart }}, {{ first_date }}, {{ second_date }})
{%- endmacro %}

{% macro mariadb__date_trunc(datepart, date) -%}
    {%- set datepart = datepart | lower -%}
    {%- if datepart == 'year' -%}
        cast(date_format({{ date }}, '%Y-01-01') as datetime)
    {%- elif datepart == 'quarter' -%}
        cast(makedate(year({{ date }}), 1) + interval (quarter({{ date }}) - 1) quarter as datetime)
    {%- elif datepart == 'month' -%}
        cast(date_format({{ date }}, '%Y-%m-01') as datetime)
    {%- elif datepart == 'week' -%}
        cast(date({{ date }}) - interval weekday({{ date }}) day as datetime)
    {%- elif datepart == 'day' -%}
        cast(date({{ date }}) as datetime)
    {%- elif datepart == 'hour' -%}
        cast(date_format({{ date }}, '%Y-%m-%d %H:00:00') as datetime)
    {%- elif datepart == 'minute' -%}
        cast(date_format({{ date }}, '%Y-%m-%d %H:%i:00') as datetime)
    {%- elif datepart == 'second' -%}
        cast(date_format({{ date }}, '%Y-%m-%d %H:%i:%s') as datetime)
    {%- else -%}
        {{ exceptions.raise_compiler_error("Unsupported datepart for date_trunc on MariaDB: " ~ datepart) }}
    {%- endif -%}
{%- endmacro %}

{% macro mariadb__last_day(date, datepart) -%}
    {%- if datepart | lower == 'month' -%}
        last_day({{ date }})
    {%- else -%}
        {{ dbt.default_last_day(date, datepart) }}
    {%- endif -%}
{%- endmacro %}

{% macro mariadb__split_part(string_text, delimiter_text, part_number) -%}
    {%- if part_number >= 0 -%}
        substring_index(substring_index({{ string_text }}, {{ delimiter_text }}, {{ part_number }}), {{ delimiter_text }}, -1)
    {%- else -%}
        substring_index(substring_index({{ string_text }}, {{ delimiter_text }}, {{ part_number }}), {{ delimiter_text }}, 1)
    {%- endif -%}
{%- endmacro %}

{% macro mariadb__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}
    group_concat(
        {{ measure }}
        {% if order_by_clause -%} {{ order_by_clause }} {%- endif %}
        separator {{ delimiter_text }}
        {% if limit_num -%} limit {{ limit_num }} {%- endif %}
    )
{%- endmacro %}
