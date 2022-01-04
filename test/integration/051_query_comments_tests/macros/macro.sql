{%- macro query_header_no_args() -%}
{%- set x = "are pretty cool" -%}
{{ "dbt macros" }}
{{ x }}
{%- endmacro -%}


{%- macro query_header_args(message) -%}
  {%- set comment_dict = dict(
    app='dbt++',
    macro_version='0.1.0',
    dbt_version=dbt_version,
    message='blah: '~ message) -%}
  {{ return(comment_dict) }}
{%- endmacro -%}


{%- macro ordered_to_json(dct) -%}
{{ tojson(dct, sort_keys=True) }}
{%- endmacro %}


{% macro invalid_query_header() -%}
{{ "Here is an invalid character for you: */" }}
{% endmacro %}
