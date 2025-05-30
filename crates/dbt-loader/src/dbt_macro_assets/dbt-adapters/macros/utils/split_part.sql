{% macro split_part(string_text, delimiter_text, part_number) %}
  {{ return(adapter.dispatch('split_part', 'dbt') (string_text, delimiter_text, part_number)) }}
{% endmacro %}

{% macro default__split_part(string_text, delimiter_text, part_number) %}

    split_part(
        {{ string_text }},
        {{ delimiter_text }},
        {{ part_number }}
        )

{% endmacro %}

{% macro _split_part_negative(string_text, delimiter_text, part_number) %}

    split_part(
        {{ string_text }},
        {{ delimiter_text }},
          length({{ string_text }})
          - length(
              replace({{ string_text }},  {{ delimiter_text }}, '')
          ) + 2 + {{ part_number }}
        )

{% endmacro %}
