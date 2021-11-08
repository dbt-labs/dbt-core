/* {#
    Helper macros for internal use with incremental materializations.
    Use with care if calling elsewhere.
#} */


{% macro get_quoted_csv(column_names) %}
    
    {% set quoted = [] %}
    {% for col in column_names -%}
        {%- do quoted.append(adapter.quote(col)) -%}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}

{% endmacro %}


{% macro diff_columns(source_columns, target_columns) %}

  {% set result = [] %}
  {% set source_names = source_columns | map(attribute = 'column') | list %}
  {% set target_names = target_columns | map(attribute = 'column') | list %}
   
   {# --check whether the name attribute exists in the target - this does not perform a data type check #}
   {% for sc in source_columns %}
     {% if sc.name not in target_names %}
        {{ result.append(sc) }}
     {% endif %}
   {% endfor %}
  
  {{ return(result) }}

{% endmacro %}


{% macro diff_column_data_types(source_columns, target_columns) %}
  
  {% set result = [] %}
  {% for sc in source_columns %}
    {% set tc = target_columns | selectattr("name", "equalto", sc.name) | list | first %}
    {% if tc %}
      {% if sc.data_type != tc.data_type %}
        {{ result.append( { 'column_name': tc.name, 'new_type': sc.data_type } ) }} 
      {% endif %}
    {% endif %}
  {% endfor %}

  {{ return(result) }}

{% endmacro %}