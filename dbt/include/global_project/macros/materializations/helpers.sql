{% macro run_hooks(hooks, inside_transaction=True) %}
  {% for hook in hooks | selectattr('transaction', 'equalto', inside_transaction)  %}
    {% call statement(auto_begin=inside_transaction) %}
      {{ hook.get('sql') }}
    {% endcall %}
  {% endfor %}
{% endmacro %}


{% macro column_list(columns) %}
  {%- for col in columns %}
    "{{ col.name }}" {% if not loop.last %},{% endif %}
  {% endfor -%}
{% endmacro %}


{% macro column_list_for_create_table(columns) %}
  {%- for col in columns %}
    "{{ col.name }}" {{ col.data_type }} {%- if not loop.last %},{% endif %}
  {% endfor -%}
{% endmacro %}


{% macro make_hook_config(sql, inside_transaction) %}
    {{ {"sql": sql, "transaction": inside_transaction} | tojson }}
{% endmacro %}


{% macro before_begin(sql) %}
    {{ make_hook_config(sql, inside_transaction=False) }}
{% endmacro %}


{% macro in_transaction(sql) %}
    {{ make_hook_config(sql, inside_transaction=True) }}
{% endmacro %}


{% macro after_commit(sql) %}
    {{ make_hook_config(sql, inside_transaction=False) }}
{% endmacro %}


{% macro drop_if_exists(existing, name) %}
  {% set existing_type = existing.get(name) %}
  {% if existing_type is not none %}
    {{ adapter.drop(name, existing_type) }}
  {% endif %}
{% endmacro %}
