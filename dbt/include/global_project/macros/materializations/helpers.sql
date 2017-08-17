{% macro run_hooks(hooks, inside_transaction=True) %}
  {% for hook in hooks %}
    {%- set hook_data = fromjson(render(hook), {}) -%}
    {%- set hook_is_in_transaction = hook_data.get('transaction', True) -%};
    {%- set hook_sql = hook_data.get('sql', hook) -%};

    {%- if hook_is_in_transaction == inside_transaction -%}
      {% call statement(auto_begin=inside_transaction) %}
        {{ hook_sql }}
      {% endcall %}
    {%- endif -%}
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


{% macro after_commit(sql) %}
    {{ make_hook_config(sql, inside_transaction=False) }}
{% endmacro %}


{% macro vacuum(tbl) %}
    {{ after_commit('vacuum ' ~ adapter.quote_schema_and_table(tbl.schema, tbl.name)) }}
{% endmacro %}
