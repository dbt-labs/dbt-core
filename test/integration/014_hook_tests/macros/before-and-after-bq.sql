
{% macro custom_run_hook_bq(state, target, run_started_at, invocation_id) %}

   insert into {{ target.schema }}.on_run_hook (
        state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id
   ) VALUES (
    '{{ state }}',
    '{{ target.database }}',
    '', {# bigquery has no host value #}
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '', {# bigquery has no user value #}
    '{{ target.get("pass", "") }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )

{% endmacro %}
