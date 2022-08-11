macros__before_and_after = """
{% macro custom_run_hook(state, target, run_started_at, invocation_id) %}

   insert into {{ target.schema }}.on_run_hook (
        "state",
        "target.dbname",
        "target.host",
        "target.name",
        "target.schema",
        "target.type",
        "target.user",
        "target.pass",
        "target.port",
        "target.threads",
        "run_started_at",
        "invocation_id"
   ) VALUES (
    '{{ state }}',
    '{{ target.dbname }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.port }},
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )

{% endmacro %}
"""

macros__hook = """
{% macro hook() %}
  select 1
{% endmacro %}
"""

models__hooks = """
select 1 as id
"""

seeds__example_seed_csv = """a,b,c
1,2,3
4,5,6
7,8,9
"""
