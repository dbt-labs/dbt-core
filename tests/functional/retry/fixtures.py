models__sample_model = """select 1 as id, baz as foo"""
models__second_model = """select 1 as id, 2 as bar"""

models__union_model = """
select foo + bar as sum3 from {{ ref('sample_model') }}
left join {{ ref('second_model') }} on sample_model.id = second_model.id
"""

schema_yml = """
models:
  - name: sample_model
    columns:
      - name: foo
        tests:
          - accepted_values:
              values: [3]
              quote: false
              config:
                severity: warn
  - name: second_model
    columns:
      - name: bar
        tests:
          - accepted_values:
              values: [3]
              quote: false
              config:
                severity: warn
  - name: union_model
    columns:
      - name: sum3
        tests:
          - accepted_values:
              values: [3]
              quote: false
"""

macros__alter_timezone_sql = """
{% macro alter_timezone(timezone='America/Los_Angeles') %}
{% set sql %}
    SET TimeZone='{{ timezone }}';
{% endset %}

{% do run_query(sql) %}
{% do log("Timezone set to: " + timezone, info=True) %}
{% endmacro %}
"""

simple_model = """
select null as id
"""

simple_schema = """
models:
  - name: some_model
    columns:
      - name: id
        tests:
          - not_null
"""
