models__my_model_sql = """
with my_cte as (
    select 1 as id, 'blue' as color
    union all
    select 2 as id, 'green' as red
    union all
    select 3 as id, 'red' as red
)
select * from my_cte
"""

models__schema_yml = """
models:
  - name: my_model
    columns:
      - name: id
        tests:
          - unique:
              description: "id must be unique"
          - not_null
      - name: color
        tests:
          - accepted_values:
              values: ['blue', 'green', 'red']
              description: "{{ doc('color_accepted_values') }}"
"""

models__doc_block_md = """
{% docs color_accepted_values %}

The `color` column must be one of 'blue', 'green', or 'red'.

{% enddocs %}
"""
