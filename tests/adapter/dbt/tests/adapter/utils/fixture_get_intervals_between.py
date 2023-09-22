models__test_get_intervals_between_sql = """
SELECT
  {% if target.type == 'postgres' %}
    {{ get_intervals_between("'09/01/2023'::date", "'09/12/2023'::date", "day") }} as intervals,
  {% else %}
      {{ get_intervals_between("'09/01/2023'", "'09/12/2023'", "day") }} as intervals,
  {% endif %}
  11 as expected

"""

models__test_get_intervals_between_yml = """
version: 2
models:
  - name: test_get_intervals_between
    tests:
      - assert_equal:
          actual: intervals
          expected: expected
"""
