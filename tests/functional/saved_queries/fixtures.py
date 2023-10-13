saved_query_description = """
{% docs saved_query_description %} My SavedQuery Description {% enddocs %}
"""

saved_queries_yml = """
version: 2

saved_queries:
  - name: test_saved_query
    description: "{{ doc('saved_query_description') }}"
    label: Test Saved Query
    metrics:
        - simple_metric
    group_bys:
        - "Dimension('user__ds')"
    where:
        - "{{ Dimension('user__ds', 'DAY') }} <= now()"
        - "{{ Dimension('user__ds', 'DAY') }} >= '2023-01-01'"
"""
