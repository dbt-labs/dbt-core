saved_query_description = """
{% docs saved_query_description %} My SavedQuery Description {% enddocs %}
"""

saved_queries_yml = """
version: 2

saved_queries:
  - name: test_saved_query
    description: "{{ doc('saved_query_description') }}"
    label: Test Saved Query
    query_params:
        metrics:
            - simple_metric
        group_by:
            - "Dimension('user__ds')"
        where:
            - "{{ Dimension('user__ds', 'DAY') }} <= now()"
            - "{{ Dimension('user__ds', 'DAY') }} >= '2023-01-01'"
    exports:
        - name: my_export
          config:
            alias: my_export_alias
            export_as: table
            schema: my_export_schema_name
"""
