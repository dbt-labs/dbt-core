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

saved_query_with_extra_config_attributes_yml = """
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
            my_random_config: 'I have this for some reason'
            export_as: table
"""

saved_query_with_export_configs_defined_at_saved_query_level_yml = """
version: 2

saved_queries:
  - name: test_saved_query
    description: "{{ doc('saved_query_description') }}"
    label: Test Saved Query
    config:
      export_as: table
      schema: my_default_export_schema
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
            export_as: view
            schema: my_custom_export_schema
        - name: my_export2
"""

saved_query_without_export_configs_defined_yml = """
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
"""

saved_query_with_cache_configs_defined_yml = """
saved_queries:
  - name: test_saved_query
    description: "{{ doc('saved_query_description') }}"
    label: Test Saved Query
    config:
      cache:
        enabled: True
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
