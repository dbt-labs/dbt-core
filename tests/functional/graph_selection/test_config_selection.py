"""
Functional tests for config selector functionality.

These tests validate that config selectors work correctly after the change from
configurable_nodes() to all_nodes() in ConfigSelectorMethod.
"""

import pytest

from dbt.tests.util import run_dbt
from tests.functional.graph_selection.fixtures import (
    alternative_users_sql,
    base_users_sql,
    emails_alt_sql,
    emails_sql,
    nested_users_sql,
    never_selected_sql,
    schema_yml,
    subdir_sql,
    users_rollup_dependency_sql,
    users_rollup_sql,
    users_sql,
)


class TestConfigSelection:
    """Test config selectors work on all node types including newly supported ones."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "base_users.sql": base_users_sql,
            "users.sql": users_sql,
            "users_rollup.sql": users_rollup_sql,
            "versioned_v3.sql": base_users_sql,
            "users_rollup_dependency.sql": users_rollup_dependency_sql,
            "emails.sql": emails_sql,
            "emails_alt.sql": emails_alt_sql,
            "alternative.users.sql": alternative_users_sql,
            "never_selected.sql": never_selected_sql,
            "test": {
                "subdir.sql": subdir_sql,
                "subdir": {"nested_users.sql": nested_users_sql},
            },
            "metricflow_time_spine.sql": "SELECT to_date('02/20/2023', 'mm/dd/yyyy') as date_day",
            "semantic_models.yml": """
version: 2

semantic_models:
  - name: test_semantic_model
    label: "Test Semantic Model"
    model: ref('users')
    dimensions:
      - name: created_at
        type: time
        type_params:
          time_granularity: day
    measures:
      - name: total_count
        agg: count
        expr: 1
    entities:
      - name: id
        type: primary
    defaults:
      agg_time_dimension: created_at

metrics:
  - name: test_metric
    label: "Test Metric"
    type: simple
    config:
      enabled: true
      meta:
        metric_type: simple
    type_params:
      measure: total_count

saved_queries:
  - name: test_saved_query
    label: "Test Saved Query"
    config:
      enabled: true
      meta:
        query_type: basic
        contains_pii: true
    query_params:
      metrics:
        - test_metric
      group_by:
        - "Dimension('test_semantic_model__created_at')"
    exports:
      - name: test_export
        config:
          alias: test_export_alias
          export_as: table
""",
        }

    def test_config_selector_with_resource_type_filter(self, project):
        """Test config selectors with resource type filters."""

        results = run_dbt(["list", "--resource-type", "model", "--select", "config.enabled:true"])
        selected_nodes = set(results)

        assert "saved_query:test.test_saved_query" not in selected_nodes
        assert "metric:test.test_metric" not in selected_nodes
        assert "semantic_model:test.test_semantic_model" not in selected_nodes

    def test_config_enabled_true_selects_extended_nodes(self, project):
        """Test that dbt ls -s config.enabled:true returns the test_saved_query.

        This specific test validates that the saved query (which has config.enabled:true)
        is properly selected by the config selector. This demonstrates that the change from
        configurable_nodes() to all_nodes() allows config selectors to work on saved queries.
        """

        results = run_dbt(["list", "--select", "config.enabled:true"])
        selected_nodes = set(results)

        assert "saved_query:test.test_saved_query" in selected_nodes
        assert "metric:test.test_metric" in selected_nodes
        assert "semantic_model:test.test_semantic_model" in selected_nodes

    def test_config_meta_selection(self, project):
        """ """

        results = run_dbt(["list", "--select", "config.meta.contains_pii:true"])
        selected_nodes = set(results)

        assert "test.users" in selected_nodes
        assert "saved_query:test.test_saved_query" in selected_nodes
        assert "test.unique_users_id" in selected_nodes
