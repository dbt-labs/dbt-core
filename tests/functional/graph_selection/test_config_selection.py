import pytest

from dbt.tests.util import run_dbt

# Test fixtures - dbt project files
models__model_enabled_sql = """
{{ config(materialized="table", enabled=true, meta={"env": "prod"}) }}
select 1 as id
"""

models__model_disabled_sql = """
{{ config(materialized="view", enabled=false, meta={"env": "dev"}) }}
select 1 as id
"""

models__base_model_sql = """
select 1 as id, 'test' as name
"""

schema_yml = """
version: 2

models:
  - name: base_model
    description: "Base model for semantic layer"
    columns:
      - name: id
        description: "ID column"
      - name: name
        description: "Name column"

metrics:
  - name: enabled_metric
    description: "Metric that is enabled"
    type: simple
    type_params:
      measure:
        name: base_measure
    config:
      enabled: true
      meta:
        env: "prod"

  - name: disabled_metric
    description: "Metric that is disabled"
    type: simple
    type_params:
      measure:
        name: base_measure
    config:
      enabled: false
      meta:
        env: "dev"

semantic_models:
  - name: enabled_semantic_model
    description: "Semantic model that is enabled"
    model: ref('base_model')
    config:
      enabled: true
      meta:
        env: "prod"
    dimensions:
      - name: id
        type: categorical
        type_params:
          values: [1, 2, 3]
    measures:
      - name: base_measure
        agg: count

  - name: disabled_semantic_model
    description: "Semantic model that is disabled"
    model: ref('base_model')
    config:
      enabled: false
      meta:
        env: "dev"
    dimensions:
      - name: id
        type: categorical
        type_params:
          values: [1, 2, 3]
    measures:
      - name: base_measure
        agg: count

saved_queries:
  - name: enabled_saved_query
    description: "Saved query that is enabled"
    config:
      enabled: true
      meta:
        env: "prod"
    query_params:
      metrics:
        - enabled_metric
      dimensions:
        - id

  - name: disabled_saved_query
    description: "Saved query that is disabled"
    config:
      enabled: false
      meta:
        env: "dev"
    query_params:
      metrics:
        - enabled_metric
      dimensions:
        - id
"""


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
            "model_enabled.sql": models__model_enabled_sql,
            "model_disabled.sql": models__model_disabled_sql,
            "base_model.sql": models__base_model_sql,
            "schema.yml": schema_yml,
        }

    def test_config_enabled_true_selects_all_enabled_nodes(self, project):
        """Test that config.enabled:true selects all enabled nodes including new types."""
        # First run dbt to ensure everything is parsed
        run_dbt(["parse"])

        # Test that config.enabled:true finds all enabled nodes
        results = run_dbt(["list", "--select", "config.enabled:true"])

        # Should include enabled models, metrics, semantic models, and saved queries
        selected_nodes = [result.split(".")[-1] for result in results]

        # Models
        assert "model_enabled" in selected_nodes
        assert "base_model" in selected_nodes  # enabled by default

        # New node types that should now be selectable
        assert "enabled_metric" in selected_nodes
        assert "enabled_semantic_model" in selected_nodes
        assert "enabled_saved_query" in selected_nodes

        # Should NOT include disabled nodes
        assert "model_disabled" not in selected_nodes
        assert "disabled_metric" not in selected_nodes
        assert "disabled_semantic_model" not in selected_nodes
        assert "disabled_saved_query" not in selected_nodes

    def test_config_enabled_false_selects_disabled_nodes(self, project):
        """Test that config.enabled:false selects disabled nodes including new types."""
        run_dbt(["parse"])

        results = run_dbt(["list", "--select", "config.enabled:false"])
        selected_nodes = [result.split(".")[-1] for result in results]

        # Should include disabled nodes
        assert "model_disabled" in selected_nodes
        assert "disabled_metric" in selected_nodes
        assert "disabled_semantic_model" in selected_nodes
        assert "disabled_saved_query" in selected_nodes

        # Should NOT include enabled nodes
        assert "model_enabled" not in selected_nodes
        assert "enabled_metric" not in selected_nodes
        assert "enabled_semantic_model" not in selected_nodes
        assert "enabled_saved_query" not in selected_nodes

    def test_config_meta_env_selects_nodes_by_meta(self, project):
        """Test that config.meta.env selects nodes by meta config including new types."""
        run_dbt(["parse"])

        # Test production environment nodes
        results = run_dbt(["list", "--select", "config.meta.env:prod"])
        selected_nodes = [result.split(".")[-1] for result in results]

        assert "model_enabled" in selected_nodes
        assert "enabled_metric" in selected_nodes
        assert "enabled_semantic_model" in selected_nodes
        assert "enabled_saved_query" in selected_nodes

        # Test development environment nodes
        results = run_dbt(["list", "--select", "config.meta.env:dev"])
        selected_nodes = [result.split(".")[-1] for result in results]

        assert "model_disabled" in selected_nodes
        assert "disabled_metric" in selected_nodes
        assert "disabled_semantic_model" in selected_nodes
        assert "disabled_saved_query" in selected_nodes

    def test_config_materialized_selects_models_only(self, project):
        """Test that config.materialized only selects models (since other node types don't have this config)."""
        run_dbt(["parse"])

        # Test table materialization
        results = run_dbt(["list", "--select", "config.materialized:table"])
        selected_nodes = [result.split(".")[-1] for result in results]

        assert "model_enabled" in selected_nodes
        # Should not include metrics, semantic models, or saved queries
        assert "enabled_metric" not in selected_nodes
        assert "enabled_semantic_model" not in selected_nodes
        assert "enabled_saved_query" not in selected_nodes

        # Test view materialization
        results = run_dbt(["list", "--select", "config.materialized:view"])
        selected_nodes = [result.split(".")[-1] for result in results]

        assert "model_disabled" in selected_nodes
        assert "base_model" in selected_nodes  # default materialization

    def test_config_selector_with_resource_type_filter(self, project):
        """Test combining config selectors with resource type filters."""
        run_dbt(["parse"])

        # Test enabled metrics only
        results = run_dbt(["list", "--select", "config.enabled:true", "--resource-type", "metric"])
        selected_nodes = [result.split(".")[-1] for result in results]

        assert "enabled_metric" in selected_nodes
        assert "disabled_metric" not in selected_nodes
        # Should not include models or other types
        assert "model_enabled" not in selected_nodes
        assert "enabled_semantic_model" not in selected_nodes

        # Test enabled semantic models only
        results = run_dbt(
            ["list", "--select", "config.enabled:true", "--resource-type", "semantic_model"]
        )
        selected_nodes = [result.split(".")[-1] for result in results]

        assert "enabled_semantic_model" in selected_nodes
        assert "disabled_semantic_model" not in selected_nodes
        # Should not include other types
        assert "enabled_metric" not in selected_nodes
        assert "model_enabled" not in selected_nodes

    def test_config_selector_demonstrates_expansion_from_configurable_to_all_nodes(self, project):
        """Test that demonstrates the key change: config selectors now work on all node types.

        This test specifically validates that the change from configurable_nodes() to all_nodes()
        in ConfigSelectorMethod allows selection of node types that were previously not selectable.
        """
        run_dbt(["parse"])

        # Before the change, this would only find models and sources
        # After the change, this should also find metrics, semantic models, and saved queries
        results = run_dbt(["list", "--select", "config.enabled:true"])

        # Count different node types in results
        models = [r for r in results if r.startswith("model.")]
        metrics = [r for r in results if r.startswith("metric.")]
        semantic_models = [r for r in results if r.startswith("semantic_model.")]
        saved_queries = [r for r in results if r.startswith("saved_query.")]

        # Verify we found nodes of all types (demonstrating all_nodes() expansion)
        assert len(models) > 0, "Should find model nodes"
        assert len(metrics) > 0, "Should find metric nodes (new with all_nodes())"
        assert len(semantic_models) > 0, "Should find semantic model nodes (new with all_nodes())"
        assert len(saved_queries) > 0, "Should find saved query nodes (new with all_nodes())"

        # The total should include nodes from all types
        total_nodes = len(results)
        traditional_nodes = len(models)  # + sources would be here if we had any
        new_nodes = len(metrics) + len(semantic_models) + len(saved_queries)

        assert (
            new_nodes > 0
        ), "Should find additional node types beyond traditional configurable nodes"
        assert (
            total_nodes >= traditional_nodes + new_nodes
        ), "Total should include both traditional and new node types"
